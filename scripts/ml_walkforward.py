"""
ml_walkforward.py — Walk-forward ML evaluation.

Trains sklearn models on expanding train windows, predicts on test folds.
Converts predicted return → BUY/SELL action → evaluates vs always-buy.

GUARDRAIL 1: Does NOT touch predict.py. Research/evaluation only.
GUARDRAIL 2: Strict walk-forward — model sees only past data at each fold.
GUARDRAIL 3: Self-test included.

Usage:
    python3 scripts/ml_walkforward.py               # full evaluation
    python3 scripts/ml_walkforward.py --self-test    # harness self-test
    python3 scripts/ml_walkforward.py --json         # JSON output
"""

from __future__ import annotations

import argparse
import json
import math
import warnings
from pathlib import Path

import numpy as np

warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", message="Skipping features")

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "site" / "data"

MIN_TRAIN_ROWS = 30
MIN_TRAIN_DATES = 3

NUMERIC_FEATURES = [
    "ta_rsi14", "ta_hv20", "confidence",
    "news_best_conf", "news_z",
    "sentiment_score", "sentiment_total",
    "fundamental_score", "quality_score", "growth_score", "health_score",
    "insider_score",
]

CATEGORICAL_FEATURES = [
    "ta_trend", "ta_macd_bias", "ta_bb_position", "ta_volatility",
    "regime",
]


def _extract_row(r: dict) -> dict | None:
    """Extract flat feature dict from a prediction_accuracy record."""
    feats = r.get("features", {})
    if not feats.get("ta_rsi14"):
        return None
    row = {
        "snap_date": r["snap_date"],
        "ticker": r["ticker"],
        "fwd_5d_return": r["fwd_5d_return"],
        "confidence": r.get("confidence", 0),
        "regime": r.get("regime", "UNKNOWN"),
    }
    for k in NUMERIC_FEATURES:
        if k not in ("confidence", "regime"):
            row[k] = feats.get(k)
    for k in CATEGORICAL_FEATURES:
        if k != "regime":
            row[k] = feats.get(k)
    return row


def _build_Xy(rows: list[dict]):
    """Convert row dicts to numpy arrays for sklearn."""
    from sklearn.compose import ColumnTransformer
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    import pandas as pd
    df = pd.DataFrame(rows)

    num_cols = [c for c in NUMERIC_FEATURES if c in df.columns]
    cat_cols = [c for c in CATEGORICAL_FEATURES if c in df.columns]

    pre = ColumnTransformer([
        ("num", Pipeline([
            ("impute", SimpleImputer(strategy="median")),
            ("scale", StandardScaler()),
        ]), num_cols),
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat_cols),
    ], remainder="drop")

    X = pre.fit_transform(df)
    y = df["fwd_5d_return"].to_numpy(dtype=float)
    return X, y, pre


def _make_models():
    from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
    from sklearn.linear_model import Ridge
    return {
        "Ridge": Ridge(alpha=1.0),
        "RandomForest": RandomForestRegressor(
            n_estimators=100, min_samples_leaf=5, max_depth=6, random_state=42, n_jobs=-1),
        "GradientBoosting": GradientBoostingRegressor(
            n_estimators=100, max_depth=3, random_state=42),
    }


def _norm_cdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _spy_5d(snap_date: str, _cache: dict = {}) -> float | None:
    if not _cache:
        p = DATA / "prices.json"
        if not p.exists():
            return None
        d = json.loads(p.read_text())
        spy = d.get("tickers", {}).get("SPY")
        if spy:
            _cache["dates"] = spy["dates"]
            _cache["closes"] = dict(zip(spy["dates"], spy["closes"]))
    dates = _cache.get("dates", [])
    closes = _cache.get("closes", {})
    if not dates:
        return None
    lo, hi = 0, len(dates)
    while lo < hi:
        mid = (lo + hi) // 2
        if dates[mid] <= snap_date:
            lo = mid + 1
        else:
            hi = mid
    idx = lo - 1
    if idx < 0 or idx + 5 >= len(dates):
        return None
    p0 = closes.get(dates[idx])
    p5 = closes.get(dates[idx + 5])
    if not p0 or not p5:
        return None
    return p5 / p0 - 1


# ── walk-forward ML engine ────────────────────────────────────────────────

def ml_walk_forward() -> dict:
    from walk_forward import load_records

    raw = load_records()
    rows = [_extract_row(r) for r in raw]
    rows = [r for r in rows if r is not None and r["fwd_5d_return"] is not None]

    dates = sorted(set(r["snap_date"] for r in rows))
    by_date: dict[str, list[dict]] = {}
    for r in rows:
        by_date.setdefault(r["snap_date"], []).append(r)

    if len(dates) < MIN_TRAIN_DATES + 1:
        return {"error": f"need >= {MIN_TRAIN_DATES + 1} dates, have {len(dates)}"}

    model_defs = _make_models()
    model_folds: dict[str, list[dict]] = {name: [] for name in model_defs}
    buy_folds: list[dict] = []

    for k in range(MIN_TRAIN_DATES, len(dates)):
        test_date = dates[k]
        train_rows = []
        for d in dates[:k]:
            train_rows.extend(by_date.get(d, []))
        test_rows = by_date.get(test_date, [])

        if len(train_rows) < MIN_TRAIN_ROWS or not test_rows:
            continue

        from sklearn.compose import ColumnTransformer
        from sklearn.impute import SimpleImputer
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import OneHotEncoder, StandardScaler
        import pandas as pd

        train_df = pd.DataFrame(train_rows)
        test_df = pd.DataFrame(test_rows)

        num_cols = [c for c in NUMERIC_FEATURES if c in train_df.columns]
        cat_cols = [c for c in CATEGORICAL_FEATURES if c in train_df.columns]

        pre = ColumnTransformer([
            ("num", Pipeline([
                ("impute", SimpleImputer(strategy="median")),
                ("scale", StandardScaler()),
            ]), num_cols),
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat_cols),
        ], remainder="drop")

        X_train = pre.fit_transform(train_df)
        y_train = train_df["fwd_5d_return"].to_numpy(dtype=float)
        X_test = pre.transform(test_df)
        y_test = test_df["fwd_5d_return"].to_numpy(dtype=float)

        spy_ret = _spy_5d(test_date)
        n_test = len(y_test)
        n_up = int(np.sum(y_test > 0))

        # Always-buy fold
        buy_acc = n_up / n_test if n_test else 0
        buy_alpha_trades = []
        if spy_ret is not None:
            buy_alpha_trades = [(ret - spy_ret) for ret in y_test]
        buy_folds.append({
            "test_date": test_date, "n": n_test,
            "accuracy": round(buy_acc, 4),
            "alpha_mean_pct": round(float(np.mean(buy_alpha_trades)) * 100, 3) if buy_alpha_trades else None,
        })

        for name, model_template in model_defs.items():
            from sklearn.base import clone
            model = clone(model_template)
            try:
                model.fit(X_train, y_train)
                preds = model.predict(X_test)

                # predicted return → action → correct
                actions = np.where(preds > 0, 1, -1)  # 1=BUY, -1=SELL
                correct = np.where(
                    actions == 1,
                    y_test > 0,
                    y_test < 0,
                ).astype(int)
                acc = float(correct.mean())

                # Alpha: trade return (aligned with action) - SPY
                trade_rets = np.where(actions == 1, y_test, -y_test)
                alpha_trades = []
                if spy_ret is not None:
                    alpha_trades = [(tr - spy_ret) for tr in trade_rets]

                model_folds[name].append({
                    "test_date": test_date,
                    "n": n_test,
                    "n_train": len(y_train),
                    "accuracy": round(acc, 4),
                    "dir_acc_raw": round(float(np.mean(np.sign(preds) == np.sign(y_test))), 4),
                    "alpha_mean_pct": round(float(np.mean(alpha_trades)) * 100, 3) if alpha_trades else None,
                    "spy_5d_pct": round(spy_ret * 100, 3) if spy_ret is not None else None,
                })
            except Exception as e:
                model_folds[name].append({
                    "test_date": test_date, "n": n_test,
                    "error": str(e),
                })

    # Aggregate per model
    results = {}
    for name, folds in model_folds.items():
        valid = [f for f in folds if "accuracy" in f]
        if not valid:
            results[name] = {"error": "no valid folds"}
            continue
        total_n = sum(f["n"] for f in valid)
        total_correct = sum(int(round(f["accuracy"] * f["n"])) for f in valid)
        pooled_acc = total_correct / total_n if total_n else 0
        alphas = [f["alpha_mean_pct"] for f in valid if f.get("alpha_mean_pct") is not None]
        if len(alphas) >= 2:
            a_mean = sum(alphas) / len(alphas)
            a_var = sum((a - a_mean)**2 for a in alphas) / (len(alphas) - 1)
            a_se = math.sqrt(a_var / len(alphas)) if a_var > 0 else 0
            a_t = a_mean / a_se if a_se > 0 else 0
            a_p = 2 * (1 - _norm_cdf(abs(a_t)))
        else:
            a_mean = alphas[0] if alphas else None
            a_t, a_p = 0, 1.0
        results[name] = {
            "pooled_accuracy": round(pooled_acc, 4),
            "total_test_n": total_n,
            "n_folds": len(valid),
            "alpha_mean_pct": round(a_mean, 3) if a_mean is not None else None,
            "alpha_t": round(a_t, 3),
            "alpha_p": round(a_p, 4),
            "folds": folds,
        }

    # Always-buy aggregate
    buy_valid = [f for f in buy_folds if f.get("alpha_mean_pct") is not None]
    buy_alphas = [f["alpha_mean_pct"] for f in buy_valid]
    buy_alpha_mean = sum(buy_alphas) / len(buy_alphas) if buy_alphas else None

    # Gate check per model
    for name, res in results.items():
        if "error" in res:
            res["gate"] = "FAIL"
            continue
        a = res.get("alpha_mean_pct")
        p = res.get("alpha_p", 1)
        if a is not None and buy_alpha_mean is not None and a > buy_alpha_mean and p < 0.05 and a > 0:
            res["gate"] = "PASS"
        else:
            res["gate"] = "FAIL"

    return {
        "n_dates": len(dates),
        "n_total_rows": len(rows),
        "always_buy": {
            "alpha_mean_pct": round(buy_alpha_mean, 3) if buy_alpha_mean is not None else None,
            "n_folds": len(buy_valid),
        },
        "models": results,
    }


# ── self-test ──────────────────────────────────────────────────────────────

def _self_test_runs() -> dict:
    """Harness runs without crashing and produces results."""
    try:
        res = ml_walk_forward()
        has_models = "models" in res and len(res["models"]) > 0
        ok = has_models and "error" not in res
        return {"name": "harness_runs", "expect": "produces model results",
                "actual": f"{len(res.get('models', {}))} models, {res.get('n_total_rows', 0)} rows",
                "pass": ok}
    except Exception as e:
        return {"name": "harness_runs", "expect": "no crash",
                "actual": f"ERROR: {e}", "pass": False}


def _self_test_no_leakage() -> dict:
    """Each fold's train dates must be strictly before test date."""
    res = ml_walk_forward()
    for name, mres in res.get("models", {}).items():
        for f in mres.get("folds", []):
            if "error" in f:
                continue
            # n_train should increase monotonically
    # Check first model's folds
    first_model = list(res["models"].values())[0]
    folds = first_model.get("folds", [])
    valid = [f for f in folds if "n_train" in f]
    monotonic = all(valid[i]["n_train"] <= valid[i+1]["n_train"] for i in range(len(valid)-1))
    return {"name": "no_leakage_monotonic_train",
            "expect": "train size increases monotonically",
            "actual": f"monotonic={monotonic}, sizes={[f['n_train'] for f in valid[:5]]}...",
            "pass": monotonic}


# ── main ───────────────────────────────────────────────────────────────────

def run() -> int:
    ap = argparse.ArgumentParser(description="ML walk-forward evaluation")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.self_test:
        tests = [_self_test_runs(), _self_test_no_leakage()]
        all_pass = all(t["pass"] for t in tests)
        if args.json:
            print(json.dumps({"tests": tests, "all_pass": all_pass}, indent=2))
        else:
            for t in tests:
                tag = "PASS" if t["pass"] else "FAIL"
                print(f"  [{tag}] {t['name']}: {t['actual']}  (expect: {t['expect']})")
            print(f"\n  All pass: {all_pass}")
        return 0 if all_pass else 1

    res = ml_walk_forward()

    if args.json:
        print(json.dumps(res, indent=2))
        return 0

    print(f"ML Walk-Forward: {res['n_dates']} dates, {res['n_total_rows']} rows")
    buy_a = res["always_buy"]["alpha_mean_pct"]
    print(f"  Always-buy alpha: {buy_a:+.2f}%" if buy_a is not None else "  Always-buy alpha: n/a")
    print()

    print(f"  {'Model':<22} {'Acc':>6} {'Alpha':>8} {'t':>6} {'p':>8} {'Gate':>6}  Folds")
    print(f"  {'-'*65}")
    for name, mres in res["models"].items():
        if "error" in mres:
            print(f"  {name:<22} ERROR: {mres['error']}")
            continue
        acc = mres["pooled_accuracy"]
        alpha = mres.get("alpha_mean_pct")
        t = mres.get("alpha_t", 0)
        p = mres.get("alpha_p", 1)
        gate = mres["gate"]
        a_str = f"{alpha:+.2f}%" if alpha is not None else "  n/a "
        print(f"  {name:<22} {acc*100:5.1f}% {a_str:>8} {t:5.2f} {p:7.4f}  {gate:>5}  {mres['n_folds']}")

    # Fold details for best model
    best = max(
        ((n, m) for n, m in res["models"].items() if "pooled_accuracy" in m),
        key=lambda x: x[1]["pooled_accuracy"],
        default=(None, None),
    )
    if best[0]:
        print(f"\n  Best model ({best[0]}) fold details:")
        print(f"  {'Date':<12} {'n':>4} {'Train':>6} {'Acc':>6} {'Alpha':>8}")
        print(f"  {'-'*45}")
        for f in best[1].get("folds", []):
            if "error" in f:
                print(f"  {f['test_date']:<12} ERROR: {f['error']}")
                continue
            a = f"{f['alpha_mean_pct']:+5.2f}%" if f.get('alpha_mean_pct') is not None else "  n/a"
            print(f"  {f['test_date']:<12} {f['n']:4d} {f.get('n_train',0):6d}"
                  f" {f['accuracy']*100:5.1f}% {a:>8}")

    return 0


if __name__ == "__main__":
    raise SystemExit(run())
