"""
experiment_engine.py — Honest automated exploration engine.

Completely isolated from frozen ml_monitor.py track.
Enumerates (feature × model × universe × horizon) combos,
evaluates each through 5-layer defense gate.

DEFENSE LAYERS:
  1. Walk-forward OOS (structural — never evaluate on train data)
  2. Multiple testing FDR (BH correction across ALL experiments tried)
  3. Holdout (last 20% of dates sealed — candidates re-validated there)
  4. Data quality (look-ahead scan, missing rate, outlier detection)
  5. Out-of-time (deferred — tracked via ml_monitor once new data arrives)

SELF-TEST (must pass before engine is trusted):
  Inject 200 random-noise "systems" → 5-layer defense → expect 0 survivors.

Results: experiments/results.json (append-only discovery log)

Usage:
    python3 scripts/experiment_engine.py --self-test   # MUST PASS FIRST
    python3 scripts/experiment_engine.py               # run all experiments
    python3 scripts/experiment_engine.py --json         # JSON output
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import warnings
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

from sklearn.base import clone
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge, Lasso
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "site" / "data"
EXPERIMENTS_DIR = ROOT / "experiments"
RESULTS_FILE = EXPERIMENTS_DIR / "results.json"
ACCURACY_FILE = DATA / "prediction_accuracy.json"
PRICES_FILE = DATA / "prices.json"

HOLDOUT_FRAC = 0.20  # last 20% of dates sealed

ALL_NUMERIC = [
    "ta_rsi14", "ta_hv20", "confidence",
    "news_best_conf", "news_z",
    "sentiment_score", "sentiment_total",
    "fundamental_score", "quality_score", "growth_score", "health_score",
    "insider_score",
]
ALL_CATEGORICAL = [
    "ta_trend", "ta_macd_bias", "ta_bb_position", "ta_volatility",
    "regime",
]

FEATURE_SETS = {
    "ta_only": {
        "num": ["ta_rsi14", "ta_hv20", "confidence"],
        "cat": ["ta_trend", "ta_macd_bias", "ta_bb_position", "ta_volatility", "regime"],
    },
    "ta_news": {
        "num": ["ta_rsi14", "ta_hv20", "confidence", "news_best_conf", "news_z"],
        "cat": ["ta_trend", "ta_macd_bias", "ta_bb_position", "ta_volatility", "regime"],
    },
    "ta_fundamental": {
        "num": ["ta_rsi14", "ta_hv20", "confidence",
                "fundamental_score", "quality_score", "growth_score", "health_score"],
        "cat": ["ta_trend", "ta_macd_bias", "ta_bb_position", "ta_volatility", "regime"],
    },
    "ta_sentiment": {
        "num": ["ta_rsi14", "ta_hv20", "confidence", "sentiment_score", "sentiment_total"],
        "cat": ["ta_trend", "ta_macd_bias", "ta_bb_position", "ta_volatility", "regime"],
    },
    "all_features": {
        "num": ALL_NUMERIC,
        "cat": ALL_CATEGORICAL,
    },
}

MODEL_DEFS = {
    "Ridge": Ridge(alpha=1.0),
    "Lasso": Lasso(alpha=0.001, max_iter=5000),
    "RandomForest": RandomForestRegressor(
        n_estimators=100, min_samples_leaf=5, max_depth=6, random_state=42, n_jobs=-1),
    "GradientBoosting": GradientBoostingRegressor(
        n_estimators=100, max_depth=3, random_state=42),
}

UNIVERSE_FILTERS = {
    "all": lambda r: True,
    "high_vol": lambda r: (r.get("ta_hv20") or 0) >= 35,
    "low_vol": lambda r: (r.get("ta_hv20") or 0) < 35 and r.get("ta_hv20") is not None,
}

MIN_TRAIN = 30
MIN_TEST_PER_FOLD = 3


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _norm_cdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


# ── Data loading ───────────────────────────────────────────────────────────

def _extract_row(r: dict) -> dict | None:
    feats = r.get("features", {})
    if not feats.get("ta_rsi14"):
        return None
    row = {
        "snap_date": r["snap_date"],
        "ticker": r["ticker"],
        "fwd_5d_return": r.get("fwd_5d_return"),
        "confidence": r.get("confidence", 0),
        "regime": r.get("regime", "UNKNOWN"),
    }
    for k in ALL_NUMERIC:
        if k not in ("confidence",):
            row[k] = feats.get(k)
    for k in ALL_CATEGORICAL:
        if k != "regime":
            row[k] = feats.get(k)
    return row


def load_rows() -> list[dict]:
    acc = json.loads(ACCURACY_FILE.read_text())
    rows = [_extract_row(r) for r in acc["records"] if r.get("fwd_5d_return") is not None]
    return [r for r in rows if r is not None]


def _spy_cache() -> dict[str, float]:
    if not PRICES_FILE.exists():
        return {}
    d = json.loads(PRICES_FILE.read_text())
    spy = d.get("tickers", {}).get("SPY")
    if not spy:
        return {}
    dates = spy["dates"]
    closes = spy["closes"]
    cal = sorted(dates)
    out = {}
    for i, snap in enumerate(cal):
        lo, hi = 0, len(cal)
        while lo < hi:
            mid = (lo + hi) // 2
            if cal[mid] <= snap:
                lo = mid + 1
            else:
                hi = mid
        idx = lo - 1
        if idx >= 0 and idx + 5 < len(cal):
            p0 = closes[dates.index(cal[idx])]
            p5 = closes[dates.index(cal[idx + 5])]
            if p0 and p5 and p0 > 0:
                out[snap] = p5 / p0 - 1
    return out


# ── Single experiment ──────────────────────────────────────────────────────

def run_experiment(config: dict, rows: list[dict], explore_dates: list[str],
                   spy: dict[str, float]) -> dict:
    """Run one experiment on exploration dates only. Returns fold-level alpha."""
    universe_fn = UNIVERSE_FILTERS.get(config["universe"], lambda r: True)
    filtered = [r for r in rows if universe_fn(r) and r["snap_date"] in set(explore_dates)]

    by_date: dict[str, list[dict]] = {}
    for r in filtered:
        by_date.setdefault(r["snap_date"], []).append(r)

    dates_with_data = sorted(d for d in explore_dates if d in by_date)
    if len(dates_with_data) < 4:
        return {"error": "insufficient_dates"}

    feat_cfg = FEATURE_SETS[config["features"]]
    num_cols = feat_cfg["num"]
    cat_cols = feat_cfg["cat"]
    model_template = MODEL_DEFS[config["model"]]

    fold_alphas = []
    fold_accs = []

    for k in range(3, len(dates_with_data)):
        train_rows = []
        for d in dates_with_data[:k]:
            train_rows.extend(by_date.get(d, []))
        test_rows = by_date.get(dates_with_data[k], [])

        if len(train_rows) < MIN_TRAIN or len(test_rows) < MIN_TEST_PER_FOLD:
            continue

        train_df = pd.DataFrame(train_rows)
        test_df = pd.DataFrame(test_rows)

        nc = [c for c in num_cols if c in train_df.columns]
        cc = [c for c in cat_cols if c in train_df.columns]

        pre = ColumnTransformer([
            ("num", Pipeline([("imp", SimpleImputer(strategy="median")),
                              ("scl", StandardScaler())]), nc),
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cc),
        ], remainder="drop")

        try:
            X_train = pre.fit_transform(train_df)
            y_train = train_df["fwd_5d_return"].to_numpy(float)
            X_test = pre.transform(test_df)
            y_test = test_df["fwd_5d_return"].to_numpy(float)
        except Exception:
            continue

        model = clone(model_template)
        try:
            model.fit(X_train, y_train)
            preds = model.predict(X_test)
        except Exception:
            continue

        spy_ret = spy.get(dates_with_data[k])
        if spy_ret is None:
            continue

        actions = np.where(preds > 0, 1, -1)
        correct = np.where(actions == 1, y_test > 0, y_test < 0).astype(int)
        trade_rets = np.where(actions == 1, y_test, -y_test)
        alpha_trades = (trade_rets - spy_ret) * 100

        fold_alphas.append(float(np.mean(alpha_trades)))
        fold_accs.append(float(correct.mean()))

    if len(fold_alphas) < 3:
        return {"error": "too_few_folds", "n_folds": len(fold_alphas)}

    m = np.mean(fold_alphas)
    s = np.std(fold_alphas, ddof=1)
    se = s / np.sqrt(len(fold_alphas))
    t = m / se if se > 0 else 0
    p = 2 * (1 - _norm_cdf(abs(t)))

    return {
        "alpha_mean_pct": round(m, 4),
        "alpha_t": round(t, 3),
        "alpha_p": round(p, 6),
        "accuracy": round(np.mean(fold_accs), 4),
        "n_folds": len(fold_alphas),
        "fold_alphas": [round(a, 3) for a in fold_alphas],
    }


# ── Defense gates ──────────────────────────────────────────────────────────

def gate_fdr(results: list[dict], alpha: float = 0.05) -> list[dict]:
    """Gate 2: Benjamini-Hochberg FDR correction."""
    valid = [r for r in results if "alpha_p" in r]
    if not valid:
        return []

    sorted_by_p = sorted(valid, key=lambda r: r["alpha_p"])
    n = len(sorted_by_p)
    survivors = []
    for rank, r in enumerate(sorted_by_p, 1):
        bh_threshold = alpha * rank / n
        r["bh_rank"] = rank
        r["bh_threshold"] = round(bh_threshold, 6)
        r["bh_pass"] = r["alpha_p"] <= bh_threshold and r["alpha_mean_pct"] > 0
        if r["bh_pass"]:
            survivors.append(r)

    return survivors


def gate_holdout(candidates: list[dict], rows: list[dict],
                 holdout_dates: list[str], spy: dict[str, float]) -> list[dict]:
    """Gate 3: Re-validate candidates on sealed holdout dates."""
    if not holdout_dates or not candidates:
        return []

    survivors = []
    for cand in candidates:
        result = run_experiment(cand["config"], rows, holdout_dates, spy)
        if "error" in result:
            cand["holdout"] = {"status": "insufficient_data", "detail": result["error"]}
            continue
        cand["holdout"] = result
        if result["alpha_mean_pct"] > 0:
            survivors.append(cand)

    return survivors


def gate_data_quality(rows: list[dict]) -> dict:
    """Gate 4: Data quality scan."""
    issues = []
    n = len(rows)

    # Missing rate per feature
    missing = {}
    for feat in ALL_NUMERIC:
        n_missing = sum(1 for r in rows if r.get(feat) is None)
        rate = n_missing / n if n else 0
        missing[feat] = round(rate, 3)
        if rate > 0.5:
            issues.append(f"HIGH_MISSING: {feat} = {rate:.0%}")

    # Outlier detection (z-score > 5)
    outlier_counts = {}
    for feat in ALL_NUMERIC:
        vals = [r[feat] for r in rows if r.get(feat) is not None]
        if len(vals) < 10:
            continue
        m, s = np.mean(vals), np.std(vals)
        if s > 0:
            n_outliers = sum(1 for v in vals if abs(v - m) > 5 * s)
            if n_outliers > 0:
                outlier_counts[feat] = n_outliers
                if n_outliers > len(vals) * 0.05:
                    issues.append(f"OUTLIERS: {feat} has {n_outliers} extreme values")

    # Look-ahead check: verify no future data in features
    # Features should only use data available AT snap_date.
    # Flag if fwd_5d_return correlates suspiciously with any feature (r > 0.8)
    la_suspects = []
    y = np.array([r["fwd_5d_return"] for r in rows], dtype=float)
    for feat in ALL_NUMERIC:
        vals = np.array([r.get(feat) if r.get(feat) is not None else np.nan
                         for r in rows], dtype=float)
        mask = ~np.isnan(vals)
        if mask.sum() < 20:
            continue
        corr = np.corrcoef(vals[mask], y[mask])[0, 1]
        if abs(corr) > 0.5:
            la_suspects.append(f"{feat}: r={corr:.3f}")
            if abs(corr) > 0.8:
                issues.append(f"LOOK-AHEAD SUSPECT: {feat} correlates {corr:.3f} with target")

    return {
        "n_rows": n,
        "missing_rates": missing,
        "outlier_counts": outlier_counts,
        "look_ahead_suspects": la_suspects,
        "issues": issues,
        "pass": len([i for i in issues if "LOOK-AHEAD" in i]) == 0,
    }


# ── Experiment enumeration ─────────────────────────────────────────────────

def enumerate_experiments() -> list[dict]:
    configs = []
    for feat_name in FEATURE_SETS:
        for model_name in MODEL_DEFS:
            for univ_name in UNIVERSE_FILTERS:
                configs.append({
                    "id": f"{model_name}_{feat_name}_{univ_name}_5d",
                    "model": model_name,
                    "features": feat_name,
                    "universe": univ_name,
                    "horizon": 5,
                })
    return configs


# ── Self-test ──────────────────────────────────────────────────────────────

def self_test_random_noise(rows: list[dict], n_random: int = 200) -> dict:
    """Inject N random systems → must find 0 after defense gates.

    Each "random system" is a random feature-weight vector applied to
    real features, producing random-ish predictions. If the defense
    gates let ANY through, the engine cannot be trusted.
    """
    dates = sorted(set(r["snap_date"] for r in rows))
    holdout_cut = max(1, len(dates) - int(len(dates) * HOLDOUT_FRAC))
    explore_dates = dates[:holdout_cut]
    holdout_dates = dates[holdout_cut:]
    spy = _spy_cache()

    by_date: dict[str, list[dict]] = {}
    for r in rows:
        by_date.setdefault(r["snap_date"], []).append(r)

    rng = random.Random(42)
    results = []

    for i in range(n_random):
        # Random system: assign random weights to features, predict random-ish returns
        seed = rng.randint(0, 2**31)
        np_rng = np.random.RandomState(seed)

        fold_alphas = []
        explore_dates_with_data = sorted(d for d in explore_dates if d in by_date)

        for k in range(3, len(explore_dates_with_data)):
            test_rows = by_date.get(explore_dates_with_data[k], [])
            if len(test_rows) < MIN_TEST_PER_FOLD:
                continue
            spy_ret = spy.get(explore_dates_with_data[k])
            if spy_ret is None:
                continue

            y_test = np.array([r["fwd_5d_return"] for r in test_rows])
            # Random predictions: shuffle the actual returns (destroys signal, keeps distribution)
            preds = np_rng.permutation(y_test)

            actions = np.where(preds > 0, 1, -1)
            trade_rets = np.where(actions == 1, y_test, -y_test)
            alpha_trades = (trade_rets - spy_ret) * 100
            fold_alphas.append(float(np.mean(alpha_trades)))

        if len(fold_alphas) < 3:
            continue

        m = np.mean(fold_alphas)
        s = np.std(fold_alphas, ddof=1)
        se = s / np.sqrt(len(fold_alphas))
        t = m / se if se > 0 else 0
        p = 2 * (1 - _norm_cdf(abs(t)))

        results.append({
            "id": f"random_{i:04d}",
            "alpha_mean_pct": round(m, 4),
            "alpha_t": round(t, 3),
            "alpha_p": round(p, 6),
            "n_folds": len(fold_alphas),
            "config": {"model": "random_shuffle", "features": "noise", "universe": "all"},
        })

    # Apply FDR gate
    n_raw_sig = sum(1 for r in results if r["alpha_p"] < 0.05 and r["alpha_mean_pct"] > 0)
    fdr_survivors = gate_fdr(results, alpha=0.05)

    # Apply holdout gate on FDR survivors (random systems can't be re-run on holdout
    # since they don't have real ML models — so FDR is the binding gate here)
    # For completeness, check if any FDR survivor has positive alpha
    n_fdr = len(fdr_survivors)

    test_pass = n_fdr == 0
    return {
        "name": "random_noise_200",
        "n_random": n_random,
        "n_evaluated": len(results),
        "n_raw_significant": n_raw_sig,
        "n_after_fdr": n_fdr,
        "expected": "0 survivors after FDR",
        "pass": test_pass,
        "detail": f"{n_raw_sig} raw p<0.05 → {n_fdr} after BH FDR"
    }


def self_test_perfect_signal(rows: list[dict]) -> dict:
    """A perfect-foresight system MUST survive all gates."""
    dates = sorted(set(r["snap_date"] for r in rows))
    holdout_cut = max(1, len(dates) - int(len(dates) * HOLDOUT_FRAC))
    explore_dates = dates[:holdout_cut]
    spy = _spy_cache()

    # Perfect system: use RandomForest with the actual target as a feature (cheating)
    # Instead, simulate: at each fold, "predict" the actual return perfectly
    by_date: dict[str, list[dict]] = {}
    for r in rows:
        by_date.setdefault(r["snap_date"], []).append(r)

    explore_with_data = sorted(d for d in explore_dates if d in by_date)
    fold_alphas = []

    for k in range(3, len(explore_with_data)):
        test_rows = by_date.get(explore_with_data[k], [])
        if len(test_rows) < MIN_TEST_PER_FOLD:
            continue
        spy_ret = spy.get(explore_with_data[k])
        if spy_ret is None:
            continue

        y_test = np.array([r["fwd_5d_return"] for r in test_rows])
        actions = np.where(y_test > 0, 1, -1)
        trade_rets = np.where(actions == 1, y_test, -y_test)
        alpha_trades = (trade_rets - spy_ret) * 100
        fold_alphas.append(float(np.mean(alpha_trades)))

    if len(fold_alphas) < 3:
        return {"name": "perfect_signal", "pass": False, "detail": "too few folds"}

    m = np.mean(fold_alphas)
    s = np.std(fold_alphas, ddof=1)
    se = s / np.sqrt(len(fold_alphas))
    t = m / se if se > 0 else 0
    p = 2 * (1 - _norm_cdf(abs(t)))

    passes = m > 0 and p < 0.001
    return {
        "name": "perfect_signal",
        "alpha_mean_pct": round(m, 3),
        "p": round(p, 6),
        "expected": "alpha > 0, p < 0.001",
        "pass": passes,
        "detail": f"alpha={m:+.2f}% p={p:.6f}"
    }


def run_self_tests(rows: list[dict]) -> dict:
    tests = [
        self_test_random_noise(rows),
        self_test_perfect_signal(rows),
    ]
    all_pass = all(t["pass"] for t in tests)
    return {"tests": tests, "all_pass": all_pass}


# ── Full exploration run ───────────────────────────────────────────────────

def run_exploration(rows: list[dict]) -> dict:
    dates = sorted(set(r["snap_date"] for r in rows))
    holdout_cut = max(1, len(dates) - int(len(dates) * HOLDOUT_FRAC))
    explore_dates = dates[:holdout_cut]
    holdout_dates = dates[holdout_cut:]
    spy = _spy_cache()

    # Data quality gate (run once, applies to all experiments)
    dq = gate_data_quality(rows)
    if not dq["pass"]:
        return {"error": "data_quality_failed", "data_quality": dq}

    configs = enumerate_experiments()
    n_total = len(configs)

    # Run all experiments on exploration dates
    all_results = []
    for cfg in configs:
        result = run_experiment(cfg, rows, explore_dates, spy)
        result["config"] = cfg
        result["id"] = cfg["id"]
        all_results.append(result)

    valid = [r for r in all_results if "alpha_p" in r]
    errors = [r for r in all_results if "error" in r]

    # Gate 2: FDR
    fdr_survivors = gate_fdr(valid, alpha=0.05)

    # Gate 3: Holdout
    holdout_survivors = gate_holdout(fdr_survivors, rows, holdout_dates, spy)

    # Classify
    for r in all_results:
        if "error" in r:
            r["status"] = "error"
        elif r in holdout_survivors:
            r["status"] = "CANDIDATE"
        elif r.get("bh_pass"):
            r["status"] = "fdr_pass_holdout_fail"
        elif r.get("alpha_mean_pct", 0) > 0 and r.get("alpha_p", 1) < 0.05:
            r["status"] = "raw_sig_fdr_fail"
        else:
            r["status"] = "rejected"

    candidates = [r for r in all_results if r["status"] == "CANDIDATE"]

    payload = {
        "updated": _now_iso(),
        "n_experiments": n_total,
        "n_valid": len(valid),
        "n_errors": len(errors),
        "explore_dates": explore_dates,
        "holdout_dates": holdout_dates,
        "data_quality": dq,
        "defense_funnel": {
            "raw_significant": sum(1 for r in valid
                                   if r.get("alpha_p", 1) < 0.05 and r.get("alpha_mean_pct", 0) > 0),
            "after_fdr": len(fdr_survivors),
            "after_holdout": len(holdout_survivors),
        },
        "candidates": [{
            "id": c["id"],
            "config": c["config"],
            "explore_alpha": c["alpha_mean_pct"],
            "explore_p": c["alpha_p"],
            "holdout_alpha": c.get("holdout", {}).get("alpha_mean_pct"),
            "status": "CANDIDATE — awaiting out-of-time confirmation",
        } for c in candidates],
        "all_results": [{
            "id": r.get("id", "?"),
            "status": r.get("status"),
            "alpha": r.get("alpha_mean_pct"),
            "p": r.get("alpha_p"),
            "acc": r.get("accuracy"),
            "n_folds": r.get("n_folds"),
        } for r in all_results],
    }

    # Persist
    history = {"runs": []}
    if RESULTS_FILE.exists():
        try:
            history = json.loads(RESULTS_FILE.read_text())
            if "runs" not in history:
                history = {"runs": []}
        except (json.JSONDecodeError, OSError):
            history = {"runs": []}

    history["runs"].append({
        "updated": payload["updated"],
        "n_experiments": n_total,
        "n_candidates": len(candidates),
        "funnel": payload["defense_funnel"],
    })
    history["latest"] = payload
    history["updated"] = payload["updated"]
    RESULTS_FILE.write_text(json.dumps(history, indent=2, default=str))

    return payload


# ── Main ───────────────────────────────────────────────────────────────────

def run() -> int:
    ap = argparse.ArgumentParser(description="Automated exploration engine")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    rows = load_rows()

    if args.self_test:
        result = run_self_tests(rows)
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            for t in result["tests"]:
                tag = "PASS" if t["pass"] else "FAIL"
                print(f"  [{tag}] {t['name']}: {t['detail']}  (expect: {t['expected']})")
            print(f"\n  All pass: {result['all_pass']}")
        return 0 if result["all_pass"] else 1

    # Self-test must pass before exploration
    st = run_self_tests(rows)
    if not st["all_pass"]:
        print("ABORT: Self-test failed. Engine cannot be trusted.")
        for t in st["tests"]:
            tag = "PASS" if t["pass"] else "FAIL"
            print(f"  [{tag}] {t['name']}: {t['detail']}")
        return 1

    result = run_exploration(rows)

    if args.json:
        print(json.dumps(result, indent=2, default=str))
        return 0

    funnel = result["defense_funnel"]
    print(f"Exploration: {result['n_experiments']} experiments, "
          f"{result['n_valid']} valid, {result['n_errors']} errors")
    print(f"\n  Defense funnel:")
    print(f"    Raw significant (p<0.05, alpha>0): {funnel['raw_significant']}")
    print(f"    After FDR correction:              {funnel['after_fdr']}")
    print(f"    After holdout validation:           {funnel['after_holdout']}")

    dq = result["data_quality"]
    if dq["issues"]:
        print(f"\n  Data quality issues:")
        for issue in dq["issues"]:
            print(f"    {issue}")
    else:
        print(f"\n  Data quality: CLEAN")
    if dq.get("look_ahead_suspects"):
        print(f"  Look-ahead suspects (|r|>0.5): {dq['look_ahead_suspects']}")

    candidates = result.get("candidates", [])
    if candidates:
        print(f"\n  CANDIDATES ({len(candidates)}):")
        for c in candidates:
            print(f"    {c['id']}: explore_alpha={c['explore_alpha']:+.2f}% "
                  f"holdout_alpha={c.get('holdout_alpha', 'n/a')} p={c['explore_p']:.4f}")
    else:
        print(f"\n  No candidates survived all defense gates.")

    # Top results table
    valid_results = [r for r in result["all_results"] if r.get("alpha") is not None]
    valid_results.sort(key=lambda r: -(r.get("alpha") or -999))
    print(f"\n  Top 10 by alpha:")
    print(f"  {'ID':<40} {'Alpha':>7} {'p':>8} {'Acc':>6} {'Status'}")
    print(f"  {'-'*75}")
    for r in valid_results[:10]:
        a = f"{r['alpha']:+.2f}%" if r['alpha'] is not None else "  n/a"
        p = f"{r['p']:.4f}" if r['p'] is not None else "  n/a"
        acc = f"{r['acc']*100:.1f}%" if r.get('acc') else " n/a"
        print(f"  {r['id']:<40} {a:>7} {p:>8} {acc:>6} {r['status']}")

    print(f"\n  Wrote {RESULTS_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
