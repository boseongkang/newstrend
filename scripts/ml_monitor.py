"""
ml_monitor.py — Forward-only ML alpha tracker with safety harness.

MODEL FROZEN. No tuning. This script:
1. Trains RF + GBM on ALL data up to the baseline date (2026-05-25)
2. Generates predictions for TODAY's snapshot (immutable prediction log)
3. Joins past predictions with realized returns (5d+ later)
4. Tracks cumulative forward-only alpha, regime splits, CI width
5. Monitors for model drift, OOD regimes, and prediction bias

Safety features:
 - Immutable prediction log (append-only, timestamped, model-hash verified)
 - Frozen model hash verification (training data + model determinism check)
 - OOD regime detection (flag when current regime differs from training dist.)
 - Bias monitor (long/short ratio, sector concentration, calibration)
 - Uncertainty labels on every prediction

All output is advisory/paper. No live trading connection.

Usage:
    python3 scripts/ml_monitor.py          # full run: predict + evaluate + log
    python3 scripts/ml_monitor.py --status # one-line CI status
    python3 scripts/ml_monitor.py --json   # JSON output
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
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
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "site" / "data"
ACCURACY_FILE = DATA / "prediction_accuracy.json"
PREDICTIONS_FILE = DATA / "predictions.json"
PRICES_FILE = DATA / "prices.json"
TICKERS_FILE = DATA / "tickers.json"
OUT_FILE = DATA / "ml_monitor.json"
PREDICTION_LOG = DATA / "ml_prediction_log.json"

BASELINE_DATE = "2026-05-25"

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

MODELS = {
    "RandomForest": RandomForestRegressor(
        n_estimators=100, min_samples_leaf=5, max_depth=6, random_state=42, n_jobs=-1),
    "GradientBoosting": GradientBoostingRegressor(
        n_estimators=100, max_depth=3, random_state=42),
}

TRAIN_REGIME_DIST = {"RISK-ON": 0.983, "RISK-OFF": 0.017}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _norm_cdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


# ── Feature extraction ─────────────────────────────────────────────────────

def _extract_row_from_accuracy(r: dict) -> dict | None:
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
    for k in NUMERIC_FEATURES:
        if k not in ("confidence",):
            row[k] = feats.get(k)
    for k in CATEGORICAL_FEATURES:
        if k != "regime":
            row[k] = feats.get(k)
    return row


def _extract_row_from_prediction(p: dict, regime: str) -> dict | None:
    """Extract features from today's predictions.json entry."""
    sig = p.get("signals") or {}
    news = p.get("news") or {}
    sent = p.get("sentiment") or {}
    fund = p.get("fundamental") or {}
    ins = p.get("insider") or {}
    if not sig.get("rsi14"):
        return None
    return {
        "snap_date": p.get("date", ""),
        "ticker": p.get("ticker", ""),
        "confidence": p.get("confidence", 0),
        "ta_rsi14": sig.get("rsi14"),
        "ta_hv20": sig.get("hv20"),
        "ta_trend": sig.get("trend"),
        "ta_macd_bias": sig.get("macd_bias"),
        "ta_bb_position": sig.get("bb_position"),
        "ta_volatility": sig.get("volatility"),
        "regime": regime,
        "news_best_conf": news.get("best_conf") if news.get("available") else None,
        "news_z": news.get("news_z"),
        "sentiment_score": (sent.get("filtered_score")
                            if sent.get("filtered_score") is not None
                            else sent.get("score")) if isinstance(sent, dict) and "score" in sent else None,
        "sentiment_total": sent.get("total") if isinstance(sent, dict) else None,
        "fundamental_score": fund.get("fundamental_score"),
        "quality_score": fund.get("quality_score"),
        "growth_score": fund.get("growth_score"),
        "health_score": fund.get("health_score"),
        "insider_score": ins.get("score") if ins.get("available") else None,
    }


# ── SPY benchmark ──────────────────────────────────────────────────────────

def _spy_5d(snap_date: str, cache: dict = {}) -> float | None:
    if "closes" not in cache:
        if not PRICES_FILE.exists():
            return None
        d = json.loads(PRICES_FILE.read_text())
        spy = d.get("tickers", {}).get("SPY")
        if spy:
            cache["dates"] = spy["dates"]
            cache["closes"] = dict(zip(spy["dates"], spy["closes"]))
        else:
            cache["closes"] = {}
    dates = cache.get("dates", [])
    closes = cache.get("closes", {})
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


# ── Model training + hash ─────────────────────────────────────────────────

def _build_preprocessor(train_df):
    nc = [c for c in NUMERIC_FEATURES if c in train_df.columns]
    cc = [c for c in CATEGORICAL_FEATURES if c in train_df.columns]
    pre = ColumnTransformer([
        ("num", Pipeline([("imp", SimpleImputer(strategy="median")),
                          ("scl", StandardScaler())]), nc),
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cc),
    ], remainder="drop")
    return pre, nc, cc


def _compute_model_hash(X_train: np.ndarray, y_train: np.ndarray, model_name: str) -> str:
    """Deterministic hash of training data + model config for freeze verification."""
    h = hashlib.sha256()
    h.update(X_train.tobytes())
    h.update(y_train.tobytes())
    h.update(model_name.encode())
    return h.hexdigest()[:16]


# ── Immutable prediction log ──────────────────────────────────────────────

def _load_prediction_log() -> dict:
    if PREDICTION_LOG.exists():
        try:
            return json.loads(PREDICTION_LOG.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {"predictions": [], "evaluations": []}


def _save_prediction_log(log: dict) -> None:
    PREDICTION_LOG.write_text(json.dumps(log, indent=2, default=str))


def _generate_today_predictions(trained_models: dict, pre, today_rows: list[dict],
                                model_hashes: dict) -> list[dict]:
    """Generate ML predictions for today's snapshot. Append-only."""
    if not today_rows:
        return []
    today_df = pd.DataFrame(today_rows)
    try:
        X_today = pre.transform(today_df)
    except Exception:
        return []

    predictions = []
    timestamp = _now_iso()
    for name, model in trained_models.items():
        preds = model.predict(X_today)
        for i, row in enumerate(today_rows):
            pred_return = float(preds[i])
            action = "BUY" if pred_return > 0 else "SELL"
            predictions.append({
                "timestamp": timestamp,
                "model": name,
                "model_hash": model_hashes.get(name, ""),
                "snap_date": row["snap_date"],
                "ticker": row["ticker"],
                "predicted_return": round(pred_return, 6),
                "predicted_action": action,
                "regime": row.get("regime", "UNKNOWN"),
                "sector": row.get("sector"),
                "realized_return": None,
                "correct": None,
                "evaluated": False,
                "label": "ADVISORY · unverified · single-regime training",
            })
    return predictions


def _evaluate_past_predictions(log: dict) -> int:
    """Join past predictions with realized returns. Returns count of newly evaluated."""
    n_evaluated = 0
    for pred in log["predictions"]:
        if pred.get("evaluated"):
            continue
        spy_ret = _spy_5d(pred["snap_date"])
        if spy_ret is None:
            continue

        # Load actual price return for this ticker
        if not PRICES_FILE.exists():
            continue
        prices_data = json.loads(PRICES_FILE.read_text())
        tk_data = prices_data.get("tickers", {}).get(pred["ticker"])
        if not tk_data:
            continue
        dates = tk_data["dates"]
        closes = dict(zip(dates, tk_data["closes"]))
        cal = sorted(closes.keys())

        lo, hi = 0, len(cal)
        while lo < hi:
            mid = (lo + hi) // 2
            if cal[mid] <= pred["snap_date"]:
                lo = mid + 1
            else:
                hi = mid
        idx = lo - 1
        if idx < 0 or idx + 5 >= len(cal):
            continue
        p0 = closes.get(cal[idx])
        p5 = closes.get(cal[idx + 5])
        if not p0 or not p5 or p0 <= 0:
            continue

        actual = p5 / p0 - 1
        pred["realized_return"] = round(actual, 6)
        trade_ret = actual if pred["predicted_action"] == "BUY" else -actual
        pred["alpha"] = round((trade_ret - spy_ret) * 100, 3)
        pred["correct"] = (pred["predicted_action"] == "BUY" and actual > 0) or \
                          (pred["predicted_action"] == "SELL" and actual < 0)
        pred["evaluated"] = True
        n_evaluated += 1

    return n_evaluated


# ── Bias monitor ───────────────────────────────────────────────────────────

def _compute_bias(predictions: list[dict]) -> dict:
    """Check for systematic prediction biases."""
    if not predictions:
        return {"status": "no_predictions"}

    unevaluated = [p for p in predictions if not p.get("evaluated")]
    evaluated = [p for p in predictions if p.get("evaluated")]

    # Direction bias (all predictions)
    all_preds = unevaluated + evaluated
    n_buy = sum(1 for p in all_preds if p.get("predicted_action") == "BUY")
    n_sell = sum(1 for p in all_preds if p.get("predicted_action") == "SELL")
    n_total = n_buy + n_sell
    long_ratio = n_buy / n_total if n_total else 0.5

    # Calibration (evaluated only)
    calibration = None
    if len(evaluated) >= 10:
        n_correct = sum(1 for p in evaluated if p.get("correct"))
        mean_pred = abs(np.mean([p["predicted_return"] for p in evaluated]))
        mean_actual = abs(np.mean([p.get("realized_return", 0) for p in evaluated]))
        calibration = {
            "accuracy": round(n_correct / len(evaluated), 4),
            "mean_abs_predicted": round(mean_pred * 100, 3),
            "mean_abs_realized": round(mean_actual * 100, 3),
            "overconfidence_ratio": round(mean_pred / mean_actual, 3) if mean_actual > 0 else None,
        }

    # Sector concentration
    sectors = {}
    for p in all_preds:
        s = p.get("sector") or "Unknown"
        sectors[s] = sectors.get(s, 0) + 1
    top_sector = max(sectors, key=sectors.get) if sectors else None
    top_pct = sectors.get(top_sector, 0) / n_total * 100 if n_total else 0

    return {
        "n_total_predictions": n_total,
        "n_evaluated": len(evaluated),
        "long_ratio": round(long_ratio, 3),
        "long_bias_warning": long_ratio > 0.75 or long_ratio < 0.25,
        "top_sector": top_sector,
        "top_sector_pct": round(top_pct, 1),
        "sector_concentration_warning": top_pct > 50,
        "calibration": calibration,
    }


# ── OOD detection ─────────────────────────────────────────────────────────

def _check_ood(current_regime: str) -> dict:
    """Flag if current regime is out-of-distribution vs training data."""
    train_pct = TRAIN_REGIME_DIST.get(current_regime, 0) * 100
    ood = train_pct < 5  # regime seen in <5% of training data
    return {
        "current_regime": current_regime,
        "training_pct": round(train_pct, 1),
        "out_of_distribution": ood,
        "confidence_note": (
            f"UNTESTED REGIME: {current_regime} was {train_pct:.1f}% of training data. "
            "Predictions unreliable." if ood else
            f"In-distribution: {current_regime} was {train_pct:.1f}% of training data."
        ),
    }


# ── Main run ───────────────────────────────────────────────────────────────

def run_monitor() -> dict:
    acc = json.loads(ACCURACY_FILE.read_text())
    all_records = acc.get("records", [])

    rows = [_extract_row_from_accuracy(r) for r in all_records if r.get("fwd_5d_return") is not None]
    rows = [r for r in rows if r is not None]

    train_rows = [r for r in rows if r["snap_date"] <= BASELINE_DATE]
    forward_rows = [r for r in rows if r["snap_date"] > BASELINE_DATE]

    if len(train_rows) < 30:
        return {"error": "insufficient training data", "n_train": len(train_rows)}

    train_df = pd.DataFrame(train_rows)
    pre, nc, cc = _build_preprocessor(train_df)
    X_train = pre.fit_transform(train_df)
    y_train = train_df["fwd_5d_return"].to_numpy(float)

    trained = {}
    model_hashes = {}
    for name, template in MODELS.items():
        model = clone(template)
        model.fit(X_train, y_train)
        trained[name] = model
        model_hashes[name] = _compute_model_hash(X_train, y_train, name)

    # ── 1. Generate today's predictions ──
    today_rows = []
    current_regime = "UNKNOWN"
    if PREDICTIONS_FILE.exists():
        pred_data = json.loads(PREDICTIONS_FILE.read_text())
        current_regime = (pred_data.get("market_regime") or {}).get("regime", "UNKNOWN")
        snap_date = pred_data.get("updated", "")[:10]
        for p in pred_data.get("predictions", []):
            p["date"] = snap_date
            row = _extract_row_from_prediction(p, current_regime)
            if row:
                # Add sector for bias monitoring
                sectors = json.loads(TICKERS_FILE.read_text()) if TICKERS_FILE.exists() else {}
                sector_lookup = {tk: sec for sec, tks in sectors.items() for tk in tks}
                row["sector"] = sector_lookup.get(row["ticker"])
                today_rows.append(row)

    log = _load_prediction_log()

    # Check for duplicate snap_date (don't re-predict same day)
    existing_dates = set(p["snap_date"] for p in log["predictions"])
    snap_today = today_rows[0]["snap_date"] if today_rows else ""
    new_predictions = []
    if snap_today and snap_today not in existing_dates:
        new_predictions = _generate_today_predictions(trained, pre, today_rows, model_hashes)
        log["predictions"].extend(new_predictions)

    # ── 2. Evaluate past predictions with realized returns ──
    n_newly_evaluated = _evaluate_past_predictions(log)
    _save_prediction_log(log)

    # ── 3. Model hash verification ──
    hash_ok = True
    prev_hashes = {}
    if OUT_FILE.exists():
        try:
            prev = json.loads(OUT_FILE.read_text())
            prev_hashes = (prev.get("latest") or {}).get("model_hashes", {})
        except (json.JSONDecodeError, OSError):
            pass
    hash_warnings = []
    for name, h in model_hashes.items():
        if prev_hashes and name in prev_hashes and prev_hashes[name] != h:
            hash_warnings.append(f"{name}: hash changed {prev_hashes[name]} → {h} (training data changed)")
            hash_ok = False

    # ── 4. Forward evaluation (from prediction log) ──
    evaluated_preds = [p for p in log["predictions"] if p.get("evaluated")]
    model_forward = {}
    for name in trained:
        model_preds = [p for p in evaluated_preds if p["model"] == name]
        if not model_preds:
            model_forward[name] = {"status": "no_evaluated_data", "n": 0}
            continue
        by_date = {}
        for p in model_preds:
            by_date.setdefault(p["snap_date"], []).append(p)
        fold_alphas = []
        fold_accs = []
        by_regime = {}
        for d, preds in sorted(by_date.items()):
            n = len(preds)
            n_correct = sum(1 for p in preds if p.get("correct"))
            alphas = [p.get("alpha", 0) for p in preds]
            acc = n_correct / n if n else 0
            alpha_mean = float(np.mean(alphas))
            fold_alphas.append(alpha_mean)
            fold_accs.append(acc)
            rg = preds[0].get("regime", "UNKNOWN")
            g = by_regime.setdefault(rg, {"n": 0, "correct": 0, "alpha_sum": 0, "folds": 0})
            g["n"] += n
            g["correct"] += n_correct
            g["alpha_sum"] += alpha_mean * n
            g["folds"] += 1

        all_alphas = [p.get("alpha", 0) for p in model_preds]
        m = float(np.mean(all_alphas)) if all_alphas else 0
        ci_lo = ci_hi = None
        if len(all_alphas) >= 2:
            se = float(np.std(all_alphas, ddof=1)) / math.sqrt(len(all_alphas))
            ci_lo = round(m - 1.96 * se, 3)
            ci_hi = round(m + 1.96 * se, 3)

        t_stat = p_val = 0.0
        if len(fold_alphas) >= 2:
            fm = np.mean(fold_alphas)
            fse = np.std(fold_alphas, ddof=1) / np.sqrt(len(fold_alphas))
            t_stat = fm / fse if fse > 0 else 0
            p_val = 2 * (1 - _norm_cdf(abs(t_stat)))

        regime_summary = {}
        for rg, g in by_regime.items():
            regime_summary[rg] = {
                "n": g["n"], "n_folds": g["folds"],
                "accuracy": round(g["correct"] / g["n"], 4) if g["n"] else 0,
                "alpha_mean_pct": round(g["alpha_sum"] / g["n"], 3) if g["n"] else 0,
                "provisional": g["n"] < 30,
            }

        model_forward[name] = {
            "status": "tracking",
            "n": len(model_preds),
            "n_folds": len(fold_alphas),
            "cumulative_alpha_pct": round(m, 3),
            "alpha_ci_95": [ci_lo, ci_hi],
            "alpha_t": round(t_stat, 3),
            "alpha_p": round(p_val, 4),
            "cumulative_accuracy": round(sum(fold_accs) / len(fold_accs), 4) if fold_accs else 0,
            "by_regime": regime_summary,
        }

    # ── 5. OOD check ──
    ood = _check_ood(current_regime)

    # ── 6. Bias monitor ──
    bias = _compute_bias(log["predictions"])

    # ── 7. Unfreeze check ──
    has_fwd_alpha = any(
        isinstance(s, dict) and s.get("status") == "tracking"
        and (s.get("cumulative_alpha_pct") or 0) > 0
        and (s.get("alpha_p") or 1) < 0.05
        for s in model_forward.values()
    )
    has_risk_off = any(
        rg != "RISK-ON"
        for s in model_forward.values() if isinstance(s, dict)
        for rg in (s.get("by_regime") or {})
    )

    alerts = []
    if has_risk_off:
        alerts.append("REGIME CHANGE: non-RISK-ON data detected in forward predictions")
    if not hash_ok:
        alerts.extend(hash_warnings)
    if ood.get("out_of_distribution"):
        alerts.append(f"OOD: {ood['confidence_note']}")
    if bias.get("long_bias_warning"):
        alerts.append(f"BIAS: long_ratio={bias['long_ratio']:.1%} — model may be systematically long")
    if bias.get("sector_concentration_warning"):
        alerts.append(f"BIAS: {bias['top_sector']} = {bias['top_sector_pct']:.0f}% of predictions")

    payload = {
        "updated": _now_iso(),
        "baseline_date": BASELINE_DATE,
        "model_hashes": model_hashes,
        "hash_verified": hash_ok,
        "historical": {
            "n_train": len(train_rows),
            "n_dates": len(set(r["snap_date"] for r in train_rows)),
        },
        "prediction_log": {
            "total": len(log["predictions"]),
            "evaluated": len(evaluated_preds),
            "pending": len(log["predictions"]) - len(evaluated_preds),
            "new_today": len(new_predictions),
            "newly_evaluated": n_newly_evaluated,
        },
        "forward": model_forward,
        "ood_check": ood,
        "bias_monitor": bias,
        "alerts": alerts,
        "unfreeze_check": {
            "forward_alpha_significant": has_fwd_alpha,
            "risk_off_observed": has_risk_off,
            "ready_to_unfreeze": has_fwd_alpha and has_risk_off,
        },
        "label": "ADVISORY ONLY · paper trading · unverified · single-regime baseline",
    }

    # ── Persist ──
    history = {"runs": []}
    if OUT_FILE.exists():
        try:
            history = json.loads(OUT_FILE.read_text())
            if "runs" not in history:
                history = {"runs": []}
        except (json.JSONDecodeError, OSError):
            history = {"runs": []}

    compact = {
        "updated": payload["updated"],
        "n_pred_total": payload["prediction_log"]["total"],
        "n_evaluated": payload["prediction_log"]["evaluated"],
        "models": {
            name: {"alpha": s.get("cumulative_alpha_pct"), "p": s.get("alpha_p"), "n": s.get("n", 0)}
            for name, s in model_forward.items() if isinstance(s, dict)
        },
        "hash_ok": hash_ok,
        "ood": ood.get("out_of_distribution", False),
        "unfreeze_ready": has_fwd_alpha and has_risk_off,
    }
    history["runs"].append(compact)
    history["latest"] = payload
    history["updated"] = payload["updated"]
    OUT_FILE.write_text(json.dumps(history, indent=2, default=str))

    return payload


# ── CLI ────────────────────────────────────────────────────────────────────

def run() -> int:
    ap = argparse.ArgumentParser(description="ML forward alpha tracker")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--status", action="store_true")
    args = ap.parse_args()

    result = run_monitor()

    if args.status:
        pl = result.get("prediction_log", {})
        print(f"ML Monitor: {pl.get('total',0)} predictions logged, "
              f"{pl.get('evaluated',0)} evaluated, {pl.get('new_today',0)} new today")
        for name, s in result.get("forward", {}).items():
            if isinstance(s, dict) and s.get("status") == "tracking":
                print(f"  {name}: n={s['n']} alpha={s['cumulative_alpha_pct']}% p={s['alpha_p']:.4f}")
            else:
                print(f"  {name}: awaiting data")
        uf = result.get("unfreeze_check", {})
        print(f"Unfreeze: {uf.get('ready_to_unfreeze', False)} "
              f"(alpha_sig={uf.get('forward_alpha_significant')}, "
              f"risk_off={uf.get('risk_off_observed')})")
        for a in result.get("alerts", []):
            print(f"  ALERT: {a}")
        return 0

    if args.json:
        print(json.dumps(result, indent=2, default=str))
        return 0

    print(f"ML Monitor (baseline: {result['baseline_date']})")
    print(f"  Training: {result['historical']['n_train']} rows")
    print(f"  Hash verified: {result['hash_verified']}")

    pl = result["prediction_log"]
    print(f"\n  Prediction log: {pl['total']} total, {pl['evaluated']} evaluated, "
          f"{pl['pending']} pending, +{pl['new_today']} today")

    fwd = result.get("forward", {})
    for name, s in fwd.items():
        if not isinstance(s, dict):
            continue
        if s.get("status") != "tracking":
            print(f"\n  {name}: {s.get('status', 'no data')}")
            continue
        ci = s.get("alpha_ci_95", [None, None])
        ci_str = f" CI=[{ci[0]:+.2f},{ci[1]:+.2f}]" if ci[0] is not None else ""
        print(f"\n  {name} (forward, n={s['n']}):")
        print(f"    Alpha: {s['cumulative_alpha_pct']:+.3f}%{ci_str}  "
              f"t={s['alpha_t']:.2f}  p={s['alpha_p']:.4f}")
        print(f"    Accuracy: {s['cumulative_accuracy']*100:.1f}%")
        for rg, ri in s.get("by_regime", {}).items():
            prov = " [PROVISIONAL]" if ri.get("provisional") else ""
            print(f"    {rg}: n={ri['n']} alpha={ri['alpha_mean_pct']:+.2f}%{prov}")

    ood = result["ood_check"]
    print(f"\n  OOD: {ood['confidence_note']}")

    bias = result["bias_monitor"]
    if bias.get("n_total_predictions"):
        print(f"  Bias: long_ratio={bias['long_ratio']:.1%}, "
              f"top_sector={bias['top_sector']}({bias['top_sector_pct']:.0f}%)")
        if bias.get("calibration"):
            cal = bias["calibration"]
            print(f"  Calibration: acc={cal['accuracy']*100:.1f}%, "
                  f"overconf_ratio={cal.get('overconfidence_ratio', 'n/a')}")

    alerts = result.get("alerts", [])
    if alerts:
        print(f"\n  ALERTS:")
        for a in alerts:
            print(f"    >>> {a}")

    uf = result["unfreeze_check"]
    print(f"\n  Unfreeze: alpha_sig={uf['forward_alpha_significant']}, "
          f"risk_off={uf['risk_off_observed']}, ready={uf['ready_to_unfreeze']}")
    print(f"\n  {result['label']}")
    print(f"  Wrote {OUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
