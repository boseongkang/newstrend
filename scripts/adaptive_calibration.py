"""
adaptive_calibration.py - propose pillar weights & threshold updates from
realized prediction accuracy. Advisory by default (does not modify
predict.py); when sample size crosses MIN_N gates, the recommendation is
flagged `applied=true` and pillar_weights.json becomes authoritative for
downstream consumers (paper_trade strategies, predict.py reranking when
later wired).

Inputs:
  - site/data/prediction_accuracy.json   (records + tertile aggregations)
  - site/data/calibration_history.json   (previous run, optional, for EMA)
  - site/data/paper_trading_history.json (completed trades, optional, for stop-loss diagnostics)

Outputs:
  - site/data/pillar_weights.json   (latest snapshot — single object)
  - site/data/calibration_history.json (append-only log of all runs)

Recommendation logic:
  Per pillar:
    raw_alpha   = high_tertile_avg_return - low_tertile_avg_return  (% points)
    raw_weight  = clamp(1.0 + raw_alpha * ALPHA_SCALE, [0.5, 2.0])
    shrunk      = raw_weight * (n / (n + SHRINK_K)) + 1.0 * (SHRINK_K / (n + SHRINK_K))
    ema_weight  = EMA_ALPHA * shrunk + (1 - EMA_ALPHA) * prev_weight
    applied     = n >= MIN_N_PILLAR

  Confidence threshold:
    For each candidate threshold, score = (accuracy - 0.5) * sqrt(n).
    recommended = argmax(score) where n >= MIN_N_THRESHOLD; else current.
    EMA-blended toward previous applied threshold.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "site" / "data"
ACCURACY_FILE = DATA_DIR / "prediction_accuracy.json"
PAPER_FILE = DATA_DIR / "paper_trading_history.json"
WEIGHTS_FILE = DATA_DIR / "pillar_weights.json"
HISTORY_FILE = DATA_DIR / "calibration_history.json"

# Pillar score keys we calibrate over (must match prediction_tracker output)
PILLAR_KEYS = (
    "fundamental_score",
    "quality_score",
    "growth_score",
    "health_score",
    "sentiment_score",
    "news_best_conf",
    "insider_p_score",
    "insider_score",
)

# Default prior weight when no signal
PRIOR_WEIGHT = 1.0
DEFAULT_THRESHOLD = 0.70

# Tunables
MIN_N_PILLAR = 30          # below this n, weight stays advisory
MIN_N_THRESHOLD = 20       # below this n in best bucket, threshold stays advisory
ALPHA_SCALE = 5.0          # 1pp tertile spread → +5% weight tilt
WEIGHT_CLAMP = (0.5, 2.0)
SHRINK_K = 20              # pseudo-count pulling toward prior
EMA_ALPHA = 0.3            # how aggressively new run overrides prior

CONF_CANDIDATES = (0.40, 0.50, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85)


# ── helpers ──────────────────────────────────────────────────────────────
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _load_prev_calibration() -> dict | None:
    if not HISTORY_FILE.exists():
        return None
    try:
        log = json.loads(HISTORY_FILE.read_text())
        runs = log.get("runs") or []
        return runs[-1] if runs else None
    except (json.JSONDecodeError, KeyError):
        return None


# ── pillar α estimation ─────────────────────────────────────────────────
def _pillar_alpha(accuracy: dict, pkey: str) -> dict:
    """Use the by_pillar_tertile aggregation from prediction_tracker."""
    bucket = (accuracy.get("by_pillar_tertile") or {}).get(pkey)
    if not bucket:
        return {
            "n": 0,
            "alpha_pct": None,
            "high_avg_pct": None,
            "low_avg_pct": None,
            "high_acc": None,
            "low_acc": None,
        }
    buckets = bucket["buckets"]
    high = buckets.get("high")
    low = buckets.get("low")
    n_total = sum(b["n"] for b in buckets.values())
    if not high or not low:
        return {
            "n": n_total,
            "alpha_pct": None,
            "high_avg_pct": (high or {}).get("avg_return_pct"),
            "low_avg_pct": (low or {}).get("avg_return_pct"),
            "high_acc": (high or {}).get("accuracy"),
            "low_acc": (low or {}).get("accuracy"),
        }
    return {
        "n": n_total,
        "alpha_pct": round(high["avg_return_pct"] - low["avg_return_pct"], 4),
        "high_avg_pct": high["avg_return_pct"],
        "low_avg_pct": low["avg_return_pct"],
        "high_acc": high["accuracy"],
        "low_acc": low["accuracy"],
    }


def _recommend_pillar_weight(stats: dict, prev_weight: float) -> dict:
    n = stats["n"]
    alpha = stats["alpha_pct"]
    if alpha is None or n == 0:
        return {
            "raw_weight": PRIOR_WEIGHT,
            "shrunk_weight": PRIOR_WEIGHT,
            "ema_weight": prev_weight,  # decay nothing
            "applied": False,
            "reason": "no_signal" if n == 0 else "missing_tertile_pair",
        }
    raw = _clamp(PRIOR_WEIGHT + alpha * ALPHA_SCALE / 100.0, *WEIGHT_CLAMP)
    shrunk = raw * (n / (n + SHRINK_K)) + PRIOR_WEIGHT * (SHRINK_K / (n + SHRINK_K))
    ema = EMA_ALPHA * shrunk + (1.0 - EMA_ALPHA) * prev_weight
    applied = n >= MIN_N_PILLAR
    return {
        "raw_weight": round(raw, 4),
        "shrunk_weight": round(shrunk, 4),
        "ema_weight": round(ema, 4),
        "applied": applied,
        "reason": f"n={n}≥{MIN_N_PILLAR}" if applied else f"n={n}<{MIN_N_PILLAR}_advisory",
    }


# ── confidence threshold sweep ──────────────────────────────────────────
def _threshold_sweep(records: list[dict], horizon: int = 5) -> list[dict]:
    correct_key = f"correct_{horizon}d"
    ret_key = f"fwd_{horizon}d_return"
    rows: list[dict] = []
    for thr in CONF_CANDIDATES:
        n = 0
        n_correct = 0
        ret_sum = 0.0
        for r in records:
            if r.get(correct_key) is None:
                continue
            conf = r.get("confidence")
            if conf is None or conf < thr:
                continue
            n += 1
            n_correct += int(r[correct_key])
            ret_sum += r[ret_key]
        if n == 0:
            rows.append({"threshold": thr, "n": 0, "accuracy": None, "avg_return_pct": None, "score": None})
            continue
        acc = n_correct / n
        score = (acc - 0.5) * math.sqrt(n)
        rows.append({
            "threshold": thr,
            "n": n,
            "accuracy": round(acc, 3),
            "avg_return_pct": round(ret_sum / n * 100, 3),
            "score": round(score, 3),
        })
    return rows


def _recommend_threshold(sweep: list[dict], prev_threshold: float) -> dict:
    eligible = [r for r in sweep if r["n"] is not None and r["n"] >= MIN_N_THRESHOLD and r["score"] is not None]
    if not eligible:
        return {
            "recommended": prev_threshold,
            "ema": prev_threshold,
            "applied": False,
            "reason": f"no_threshold_with_n>={MIN_N_THRESHOLD}",
        }
    best = max(eligible, key=lambda r: r["score"])
    rec = best["threshold"]
    ema = EMA_ALPHA * rec + (1.0 - EMA_ALPHA) * prev_threshold
    return {
        "recommended": rec,
        "best_score": best["score"],
        "best_n": best["n"],
        "best_accuracy": best["accuracy"],
        "ema": round(ema, 4),
        "applied": True,
        "reason": f"argmax_score n={best['n']} acc={best['accuracy']}",
    }


# ── stop-loss diagnostics (advisory only, no auto-change) ───────────────
def _stop_loss_diagnostics() -> dict:
    if not PAPER_FILE.exists():
        return {"available": False, "reason": "no_paper_trading_history"}
    paper = json.loads(PAPER_FILE.read_text())
    completed = []
    for st in paper.get("strategies", {}).values():
        for t in st.get("trades", []):
            if t.get("action") == "SELL" and t.get("return_pct") is not None:
                completed.append(t)
    if not completed:
        return {"available": False, "reason": "no_completed_trades"}
    rets = sorted(t["return_pct"] for t in completed)
    n = len(rets)
    losses = [r for r in rets if r < 0]
    stopped_out = [t for t in completed if t.get("reason") == "stop_loss"]
    take_profit = [t for t in completed if t.get("reason") == "take_profit"]
    p10 = rets[int(0.10 * n)] if n >= 10 else None
    return {
        "available": True,
        "n_completed": n,
        "n_losses": len(losses),
        "n_stopped_out": len(stopped_out),
        "n_take_profit": len(take_profit),
        "min_return_pct": rets[0],
        "max_return_pct": rets[-1],
        "p10_return_pct": p10,
        "current_stop_loss_pct": -8.0,
        "recommendation": "hold_current",
        "reason": "insufficient_completed_trades_for_optimization",
    }


# ── main ────────────────────────────────────────────────────────────────
def run() -> None:
    if not ACCURACY_FILE.exists():
        raise SystemExit(f"Missing {ACCURACY_FILE} — run prediction_tracker.py first")
    accuracy = json.loads(ACCURACY_FILE.read_text())
    prev = _load_prev_calibration()

    # Resolve previous weights / threshold for EMA
    prev_weights: dict[str, float] = {}
    prev_threshold: float = DEFAULT_THRESHOLD
    if prev:
        for pkey, info in (prev.get("pillars") or {}).items():
            prev_weights[pkey] = info.get("ema_weight", PRIOR_WEIGHT)
        prev_threshold = (prev.get("confidence_threshold") or {}).get("ema", DEFAULT_THRESHOLD)

    # ── per-pillar recommendations ──────────────────────────────────────
    pillars: dict[str, dict] = {}
    for pkey in PILLAR_KEYS:
        stats = _pillar_alpha(accuracy, pkey)
        rec = _recommend_pillar_weight(stats, prev_weights.get(pkey, PRIOR_WEIGHT))
        pillars[pkey] = {**stats, **rec}

    # ── confidence threshold ───────────────────────────────────────────
    sweep = _threshold_sweep(accuracy.get("records") or [], horizon=5)
    thr_rec = _recommend_threshold(sweep, prev_threshold)

    # ── stop loss (diagnostic only) ────────────────────────────────────
    stop_diag = _stop_loss_diagnostics()

    # Overall mode flag
    any_applied = any(p["applied"] for p in pillars.values()) or thr_rec["applied"]
    mode = "active" if any_applied else "advisory"

    payload = {
        "updated": _now_iso(),
        "mode": mode,
        "config": {
            "min_n_pillar": MIN_N_PILLAR,
            "min_n_threshold": MIN_N_THRESHOLD,
            "alpha_scale": ALPHA_SCALE,
            "weight_clamp": list(WEIGHT_CLAMP),
            "shrink_k": SHRINK_K,
            "ema_alpha": EMA_ALPHA,
            "candidates": list(CONF_CANDIDATES),
        },
        "input_summary": {
            "n_records": accuracy.get("n_records"),
            "n_actionable_5d": accuracy.get("n_actionable_5d"),
            "snapshot_count": (accuracy.get("coverage") or {}).get("snapshot_count"),
        },
        "pillars": pillars,
        "confidence_threshold": {
            "current_default": DEFAULT_THRESHOLD,
            "previous_ema": prev_threshold,
            "sweep": sweep,
            **thr_rec,
        },
        "stop_loss": stop_diag,
    }

    # Latest snapshot file (single object)
    WEIGHTS_FILE.write_text(json.dumps(payload, indent=2, default=str))

    # Append to history
    log: dict
    if HISTORY_FILE.exists():
        log = json.loads(HISTORY_FILE.read_text())
        if "runs" not in log:
            log = {"runs": []}
    else:
        log = {"runs": []}
    log["runs"].append(payload)
    log["updated"] = payload["updated"]
    HISTORY_FILE.write_text(json.dumps(log, indent=2, default=str))

    # ── stdout summary ──────────────────────────────────────────────────
    print(f"Wrote {WEIGHTS_FILE}  (mode={mode})")
    print(f"  history runs: {len(log['runs'])}")
    print()
    print("pillar weights:")
    for pkey, info in pillars.items():
        marker = "✓" if info["applied"] else "·"
        alpha = info["alpha_pct"]
        alpha_str = f"{alpha:+.2f}pp" if alpha is not None else "  n/a "
        print(
            f"  {marker} {pkey:22s} n={info['n']:3d}  "
            f"α={alpha_str}  ema={info['ema_weight']:.3f}  ({info['reason']})"
        )
    print()
    print("confidence threshold sweep:")
    for r in sweep:
        if r["n"] == 0:
            continue
        acc = f"{r['accuracy']*100:5.1f}%"
        avg = f"{r['avg_return_pct']:+5.2f}%"
        print(
            f"  thr={r['threshold']:.2f}  n={r['n']:3d}  acc={acc}  "
            f"ret={avg}  score={r['score']:+.2f}"
        )
    print(f"  → recommended={thr_rec.get('recommended')}  "
          f"ema={thr_rec.get('ema')}  applied={thr_rec['applied']}  ({thr_rec['reason']})")


if __name__ == "__main__":
    run()
