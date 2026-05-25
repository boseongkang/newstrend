"""
gap_analyzer.py — measure and minimize the prediction-vs-actual gap.

For every (ticker, snap_date) we form an *implicit predicted return*
from the system's action + confidence and compare it to the realised
5d forward return. Aggregate the absolute gap by every dimension we
have (pillar tertiles, confidence band, sector, regime, volatility
regime, day-of-week, action), compute per-pillar gradients (the
direction in which a pillar weight would have to move to reduce
average gap), and append a weekly snapshot to gap_history.json so
convergence can be tracked over months.

Inputs:
  - site/data/predictions_history/{date}.json
  - site/data/prices.json
  - site/data/tickers.json (sector lookup)

Output:
  - site/data/gap_analysis.json   (latest run, single object)
  - site/data/gap_history.json    (append-only weekly aggregates)

Predicted-return heuristic:
  BUY    +0.040 * conf
  WATCH  +0.015 * conf
  HOLD    0
  REDUCE -0.015 * conf
  SELL   -0.040 * conf

These multipliers are the system's notional "max expected 5d return"
at confidence 1.0 and were chosen to fall inside the realised 5d
return distribution (which sits roughly in [−5 %, +5 %]). They are
not fitted parameters; the gap-minimization loop's job is to surface
where this heuristic is systematically wrong.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "site" / "data"
HIST = DATA / "predictions_history"
PRICES_FILE = DATA / "prices.json"
TICKERS_FILE = DATA / "tickers.json"
OUT_LATEST = DATA / "gap_analysis.json"
OUT_HISTORY = DATA / "gap_history.json"

HORIZON = 5

# Predicted return heuristic
ACTION_PR = {
    "BUY":     0.040,
    "WATCH":   0.015,
    "HOLD":    0.000,
    "REDUCE": -0.015,
    "SELL":   -0.040,
}

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

CONF_BANDS = [
    ("[0.0, 0.5)", 0.0, 0.5),
    ("[0.5, 0.7)", 0.5, 0.7),
    ("[0.7, 0.8)", 0.7, 0.8),
    ("[0.8, 1.0]", 0.8, 1.01),
]

# Smoothness for gradient → weight delta translation
GRADIENT_LR = 0.05  # 1pp gap differential → 5% weight nudge
GRADIENT_CLAMP = 0.10  # max ±10 % per run

# EMA smoothing for market-drift offset
DRIFT_EMA_ALPHA = 0.3


# ── price calendar ──────────────────────────────────────────────────────
class PriceCache:
    def __init__(self, path: Path):
        with path.open() as f:
            data = json.load(f)
        cal: set[str] = set()
        self.lookup: dict[str, dict[str, float]] = {}
        for tk, payload in data["tickers"].items():
            self.lookup[tk] = dict(zip(payload["dates"], payload["closes"]))
            cal.update(payload["dates"])
        self.calendar: list[str] = sorted(cal)

    def anchor_index(self, snap_date: str) -> int:
        lo, hi = 0, len(self.calendar)
        while lo < hi:
            mid = (lo + hi) // 2
            if self.calendar[mid] <= snap_date:
                lo = mid + 1
            else:
                hi = mid
        return lo - 1

    def fwd_return(self, ticker: str, snap_date: str, n: int):
        i = self.anchor_index(snap_date)
        if i < 0 or i + n >= len(self.calendar):
            return None
        ts = self.lookup.get(ticker, {})
        p0 = ts.get(self.calendar[i])
        p1 = ts.get(self.calendar[i + n])
        if p0 is None or p1 is None or p0 <= 0:
            return None
        return p1 / p0 - 1


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _predicted_return(action: str | None, confidence: float | None) -> float:
    base = ACTION_PR.get(action or "HOLD", 0.0)
    if base == 0.0:
        return 0.0
    return base * (confidence or 0.0)


def _features(p: dict) -> dict:
    sig = p.get("signals") or {}
    news = p.get("news") or {}
    sent = p.get("sentiment") or {}
    fund = p.get("fundamental") or {}
    ins = p.get("insider") or {}
    out: dict = {"hv20": sig.get("hv20")}
    if news.get("available"):
        out["news_best_conf"] = news.get("best_conf")
    if isinstance(sent, dict) and "score" in sent:
        out["sentiment_score"] = (
            sent.get("filtered_score")
            if sent.get("filtered_score") is not None
            else sent.get("score")
        )
    if fund.get("fundamental_score") is not None:
        out["fundamental_score"] = fund.get("fundamental_score")
        out["quality_score"] = fund.get("quality_score")
        out["growth_score"] = fund.get("growth_score")
        out["health_score"] = fund.get("health_score")
    if ins.get("available"):
        for k in ("p_score", "score"):
            if k in ins:
                out[f"insider_{k}"] = ins[k]
    return out


def _conf_band(conf: float | None) -> str | None:
    if conf is None:
        return None
    for label, lo, hi in CONF_BANDS:
        if lo <= conf < hi:
            return label
    return None


def _vol_band(hv20: float | None) -> str | None:
    if hv20 is None:
        return None
    if hv20 < 20:
        return "low"
    if hv20 < 35:
        return "mid"
    return "high"


def _dow(d: str) -> str:
    return ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][datetime.fromisoformat(d).weekday()]


def _isoweek(d: str) -> str:
    dt = datetime.fromisoformat(d).date()
    yr, wk, _ = dt.isocalendar()
    return f"{yr}-W{wk:02d}"


def _compute_drift_ema(convergence_weeks: list[dict]) -> float:
    """EMA of weekly signed_gap — estimates persistent market drift.

    Warm-starts on first week, then rolls forward at DRIFT_EMA_ALPHA.
    Recent weeks receive higher weight so the offset adapts when
    the market regime shifts.
    """
    if not convergence_weeks:
        return 0.0
    drift = convergence_weeks[0]["avg_signed_gap_pct"]
    for w in convergence_weeks[1:]:
        drift = DRIFT_EMA_ALPHA * w["avg_signed_gap_pct"] + (1 - DRIFT_EMA_ALPHA) * drift
    return round(drift, 4)


def _drift_adjusted_metrics(actionable: list[dict], drift_pct: float) -> tuple[dict, dict]:
    """Counterfactual metrics if predicted_return were drift-corrected."""
    drift_dec = drift_pct / 100.0
    agg: dict[str, dict] = {}
    totals = {"n": 0, "abs_sum": 0.0, "signed_sum": 0.0, "dir": 0}
    for r in actionable:
        pred_adj = r["predicted_return"] - drift_dec
        actual = r["actual_return"]
        sgap = (pred_adj - actual) * 100
        dc = int(
            (pred_adj > 0 and actual > 0)
            or (pred_adj < 0 and actual < 0)
            or (pred_adj == 0 and abs(actual) < 0.01)
        )
        totals["n"] += 1
        totals["abs_sum"] += abs(sgap)
        totals["signed_sum"] += sgap
        totals["dir"] += dc
        g = agg.setdefault(r["action"], {"n": 0, "abs_sum": 0.0, "signed_sum": 0.0, "dir": 0})
        g["n"] += 1
        g["abs_sum"] += abs(sgap)
        g["signed_sum"] += sgap
        g["dir"] += dc
    n = totals["n"]
    summary = {
        "avg_abs_gap_pct": round(totals["abs_sum"] / n, 3),
        "avg_signed_gap_pct": round(totals["signed_sum"] / n, 3),
        "directional_acc": round(totals["dir"] / n, 3),
    }
    by_action = {}
    for act in sorted(agg):
        g = agg[act]
        by_action[act] = {
            "n": g["n"],
            "avg_abs_gap_pct": round(g["abs_sum"] / g["n"], 3),
            "avg_signed_gap_pct": round(g["signed_sum"] / g["n"], 3),
            "directional_acc": round(g["dir"] / g["n"], 3),
        }
    return summary, by_action


def _aggregate(records: list[dict], key_fn) -> dict:
    groups: dict = {}
    for r in records:
        if r.get("abs_gap_pct") is None:
            continue
        k = key_fn(r)
        if k is None:
            continue
        g = groups.setdefault(k, {"n": 0, "abs_sum": 0.0, "signed_sum": 0.0, "dir_correct": 0})
        g["n"] += 1
        g["abs_sum"] += r["abs_gap_pct"]
        g["signed_sum"] += r["signed_gap_pct"]
        g["dir_correct"] += int(r["dir_correct"])
    out: dict = {}
    for k in sorted(groups):
        g = groups[k]
        out[k] = {
            "n": g["n"],
            "avg_abs_gap_pct": round(g["abs_sum"] / g["n"], 3),
            "avg_signed_gap_pct": round(g["signed_sum"] / g["n"], 3),
            "directional_acc": round(g["dir_correct"] / g["n"], 3),
        }
    return out


def _tertile_bounds(values: list[float]) -> tuple[float, float] | None:
    vs = sorted(v for v in values if v is not None)
    if len(vs) < 6:
        return None
    return vs[len(vs) // 3], vs[2 * len(vs) // 3]


def _by_pillar_gap(records: list[dict]) -> dict:
    """For each pillar, bucket by tertile and report avg signed gap.

    A high tertile with strongly-positive gap means the system
    over-predicts (predicted > actual) at high pillar values; signal
    that pillar weight should shrink.
    """
    out: dict = {}
    for pkey in PILLAR_KEYS:
        vals = [r["features"].get(pkey) for r in records
                if r.get("abs_gap_pct") is not None]
        bounds = _tertile_bounds(vals)
        if bounds is None:
            continue
        t1, t2 = bounds

        def keyfn(r, _t1=t1, _t2=t2, _pk=pkey):
            v = r["features"].get(_pk)
            if v is None:
                return None
            if v < _t1:
                return "low"
            if v < _t2:
                return "mid"
            return "high"

        agg = _aggregate(records, keyfn)
        if not agg:
            continue
        out[pkey] = {"tertile_bounds": {"t1": round(t1, 4), "t2": round(t2, 4)},
                     "buckets": agg}
    return out


def _gradients(by_pillar: dict) -> dict:
    """Translate high-vs-low signed-gap differential into a weight delta.

    ΔW = clamp(LR * (gap_low - gap_high), ±CLAMP)

    A pillar that over-predicts at high values (signed_gap_high > 0,
    signed_gap_low ≈ 0) yields negative ΔW (shrink). The opposite case
    yields positive ΔW (grow).
    """
    out: dict = {}
    for pkey, info in by_pillar.items():
        b = info["buckets"]
        lo = b.get("low")
        hi = b.get("high")
        if not lo or not hi:
            out[pkey] = {"delta": 0.0, "reason": "missing_tier"}
            continue
        diff_pp = lo["avg_signed_gap_pct"] - hi["avg_signed_gap_pct"]
        delta = max(-GRADIENT_CLAMP, min(GRADIENT_CLAMP, GRADIENT_LR * diff_pp))
        out[pkey] = {
            "delta": round(delta, 4),
            "diff_pp": round(diff_pp, 4),
            "low_signed_gap_pct": lo["avg_signed_gap_pct"],
            "high_signed_gap_pct": hi["avg_signed_gap_pct"],
            "n_low": lo["n"],
            "n_high": hi["n"],
        }
    return out


def _convergence(records: list[dict]) -> dict:
    weeks: dict = {}
    for r in records:
        if r.get("abs_gap_pct") is None:
            continue
        w = _isoweek(r["snap_date"])
        g = weeks.setdefault(w, {"n": 0, "abs_sum": 0.0, "signed_sum": 0.0, "correct": 0})
        g["n"] += 1
        g["abs_sum"] += r["abs_gap_pct"]
        g["signed_sum"] += r["signed_gap_pct"]
        g["correct"] += int(r["dir_correct"])
    series = []
    for w in sorted(weeks):
        g = weeks[w]
        series.append({
            "week": w,
            "n": g["n"],
            "avg_abs_gap_pct": round(g["abs_sum"] / g["n"], 3),
            "avg_signed_gap_pct": round(g["signed_sum"] / g["n"], 3),
            "directional_acc": round(g["correct"] / g["n"], 3),
        })
    trend = "n/a"
    improvement_pct = None
    if len(series) >= 2:
        first = series[0]["avg_abs_gap_pct"]
        last = series[-1]["avg_abs_gap_pct"]
        if first > 0:
            improvement_pct = round((first - last) / first * 100, 2)
        trend = "decreasing" if last < first - 0.1 else ("increasing" if last > first + 0.1 else "plateauing")
    return {"weeks": series, "trend": trend, "improvement_pct": improvement_pct}


def run() -> None:
    snapshots = sorted(HIST.glob("*.json"))
    if not snapshots:
        raise SystemExit(f"No snapshots in {HIST}")
    prices = PriceCache(PRICES_FILE)
    sectors = json.loads(TICKERS_FILE.read_text()) if TICKERS_FILE.exists() else {}
    sector_lookup = {tk: sec for sec, ticks in sectors.items() for tk in ticks}

    records: list[dict] = []
    for f in snapshots:
        try:
            datetime.fromisoformat(f.stem)
        except ValueError:
            continue
        with f.open() as fh:
            snap = json.load(fh)
        snap_date = f.stem
        regime = (snap.get("market_regime") or {}).get("regime")
        for p in snap.get("predictions") or []:
            tk = p.get("ticker")
            action = p.get("action")
            conf = p.get("confidence")
            if not tk or not action:
                continue
            actual = prices.fwd_return(tk, snap_date, HORIZON)
            predicted = _predicted_return(action, conf)
            feats = _features(p)
            rec: dict = {
                "snap_date": snap_date,
                "ticker": tk,
                "action": action,
                "confidence": conf,
                "predicted_return": round(predicted, 5),
                "actual_return": None if actual is None else round(actual, 5),
                "signed_gap_pct": None if actual is None else round((predicted - actual) * 100, 4),
                "abs_gap_pct": None if actual is None else round(abs(predicted - actual) * 100, 4),
                "dir_correct": (
                    None if actual is None
                    else int((predicted > 0 and actual > 0) or (predicted < 0 and actual < 0) or (predicted == 0 and abs(actual) < 0.01))
                ),
                "regime": regime,
                "sector": sector_lookup.get(tk),
                "features": feats,
            }
            records.append(rec)

    actionable = [r for r in records if r.get("abs_gap_pct") is not None]

    by_pillar = _by_pillar_gap(actionable)
    aggregations = {
        "by_action":     _aggregate(actionable, lambda r: r["action"]),
        "by_confidence": _aggregate(actionable, lambda r: _conf_band(r["confidence"])),
        "by_sector":     _aggregate(actionable, lambda r: r["sector"]),
        "by_regime":     _aggregate(actionable, lambda r: r["regime"]),
        "by_volatility": _aggregate(actionable, lambda r: _vol_band((r["features"] or {}).get("hv20"))),
        "by_dow":        _aggregate(actionable, lambda r: _dow(r["snap_date"])),
    }
    grads = _gradients(by_pillar)
    convergence = _convergence(actionable)

    # Market-drift offset (EMA of weekly signed gaps)
    drift_ema_pct = _compute_drift_ema(convergence["weeks"])
    summary_adj, by_action_adj = _drift_adjusted_metrics(actionable, drift_ema_pct)

    summary_n = len(actionable)
    avg_abs = round(sum(r["abs_gap_pct"] for r in actionable) / summary_n, 3) if summary_n else None
    avg_signed = round(sum(r["signed_gap_pct"] for r in actionable) / summary_n, 3) if summary_n else None
    dir_acc = round(sum(r["dir_correct"] for r in actionable) / summary_n, 3) if summary_n else None

    payload = {
        "updated": _now_iso(),
        "horizon_days": HORIZON,
        "n_records": len(records),
        "n_actionable": summary_n,
        "summary": {
            "avg_abs_gap_pct": avg_abs,
            "avg_signed_gap_pct": avg_signed,
            "directional_acc": dir_acc,
        },
        "predicted_return_heuristic": ACTION_PR,
        "market_drift": {
            "ema_offset_pct": drift_ema_pct,
            "ema_alpha": DRIFT_EMA_ALPHA,
        },
        "summary_drift_adjusted": summary_adj,
        "by_action_drift_adjusted": by_action_adj,
        "aggregations": aggregations,
        "by_pillar_tertile": by_pillar,
        "gradients": grads,
        "convergence": convergence,
    }
    OUT_LATEST.write_text(json.dumps(payload, indent=2, default=str))

    # Append-only history (one entry per run)
    history: dict
    if OUT_HISTORY.exists():
        history = json.loads(OUT_HISTORY.read_text())
        if "runs" not in history:
            history = {"runs": []}
    else:
        history = {"runs": []}
    history["runs"].append({
        "updated": payload["updated"],
        "n_actionable": summary_n,
        "summary": payload["summary"],
        "market_drift_ema_pct": drift_ema_pct,
        "convergence": convergence,
    })
    history["updated"] = payload["updated"]
    OUT_HISTORY.write_text(json.dumps(history, indent=2, default=str))

    # Stdout
    print(f"Wrote {OUT_LATEST}")
    print(f"  n_actionable={summary_n}  avg_abs_gap={avg_abs}%  avg_signed={avg_signed}%  dir_acc={dir_acc}")
    print()
    print("by_action:")
    for k, v in aggregations["by_action"].items():
        print(f"  {k:8s} n={v['n']:3d}  abs={v['avg_abs_gap_pct']:.2f}%  signed={v['avg_signed_gap_pct']:+.2f}%  dir={v['directional_acc']*100:.0f}%")
    print()
    print("by_confidence:")
    for k, v in aggregations["by_confidence"].items():
        print(f"  {k:14s} n={v['n']:3d}  abs={v['avg_abs_gap_pct']:.2f}%  signed={v['avg_signed_gap_pct']:+.2f}%")
    print()
    if grads:
        print("pillar gradients (Δweight to reduce gap):")
        for pk, gi in grads.items():
            d = gi['delta']
            why = gi.get('reason') or f"diff={gi.get('diff_pp', 'n/a')}pp"
            print(f"  {pk:22s} Δ={d:+.4f}  ({why})")
    if convergence["weeks"]:
        print()
        print("weekly convergence:")
        for w in convergence["weeks"]:
            print(f"  {w['week']} n={w['n']:3d} avg_abs={w['avg_abs_gap_pct']:.2f}%")
        print(f"  trend: {convergence['trend']}  improvement: {convergence['improvement_pct']}%")

    print()
    print(f"market drift (EMA of weekly signed gaps): {drift_ema_pct:+.3f}%")
    print()
    print("drift-adjusted vs raw:")
    print(f"  {'':18s} {'raw':>10s} {'adjusted':>10s} {'delta':>8s}")
    print(f"  {'avg_abs_gap':18s} {avg_abs:10.3f}% {summary_adj['avg_abs_gap_pct']:10.3f}% {summary_adj['avg_abs_gap_pct'] - avg_abs:+7.3f}%")
    print(f"  {'avg_signed_gap':18s} {avg_signed:+10.3f}% {summary_adj['avg_signed_gap_pct']:+10.3f}% {summary_adj['avg_signed_gap_pct'] - avg_signed:+7.3f}%")
    print(f"  {'directional_acc':18s} {dir_acc*100:9.1f}% {summary_adj['directional_acc']*100:9.1f}% {(summary_adj['directional_acc'] - dir_acc)*100:+6.1f}%p")
    print()
    print("by_action drift-adjusted:")
    for act in ["BUY", "WATCH", "HOLD", "REDUCE", "SELL"]:
        raw = aggregations["by_action"].get(act, {})
        adj = by_action_adj.get(act, {})
        if not raw or not adj:
            continue
        print(f"  {act:8s} signed: {raw['avg_signed_gap_pct']:+.2f}→{adj['avg_signed_gap_pct']:+.2f}%  "
              f"abs: {raw['avg_abs_gap_pct']:.2f}→{adj['avg_abs_gap_pct']:.2f}%  "
              f"dir: {raw['directional_acc']*100:.0f}→{adj['directional_acc']*100:.0f}%")


if __name__ == "__main__":
    run()
