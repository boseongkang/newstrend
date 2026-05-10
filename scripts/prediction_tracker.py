"""
prediction_tracker.py - measure prediction accuracy vs realized returns.

For each archived prediction snapshot, look up the realized N-trading-day
return for each ticker and classify whether the directional call was
correct. Then aggregate accuracy by action, confidence band, pillar
strength, sector, regime, and time-of-week/month.

Inputs:
  - site/data/predictions_history/{date}.json
  - site/data/prices.json
  - site/data/tickers.json   ({sector: [tickers]})

Output:
  - site/data/prediction_accuracy.json

Correctness rules (5-day horizon):
  BUY      → ret > 0
  WATCH    → ret > 0
  SELL     → ret < 0
  REDUCE   → ret < 0
  HOLD     → not actionable; recorded but excluded from accuracy stats
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "site" / "data"
HISTORY_DIR = DATA_DIR / "predictions_history"
PRICES_FILE = DATA_DIR / "prices.json"
TICKERS_FILE = DATA_DIR / "tickers.json"
OUT_FILE = DATA_DIR / "prediction_accuracy.json"

HORIZONS_DAYS = (5, 10)  # forward trading-day returns to compute

# Confidence bands
CONF_BANDS = [
    ("[0.0, 0.5)", 0.0, 0.5),
    ("[0.5, 0.7)", 0.5, 0.7),
    ("[0.7, 0.8)", 0.7, 0.8),
    ("[0.8, 1.0]", 0.8, 1.01),
]

# Pillar score keys to bucket on (value field name in extracted record)
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


# ── price calendar helpers ───────────────────────────────────────────────
class PriceCache:
    def __init__(self, path: Path):
        with path.open() as f:
            data = json.load(f)
        self.tickers = data["tickers"]
        cal: set[str] = set()
        self.lookup: dict[str, dict[str, float]] = {}
        for tk, payload in self.tickers.items():
            self.lookup[tk] = dict(zip(payload["dates"], payload["closes"]))
            cal.update(payload["dates"])
        self.calendar: list[str] = sorted(cal)

    def anchor_index(self, snap_date: str) -> int:
        """Largest calendar index with date <= snap_date (-1 if none)."""
        lo, hi = 0, len(self.calendar)
        while lo < hi:
            mid = (lo + hi) // 2
            if self.calendar[mid] <= snap_date:
                lo = mid + 1
            else:
                hi = mid
        return lo - 1

    def forward_return(self, ticker: str, snap_date: str, n: int):
        """(return, anchor_date, fwd_date) or (None, None, None) if unavailable."""
        anchor = self.anchor_index(snap_date)
        if anchor < 0:
            return None, None, None
        fwd = anchor + n
        if fwd >= len(self.calendar):
            return None, None, None
        anchor_date = self.calendar[anchor]
        fwd_date = self.calendar[fwd]
        ts = self.lookup.get(ticker, {})
        p0 = ts.get(anchor_date)
        p1 = ts.get(fwd_date)
        if p0 is None or p1 is None or p0 <= 0:
            return None, None, None
        return (p1 / p0 - 1), anchor_date, fwd_date


# ── feature extraction ──────────────────────────────────────────────────
def extract_features(p: dict) -> dict:
    out: dict = {}
    sig = p.get("signals") or {}
    if sig:
        for k in ("rsi14", "trend", "macd_bias", "bb_position", "volatility", "hv20"):
            if k in sig:
                out[f"ta_{k}"] = sig[k]
    news = p.get("news") or {}
    if news.get("available"):
        out["news_best_conf"] = news.get("best_conf")
        out["news_z"] = news.get("news_z_today")
    sent = p.get("sentiment") or {}
    if isinstance(sent, dict) and "score" in sent:
        out["sentiment_score"] = (
            sent.get("filtered_score")
            if sent.get("filtered_score") is not None
            else sent.get("score")
        )
        out["sentiment_total"] = sent.get("total")
    fund = p.get("fundamental") or {}
    if fund.get("fundamental_score") is not None:
        out["fundamental_score"] = fund.get("fundamental_score")
        out["quality_score"] = fund.get("quality_score")
        out["growth_score"] = fund.get("growth_score")
        out["health_score"] = fund.get("health_score")
    ins = p.get("insider") or {}
    if ins.get("available"):
        for k in ("p_score", "score", "p", "cluster_size", "net_buy_value", "n_buyers"):
            if k in ins:
                out[f"insider_{k}"] = ins[k]
    return out


def is_correct(action: str, ret: float | None) -> bool | None:
    if ret is None:
        return None
    if action == "BUY" or action == "WATCH":
        return ret > 0
    if action == "SELL" or action == "REDUCE":
        return ret < 0
    return None  # HOLD or unknown — not a directional call


# ── ticker → sector lookup ───────────────────────────────────────────────
def load_sector_lookup() -> dict[str, str]:
    if not TICKERS_FILE.exists():
        return {}
    by_sector = json.loads(TICKERS_FILE.read_text())
    out: dict[str, str] = {}
    for sector, tickers in by_sector.items():
        for t in tickers:
            out[t] = sector
    return out


# ── snapshot loader ─────────────────────────────────────────────────────
def load_snapshots() -> list[tuple[str, dict]]:
    files = sorted(HISTORY_DIR.glob("*.json"))
    out = []
    for f in files:
        try:
            datetime.fromisoformat(f.stem)
        except ValueError:
            continue
        with f.open() as fh:
            out.append((f.stem, json.load(fh)))
    return out


# ── aggregation helpers ─────────────────────────────────────────────────
# Time-decay parameters for pillar buckets. τ=14 calendar days means a
# record from 14 days ago contributes 1/e (~37%) of one from today. This
# keeps pillar weights responsive when market regime shifts. Other
# aggregations (by_action, by_dow, etc.) intentionally stay unweighted.
DECAY_TAU_DAYS = 14.0


def _decay_weight(snap_date: str, ref_date: datetime, tau_days: float) -> float:
    """exp(-Δdays / τ). Returns 1.0 if snap_date == ref_date."""
    import math
    days_old = max(0, (ref_date.date() - datetime.fromisoformat(snap_date).date()).days)
    return math.exp(-days_old / tau_days)


def _aggregate(records: list[dict], key_fn, *, horizon: int = 5, weight_fn=None) -> dict:
    """Group records by key_fn, compute accuracy + mean return.

    Records with non-directional actions or missing returns at this horizon
    are skipped. Keys returning None are also skipped.

    If `weight_fn` is provided, accuracy and avg_return are computed as
    weighted means (Σ w·x / Σ w). `n` stays as the raw record count so
    downstream gates (e.g. n ≥ 30) reason about real data volume, not
    effective sample size.
    """
    ret_key = f"fwd_{horizon}d_return"
    correct_key = f"correct_{horizon}d"
    groups: dict = {}
    for r in records:
        if r.get(correct_key) is None:
            continue
        k = key_fn(r)
        if k is None:
            continue
        w = float(weight_fn(r)) if weight_fn else 1.0
        g = groups.setdefault(k, {
            "n": 0, "n_correct": 0, "ret_sum": 0.0,
            "w_sum": 0.0, "w_correct": 0.0, "w_ret_sum": 0.0,
        })
        g["n"] += 1
        g["n_correct"] += int(r[correct_key])
        g["ret_sum"] += r[ret_key]
        g["w_sum"] += w
        g["w_correct"] += w * int(r[correct_key])
        g["w_ret_sum"] += w * r[ret_key]
    out = {}
    for k in sorted(groups):
        g = groups[k]
        if weight_fn and g["w_sum"] > 0:
            acc = g["w_correct"] / g["w_sum"]
            avg_ret = g["w_ret_sum"] / g["w_sum"]
        else:
            acc = g["n_correct"] / g["n"]
            avg_ret = g["ret_sum"] / g["n"]
        entry = {
            "n": g["n"],
            "n_correct": g["n_correct"],
            "accuracy": round(acc, 3),
            "avg_return_pct": round(avg_ret * 100, 3),
        }
        if weight_fn:
            entry["n_eff"] = round(g["w_sum"], 2)
        out[k] = entry
    return out


def _confidence_band(conf: float | None) -> str | None:
    if conf is None:
        return None
    for label, lo, hi in CONF_BANDS:
        if lo <= conf < hi:
            return label
    return None


def _tertile_bounds(values: list[float]) -> tuple[float, float] | None:
    vs = sorted(v for v in values if v is not None)
    if len(vs) < 6:
        return None
    return vs[len(vs) // 3], vs[2 * len(vs) // 3]


def _bucket_pillar(records: list[dict], pkey: str, horizon: int = 5,
                   ref_date: datetime | None = None,
                   tau_days: float = DECAY_TAU_DAYS) -> dict | None:
    """Tertile-bucket records by `pkey`, then aggregate with time-decay.

    Tertile bounds use raw values (geometric binning is regime-independent).
    Inside each bucket, accuracy and avg_return are exp(-Δt/τ)-weighted so
    recent records dominate — keeps the calibrator responsive to regime
    shifts without throwing away historical data.
    """
    vals = [r["features"].get(pkey) for r in records if r.get(f"correct_{horizon}d") is not None]
    bounds = _tertile_bounds(vals)
    if bounds is None:
        return None
    t1, t2 = bounds

    def key_fn(r):
        v = r["features"].get(pkey)
        if v is None:
            return None
        if v < t1:
            return "low"
        if v < t2:
            return "mid"
        return "high"

    if ref_date is None:
        ref_date = datetime.now(timezone.utc)
    weight_fn = lambda r: _decay_weight(r["snap_date"], ref_date, tau_days)
    agg = _aggregate(records, key_fn, horizon=horizon, weight_fn=weight_fn)
    if not agg:
        return None
    return {
        "tertile_bounds": {"t1": round(t1, 4), "t2": round(t2, 4)},
        "decay_tau_days": tau_days,
        "buckets": agg,
    }


def _dow_label(d: str) -> str:
    return ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][datetime.fromisoformat(d).weekday()]


def _month_label(d: str) -> str:
    return d[:7]


# ── main ────────────────────────────────────────────────────────────────
def run() -> None:
    prices = PriceCache(PRICES_FILE)
    sectors = load_sector_lookup()
    snapshots = load_snapshots()
    if not snapshots:
        raise SystemExit(f"No snapshots in {HISTORY_DIR}")

    records: list[dict] = []
    for snap_date, snap in snapshots:
        regime = (snap.get("market_regime") or {}).get("regime")
        for p in snap.get("predictions") or []:
            tk = p.get("ticker")
            action = p.get("action")
            if not tk or not action:
                continue
            rec: dict = {
                "snap_date": snap_date,
                "ticker": tk,
                "action": action,
                "confidence": p.get("confidence"),
                "regime": regime,
                "sector": sectors.get(tk),
                "features": extract_features(p),
            }
            for h in HORIZONS_DAYS:
                ret, anchor, fwd = prices.forward_return(tk, snap_date, h)
                rec[f"fwd_{h}d_return"] = round(ret, 5) if ret is not None else None
                rec[f"fwd_{h}d_anchor_date"] = anchor
                rec[f"fwd_{h}d_fwd_date"] = fwd
                rec[f"correct_{h}d"] = is_correct(action, ret)
            records.append(rec)

    # ── aggregations (5-day horizon is primary; 10d secondary) ──────────
    primary_h = 5
    aggregations = {
        "by_action":        _aggregate(records, lambda r: r["action"], horizon=primary_h),
        "by_confidence":    _aggregate(records, lambda r: _confidence_band(r["confidence"]), horizon=primary_h),
        "by_sector":        _aggregate(records, lambda r: r["sector"], horizon=primary_h),
        "by_regime":        _aggregate(records, lambda r: r["regime"], horizon=primary_h),
        "by_dow":           _aggregate(records, lambda r: _dow_label(r["snap_date"]), horizon=primary_h),
        "by_month":         _aggregate(records, lambda r: _month_label(r["snap_date"]), horizon=primary_h),
        "by_snapshot":      _aggregate(records, lambda r: r["snap_date"], horizon=primary_h),
    }
    aggregations_10d = {
        "by_action":     _aggregate(records, lambda r: r["action"], horizon=10),
        "by_confidence": _aggregate(records, lambda r: _confidence_band(r["confidence"]), horizon=10),
    }

    by_pillar: dict[str, dict] = {}
    for pkey in PILLAR_KEYS:
        bucket = _bucket_pillar(records, pkey, horizon=primary_h)
        if bucket is not None:
            by_pillar[pkey] = bucket

    # Regime-conditional buckets: {regime_label: {pkey: bucket}}.
    # Tertile bounds are computed within each regime so high/low are
    # regime-relative — a "high quality_score" in RISK-OFF can differ
    # from "high quality_score" in RISK-ON.
    by_pillar_by_regime: dict[str, dict] = {}
    regimes = sorted({r.get("regime") for r in records if r.get("regime")})
    for rg in regimes:
        rg_records = [r for r in records if r.get("regime") == rg]
        rg_buckets: dict[str, dict] = {}
        for pkey in PILLAR_KEYS:
            bucket = _bucket_pillar(rg_records, pkey, horizon=primary_h)
            if bucket is not None:
                rg_buckets[pkey] = bucket
        if rg_buckets:
            by_pillar_by_regime[rg] = rg_buckets

    # Coverage summary
    actionable = [r for r in records if r.get(f"correct_{primary_h}d") is not None]
    pending = [r for r in records if r.get(f"fwd_{primary_h}d_return") is None]

    out = {
        "updated": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "horizon_days": list(HORIZONS_DAYS),
        "n_records": len(records),
        "n_actionable_5d": len(actionable),
        "n_pending_5d": len(pending),
        "coverage": {
            "snapshots": [d for d, _ in snapshots],
            "snapshot_count": len(snapshots),
        },
        "aggregations": aggregations,
        "aggregations_10d": aggregations_10d,
        "by_pillar_tertile": by_pillar,
        "by_pillar_tertile_by_regime": by_pillar_by_regime,
        "records": records,
    }

    OUT_FILE.write_text(json.dumps(out, indent=2, default=str))
    print(f"Wrote {OUT_FILE}")
    print(f"  records={len(records)}  actionable_5d={len(actionable)}  pending_5d={len(pending)}")
    print()
    print("by_action (5d):")
    for k, v in aggregations["by_action"].items():
        print(f"  {k:8s} n={v['n']:3d}  acc={v['accuracy']*100:5.1f}%  avg_ret={v['avg_return_pct']:+.2f}%")
    print("by_confidence (5d):")
    for k, v in aggregations["by_confidence"].items():
        print(f"  {k:14s} n={v['n']:3d}  acc={v['accuracy']*100:5.1f}%  avg_ret={v['avg_return_pct']:+.2f}%")
    if by_pillar:
        print("by_pillar_tertile (5d):")
        for pk, bucket in by_pillar.items():
            print(f"  {pk}:")
            for tier, v in bucket["buckets"].items():
                print(f"    {tier:5s} n={v['n']:3d}  acc={v['accuracy']*100:5.1f}%  avg_ret={v['avg_return_pct']:+.2f}%")


if __name__ == "__main__":
    run()
