"""
feature_engineering.py — extract a flat feature matrix from archived
predictions for downstream ML.

For each (ticker, snap_date) row in predictions_history we emit:
  - 4 TA categoricals (trend, macd_bias, bb_position, volatility)
  - 4 TA numerics (rsi14, hv20, atr14, confidence)
  - 2 news (best_conf, news_z)
  - 3 sentiment (filtered_score, total, bullish_ratio)
  - 4 fundamental (quality, growth, health, fundamental_score)
  - 5 insider (p_score, score, cluster_size, n_buyers, net_buy_value)
  - 3 metadata (sector, regime, dow)
  - 1 action (categorical)
  = 26 features
  + targets: fwd_5d_return, fwd_10d_return, correct_5d, correct_10d

Output: site/data/ml_features.csv

Rows with no realised target (snapshots within HORIZON of today) keep
the features but the target columns are blank so downstream training
can drop them. Inputs are otherwise self-contained — no external API.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "site" / "data"
HIST = DATA / "predictions_history"
PRICES_FILE = DATA / "prices.json"
TICKERS_FILE = DATA / "tickers.json"
OUT_CSV = DATA / "ml_features.csv"

HORIZONS = (5, 10)


# ── price helpers ───────────────────────────────────────────────────────
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
        anchor = self.calendar[i]
        fwd = self.calendar[i + n]
        ts = self.lookup.get(ticker, {})
        p0 = ts.get(anchor)
        p1 = ts.get(fwd)
        if p0 is None or p1 is None or p0 <= 0:
            return None
        return p1 / p0 - 1


# ── feature row ─────────────────────────────────────────────────────────
def _feature_row(ticker: str, snap_date: str, p: dict, sectors: dict[str, str], regime: str | None) -> dict:
    sig = p.get("signals") or {}
    news = p.get("news") or {}
    sent = p.get("sentiment") or {}
    fund = p.get("fundamental") or {}
    ins = p.get("insider") or {}

    # sentiment ratios
    sent_total = sent.get("total")
    sent_bullish = sent.get("bullish")
    bullish_ratio = (sent_bullish / sent_total) if sent_total else None

    return {
        "snap_date": snap_date,
        "ticker": ticker,
        "action": p.get("action"),
        "confidence": p.get("confidence"),
        # TA categoricals
        "ta_trend": sig.get("trend"),
        "ta_macd_bias": sig.get("macd_bias"),
        "ta_bb_position": sig.get("bb_position"),
        "ta_volatility": sig.get("volatility"),
        # TA numerics
        "ta_rsi14": sig.get("rsi14"),
        "ta_hv20": sig.get("hv20"),
        "ta_atr14": sig.get("atr14"),
        # News
        "news_best_conf": news.get("best_conf") if news.get("available") else None,
        "news_z": news.get("news_z_today") if news.get("available") else None,
        # Sentiment
        "sent_filtered": sent.get("filtered_score") if sent.get("filtered_score") is not None else sent.get("score"),
        "sent_total": sent_total,
        "sent_bullish_ratio": bullish_ratio,
        # Fundamentals
        "fund_quality": fund.get("quality_score"),
        "fund_growth": fund.get("growth_score"),
        "fund_health": fund.get("health_score"),
        "fund_score": fund.get("fundamental_score"),
        # Insider
        "ins_p_score": ins.get("p_score") if ins.get("available") else None,
        "ins_score": ins.get("score") if ins.get("available") else None,
        "ins_cluster_size": ins.get("cluster_size") if ins.get("available") else None,
        "ins_n_buyers": ins.get("n_buyers") if ins.get("available") else None,
        "ins_net_buy_value": ins.get("net_buy_value") if ins.get("available") else None,
        # Metadata
        "sector": sectors.get(ticker),
        "regime": regime,
        "dow": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][datetime.fromisoformat(snap_date).weekday()],
    }


def _correct(action: str, ret: float | None) -> int | None:
    if ret is None:
        return None
    if action in ("BUY", "WATCH"):
        return int(ret > 0)
    if action in ("SELL", "REDUCE"):
        return int(ret < 0)
    return None  # HOLD or unknown — not directional


# ── snapshot loader ─────────────────────────────────────────────────────
def _load_snapshots() -> list[tuple[str, dict]]:
    out = []
    for f in sorted(HIST.glob("*.json")):
        try:
            datetime.fromisoformat(f.stem)
        except ValueError:
            continue
        with f.open() as fh:
            out.append((f.stem, json.load(fh)))
    return out


def _load_sector_lookup() -> dict[str, str]:
    if not TICKERS_FILE.exists():
        return {}
    by_sector = json.loads(TICKERS_FILE.read_text())
    return {tk: sector for sector, ticks in by_sector.items() for tk in ticks}


# ── main ────────────────────────────────────────────────────────────────
def run() -> None:
    snapshots = _load_snapshots()
    if not snapshots:
        raise SystemExit(f"No snapshots in {HIST}")
    prices = PriceCache(PRICES_FILE)
    sectors = _load_sector_lookup()

    rows: list[dict] = []
    for snap_date, snap in snapshots:
        regime = (snap.get("market_regime") or {}).get("regime")
        for p in snap.get("predictions") or []:
            tk = p.get("ticker")
            action = p.get("action")
            if not tk or not action:
                continue
            row = _feature_row(tk, snap_date, p, sectors, regime)
            for h in HORIZONS:
                ret = prices.fwd_return(tk, snap_date, h)
                row[f"fwd_{h}d_return"] = round(ret, 5) if ret is not None else None
                row[f"correct_{h}d"] = _correct(action, ret)
            rows.append(row)

    # Stable column order: features first, targets last
    feature_cols = [k for k in rows[0] if not k.startswith("fwd_") and not k.startswith("correct_")]
    target_cols = [k for k in rows[0] if k.startswith("fwd_") or k.startswith("correct_")]
    cols = feature_cols + target_cols

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in cols})

    n_total = len(rows)
    n_actionable = sum(1 for r in rows if r["correct_5d"] is not None)
    n_with_ret = sum(1 for r in rows if r["fwd_5d_return"] is not None)
    print(f"Wrote {OUT_CSV}")
    print(f"  rows={n_total}  with_5d_return={n_with_ret}  actionable_5d={n_actionable}")
    print(f"  features={len(feature_cols)-2} (excluding snap_date+ticker)  targets={len(target_cols)}")


if __name__ == "__main__":
    run()
