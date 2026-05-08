"""
paper_trade.py - Self-contained paper trading on archived predictions.

Inputs:
  - site/data/predictions_history/{date}.json  (one snapshot per decision date)
  - site/data/prices.json                       (close-only OHLC for 84-universe)

Output:
  - site/data/paper_trading_history.json

Strategies (MVP, Phase 1):
  - main_system:  action == 'BUY'
  - conservative: action == 'BUY' AND confidence >= 0.80
  - aggressive:   action in ('BUY','WATCH') AND confidence >= 0.40

Mechanics:
  - Initial cash $10,000 per strategy
  - Position size: $1,000 (equal $)
  - Max positions: 10
  - Entry/exit fill: next trading day close after snapshot date
  - Exits: stop loss -8%, take profit +20%, action flip to SELL/REDUCE, max hold 30 days
  - No shorts, no leverage, no fees

The replay walks the prices.json calendar from the first snapshot forward.
Any snapshot whose next trading day equals today's calendar date triggers
entry/exit decisions; remaining positions are stop/profit checked daily.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "site" / "data"
HISTORY_DIR = DATA_DIR / "predictions_history"
PRICES_FILE = DATA_DIR / "prices.json"
OUT_FILE = DATA_DIR / "paper_trading_history.json"

INITIAL_CASH = 10_000.0
POSITION_SIZE = 1_000.0
MAX_POSITIONS = 10
STOP_LOSS = -0.08
TAKE_PROFIT = 0.20
MAX_HOLD_DAYS = 30


# ── price cache ──────────────────────────────────────────────────────────
class PriceCache:
    def __init__(self, prices_file: Path):
        with prices_file.open() as f:
            data = json.load(f)
        self.tickers = data["tickers"]
        cal: set[str] = set()
        self.lookup: dict[str, dict[str, float]] = {}
        for tk, payload in self.tickers.items():
            self.lookup[tk] = dict(zip(payload["dates"], payload["closes"]))
            cal.update(payload["dates"])
        self.calendar: list[str] = sorted(cal)

    def next_trading_day(self, d: str) -> str | None:
        for cd in self.calendar:
            if cd > d:
                return cd
        return None

    def close(self, ticker: str, d: str) -> float | None:
        v = self.lookup.get(ticker, {}).get(d)
        return v if v is not None else None

    def latest_close(self, ticker: str, on_or_before: str) -> float | None:
        ts = self.lookup.get(ticker, {})
        if not ts:
            return None
        for cd in reversed(self.calendar):
            if cd <= on_or_before and ts.get(cd) is not None:
                return ts[cd]
        return None


# ── strategy selectors ──────────────────────────────────────────────────
def _sel_main(p: dict) -> bool:
    return p.get("action") == "BUY"


def _sel_conservative(p: dict) -> bool:
    return p.get("action") == "BUY" and (p.get("confidence") or 0) >= 0.80


def _sel_aggressive(p: dict) -> bool:
    return p.get("action") in ("BUY", "WATCH") and (p.get("confidence") or 0) >= 0.40


STRATEGIES: dict[str, dict[str, Any]] = {
    "main_system":  {"selector": _sel_main,         "label": "BUY only"},
    "conservative": {"selector": _sel_conservative, "label": "BUY ∧ conf≥0.80"},
    "aggressive":   {"selector": _sel_aggressive,   "label": "BUY∨WATCH ∧ conf≥0.40"},
}


# ── helpers ──────────────────────────────────────────────────────────────
def _days_between(d1: str, d2: str) -> int:
    a = datetime.fromisoformat(d1).date()
    b = datetime.fromisoformat(d2).date()
    return (b - a).days


def _extract_pillars(p: dict) -> dict:
    """Snapshot pillar/feature scores at decision time. Best-effort: missing keys are skipped."""
    out: dict[str, Any] = {
        "confidence": p.get("confidence"),
        "action": p.get("action"),
    }
    sig = p.get("signals") or {}
    if sig:
        for k in ("rsi14", "macd_bias", "trend", "bb_position", "volatility", "hv20"):
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


# ── portfolio ────────────────────────────────────────────────────────────
class PaperPortfolio:
    def __init__(self, name: str, label: str):
        self.name = name
        self.label = label
        self.cash: float = INITIAL_CASH
        self.positions: dict[str, dict] = {}
        self.trades: list[dict] = []
        self.value_history: list[dict] = []

    def buy(self, ticker, fill_date, fill_price, target, stop, confidence, pillars, reason):
        if ticker in self.positions:
            return False
        if len(self.positions) >= MAX_POSITIONS:
            return False
        if self.cash < POSITION_SIZE:
            return False
        if not fill_price or fill_price <= 0:
            return False
        shares = POSITION_SIZE / fill_price
        cost = shares * fill_price
        self.cash -= cost
        self.positions[ticker] = {
            "shares": shares,
            "entry_price": fill_price,
            "entry_date": fill_date,
            "target": target,
            "stop": stop,
            "entry_confidence": confidence,
            "entry_pillars": pillars,
        }
        self.trades.append({
            "date": fill_date,
            "ticker": ticker,
            "action": "BUY",
            "shares": round(shares, 4),
            "price": round(fill_price, 4),
            "value": round(cost, 2),
            "reason": reason,
            "confidence": confidence,
            "pillars": pillars,
        })
        return True

    def sell(self, ticker, fill_date, fill_price, reason):
        if ticker not in self.positions:
            return False
        if not fill_price or fill_price <= 0:
            return False
        pos = self.positions[ticker]
        proceeds = pos["shares"] * fill_price
        ret_pct = (fill_price / pos["entry_price"] - 1) * 100
        hold_days = _days_between(pos["entry_date"], fill_date)
        self.cash += proceeds
        self.trades.append({
            "date": fill_date,
            "ticker": ticker,
            "action": "SELL",
            "shares": round(pos["shares"], 4),
            "price": round(fill_price, 4),
            "value": round(proceeds, 2),
            "entry_price": round(pos["entry_price"], 4),
            "entry_date": pos["entry_date"],
            "return_pct": round(ret_pct, 3),
            "hold_days": hold_days,
            "reason": reason,
            "entry_confidence": pos["entry_confidence"],
            "entry_pillars": pos["entry_pillars"],
        })
        del self.positions[ticker]
        return True

    def mark_to_market(self, prices: PriceCache, d: str):
        positions_value = 0.0
        for tk, pos in self.positions.items():
            px = prices.latest_close(tk, d)
            positions_value += pos["shares"] * (px if px is not None else pos["entry_price"])
        total = self.cash + positions_value
        self.value_history.append({
            "date": d,
            "cash": round(self.cash, 2),
            "positions_value": round(positions_value, 2),
            "total": round(total, 2),
            "n_positions": len(self.positions),
        })


# ── snapshot loader ─────────────────────────────────────────────────────
def load_snapshots() -> list[tuple[str, dict]]:
    files = sorted(HISTORY_DIR.glob("*.json"))
    out = []
    for f in files:
        snap_date = f.stem
        try:
            datetime.fromisoformat(snap_date)
        except ValueError:
            continue
        with f.open() as fh:
            out.append((snap_date, json.load(fh)))
    return out


# ── replay engine ───────────────────────────────────────────────────────
def run() -> None:
    prices = PriceCache(PRICES_FILE)
    snapshots = load_snapshots()
    if not snapshots:
        raise SystemExit(f"No snapshots in {HISTORY_DIR}")

    portfolios = {
        name: PaperPortfolio(name=name, label=cfg["label"])
        for name, cfg in STRATEGIES.items()
    }

    snap_by_date = {d: snap for d, snap in snapshots}
    fill_day_for_snap: dict[str, list[str]] = {}
    for sd in snap_by_date:
        ntd = prices.next_trading_day(sd)
        if ntd is not None:
            fill_day_for_snap.setdefault(ntd, []).append(sd)

    start_snap = snapshots[0][0]
    trading_days = [d for d in prices.calendar if d >= start_snap]
    if not trading_days:
        raise SystemExit("No trading days in prices.json after first snapshot")

    for d in trading_days:
        # 1) Snapshot-driven entries/exits whose fill day is today.
        for snap_date in fill_day_for_snap.get(d, []):
            snap = snap_by_date[snap_date]
            preds = snap.get("predictions", []) or []
            actions_by_ticker = {p["ticker"]: p for p in preds if "ticker" in p}

            for name, pf in portfolios.items():
                # exits driven by SELL/REDUCE in this snapshot
                for tk in list(pf.positions.keys()):
                    a = (actions_by_ticker.get(tk) or {}).get("action")
                    if a in ("SELL", "REDUCE"):
                        px = prices.close(tk, d) or pf.positions[tk]["entry_price"]
                        pf.sell(tk, d, px, reason=f"signal_{a.lower()}@{snap_date}")

                # entries
                selector = STRATEGIES[name]["selector"]
                for p in preds:
                    if not selector(p):
                        continue
                    tk = p.get("ticker")
                    if not tk:
                        continue
                    fill = prices.close(tk, d)
                    if fill is None:
                        continue
                    target = p.get("target") or fill * (1 + TAKE_PROFIT)
                    stop_px = p.get("stop") or fill * (1 + STOP_LOSS)
                    pf.buy(
                        ticker=tk,
                        fill_date=d,
                        fill_price=fill,
                        target=target,
                        stop=stop_px,
                        confidence=p.get("confidence"),
                        pillars=_extract_pillars(p),
                        reason=f"{p.get('action')}_{snap_date}",
                    )

        # 2) Daily stop / take-profit / max-hold checks
        for pf in portfolios.values():
            for tk in list(pf.positions.keys()):
                pos = pf.positions[tk]
                px = prices.close(tk, d)
                if px is None:
                    continue
                ret = px / pos["entry_price"] - 1
                if ret <= STOP_LOSS:
                    pf.sell(tk, d, px, reason="stop_loss")
                elif ret >= TAKE_PROFIT:
                    pf.sell(tk, d, px, reason="take_profit")
                elif _days_between(pos["entry_date"], d) >= MAX_HOLD_DAYS:
                    pf.sell(tk, d, px, reason="max_hold")

        # 3) Mark-to-market
        for pf in portfolios.values():
            pf.mark_to_market(prices, d)

    # ── summarize ────────────────────────────────────────────────────────
    out = {
        "updated": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "initial_cash": INITIAL_CASH,
        "start_date": trading_days[0],
        "end_date": trading_days[-1],
        "n_snapshots": len(snapshots),
        "snapshot_dates": [d for d, _ in snapshots],
        "config": {
            "position_size": POSITION_SIZE,
            "max_positions": MAX_POSITIONS,
            "stop_loss": STOP_LOSS,
            "take_profit": TAKE_PROFIT,
            "max_hold_days": MAX_HOLD_DAYS,
        },
        "strategies": {},
    }
    for name, pf in portfolios.items():
        completed = [t for t in pf.trades if t["action"] == "SELL"]
        wins = [t for t in completed if t.get("return_pct", 0) > 0]
        avg_ret = (sum(t["return_pct"] for t in completed) / len(completed)) if completed else 0.0
        last_total = pf.value_history[-1]["total"] if pf.value_history else INITIAL_CASH
        total_ret = (last_total / INITIAL_CASH - 1) * 100
        # holding period return (open positions)
        open_positions = []
        for tk, pos in pf.positions.items():
            px = prices.latest_close(tk, trading_days[-1])
            unreal = ((px / pos["entry_price"] - 1) * 100) if px else 0.0
            open_positions.append({
                "ticker": tk,
                "entry_date": pos["entry_date"],
                "entry_price": round(pos["entry_price"], 4),
                "current_price": round(px, 4) if px else None,
                "shares": round(pos["shares"], 4),
                "unrealized_pct": round(unreal, 3),
                "entry_confidence": pos["entry_confidence"],
                "entry_pillars": pos["entry_pillars"],
            })
        out["strategies"][name] = {
            "label": pf.label,
            "cash": round(pf.cash, 2),
            "open_positions": open_positions,
            "trades": pf.trades,
            "value_history": pf.value_history,
            "metrics": {
                "total_return_pct": round(total_ret, 3),
                "n_trades_total": len(pf.trades),
                "n_completed": len(completed),
                "n_open": len(pf.positions),
                "n_wins": len(wins),
                "win_rate": round(len(wins) / len(completed), 3) if completed else None,
                "avg_return_pct": round(avg_ret, 3),
                "current_value": round(last_total, 2),
            },
        }

    OUT_FILE.write_text(json.dumps(out, indent=2, default=str))
    print(f"Wrote {OUT_FILE}")
    print(f"  Period: {out['start_date']} → {out['end_date']}  ({len(trading_days)} trading days, {len(snapshots)} snapshots)")
    for name, st in out["strategies"].items():
        m = st["metrics"]
        wr = f"{m['win_rate']*100:.0f}%" if m["win_rate"] is not None else "n/a"
        print(
            f"  {name:14s} "
            f"return={m['total_return_pct']:+6.2f}%  "
            f"trades={m['n_trades_total']:3d}  "
            f"open={m['n_open']:2d}  "
            f"wins={m['n_wins']:2d}/{m['n_completed']:<2d} "
            f"({wr})"
        )


if __name__ == "__main__":
    run()
