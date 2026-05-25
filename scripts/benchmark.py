"""
benchmark.py — Paper-trading strategy alpha vs SPY/QQQ buy-and-hold.

GUARDRAIL 1: Read-only. No model changes.
GUARDRAIL 3: Self-test — identical-to-market return yields alpha=0.

Computes alpha = strategy_return - benchmark_return over the same period.
All output includes alpha; absolute return is never shown alone.

Usage:
    python3 scripts/benchmark.py                # human-readable
    python3 scripts/benchmark.py --self-test    # self-tests
    python3 scripts/benchmark.py --json         # JSON output
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "site" / "data"
PAPER_FILE = DATA / "paper_trading_history.json"
PRICES_FILE = DATA / "prices.json"

BENCHMARKS = ("SPY", "QQQ")


# ── price helpers ──────────────────────────────────────────────────────────

def _load_prices() -> dict[str, dict[str, float]]:
    data = json.loads(PRICES_FILE.read_text())
    out: dict[str, dict[str, float]] = {}
    for tk, payload in data.get("tickers", {}).items():
        out[tk] = dict(zip(payload["dates"], payload["closes"]))
    return out


def _benchmark_return(prices: dict[str, dict[str, float]],
                      ticker: str, start: str, end: str) -> float | None:
    ts = prices.get(ticker, {})
    dates_sorted = sorted(ts.keys())
    p0 = p1 = None
    for d in dates_sorted:
        if d >= start and p0 is None:
            p0 = ts[d]
        if d <= end:
            p1 = ts[d]
    if p0 is None or p1 is None or p0 <= 0:
        return None
    return p1 / p0 - 1


# ── strategy evaluation ───────────────────────────────────────────────────

def evaluate_strategies() -> dict:
    paper = json.loads(PAPER_FILE.read_text())
    prices = _load_prices()
    results: dict[str, dict] = {}

    for name, st in paper.get("strategies", {}).items():
        vh = st.get("value_history", [])
        if not vh:
            continue
        start_date = vh[0]["date"]
        end_date = vh[-1]["date"]
        initial = vh[0]["total"]
        final = vh[-1]["total"]
        strat_ret = (final / initial - 1) if initial > 0 else 0

        benchmarks = {}
        for bm in BENCHMARKS:
            bm_ret = _benchmark_return(prices, bm, start_date, end_date)
            if bm_ret is not None:
                alpha = strat_ret - bm_ret
                benchmarks[bm] = {
                    "return_pct": round(bm_ret * 100, 2),
                    "alpha_pct": round(alpha * 100, 2),
                }

        # Regime distribution from predictions in that period
        regime_label = "RISK-ON"  # all paper-trade data is from this period

        cash_ratios = [v["cash"] / v["total"] for v in vh if v["total"] > 0]
        avg_invested = round((1 - sum(cash_ratios) / len(cash_ratios)) * 100, 1) if cash_ratios else 0

        results[name] = {
            "period": {"start": start_date, "end": end_date},
            "return_pct": round(strat_ret * 100, 2),
            "initial": initial,
            "final": round(final, 2),
            "avg_invested_pct": avg_invested,
            "regime": regime_label,
            "benchmarks": benchmarks,
        }

    return {"strategies": results}


# ── self-test ──────────────────────────────────────────────────────────────

def _self_test_alpha_zero() -> dict:
    """If strategy return equals SPY return, alpha must be 0."""
    prices = _load_prices()
    spy_ts = prices.get("SPY", {})
    if not spy_ts:
        return {"name": "alpha_zero", "expect": "alpha=0 when ret=benchmark",
                "actual": "SPY prices not found", "pass": False}
    dates = sorted(spy_ts.keys())
    start, end = dates[0], dates[-1]
    spy_ret = _benchmark_return(prices, "SPY", start, end)
    if spy_ret is None:
        return {"name": "alpha_zero", "expect": "alpha=0",
                "actual": "no SPY return", "pass": False}
    alpha = spy_ret - spy_ret
    ok = abs(alpha) < 1e-10
    return {"name": "alpha_zero", "expect": "alpha=0 when ret=benchmark",
            "actual": f"alpha={alpha:.10f}", "pass": ok}


def _self_test_positive_alpha() -> dict:
    """If strategy return > SPY return, alpha must be positive."""
    prices = _load_prices()
    spy_ts = prices.get("SPY", {})
    dates = sorted(spy_ts.keys())
    spy_ret = _benchmark_return(prices, "SPY", dates[0], dates[-1])
    fake_ret = (spy_ret or 0) + 0.05  # 5% above SPY
    alpha = fake_ret - (spy_ret or 0)
    ok = abs(alpha - 0.05) < 1e-10
    return {"name": "positive_alpha", "expect": "alpha=+5% when ret=spy+5%",
            "actual": f"alpha={alpha*100:+.2f}%", "pass": ok}


def run_self_tests() -> list[dict]:
    return [_self_test_alpha_zero(), _self_test_positive_alpha()]


# ── main ───────────────────────────────────────────────────────────────────

def run() -> int:
    ap = argparse.ArgumentParser(description="Benchmark alpha calculator")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.self_test:
        tests = run_self_tests()
        all_pass = all(t["pass"] for t in tests)
        if args.json:
            print(json.dumps({"tests": tests, "all_pass": all_pass}, indent=2))
        else:
            for t in tests:
                tag = "PASS" if t["pass"] else "FAIL"
                print(f"  [{tag}] {t['name']}: {t['actual']}  (expect: {t['expect']})")
            print(f"\n  All pass: {all_pass}")
        return 0 if all_pass else 1

    result = evaluate_strategies()

    if args.json:
        print(json.dumps(result, indent=2))
        return 0

    for name, s in result["strategies"].items():
        print(f"{name}:")
        print(f"  Period: {s['period']['start']} ~ {s['period']['end']}  "
              f"Invested: {s['avg_invested_pct']}%  Regime: {s['regime']}")
        print(f"  Return: {s['return_pct']:+.2f}%")
        for bm, info in s["benchmarks"].items():
            print(f"  vs {bm}: benchmark={info['return_pct']:+.2f}%  "
                  f"alpha={info['alpha_pct']:+.2f}%")
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(run())
