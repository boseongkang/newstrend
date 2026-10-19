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
COST_BPS_ONE_WAY = 5  # flat assumption for the cost line (2026-10-18)


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


def _close_on_or_before(ts: dict[str, float], dates_sorted: list[str], d: str) -> float | None:
    import bisect
    i = bisect.bisect_right(dates_sorted, d) - 1
    return ts[dates_sorted[i]] if i >= 0 else None


def _time_weighted_benchmark_leg(prices: dict[str, dict[str, float]],
                                 ticker: str, value_history: list[dict]) -> float | None:
    """Σ over consecutive value_history points of exposure_t × bm_ret(t→t+1).

    exposure_t = 1 − cash/total at the START of the interval (what the
    portfolio actually held while the benchmark moved). Simple sum, not
    compounded — consistent with strat_ret being a simple period return
    for the alpha subtraction at these magnitudes.
    """
    ts = prices.get(ticker, {})
    if not ts or len(value_history) < 2:
        return None
    ds = sorted(ts.keys())
    leg = 0.0
    for a, b in zip(value_history, value_history[1:]):
        if not a.get("total"):
            continue
        e = 1.0 - a["cash"] / a["total"]
        pa = _close_on_or_before(ts, ds, a["date"])
        pb = _close_on_or_before(ts, ds, b["date"])
        if pa and pb and pa > 0:
            leg += e * (pb / pa - 1)
    return leg


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

        cash_ratios = [v["cash"] / v["total"] for v in vh if v["total"] > 0]
        avg_invested = round((1 - sum(cash_ratios) / len(cash_ratios)) * 100, 1) if cash_ratios else 0

        # Exposure-adjusted alpha (STEP 3, 2026-08-05): the paper portfolio
        # averages only ~80-94% invested, so comparing its raw return against
        # a 100%-invested benchmark charges the strategy a mechanical
        # exposure drag of (1 - invested) * bm_ret (~0.7-1.3pp over this
        # period — larger than the alphas being reported). Headline alpha
        # nets the benchmark leg to the strategy's average exposure; the
        # unadjusted number is kept as alpha_raw_pct.
        # Time-weighted netting (2026-10-18 audit): netting the benchmark to
        # the *average* exposure (above) overstated alpha by ~2pp for the
        # main strategy, because the portfolio was ~95% invested in the
        # weeks SPY fell and ~50% invested in the weeks it rallied — the
        # average-exposure leg charged it 73.8% of a 7.72% rally it mostly
        # did not sit out. The headline now nets each value_history interval
        # at the exposure actually held going into it:
        #   bm_leg = Σ_t exposure_t × bm_ret(t → t+1)
        # The average-exposure figure is kept as alpha_avg_exposure_pct and
        # the 100%-invested comparison as alpha_raw_pct.
        # Cost line: the paper trader books no fees (paper_trade.py); a flat
        # 5 bp one-way assumption on booked trade value is shown next to the
        # headline so the reader sees what a realistic haircut does.
        benchmarks = {}
        exposure = avg_invested / 100.0
        turnover = sum(float(t.get("value") or 0) for t in st.get("trades", []))
        cost_frac = turnover * COST_BPS_ONE_WAY / 10_000 / initial if initial > 0 else 0.0
        for bm in BENCHMARKS:
            bm_ret = _benchmark_return(prices, bm, start_date, end_date)
            if bm_ret is not None:
                alpha_raw = strat_ret - bm_ret
                alpha_avg = strat_ret - exposure * bm_ret
                bm_leg_tw = _time_weighted_benchmark_leg(prices, bm, vh)
                alpha_tw = strat_ret - bm_leg_tw if bm_leg_tw is not None else None
                benchmarks[bm] = {
                    "return_pct": round(bm_ret * 100, 2),
                    "alpha_pct": round(alpha_tw * 100, 2) if alpha_tw is not None else None,
                    "alpha_def": "strategy_return − Σ exposure_t × benchmark_return_t (time-weighted)",
                    "alpha_after_cost_pct": round((alpha_tw - cost_frac) * 100, 2) if alpha_tw is not None else None,
                    "benchmark_leg_tw_pct": round(bm_leg_tw * 100, 2) if bm_leg_tw is not None else None,
                    "alpha_avg_exposure_pct": round(alpha_avg * 100, 2),
                    "alpha_raw_pct": round(alpha_raw * 100, 2),
                    "exposure_used": round(exposure, 3),
                }

        # Regime distribution from predictions in that period
        regime_label = "RISK-ON"  # all paper-trade data is from this period

        results[name] = {
            "period": {"start": start_date, "end": end_date},
            "return_pct": round(strat_ret * 100, 2),
            "initial": initial,
            "final": round(final, 2),
            "avg_invested_pct": avg_invested,
            "cost_assumption": {"bps_one_way": COST_BPS_ONE_WAY,
                                "turnover": round(turnover, 2),
                                "cost_pct": round(cost_frac * 100, 2),
                                "note": "paper_trade.py books no fees; applied here only"},
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


def _self_test_tw_full_exposure() -> dict:
    """At constant 100% exposure the time-weighted leg must equal the plain
    benchmark return (telescoping product ≈ sum for small moves, so compare
    against the summed daily returns, not the compounded one)."""
    prices = _load_prices()
    spy_ts = prices.get("SPY", {})
    if not spy_ts:
        return {"name": "tw_full_exposure", "expect": "leg == Σ daily ret",
                "actual": "SPY prices not found", "pass": False}
    ds = sorted(spy_ts.keys())[-30:]
    vh = [{"date": d, "cash": 0.0, "total": 100.0} for d in ds]
    leg = _time_weighted_benchmark_leg(prices, "SPY", vh)
    expect = sum(spy_ts[b] / spy_ts[a] - 1 for a, b in zip(ds, ds[1:]))
    ok = leg is not None and abs(leg - expect) < 1e-9
    vh0 = [{"date": d, "cash": 100.0, "total": 100.0} for d in ds]
    leg0 = _time_weighted_benchmark_leg(prices, "SPY", vh0)
    ok = ok and leg0 is not None and abs(leg0) < 1e-12
    return {"name": "tw_full_exposure", "expect": "100% → Σ daily ret; 0% → 0",
            "actual": f"leg={leg:.6f} expect={expect:.6f} zero_leg={leg0}", "pass": ok}


def run_self_tests() -> list[dict]:
    return [_self_test_alpha_zero(), _self_test_positive_alpha(), _self_test_tw_full_exposure()]


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
                  f"alpha={info['alpha_pct']:+.2f}% (after 5bp: {info['alpha_after_cost_pct']:+.2f}%, avg-exp: {info['alpha_avg_exposure_pct']:+.2f}%)")
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(run())
