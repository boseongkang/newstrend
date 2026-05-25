"""
walk_forward.py — Phase 0 expanding-window out-of-sample evaluation harness.

GUARDRAIL 1: Read-only on predictions. No model/scoring changes.
GUARDRAIL 2: All metrics computed on test folds only. No in-sample tuning.
GUARDRAIL 3: Self-test with synthetic systems verifies harness correctness.

Expanding window: train dates [0..k], test date [k+1].

Train window role (Phase 0 vs Phase 2):
  Phase 0 (current): train window is NOT used for any computation. It exists
  solely to enforce temporal ordering — no test record's date can appear in
  the train set, guaranteeing out-of-sample evaluation. MIN_TRAIN_DATES
  controls the burn-in: the first 3 dates are skipped as test targets so
  that future Phase 2 models have a minimum context window. This costs us
  3 folds (44 records) but prevents a degenerate k=0 train set from
  appearing in any future use.

  Phase 2 (future placeholder): a model could fit on train records (e.g.
  recalibrate pillar weights or thresholds) before evaluating on the test
  fold. The harness signature walk_forward(records, system_fn) already
  supports this — system_fn receives the full record and can incorporate
  train-set-derived parameters passed via closure.

Usage:
    python3 scripts/walk_forward.py                  # evaluate real system
    python3 scripts/walk_forward.py --self-test       # run self-tests only
    python3 scripts/walk_forward.py --json            # JSON output
"""

from __future__ import annotations

import argparse
import json
import math
import random
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "site" / "data"
ACCURACY_FILE = DATA / "prediction_accuracy.json"
PRICES_FILE = DATA / "prices.json"

MIN_TRAIN_DATES = 3
PROVISIONAL_N = 30


# ── price cache (for SPY benchmark alpha) ──────────────────────────────────

class _PriceCache:
    """Lazy-loaded SPY price cache for benchmark alpha calculation."""

    def __init__(self):
        self._loaded = False
        self._calendar: list[str] = []
        self._closes: dict[str, float] = {}

    def _load(self):
        if self._loaded:
            return
        p = PRICES_FILE
        if not p.exists():
            self._loaded = True
            return
        data = json.loads(p.read_text())
        spy = data.get("tickers", {}).get("SPY")
        if spy:
            self._calendar = spy["dates"]
            self._closes = dict(zip(spy["dates"], spy["closes"]))
        self._loaded = True

    def fwd_return_5d(self, snap_date: str) -> float | None:
        self._load()
        if not self._calendar:
            return None
        idx = self._anchor(snap_date)
        if idx is None or idx + 5 >= len(self._calendar):
            return None
        d0 = self._calendar[idx]
        d5 = self._calendar[idx + 5]
        p0 = self._closes.get(d0)
        p5 = self._closes.get(d5)
        if p0 is None or p5 is None or p0 <= 0:
            return None
        return p5 / p0 - 1

    def _anchor(self, snap_date: str) -> int | None:
        lo, hi = 0, len(self._calendar)
        while lo < hi:
            mid = (lo + hi) // 2
            if self._calendar[mid] <= snap_date:
                lo = mid + 1
            else:
                hi = mid
        return lo - 1 if lo > 0 else None


_spy_cache = _PriceCache()


# ── helpers ────────────────────────────────────────────────────────────────

def load_records(path: Path = ACCURACY_FILE) -> list[dict]:
    acc = json.loads(path.read_text())
    return [r for r in acc["records"] if r.get("correct_5d") is not None]


def is_correct(action: str, fwd_return: float) -> bool | None:
    if action in ("BUY", "WATCH"):
        return fwd_return > 0
    if action in ("SELL", "REDUCE"):
        return fwd_return < 0
    return None


def wilson_ci(k: int, n: int, z: float = 1.96) -> tuple[float, float, float]:
    if n == 0:
        return 0.0, 0.0, 0.0
    p = k / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n) / denom
    return round(center, 4), round(max(0, center - margin), 4), round(min(1, center + margin), 4)


def proportion_z(k: int, n: int, p0: float = 0.5) -> tuple[float, float]:
    """Two-sided z-test. Returns (z_stat, p_value)."""
    if n == 0:
        return 0.0, 1.0
    p = k / n
    se = math.sqrt(p0 * (1 - p0) / n)
    if se == 0:
        return 0.0, 1.0
    z = (p - p0) / se
    p_val = 2 * (1 - _norm_cdf(abs(z)))
    return round(z, 4), round(p_val, 4)


def _norm_cdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def mean_t_test(values: list[float]) -> tuple[float, float, float]:
    """One-sample t-test H0: mean=0. Returns (mean, t_stat, p_value)."""
    n = len(values)
    if n < 2:
        return (values[0] if values else 0.0), 0.0, 1.0
    m = sum(values) / n
    var = sum((v - m) ** 2 for v in values) / (n - 1)
    se = math.sqrt(var / n) if var > 0 else 0
    if se == 0:
        return m, 0.0, 1.0
    t = m / se
    # Approximate two-sided p from t using normal (good enough for n>10)
    p = 2 * (1 - _norm_cdf(abs(t)))
    return round(m, 4), round(t, 4), round(p, 4)


# ── fold metrics ───────────────────────────────────────────────────────────

def fold_metrics(test_records: list[dict], snap_date: str) -> dict | None:
    n = len(test_records)
    if n == 0:
        return None

    n_correct = sum(1 for r in test_records if r["correct_5d"])
    accuracy = n_correct / n

    avg_ret = sum(r["fwd_5d_return"] for r in test_records) / n
    n_up = sum(1 for r in test_records if r["fwd_5d_return"] > 0)
    market_up_rate = n_up / n

    best_naive = max(market_up_rate, 1 - market_up_rate, 0.5)
    edge_vs_buy = accuracy - market_up_rate
    edge_vs_coin = accuracy - 0.5
    edge_vs_best = accuracy - best_naive

    z_coin, p_coin = proportion_z(n_correct, n, 0.5)
    _, ci_lo, ci_hi = wilson_ci(n_correct, n)

    regimes = set(r.get("regime") for r in test_records)
    regime = regimes.pop() if len(regimes) == 1 else "MIXED"

    # ── Alpha: per-trade and portfolio-level vs SPY ──
    spy_5d = _spy_cache.fwd_return_5d(snap_date)
    alpha_per_trade: list[float] = []
    if spy_5d is not None:
        for r in test_records:
            trade_ret = r["fwd_5d_return"]
            if r["action"] in ("SELL", "REDUCE"):
                trade_ret = -trade_ret
            alpha_per_trade.append(trade_ret - spy_5d)

    alpha_mean = None
    alpha_portfolio = None
    if alpha_per_trade:
        alpha_mean = round(sum(alpha_per_trade) / len(alpha_per_trade) * 100, 3)
        portfolio_ret = sum(
            r["fwd_5d_return"] if r["action"] in ("BUY", "WATCH")
            else -r["fwd_5d_return"]
            for r in test_records
        ) / n
        alpha_portfolio = round((portfolio_ret - spy_5d) * 100, 3)

    return {
        "n": n,
        "n_correct": n_correct,
        "accuracy": round(accuracy, 4),
        "ci_95": [ci_lo, ci_hi],
        "avg_return_pct": round(avg_ret * 100, 3),
        "market_up_rate": round(market_up_rate, 4),
        "edge_vs_buy_pp": round(edge_vs_buy * 100, 2),
        "edge_vs_coin_pp": round(edge_vs_coin * 100, 2),
        "edge_vs_best_pp": round(edge_vs_best * 100, 2),
        "best_naive": round(best_naive, 4),
        "z_vs_coin": z_coin,
        "p_vs_coin": p_coin,
        "regime": regime,
        "spy_5d_pct": round(spy_5d * 100, 3) if spy_5d is not None else None,
        "alpha_per_trade_pct": alpha_mean,
        "alpha_portfolio_pct": alpha_portfolio,
    }


# ── walk-forward engine ───────────────────────────────────────────────────

def walk_forward(records: list[dict], system_fn=None) -> dict:
    """Expanding-window walk-forward evaluation.

    system_fn: optional (record) -> action override. Recomputes correct_5d.
               In Phase 0 with system_fn=None, the train window is unused —
               we evaluate the system's own recorded predictions as-is.
    """
    dates = sorted(set(r["snap_date"] for r in records))
    by_date: dict[str, list[dict]] = {}
    for r in records:
        by_date.setdefault(r["snap_date"], []).append(r)

    if len(dates) < MIN_TRAIN_DATES + 1:
        return {"error": f"need >= {MIN_TRAIN_DATES + 1} dates, have {len(dates)}"}

    folds: list[dict] = []

    for k in range(MIN_TRAIN_DATES, len(dates)):
        test_date = dates[k]
        raw = by_date.get(test_date, [])

        if system_fn is not None:
            test_recs = []
            for r in raw:
                r2 = dict(r)
                r2["action"] = system_fn(r2)
                c = is_correct(r2["action"], r2["fwd_5d_return"])
                if c is None:
                    continue
                r2["correct_5d"] = c
                test_recs.append(r2)
        else:
            test_recs = list(raw)

        m = fold_metrics(test_recs, test_date)
        if m is None:
            continue
        m["test_date"] = test_date
        m["train_dates"] = k
        folds.append(m)

    if not folds:
        return {"error": "no valid folds"}

    return _aggregate(folds)


def _aggregate(folds: list[dict]) -> dict:
    total_n = sum(f["n"] for f in folds)
    total_correct = sum(f["n_correct"] for f in folds)
    total_up = sum(round(f["market_up_rate"] * f["n"]) for f in folds)

    pooled_acc = total_correct / total_n
    pooled_mup = total_up / total_n
    _, ci_lo, ci_hi = wilson_ci(total_correct, total_n)
    z_coin, p_coin = proportion_z(total_correct, total_n, 0.5)

    # ── Regime breakdown ──
    by_regime: dict[str, dict] = {}
    for f in folds:
        rg = f["regime"]
        g = by_regime.setdefault(rg, {"n": 0, "correct": 0, "folds": 0,
                                       "alpha_trades": [], "alpha_ports": []})
        g["n"] += f["n"]
        g["correct"] += f["n_correct"]
        g["folds"] += 1
        if f["alpha_per_trade_pct"] is not None:
            g["alpha_trades"].append(f["alpha_per_trade_pct"])
        if f["alpha_portfolio_pct"] is not None:
            g["alpha_ports"].append(f["alpha_portfolio_pct"])

    regime_summary = {}
    for rg, g in sorted(by_regime.items()):
        acc = g["correct"] / g["n"] if g["n"] else 0
        _, rlo, rhi = wilson_ci(g["correct"], g["n"])
        rz, rp = proportion_z(g["correct"], g["n"], 0.5)
        a_mean, a_t, a_p = mean_t_test(g["alpha_trades"]) if g["alpha_trades"] else (None, None, None)
        regime_summary[rg] = {
            "n": g["n"],
            "n_folds": g["folds"],
            "accuracy": round(acc, 4),
            "ci_95": [rlo, rhi],
            "z_vs_coin": rz,
            "p_vs_coin": rp,
            "alpha_per_trade_mean_pct": a_mean,
            "alpha_t_stat": a_t,
            "alpha_p_value": a_p,
            "provisional": g["n"] < PROVISIONAL_N,
        }

    # ── Pooled alpha ──
    alpha_trade_vals = [f["alpha_per_trade_pct"] for f in folds
                        if f["alpha_per_trade_pct"] is not None]
    alpha_port_vals = [f["alpha_portfolio_pct"] for f in folds
                       if f["alpha_portfolio_pct"] is not None]

    at_mean, at_t, at_p = mean_t_test(alpha_trade_vals) if alpha_trade_vals else (None, None, None)
    ap_mean, ap_t, ap_p = mean_t_test(alpha_port_vals) if alpha_port_vals else (None, None, None)

    return {
        "n_folds": len(folds),
        "total_test_n": total_n,
        "pooled_accuracy": round(pooled_acc, 4),
        "pooled_ci_95": [ci_lo, ci_hi],
        "pooled_market_up_rate": round(pooled_mup, 4),
        "pooled_edge_vs_buy_pp": round((pooled_acc - pooled_mup) * 100, 2),
        "pooled_edge_vs_coin_pp": round((pooled_acc - 0.5) * 100, 2),
        "z_vs_coin": z_coin,
        "p_vs_coin": p_coin,
        "alpha": {
            "per_trade_mean_pct": at_mean,
            "per_trade_t": at_t,
            "per_trade_p": at_p,
            "portfolio_mean_pct": ap_mean,
            "portfolio_t": ap_t,
            "portfolio_p": ap_p,
            "n_folds_with_alpha": len(alpha_trade_vals),
        },
        "by_regime": regime_summary,
        "folds": folds,
    }


# ── self-tests ─────────────────────────────────────────────────────────────

def _self_test_random(records: list[dict]) -> dict:
    """Random BUY/SELL → edge ≈ 0, p > 0.05."""
    rng = random.Random(42)

    def fn(r):
        return rng.choice(["BUY", "SELL"])

    res = walk_forward(records, system_fn=fn)
    edge = res["pooled_edge_vs_coin_pp"]
    p = res["p_vs_coin"]
    ok = abs(edge) < 10 and p > 0.05
    return {"name": "random_system", "expect": "|edge|<10pp, p>0.05",
            "actual": f"edge={edge:+.1f}pp p={p:.3f}", "pass": ok}


def _self_test_always_buy(records: list[dict]) -> dict:
    """Always-buy → edge_vs_buy = 0."""
    res = walk_forward(records, system_fn=lambda r: "BUY")
    edge = res["pooled_edge_vs_buy_pp"]
    ok = abs(edge) < 0.1
    return {"name": "always_buy", "expect": "edge_vs_buy=0",
            "actual": f"edge_vs_buy={edge:+.2f}pp", "pass": ok}


def _self_test_always_sell(records: list[dict]) -> dict:
    """Always-sell accuracy = 1 - market_up_rate."""
    res = walk_forward(records, system_fn=lambda r: "SELL")
    acc = res["pooled_accuracy"]
    mup = res["pooled_market_up_rate"]
    expected = round(1 - mup, 4)
    ok = abs(acc - expected) < 0.01
    return {"name": "always_sell", "expect": f"acc≈{expected:.3f} (1-mkt_up)",
            "actual": f"acc={acc:.3f}", "pass": ok}


def _self_test_perfect(records: list[dict]) -> dict:
    """Perfect foresight → accuracy=100%, significant."""
    def fn(r):
        return "BUY" if r["fwd_5d_return"] > 0 else "SELL"

    res = walk_forward(records, system_fn=fn)
    acc = res["pooled_accuracy"]
    p = res["p_vs_coin"]
    ok = acc > 0.99 and p < 0.001
    return {"name": "perfect_foresight", "expect": "acc>99%, p<0.001",
            "actual": f"acc={acc:.3f} p={p:.4f}", "pass": ok}


def _self_test_anti_perfect(records: list[dict]) -> dict:
    """Perfectly wrong → accuracy=0%, significant."""
    def fn(r):
        return "SELL" if r["fwd_5d_return"] > 0 else "BUY"

    res = walk_forward(records, system_fn=fn)
    acc = res["pooled_accuracy"]
    p = res["p_vs_coin"]
    ok = acc < 0.01 and p < 0.001
    return {"name": "anti_perfect", "expect": "acc<1%, p<0.001",
            "actual": f"acc={acc:.3f} p={p:.4f}", "pass": ok}


def _self_test_alpha_zero(records: list[dict]) -> dict:
    """Always-buy should have alpha ≈ 0 (matches market by construction)."""
    res = walk_forward(records, system_fn=lambda r: "BUY")
    a = res["alpha"]
    # Per-trade alpha for always-buy: each trade return = fwd_5d - spy_5d
    # Portfolio alpha should also be ≈ 0 when all picks are BUY (avg stock ≈ SPY)
    # But it won't be exactly 0 because our stock universe ≠ SPY.
    # So we just check that per_trade_p > 0.05 (not significantly different from 0).
    p = a["per_trade_p"]
    ok = p is not None and p > 0.05
    return {"name": "alpha_zero_always_buy",
            "expect": "alpha p>0.05 (not significant)",
            "actual": f"alpha_mean={a['per_trade_mean_pct']}% p={p}",
            "pass": ok}


def run_self_tests(records: list[dict]) -> list[dict]:
    return [
        _self_test_random(records),
        _self_test_always_buy(records),
        _self_test_always_sell(records),
        _self_test_perfect(records),
        _self_test_anti_perfect(records),
        _self_test_alpha_zero(records),
    ]


# ── main ───────────────────────────────────────────────────────────────────

def run() -> int:
    ap = argparse.ArgumentParser(description="Walk-forward OOS evaluation")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    records = load_records()

    if args.self_test:
        tests = run_self_tests(records)
        all_pass = all(t["pass"] for t in tests)
        if args.json:
            print(json.dumps({"tests": tests, "all_pass": all_pass}, indent=2))
        else:
            for t in tests:
                tag = "PASS" if t["pass"] else "FAIL"
                print(f"  [{tag}] {t['name']}: {t['actual']}  (expect: {t['expect']})")
            print(f"\n  All pass: {all_pass}")
        return 0 if all_pass else 1

    result = walk_forward(records)

    if args.json:
        print(json.dumps(result, indent=2))
        return 0

    # ── Human-readable output ──
    print(f"Walk-forward: {result['n_folds']} folds, {result['total_test_n']} test records")
    print(f"  Pooled accuracy:  {result['pooled_accuracy']*100:.1f}%"
          f"  CI=[{result['pooled_ci_95'][0]*100:.1f}, {result['pooled_ci_95'][1]*100:.1f}]%")
    print(f"  Market up rate:   {result['pooled_market_up_rate']*100:.1f}%")
    print(f"  Edge vs buy:      {result['pooled_edge_vs_buy_pp']:+.1f}pp")
    print(f"  Edge vs coin:     {result['pooled_edge_vs_coin_pp']:+.1f}pp")
    print(f"  z={result['z_vs_coin']:.2f}  p={result['p_vs_coin']:.4f}")

    a = result["alpha"]
    if a["per_trade_mean_pct"] is not None:
        print(f"\n  Alpha vs SPY ({a['n_folds_with_alpha']} folds):")
        print(f"    Per-trade mean: {a['per_trade_mean_pct']:+.2f}%  t={a['per_trade_t']:.2f}  p={a['per_trade_p']:.4f}")
        print(f"    Portfolio mean: {a['portfolio_mean_pct']:+.2f}%  t={a['portfolio_t']:.2f}  p={a['portfolio_p']:.4f}")

    print(f"\nBy regime:")
    for rg, info in result["by_regime"].items():
        prov = "  [PROVISIONAL n<30]" if info["provisional"] else ""
        alpha_str = ""
        if info["alpha_per_trade_mean_pct"] is not None:
            alpha_str = f"  alpha={info['alpha_per_trade_mean_pct']:+.2f}%"
        print(f"  {rg}: n={info['n']}  acc={info['accuracy']*100:.1f}%"
              f"  CI=[{info['ci_95'][0]*100:.1f},{info['ci_95'][1]*100:.1f}]%"
              f"  p={info['p_vs_coin']:.3f}{alpha_str}{prov}")

    print(f"\n  {'Date':<12} {'n':>4} {'Acc':>6} {'MktUp':>6} {'Edge':>7}"
          f" {'SPY5d':>7} {'Alpha':>7} {'p':>7}  Regime")
    print(f"  {'-'*75}")
    for f in result["folds"]:
        prov = "*" if f["n"] < PROVISIONAL_N else " "
        spy = f"{f['spy_5d_pct']:+5.2f}%" if f["spy_5d_pct"] is not None else "  n/a "
        alpha = f"{f['alpha_per_trade_pct']:+5.2f}%" if f["alpha_per_trade_pct"] is not None else "  n/a "
        print(f"  {f['test_date']:<12} {f['n']:4d} {f['accuracy']*100:5.1f}%"
              f" {f['market_up_rate']*100:5.1f}% {f['edge_vs_buy_pp']:+5.1f}pp"
              f" {spy} {alpha} {f['p_vs_coin']:6.3f}  {f['regime']}{prov}")

    return 0


if __name__ == "__main__":
    raise SystemExit(run())
