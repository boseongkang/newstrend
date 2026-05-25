"""
naive_baselines.py — Always-buy / random / always-hold baselines.

GUARDRAIL 1: Read-only on predictions. No model changes.
GUARDRAIL 2: Uses walk_forward engine — all metrics are OOS (test-fold only).
GUARDRAIL 3: Self-test verifies edge=0 when system IS the baseline.

Every evaluation accompanies the real system with baselines.
edge = system_metric - best_baseline_metric, with p-value.

Usage:
    python3 scripts/naive_baselines.py                # human-readable
    python3 scripts/naive_baselines.py --self-test    # self-tests
    python3 scripts/naive_baselines.py --json         # JSON output
"""

from __future__ import annotations

import argparse
import json
import math
import random
from pathlib import Path

from walk_forward import (
    load_records, walk_forward, proportion_z, wilson_ci,
    mean_t_test, PROVISIONAL_N,
)

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "site" / "data"


# ── baseline system functions ──────────────────────────────────────────────

def _always_buy(_r: dict) -> str:
    return "BUY"


def _always_sell(_r: dict) -> str:
    return "SELL"


def _random_system(seed: int = 0):
    rng = random.Random(seed)
    def fn(_r: dict) -> str:
        return rng.choice(["BUY", "SELL"])
    return fn


# ── comparison engine ──────────────────────────────────────────────────────

def compare_vs_baselines(records: list[dict]) -> dict:
    """Run the real system and all baselines through walk-forward,
    then compute edge = system - best_baseline for each metric."""

    system = walk_forward(records, system_fn=None)
    baselines = {
        "always_buy": walk_forward(records, system_fn=_always_buy),
        "always_sell": walk_forward(records, system_fn=_always_sell),
        "random_42": walk_forward(records, system_fn=_random_system(42)),
        "random_99": walk_forward(records, system_fn=_random_system(99)),
    }

    sys_acc = system["pooled_accuracy"]
    sys_alpha = (system["alpha"] or {}).get("per_trade_mean_pct")

    # Best baseline accuracy
    bl_accs = {k: v["pooled_accuracy"] for k, v in baselines.items()}
    best_bl_name = max(bl_accs, key=bl_accs.get)
    best_bl_acc = bl_accs[best_bl_name]

    edge_pp = round((sys_acc - best_bl_acc) * 100, 2)
    z_edge, p_edge = proportion_z(
        system["total_test_n"] * int(round(sys_acc * 1000)) // 1000,
        system["total_test_n"],
        best_bl_acc,
    )
    # More precise: recompute from exact counts
    sys_correct = sum(f["n_correct"] for f in system["folds"])
    total_n = system["total_test_n"]
    se = math.sqrt(best_bl_acc * (1 - best_bl_acc) / total_n) if total_n > 0 and 0 < best_bl_acc < 1 else 1
    z_edge = round((sys_acc - best_bl_acc) / se, 4) if se > 0 else 0
    p_edge = round(2 * (1 - 0.5 * (1 + math.erf(abs(z_edge) / math.sqrt(2)))), 4)

    # Regime breakdown
    regime_comparison = {}
    for rg, rg_info in system.get("by_regime", {}).items():
        bl_regime_accs = {}
        for bname, bres in baselines.items():
            br = bres.get("by_regime", {}).get(rg)
            if br:
                bl_regime_accs[bname] = br["accuracy"]
        best_rg_bl = max(bl_regime_accs, key=bl_regime_accs.get) if bl_regime_accs else None
        best_rg_acc = bl_regime_accs.get(best_rg_bl, 0.5) if best_rg_bl else 0.5
        regime_comparison[rg] = {
            "system_accuracy": rg_info["accuracy"],
            "best_baseline": best_rg_bl,
            "best_baseline_accuracy": best_rg_acc,
            "edge_pp": round((rg_info["accuracy"] - best_rg_acc) * 100, 2),
            "provisional": rg_info.get("provisional", False),
            "n": rg_info["n"],
        }

    return {
        "system": {
            "pooled_accuracy": sys_acc,
            "pooled_ci_95": system["pooled_ci_95"],
            "alpha_per_trade_pct": sys_alpha,
            "total_test_n": total_n,
            "n_folds": system["n_folds"],
        },
        "baselines": {
            k: {
                "pooled_accuracy": v["pooled_accuracy"],
                "alpha_per_trade_pct": (v.get("alpha") or {}).get("per_trade_mean_pct"),
            }
            for k, v in baselines.items()
        },
        "edge": {
            "vs_best_baseline": best_bl_name,
            "best_baseline_accuracy": round(best_bl_acc, 4),
            "edge_pp": edge_pp,
            "z": z_edge,
            "p_value": p_edge,
        },
        "by_regime": regime_comparison,
    }


# ── self-tests ─────────────────────────────────────────────────────────────

def _self_test_edge_zero(records: list[dict]) -> dict:
    """When system IS always-buy, edge vs always-buy = 0."""
    sys_res = walk_forward(records, system_fn=_always_buy)
    bl_res = walk_forward(records, system_fn=_always_buy)
    edge = round((sys_res["pooled_accuracy"] - bl_res["pooled_accuracy"]) * 100, 2)
    ok = abs(edge) < 0.01
    return {"name": "edge_zero_when_same", "expect": "edge=0, p>0.05",
            "actual": f"edge={edge:+.2f}pp", "pass": ok}


def _self_test_perfect_beats_all(records: list[dict]) -> dict:
    """Perfect system should have positive edge vs all baselines."""
    def perfect(r):
        return "BUY" if r["fwd_5d_return"] > 0 else "SELL"

    perf = walk_forward(records, system_fn=perfect)
    buy = walk_forward(records, system_fn=_always_buy)
    sell = walk_forward(records, system_fn=_always_sell)

    edge_buy = perf["pooled_accuracy"] - buy["pooled_accuracy"]
    edge_sell = perf["pooled_accuracy"] - sell["pooled_accuracy"]
    ok = edge_buy > 0 and edge_sell > 0
    return {"name": "perfect_beats_all",
            "expect": "positive edge vs all baselines",
            "actual": f"vs_buy={edge_buy*100:+.1f}pp vs_sell={edge_sell*100:+.1f}pp",
            "pass": ok}


def _self_test_p_value_not_significant(records: list[dict]) -> dict:
    """Random system vs always-buy should not be significant."""
    rng_res = walk_forward(records, system_fn=_random_system(42))
    buy_res = walk_forward(records, system_fn=_always_buy)
    diff = rng_res["pooled_accuracy"] - buy_res["pooled_accuracy"]
    n = rng_res["total_test_n"]
    bl_acc = buy_res["pooled_accuracy"]
    se = math.sqrt(bl_acc * (1 - bl_acc) / n) if n > 0 and 0 < bl_acc < 1 else 1
    z = diff / se if se > 0 else 0
    p = 2 * (1 - 0.5 * (1 + math.erf(abs(z) / math.sqrt(2))))
    ok = p > 0.05
    return {"name": "random_vs_buy_not_significant",
            "expect": "p>0.05",
            "actual": f"diff={diff*100:+.1f}pp p={p:.3f}", "pass": ok}


def run_self_tests(records: list[dict]) -> list[dict]:
    return [
        _self_test_edge_zero(records),
        _self_test_perfect_beats_all(records),
        _self_test_p_value_not_significant(records),
    ]


# ── main ───────────────────────────────────────────────────────────────────

def run() -> int:
    ap = argparse.ArgumentParser(description="Naive baselines comparison")
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

    result = compare_vs_baselines(records)

    if args.json:
        print(json.dumps(result, indent=2))
        return 0

    s = result["system"]
    e = result["edge"]
    print(f"System: acc={s['pooled_accuracy']*100:.1f}%"
          f"  CI=[{s['pooled_ci_95'][0]*100:.1f},{s['pooled_ci_95'][1]*100:.1f}]%"
          f"  alpha={s['alpha_per_trade_pct']:+.2f}%" if s["alpha_per_trade_pct"] is not None else
          f"System: acc={s['pooled_accuracy']*100:.1f}%"
          f"  CI=[{s['pooled_ci_95'][0]*100:.1f},{s['pooled_ci_95'][1]*100:.1f}]%")
    print()
    print("Baselines (walk-forward OOS):")
    for bname, binfo in result["baselines"].items():
        a_str = f"  alpha={binfo['alpha_per_trade_pct']:+.2f}%" if binfo.get("alpha_per_trade_pct") is not None else ""
        print(f"  {bname:20s} acc={binfo['pooled_accuracy']*100:5.1f}%{a_str}")

    print(f"\nEdge vs best baseline ({e['vs_best_baseline']}):")
    print(f"  baseline acc={e['best_baseline_accuracy']*100:.1f}%  "
          f"edge={e['edge_pp']:+.1f}pp  z={e['z']:.2f}  p={e['p_value']:.4f}")

    print(f"\nBy regime:")
    for rg, info in result["by_regime"].items():
        prov = "  [PROVISIONAL]" if info["provisional"] else ""
        print(f"  {rg}: n={info['n']}  sys={info['system_accuracy']*100:.1f}%"
              f"  best_bl={info['best_baseline']}({info['best_baseline_accuracy']*100:.1f}%)"
              f"  edge={info['edge_pp']:+.1f}pp{prov}")

    return 0


if __name__ == "__main__":
    raise SystemExit(run())
