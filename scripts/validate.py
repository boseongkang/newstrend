"""
validate.py — Unified Phase 0 validation entry point.

Runs all measurement harnesses and writes a single validation.json.
GUARDRAIL 1: Read-only on predictions — measurement infrastructure only.

Usage:
    python3 scripts/validate.py                  # run all + print summary
    python3 scripts/validate.py --self-test      # run all self-tests
    python3 scripts/validate.py --json           # full JSON output
    python3 scripts/validate.py --ci             # CI mode: exit 0 always
                                                 #   (measurement, not gate)

Output: site/data/validation.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "site" / "data"
SCRIPTS = ROOT / "scripts"
OUT_FILE = DATA / "validation.json"

sys.path.insert(0, str(SCRIPTS))

from walk_forward import load_records, walk_forward, run_self_tests as wf_self_tests
from benchmark import evaluate_strategies, run_self_tests as bm_self_tests
from naive_baselines import compare_vs_baselines, run_self_tests as nb_self_tests


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _load_previous_validation() -> dict | None:
    if not OUT_FILE.exists():
        return None
    try:
        return json.loads(OUT_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return None


# ── full validation run ───────────────────────────────────────────────────

def run_validation() -> dict:
    records = load_records()

    wf = walk_forward(records)
    bm = evaluate_strategies()
    nb = compare_vs_baselines(records)

    # Verdict: summarize the key findings
    acc = wf["pooled_accuracy"]
    p = wf["p_vs_coin"]
    alpha_info = wf.get("alpha", {})
    alpha_mean = alpha_info.get("per_trade_mean_pct")
    alpha_p = alpha_info.get("per_trade_p")
    edge = nb["edge"]

    verdict_lines = []
    if p < 0.05 and acc < 0.5:
        verdict_lines.append(f"Accuracy {acc*100:.1f}% significantly below 50% (p={p:.4f})")
    elif p < 0.05 and acc > 0.5:
        verdict_lines.append(f"Accuracy {acc*100:.1f}% significantly above 50% (p={p:.4f})")
    else:
        verdict_lines.append(f"Accuracy {acc*100:.1f}% not significantly different from 50% (p={p:.4f})")

    if alpha_mean is not None and alpha_p is not None:
        if alpha_p < 0.05 and alpha_mean < 0:
            verdict_lines.append(f"Alpha {alpha_mean:+.2f}% significantly negative (p={alpha_p:.4f})")
        elif alpha_p < 0.05 and alpha_mean > 0:
            verdict_lines.append(f"Alpha {alpha_mean:+.2f}% significantly positive (p={alpha_p:.4f})")
        else:
            verdict_lines.append(f"Alpha {alpha_mean:+.2f}% not significant (p={alpha_p:.4f})")

    if edge["p_value"] < 0.05 and edge["edge_pp"] < 0:
        verdict_lines.append(f"Edge vs {edge['vs_best_baseline']}: {edge['edge_pp']:+.1f}pp (p={edge['p_value']:.4f})")

    # ── PASS/FAIL gate ──
    # Criterion: system must beat always-buy on alpha AND p < 0.05.
    # "always-buy alpha" is the always-buy system's per-trade alpha vs SPY.
    buy_res = walk_forward(records, system_fn=lambda r: "BUY")
    buy_alpha_info = buy_res.get("alpha", {})
    buy_alpha = buy_alpha_info.get("per_trade_mean_pct")

    gate_pass = False
    gate_reason = ""
    if alpha_mean is not None and buy_alpha is not None:
        alpha_edge = alpha_mean - buy_alpha
        # Test: is system alpha significantly better than buy alpha?
        # Use the system's alpha t-test directly — if system alpha > buy alpha
        # AND system alpha itself is significantly positive (p<0.05), pass.
        # More practically: system alpha > buy alpha AND system p < 0.05 AND alpha > 0
        if alpha_mean > buy_alpha and alpha_p < 0.05 and alpha_mean > 0:
            gate_pass = True
            gate_reason = (f"PASS: system alpha ({alpha_mean:+.2f}%) > "
                           f"always-buy alpha ({buy_alpha:+.2f}%) with p={alpha_p:.4f}")
        else:
            gate_reason = (f"FAIL: system alpha ({alpha_mean:+.2f}%) vs "
                           f"always-buy alpha ({buy_alpha:+.2f}%), p={alpha_p:.4f}")
    else:
        gate_reason = "FAIL: insufficient data for alpha gate"

    verdict_lines.append(f"Gate: {gate_reason}")

    # Regime coverage warning + change detection
    regime_warnings = []
    alerts = []
    for rg, info in wf.get("by_regime", {}).items():
        if info.get("provisional"):
            regime_warnings.append(f"{rg} n={info['n']} — provisional, insufficient data")

    risk_off_n = wf.get("by_regime", {}).get("RISK-OFF", {}).get("n", 0)
    risk_off_present = risk_off_n > 0
    if not risk_off_present:
        regime_warnings.append("RISK-OFF: no test data — regime robustness unverifiable")

    # Compare with previous run to detect regime change
    prev = _load_previous_validation()
    prev_risk_off_n = (prev.get("data_sufficiency", {}).get("risk_off_n", 0)
                       if prev else 0)
    if risk_off_n > 0 and prev_risk_off_n == 0:
        alerts.append("REGIME CHANGE: RISK-OFF data appeared for the first time — "
                       "regime-conditional analysis now possible")
    prev_n = prev.get("data_sufficiency", {}).get("total_test_n", 0) if prev else 0
    delta_n = wf["total_test_n"] - prev_n

    # Data sufficiency progress
    target_n = 1785  # 5x current baseline of 357
    target_min_regimes = 2
    n_regimes = len(wf.get("by_regime", {}))
    pct_n = round(wf["total_test_n"] / target_n * 100, 1) if target_n else 0
    data_ready = wf["total_test_n"] >= target_n and n_regimes >= target_min_regimes

    return {
        "updated": _now_iso(),
        "gate": {
            "pass": gate_pass,
            "criterion": "walk-forward OOS alpha vs SPY > always-buy alpha, p<0.05",
            "system_alpha_pct": alpha_mean,
            "always_buy_alpha_pct": buy_alpha,
            "system_alpha_p": alpha_p,
            "reason": gate_reason,
        },
        "walk_forward": wf,
        "benchmark": bm,
        "baselines": nb,
        "verdict": verdict_lines,
        "alerts": alerts,
        "regime_warnings": regime_warnings,
        "data_sufficiency": {
            "total_test_n": wf["total_test_n"],
            "target_n": target_n,
            "progress_pct": pct_n,
            "delta_n_since_last": delta_n,
            "n_folds": wf["n_folds"],
            "regimes_tested": list(wf.get("by_regime", {}).keys()),
            "regimes_needed": target_min_regimes,
            "risk_off_n": risk_off_n,
            "data_ready": data_ready,
        },
        "model_freeze": {
            "status": "FROZEN",
            "reason": "Gate not passed — no model changes until data_ready=true "
                      "and a hypothesis passes the gate via system_fn injection",
        },
    }


# ── self-tests ─────────────────────────────────────────────────────────────

def run_all_self_tests() -> dict:
    records = load_records()
    results = {
        "walk_forward": wf_self_tests(records),
        "benchmark": bm_self_tests(),
        "naive_baselines": nb_self_tests(records),
    }
    all_pass = all(
        t["pass"]
        for group in results.values()
        for t in group
    )
    return {"tests": results, "all_pass": all_pass}


# ── print helpers ──────────────────────────────────────────────────────────

def print_summary(v: dict) -> None:
    wf = v["walk_forward"]
    bm = v["benchmark"]
    nb = v["baselines"]

    print("=" * 70)
    print("PHASE 0 VALIDATION REPORT")
    print("=" * 70)

    # Walk-forward
    print(f"\n1. WALK-FORWARD OOS ({wf['n_folds']} folds, {wf['total_test_n']} records)")
    print(f"   Accuracy:  {wf['pooled_accuracy']*100:.1f}%  "
          f"CI=[{wf['pooled_ci_95'][0]*100:.1f}, {wf['pooled_ci_95'][1]*100:.1f}]%")
    print(f"   vs coin:   {wf['pooled_edge_vs_coin_pp']:+.1f}pp  "
          f"z={wf['z_vs_coin']:.2f}  p={wf['p_vs_coin']:.4f}")
    print(f"   vs buy:    {wf['pooled_edge_vs_buy_pp']:+.1f}pp")

    a = wf.get("alpha", {})
    if a.get("per_trade_mean_pct") is not None:
        print(f"\n   Alpha vs SPY (per-trade): {a['per_trade_mean_pct']:+.2f}%"
              f"  t={a['per_trade_t']:.2f}  p={a['per_trade_p']:.4f}")
        print(f"   Alpha vs SPY (portfolio): {a['portfolio_mean_pct']:+.2f}%"
              f"  t={a['portfolio_t']:.2f}  p={a['portfolio_p']:.4f}")

    # Fold detail
    print(f"\n   {'Date':<12} {'n':>4} {'Acc':>6} {'SPY5d':>7} {'Alpha':>7}  Regime")
    print(f"   {'-'*55}")
    for f in wf["folds"]:
        prov = "*" if f["n"] < 30 else " "
        spy = f"{f['spy_5d_pct']:+5.2f}%" if f.get("spy_5d_pct") is not None else "  n/a "
        alpha = f"{f['alpha_per_trade_pct']:+5.2f}%" if f.get("alpha_per_trade_pct") is not None else "  n/a "
        print(f"   {f['test_date']:<12} {f['n']:4d} {f['accuracy']*100:5.1f}%"
              f" {spy} {alpha}  {f['regime']}{prov}")

    # Benchmark
    print(f"\n2. PAPER TRADING ALPHA")
    for name, s in bm["strategies"].items():
        bms = "  ".join(f"{k}={info['alpha_pct']:+.2f}%"
                        for k, info in s["benchmarks"].items())
        print(f"   {name}: ret={s['return_pct']:+.2f}%  {bms}"
              f"  ({s['avg_invested_pct']}% invested)")

    # Baselines
    print(f"\n3. VS NAIVE BASELINES (walk-forward OOS)")
    e = nb["edge"]
    s_info = nb["system"]
    print(f"   System:      {s_info['pooled_accuracy']*100:.1f}%")
    for bname, binfo in nb["baselines"].items():
        print(f"   {bname:20s} {binfo['pooled_accuracy']*100:5.1f}%")
    print(f"   Edge vs {e['vs_best_baseline']}: {e['edge_pp']:+.1f}pp  "
          f"z={e['z']:.2f}  p={e['p_value']:.4f}")

    # Regime
    print(f"\n4. REGIME COVERAGE")
    for rg, info in wf.get("by_regime", {}).items():
        prov = " [PROVISIONAL]" if info.get("provisional") else ""
        a_str = ""
        if info.get("alpha_per_trade_mean_pct") is not None:
            a_str = f"  alpha={info['alpha_per_trade_mean_pct']:+.2f}%"
        print(f"   {rg}: n={info['n']}  acc={info['accuracy']*100:.1f}%{a_str}{prov}")
    for w in v.get("regime_warnings", []):
        print(f"   WARNING: {w}")

    # Gate
    gate = v.get("gate", {})
    print(f"\n5. ADOPTION GATE")
    status = "PASS" if gate.get("pass") else "FAIL"
    print(f"   [{status}] {gate.get('reason', 'n/a')}")
    print(f"   Criterion: {gate.get('criterion', 'n/a')}")

    # Data sufficiency
    ds = v.get("data_sufficiency", {})
    print(f"\n6. DATA SUFFICIENCY")
    print(f"   Test records: {ds.get('total_test_n', '?')}/{ds.get('target_n', '?')}"
          f"  ({ds.get('progress_pct', '?')}%)")
    delta = ds.get("delta_n_since_last", 0)
    if delta:
        print(f"   Delta since last run: +{delta}")
    print(f"   Regimes tested: {ds.get('regimes_tested', [])} "
          f"(need {ds.get('regimes_needed', '?')})")
    print(f"   RISK-OFF records: {ds.get('risk_off_n', 0)}")
    print(f"   Data ready: {ds.get('data_ready', False)}")

    # Model freeze
    mf = v.get("model_freeze", {})
    print(f"\n7. MODEL STATUS: {mf.get('status', '?')}")
    print(f"   {mf.get('reason', '')}")

    # Alerts
    alerts = v.get("alerts", [])
    if alerts:
        print(f"\n   ALERTS:")
        for a in alerts:
            print(f"   >>> {a}")

    # Verdict
    print(f"\n8. VERDICT")
    for line in v.get("verdict", []):
        print(f"   {line}")

    print()


# ── main ───────────────────────────────────────────────────────────────────

def run() -> int:
    ap = argparse.ArgumentParser(description="Unified Phase 0 validation")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--ci", action="store_true",
                    help="CI mode: always exit 0 (measurement, not gate)")
    args = ap.parse_args()

    if args.self_test:
        result = run_all_self_tests()
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            for group_name, tests in result["tests"].items():
                print(f"\n  {group_name}:")
                for t in tests:
                    tag = "PASS" if t["pass"] else "FAIL"
                    print(f"    [{tag}] {t['name']}: {t['actual']}")
            print(f"\n  All pass: {result['all_pass']}")
        return 0 if result["all_pass"] else 1

    validation = run_validation()

    OUT_FILE.write_text(json.dumps(validation, indent=2, default=str))

    if args.json:
        print(json.dumps(validation, indent=2, default=str))
    else:
        print_summary(validation)
        print(f"Wrote {OUT_FILE}")

    return 0 if args.ci else 0


if __name__ == "__main__":
    raise SystemExit(run())
