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

from walk_forward import load_records, walk_forward, nw_t_test, run_self_tests as wf_self_tests
from benchmark import evaluate_strategies, run_self_tests as bm_self_tests
from naive_baselines import compare_vs_baselines, run_self_tests as nb_self_tests


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _gate_v2_fold_diffs(records: list[dict]) -> tuple[list[float], int, int]:
    """Adoption Gate v2 — 매칭 페어 (docs/gate_v2_definition.md).

    정의는 적용 전에 동결됨 (해당 문서 참조). 요약: 각 dedup 의사결정
    (BUY/WATCH=+1, SELL/REDUCE=−1, HOLD 제외)에 대해 같은 anchor·같은
    sector의 전체 레코드(HOLD 포함, ex-self) 동일가중 바스켓을 페어로 붙여
    d_i = s_i·r_i − r̄_basket. 섹터 ex-self 표본 <5면 유니버스 ex-self로
    fallback. anchor date별 평균 → fold 시계열 반환 (pp 단위).
    """
    by_anchor: dict[str, list[dict]] = {}
    for r in records:
        a = r.get("fwd_5d_anchor_date")
        if a is not None and r.get("fwd_5d_return") is not None:
            by_anchor.setdefault(a, []).append(r)

    fold_ds: dict[str, float] = {}
    n_pairs = 0
    n_sector_basket = 0
    for a, recs in by_anchor.items():
        ds = []
        for r in recs:
            act = r.get("action")
            if act in ("BUY", "WATCH"):
                s = 1.0
            elif act in ("SELL", "REDUCE"):
                s = -1.0
            else:
                continue
            sec = r.get("sector")
            peers = ([x["fwd_5d_return"] for x in recs
                      if x is not r and x.get("sector") == sec]
                     if sec else [])
            if len(peers) >= 5:
                basket = sum(peers) / len(peers)
                n_sector_basket += 1
            else:
                uni = [x["fwd_5d_return"] for x in recs if x is not r]
                if not uni:
                    continue
                basket = sum(uni) / len(uni)
            ds.append((s * r["fwd_5d_return"] - basket) * 100)
        if ds:
            fold_ds[a] = sum(ds) / len(ds)
            n_pairs += len(ds)
    return [fold_ds[d] for d in sorted(fold_ds)], n_pairs, n_sector_basket


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

    # Verdict: summarize the key findings. Accuracy inference uses the
    # Newey-West fold-level test (STEP 3) — the pooled-record z remains in
    # walk_forward output but is anti-conservative under 5-day overlap.
    acc = wf["pooled_accuracy"]
    p = wf.get("accuracy_nw", {}).get("p_vs_coin", wf["p_vs_coin"])
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
    # Criterion (STEP 3, 2026-08-05): paired per-fold difference
    # (system alpha − always-buy alpha, matched by test_date) must be
    # positive with Newey-West p < 0.05, AND system alpha itself > 0.
    # Both series are evaluated on the same records and share the SPY leg,
    # so the old substitute — testing the system's own H0:alpha=0 — used
    # the wrong SE for the question the gate asks.
    buy_res = walk_forward(records, system_fn=lambda r: "BUY")
    buy_alpha_info = buy_res.get("alpha", {})
    buy_alpha = buy_alpha_info.get("per_trade_mean_pct")

    sys_by_date = {f["test_date"]: f["alpha_per_trade_pct"]
                   for f in wf.get("folds", [])
                   if f.get("alpha_per_trade_pct") is not None}
    buy_by_date = {f["test_date"]: f["alpha_per_trade_pct"]
                   for f in buy_res.get("folds", [])
                   if f.get("alpha_per_trade_pct") is not None}
    common_dates = sorted(set(sys_by_date) & set(buy_by_date))
    diffs = [sys_by_date[d] - buy_by_date[d] for d in common_dates]

    gate_pass = False
    gate_reason = ""
    diff_mean = diff_t = diff_p = None
    if alpha_mean is not None and buy_alpha is not None and len(diffs) >= 2:
        diff_mean, diff_t, diff_p = nw_t_test(diffs)
        if diff_mean > 0 and diff_p < 0.05 and alpha_mean > 0:
            gate_pass = True
            gate_reason = (f"PASS: paired (system − always-buy) alpha "
                           f"{diff_mean:+.2f}pp, NW p={diff_p:.4f} "
                           f"(system {alpha_mean:+.2f}%, buy {buy_alpha:+.2f}%)")
        else:
            gate_reason = (f"FAIL: paired (system − always-buy) alpha "
                           f"{diff_mean:+.2f}pp, NW p={diff_p:.4f} "
                           f"(system {alpha_mean:+.2f}%, buy {buy_alpha:+.2f}%)")
    else:
        gate_reason = "FAIL: insufficient data for alpha gate"

    # ── Gate v2 — FAILED redesign, 참조용 병행 보고 (2026-08-06 원복).
    # 정의는 적용 전 동결됐으나(aff11ec6d) 정밀도 개선에 실패: NW sd
    # 6.19→7.77pp (short 페어에서 d=-(r+basket)로 공통성분이 2배 진입하는
    # 대수 결함 — docs/gate_v2_definition.md 결과 절 참조). 양쪽 판정이
    # FAIL로 동일해 v1 원복은 결과 기반 선택이 아니라 정밀도 근거
    # (checkpoint_registration.json GATE-V2-REVERT 항목).
    v2_diffs, v2_n_pairs, v2_n_sector = _gate_v2_fold_diffs(records)
    v2_pass = False
    v2_mean = v2_t = v2_p = None
    if len(v2_diffs) >= 2:
        v2_mean, v2_t, v2_p = nw_t_test(v2_diffs)
        v2_pass = bool(v2_mean > 0 and v2_p < 0.05)
        v2_reason = (f"{'PASS' if v2_pass else 'FAIL'}: sector-matched paired "
                     f"alpha {v2_mean:+.2f}pp, NW p={v2_p:.4f} "
                     f"({v2_n_pairs} pairs, {len(v2_diffs)} folds)")
    else:
        v2_reason = "FAIL: insufficient data for gate v2"

    verdict_lines.append(f"Gate: {gate_reason}")
    verdict_lines.append(f"Gate v2 (failed redesign, reference): {v2_reason}")

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

    # Calibrator-taint marker (2026-08-05 audit). pillar_weights.json was
    # mode="active" with multipliers deviating from 1.0 between 2026-05-15
    # and 2026-08-05 (demoted to shadow in commit 515e58178). Snapshots in
    # that window were produced by a daily-drifting scorer, so pooled
    # walk-forward stats over it measure a changing system, not the frozen
    # predict.py. Measured impact on outputs was small (same-day A/B on
    # 2026-08-05: 0 action flips, max confidence delta 0.001), but folds in
    # the window are formally not fixed-system forward results.
    TAINT_START, TAINT_END = "2026-05-16", "2026-08-05"
    tainted_folds = [f for f in wf.get("folds", [])
                     if TAINT_START <= f.get("test_date", "") <= TAINT_END]
    taint = {
        "calibrator_live_window": [TAINT_START, TAINT_END],
        "cause": "pillar_weights.json mode=active fed in-sample EMA multipliers "
                 "into predict.py daily (demoted to shadow 2026-08-05, "
                 "commit 515e58178)",
        "n_folds_affected": len(tainted_folds),
        "n_records_affected": sum(f.get("n", 0) for f in tainted_folds),
        "note": "folds with test_date in this window are not fixed-system "
                "forward results; measured behavioural drift was small "
                "(0 action flips, max conf delta 0.001 in same-day A/B)",
    }

    return {
        "updated": _now_iso(),
        "taint": taint,
        "gate": {
            "version": "v1",
            "pass": gate_pass,
            "criterion": ("paired per-fold (system − always-buy) alpha > 0 "
                          "with Newey-West p<0.05, and system alpha > 0"),
            "system_alpha_pct": alpha_mean,
            "always_buy_alpha_pct": buy_alpha,
            "system_alpha_p": alpha_p,
            "paired_diff_pp": diff_mean,
            "paired_t": diff_t,
            "paired_p": diff_p,
            "paired_n_folds": len(diffs),
            "reason": gate_reason,
        },
        "gate_v2_failed": {
            "note": ("FAILED redesign (2026-08-06) — 정밀도 개선 실패로 원복. "
                     "docs/gate_v2_definition.md 결과 절 + "
                     "checkpoint_registration.json GATE-V2 항목 참조. "
                     "판정은 v1과 동일(FAIL)이므로 원복은 정밀도 근거."),
            "definition": "docs/gate_v2_definition.md (frozen aff11ec6d)",
            "pass": v2_pass,
            "paired_diff_pp": v2_mean,
            "paired_t": v2_t,
            "paired_p": v2_p,
            "paired_n_folds": len(v2_diffs),
            "n_pairs": v2_n_pairs,
            "n_sector_basket_pairs": v2_n_sector,
            "reason": v2_reason,
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
