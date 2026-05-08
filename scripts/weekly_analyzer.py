"""
weekly_analyzer.py — synthesize the week's outcomes and propose
the next week's improvement plan.

Reads the artifacts the daily pipeline already produces:
  - site/data/paper_trading_history.json   (trades, equity)
  - site/data/prediction_accuracy.json     (per-prediction accuracy)
  - site/data/gap_analysis.json            (gap by every dimension)
  - site/data/gap_history.json             (weekly gap trend)
  - site/data/calibration_history.json     (what calibrator changed)

Produces:
  - site/data/weekly_analysis.json         (latest week's report)
  - site/data/weekly_history.json          (append-only weekly log)

Each weekly entry contains:
  - summary: trade count, win rate, gap, directional accuracy
  - top_issues: ranked list of largest absolute gaps
  - plan: actionable suggestions derived from those issues
  - validation: did last week's plan work? (gap delta vs prior week)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "site" / "data"
OUT_LATEST = DATA / "weekly_analysis.json"
OUT_HISTORY = DATA / "weekly_history.json"

# Heuristic thresholds for issue surfacing
ISSUE_GAP_THRESHOLD_PCT = 5.0       # |gap| above this → flagged
ISSUE_DIR_ACC_THRESHOLD = 0.40      # below this → flagged
TOP_K_ISSUES = 5


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _isoweek(d: str) -> str:
    dt = datetime.fromisoformat(d).date()
    yr, wk, _ = dt.isocalendar()
    return f"{yr}-W{wk:02d}"


def _load(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}


def _gap_summary_for_week(gap: dict, target_week: str | None = None) -> dict:
    """Pull the latest week (or named week) from gap convergence series."""
    weeks = ((gap.get("convergence") or {}).get("weeks") or [])
    if not weeks:
        return {}
    chosen = next((w for w in reversed(weeks) if w["week"] == target_week), None) if target_week else weeks[-1]
    if chosen is None:
        return {}
    return chosen


def _identify_issues(gap: dict) -> list[dict]:
    """Find the most divergent buckets across all gap aggregations."""
    issues: list[dict] = []
    aggs = gap.get("aggregations") or {}
    for dim, mapping in aggs.items():
        for bucket, info in mapping.items():
            n = info.get("n", 0)
            abs_gap = info.get("avg_abs_gap_pct")
            dir_acc = info.get("directional_acc")
            if n < 2 or abs_gap is None:
                continue
            severity = 0.0
            why = []
            if abs_gap >= ISSUE_GAP_THRESHOLD_PCT:
                severity += abs_gap
                why.append(f"|gap|={abs_gap:.2f}%")
            if dir_acc is not None and dir_acc <= ISSUE_DIR_ACC_THRESHOLD:
                severity += (0.5 - dir_acc) * 20  # weight directional miss
                why.append(f"dir_acc={dir_acc*100:.0f}%")
            if severity > 0:
                issues.append({
                    "dimension": dim,
                    "bucket": bucket,
                    "n": n,
                    "avg_abs_gap_pct": abs_gap,
                    "avg_signed_gap_pct": info.get("avg_signed_gap_pct"),
                    "directional_acc": dir_acc,
                    "severity": round(severity, 3),
                    "reason": " · ".join(why),
                })
    issues.sort(key=lambda i: -i["severity"])
    return issues[:TOP_K_ISSUES]


def _build_plan(issues: list[dict], gap: dict) -> list[dict]:
    """Translate top issues into concrete suggestions."""
    plan: list[dict] = []
    seen_dims: set[str] = set()
    for iss in issues:
        dim = iss["dimension"]
        bucket = iss["bucket"]
        n = iss["n"]
        signed = iss.get("avg_signed_gap_pct")
        direction = "predicts higher than reality" if (signed or 0) > 0 else "predicts lower than reality"
        if dim == "by_action":
            plan.append({
                "id": f"action_{bucket}",
                "target": f"{bucket} action calibration",
                "issue": f"|gap|={iss['avg_abs_gap_pct']:.2f}% (n={n}); system {direction}.",
                "suggestion": (
                    f"Tighten {bucket} entry rules — require higher confidence floor or regime gate."
                    if bucket in ("BUY", "SELL", "WATCH")
                    else f"Re-check {bucket} action mapping to predicted_return heuristic."
                ),
                "severity": iss["severity"],
            })
        elif dim == "by_confidence":
            plan.append({
                "id": f"conf_{bucket}",
                "target": f"confidence band {bucket}",
                "issue": f"|gap|={iss['avg_abs_gap_pct']:.2f}% in this band (n={n}).",
                "suggestion": "Add confidence-conditional position sizing — smaller bets in this band.",
                "severity": iss["severity"],
            })
        elif dim == "by_regime":
            plan.append({
                "id": f"regime_{bucket}",
                "target": f"regime {bucket}",
                "issue": f"|gap|={iss['avg_abs_gap_pct']:.2f}% under this regime (n={n}); {direction}.",
                "suggestion": f"Add a regime-aware filter that suppresses opposite-direction signals when regime={bucket}.",
                "severity": iss["severity"],
            })
        elif dim == "by_volatility":
            plan.append({
                "id": f"vol_{bucket}",
                "target": f"volatility tier {bucket}",
                "issue": f"|gap|={iss['avg_abs_gap_pct']:.2f}% at this vol level (n={n}).",
                "suggestion": f"Tighten stop-loss / shrink position when HV20 is in '{bucket}' tier.",
                "severity": iss["severity"],
            })
        elif dim == "by_sector":
            plan.append({
                "id": f"sector_{bucket}",
                "target": f"sector {bucket}",
                "issue": f"|gap|={iss['avg_abs_gap_pct']:.2f}% across this sector (n={n}).",
                "suggestion": f"Audit the pillar that's largest contributor to {bucket}-sector calls; defer untested signals.",
                "severity": iss["severity"],
            })
        else:
            plan.append({
                "id": f"{dim}_{bucket}",
                "target": f"{dim} → {bucket}",
                "issue": iss["reason"],
                "suggestion": "Investigate this bucket; review recent predictions for systematic miss.",
                "severity": iss["severity"],
            })
        seen_dims.add(dim)
    # Cross-cutting structural plan items
    grads = gap.get("gradients") or {}
    actionable_grads = [(k, v["delta"]) for k, v in grads.items() if v.get("delta") is not None and abs(v["delta"]) > 0.01]
    if actionable_grads:
        actionable_grads.sort(key=lambda x: -abs(x[1]))
        top_pillar, top_delta = actionable_grads[0]
        plan.append({
            "id": f"pillar_grad_{top_pillar}",
            "target": f"pillar weight: {top_pillar}",
            "issue": f"Gap gradient suggests Δweight={top_delta:+.3f}.",
            "suggestion": (
                f"Calibrator will absorb this signal automatically; verify after next run "
                f"that {top_pillar} EMA weight has moved in the suggested direction."
            ),
            "severity": abs(top_delta) * 5,
        })
    return plan


def _validate_last_week(gap_history: dict, gap_now: dict) -> dict:
    """Compare this week's gap vs last week's plan."""
    weeks = ((gap_now.get("convergence") or {}).get("weeks") or [])
    if len(weeks) < 2:
        return {"available": False, "reason": "fewer_than_two_weeks_of_data"}
    this_w = weeks[-1]
    last_w = weeks[-2]
    delta_abs = round(this_w["avg_abs_gap_pct"] - last_w["avg_abs_gap_pct"], 3)
    delta_signed = round(this_w["avg_signed_gap_pct"] - last_w["avg_signed_gap_pct"], 3)
    direction = "improved" if delta_abs < -0.1 else "worsened" if delta_abs > 0.1 else "flat"
    return {
        "available": True,
        "this_week": this_w,
        "last_week": last_w,
        "delta_abs_gap_pct": delta_abs,
        "delta_signed_gap_pct": delta_signed,
        "direction": direction,
        "improvement_pct": (
            round((last_w["avg_abs_gap_pct"] - this_w["avg_abs_gap_pct"]) /
                  last_w["avg_abs_gap_pct"] * 100, 2)
            if last_w["avg_abs_gap_pct"] else None
        ),
    }


def _trade_summary(paper: dict) -> dict:
    """Latest equity-curve and trade-count summary across strategies."""
    out: dict = {"strategies": {}}
    for name, st in (paper.get("strategies") or {}).items():
        m = st.get("metrics") or {}
        out["strategies"][name] = {
            "total_return_pct": m.get("total_return_pct"),
            "win_rate": m.get("win_rate"),
            "n_trades_total": m.get("n_trades_total"),
            "n_open": m.get("n_open"),
            "n_completed": m.get("n_completed"),
            "current_value": m.get("current_value"),
        }
    return out


def run() -> None:
    paper = _load(DATA / "paper_trading_history.json")
    gap = _load(DATA / "gap_analysis.json")
    gap_hist = _load(DATA / "gap_history.json")
    accuracy = _load(DATA / "prediction_accuracy.json")
    calib_hist = _load(DATA / "calibration_history.json")

    if not gap:
        raise SystemExit("Missing gap_analysis.json — run gap_analyzer.py first")

    week = _gap_summary_for_week(gap)  # latest
    issues = _identify_issues(gap)
    plan = _build_plan(issues, gap)
    validation = _validate_last_week(gap_hist, gap)

    payload = {
        "updated": _now_iso(),
        "week": week.get("week"),
        "week_summary": week,
        "trade_summary": _trade_summary(paper),
        "accuracy_summary": {
            "n_actionable": accuracy.get("n_actionable_5d"),
            "by_action": ((accuracy.get("aggregations") or {}).get("by_action") or {}),
        },
        "gap_summary": gap.get("summary"),
        "top_issues": issues,
        "improvement_plan": plan,
        "last_week_validation": validation,
        "calibration_runs_total": len((calib_hist.get("runs") or [])),
    }

    OUT_LATEST.write_text(json.dumps(payload, indent=2, default=str))

    # Append-only history (one entry per ISO week, replace if same week)
    history: dict
    if OUT_HISTORY.exists():
        history = json.loads(OUT_HISTORY.read_text())
        if "weeks" not in history:
            history = {"weeks": []}
    else:
        history = {"weeks": []}
    history["weeks"] = [w for w in history["weeks"] if w.get("week") != payload["week"]]
    history["weeks"].append(payload)
    history["weeks"].sort(key=lambda w: w.get("week") or "")
    history["updated"] = payload["updated"]
    OUT_HISTORY.write_text(json.dumps(history, indent=2, default=str))

    # Stdout digest
    print(f"Wrote {OUT_LATEST}")
    print(f"  week: {payload['week']}  history weeks: {len(history['weeks'])}")
    if week:
        print(f"  n={week.get('n')}  avg|gap|={week.get('avg_abs_gap_pct')}%  "
              f"signed={week.get('avg_signed_gap_pct')}%  dir_acc={week.get('directional_acc')}")
    print(f"\ntop issues ({len(issues)}):")
    for i, iss in enumerate(issues, 1):
        print(f"  {i}. {iss['dimension']:14s}/{iss['bucket']:14s}  sev={iss['severity']:.2f}  ({iss['reason']})")
    print(f"\nplan ({len(plan)}):")
    for i, item in enumerate(plan, 1):
        print(f"  {i}. {item['target']}: {item['suggestion']}")
    if validation.get("available"):
        v = validation
        marker = "✓" if v["direction"] == "improved" else "✗" if v["direction"] == "worsened" else "·"
        print(f"\nlast-week validation: {marker} {v['direction']}  "
              f"Δ|gap|={v['delta_abs_gap_pct']}pp  "
              f"({v['last_week']['week']} → {v['this_week']['week']})")


if __name__ == "__main__":
    run()
