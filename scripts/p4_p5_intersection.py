"""
p4_p5_intersection.py — Pillar 4 ∩ Pillar 5 cross-tab.

P4 (fundamentals) and P5 (insider Form 4) are orthogonal pillars:
  P4 measures *what the company is* (quality / growth / health).
  P5 measures *what insiders are doing* (open-market buys, asymmetric).

Tickers strong on both = highest-conviction picks distinct from Hidden Gems
(which weights P4 0.40 / P3 0.25 / P1 0.25 / P2 0.10 / × obscurity).

Output:
  - Console: 4-quadrant table + top-N conviction list
  - JSON:    site/data/p4_p5_intersection.json
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from math import sqrt
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from fundamentals_analyzer import score_universe  # noqa: E402

INSIDER_PATH = ROOT / "site" / "data" / "insider.json"
OUT_PATH = ROOT / "site" / "data" / "p4_p5_intersection.json"

# Quadrant thresholds. P5 floor is 0.5 (P>0 → ≥0.5 by design),
# so 0.5 is the natural "neutral / RSU" cut.
P4_HIGH = 0.60
P5_HIGH = 0.55  # any meaningful open-market buy lifts above 0.5


def load_p4() -> dict[str, dict]:
    rows = score_universe()
    return {
        r["ticker"]: r
        for r in rows
        if r.get("fundamental_score") is not None
    }


def load_p5() -> dict[str, dict]:
    data = json.loads(INSIDER_PATH.read_text())
    return {
        tk: v
        for tk, v in data["tickers"].items()
        if v.get("score") is not None
    }


def quadrant(p4: float, p5: float) -> str:
    hi4, hi5 = p4 >= P4_HIGH, p5 >= P5_HIGH
    if hi4 and hi5:
        return "HIGH_HIGH"  # top conviction
    if hi4 and not hi5:
        return "HIGH_LOW"  # quality, no insider signal
    if not hi4 and hi5:
        return "LOW_HIGH"  # insider catalyst, weak fundamentals
    return "LOW_LOW"


def main() -> int:
    p4 = load_p4()
    p5 = load_p5()
    common = sorted(set(p4) & set(p5))

    rows = []
    for tk in common:
        p4_score = p4[tk]["fundamental_score"]
        p5_score = p5[tk]["score"]
        # Geometric mean — both must be strong to score high.
        composite = round(sqrt(p4_score * p5_score), 3)
        rows.append({
            "ticker": tk,
            "p4_score": p4_score,
            "p5_score": round(p5_score, 3),
            "composite": composite,
            "quadrant": quadrant(p4_score, p5_score),
            "p4_summary": p4[tk].get("summary", "")[:80],
            "p5_summary": (p5[tk].get("summary") or "")[:80],
        })

    # Group by quadrant
    by_q: dict[str, list[dict]] = {
        "HIGH_HIGH": [], "HIGH_LOW": [], "LOW_HIGH": [], "LOW_LOW": [],
    }
    for r in rows:
        by_q[r["quadrant"]].append(r)
    for q in by_q:
        by_q[q].sort(key=lambda r: -r["composite"])

    # ── Console report ────────────────────────────────────────────────
    print(f"\n=== Pillar 4 ∩ Pillar 5 cross-tab ({len(common)} tickers) ===")
    print(f"Thresholds: P4≥{P4_HIGH}  P5≥{P5_HIGH}")
    print(f"\nQuadrant counts:")
    for q in ("HIGH_HIGH", "HIGH_LOW", "LOW_HIGH", "LOW_LOW"):
        print(f"  {q:<10} {len(by_q[q]):>3d}")

    print(f"\n--- HIGH_HIGH (top conviction; quality + insider buying) ---")
    print(f"{'ticker':<7s} {'P4':>5s} {'P5':>5s} {'comp':>5s}  summary")
    for r in by_q["HIGH_HIGH"]:
        print(f"{r['ticker']:<7s} {r['p4_score']:>5.2f} {r['p5_score']:>5.2f} "
              f"{r['composite']:>5.2f}  {r['p4_summary']}")

    print(f"\n--- LOW_HIGH (insider catalyst, weak fundamentals — speculative) ---")
    print(f"{'ticker':<7s} {'P4':>5s} {'P5':>5s} {'comp':>5s}  summary")
    for r in by_q["LOW_HIGH"][:10]:
        print(f"{r['ticker']:<7s} {r['p4_score']:>5.2f} {r['p5_score']:>5.2f} "
              f"{r['composite']:>5.2f}  {r['p4_summary']}")

    print(f"\n--- HIGH_LOW top 10 (quality, no insider signal — passive) ---")
    print(f"{'ticker':<7s} {'P4':>5s} {'P5':>5s} {'comp':>5s}  summary")
    for r in by_q["HIGH_LOW"][:10]:
        print(f"{r['ticker']:<7s} {r['p4_score']:>5.2f} {r['p5_score']:>5.2f} "
              f"{r['composite']:>5.2f}  {r['p4_summary']}")

    # ── JSON output ───────────────────────────────────────────────────
    out = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "thresholds": {"p4_high": P4_HIGH, "p5_high": P5_HIGH},
        "n_universe": len(common),
        "quadrant_counts": {q: len(by_q[q]) for q in by_q},
        "top_conviction": [r for r in by_q["HIGH_HIGH"]][:20],
        "all": sorted(rows, key=lambda r: -r["composite"]),
    }
    OUT_PATH.write_text(json.dumps(out, indent=2, ensure_ascii=False))
    print(f"\nWrote {OUT_PATH.relative_to(ROOT)} "
          f"({len(out['top_conviction'])} top-conviction picks)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
