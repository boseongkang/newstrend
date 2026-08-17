#!/usr/bin/env python3
"""Input-watermark gate — D-1 (ci_softfail_audit_2026-08-05.md §D), 2026-08-17.

The validation freshness gate checks "was this output rewritten this run".
That misses the failure mode that killed the sentiment pillar for 95 days:
a generator that runs fine on STALE inputs restamps its output fresh every
run and stays green. This gate reads the `input_watermark` each producer now
embeds (scripts/input_watermark.py) and fails RED when the DATA a fresh-
looking output was built from is older than its design allows.

Covers the four outputs the 2026-08-05 audit left ungated:
  signal_corr, ticker_analysis/*, ticker_weights, fundamentals.
The eight outputs already in the validation freshness gate are NOT re-checked
here — this gate is input-side, that one is output-side; both must pass.

Usage:
  python scripts/input_watermark_gate.py              # gate (exit 1 on RED)
  python scripts/input_watermark_gate.py --self-test  # harness self-tests
  python scripts/input_watermark_gate.py --root DIR   # gate a copied tree
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import os
import sys

# ── Allowed input lag per source, in days ────────────────────────────────────
# lag = (UTC today) − (watermark last_record_date).
#
# ⚠ These bounds encode DESIGN, not guesses — the 2026-08-07 incident (gate
# STALE_DAYS=1 contradicting the deliberate sentiment D-2 clamp) came from a
# bound tighter than the pipeline's design. If a producer's cadence or an
# upstream clamp changes, change the bound HERE in the same commit, with the
# rationale updated. Never tighten below the documented design lag.
MAX_LAG_DAYS = {
    # trends.json news-token calendar: warehouse gets a D-0 daily file within
    # minutes of the first collect run of the day, so latest is normally D-0.
    # 2 (not 1) absorbs the 00:00-UTC race plus one quiet/holiday day.
    # (No FinBERT/sentiment source here — the D-2 sentiment clamp does NOT
    # apply to these four outputs; ticker_sentiment has its own gate.)
    "trends": 2,
    # prices.json trading-day series: Friday close seen on Monday = 3 days;
    # a long weekend (Monday market holiday) = 4. Calendar-day bound must
    # cover the longest normal market gap, not the average one.
    "prices": 4,
    # SEC EDGAR fundamentals: quarterly cadence. max(as_of) across an 80+
    # large-cap universe advances every earnings season (10-Q deadline is
    # ≤45d after quarter end; observed universe max lag ≈45-75d). 120 gives
    # headroom against off-cycle fiscal calendars while still catching a
    # fetcher that silently died for a whole earnings season. This is why
    # fundamentals gets a months-scale bound: its output legitimately looks
    # unchanged for weeks — a false RED here would train people to ignore
    # the gate.
    "edgar": 120,
}

# output file (glob ok) -> input sources its watermark must carry, each
# within MAX_LAG_DAYS. Keep in sync with what the producers actually read.
TARGETS = {
    "site/data/signal_corr.json":      ("trends", "prices"),
    "site/data/ticker_analysis/*.json": ("trends", "prices"),
    "site/data/ticker_weights.json":   ("trends", "prices"),
    "site/data/fundamentals.json":     ("edgar",),
}


def check_file(path: str, sources: tuple, today: dt.date) -> list[str]:
    """Return list of failure strings for one output file (empty = OK)."""
    fails = []
    try:
        data = json.loads(open(path).read())
    except Exception as e:
        return [f"{path}: unreadable ({e})"]

    wm = data.get("input_watermark")
    if not isinstance(wm, dict):
        # Pre-watermark file surviving in the tree == the producer did not
        # run this time (crashed under `|| true`) — exactly what we catch.
        return [f"{path}: no input_watermark — producer crashed and a stale "
                f"pre-watermark file survived?"]

    for src in sources:
        entry = wm.get(src)
        if not isinstance(entry, dict):
            fails.append(f"{path}: watermark missing source '{src}'")
            continue
        last = entry.get("last_record_date")
        count = entry.get("record_count") or 0
        if count < 1:
            fails.append(f"{path}: {src} record_count={count} — input was empty")
            continue
        if not last:
            fails.append(f"{path}: {src} has no last_record_date — input had no dated records")
            continue
        try:
            last_d = dt.date.fromisoformat(str(last)[:10])
        except ValueError as e:
            fails.append(f"{path}: {src} last_record_date={last!r} unparseable ({e})")
            continue
        lag = (today - last_d).days
        limit = MAX_LAG_DAYS[src]
        if lag > limit:
            fails.append(
                f"{path}: STALE INPUT — {src} last_record_date={last} "
                f"({lag}d old > {limit}d allowed). Output looks fresh but was "
                f"built from old data.")
        else:
            print(f"OK  {path}  [{src}] {last} ({lag}d ≤ {limit}d, n={count})")
    return fails


def run_gate(root: str) -> int:
    today = dt.datetime.now(dt.timezone.utc).date()
    failures = []
    for pattern, sources in TARGETS.items():
        paths = sorted(glob.glob(os.path.join(root, pattern)))
        if not paths:
            failures.append(f"{pattern}: no files found — producer never ran?")
            continue
        for p in paths:
            failures.extend(check_file(p, sources, today))

    if failures:
        print("\n=== INPUT-WATERMARK GATE FAILED ===", file=sys.stderr)
        for f in failures:
            print("  ✗ " + f, file=sys.stderr)
        return 1
    print("\nInput-watermark gate: all inputs within design lag.")
    return 0


# ── self-tests ───────────────────────────────────────────────────────────────

def self_test() -> int:
    import tempfile
    today = dt.date(2026, 8, 17)

    def write(dirpath, name, wm):
        p = os.path.join(dirpath, name)
        json.dump({"updated": "x", "input_watermark": wm}, open(p, "w"))
        return p

    results = []

    with tempfile.TemporaryDirectory() as d:
        fresh = {"trends": {"source": "t", "last_record_date": "2026-08-17", "record_count": 180},
                 "prices": {"source": "p", "last_record_date": "2026-08-15", "record_count": 31050}}
        p = write(d, "fresh.json", fresh)
        results.append(("fresh trends+prices pass",
                        check_file(p, ("trends", "prices"), today) == []))

        stale = {"trends": {"source": "t", "last_record_date": "2026-08-10", "record_count": 180},
                 "prices": fresh["prices"]}
        p = write(d, "stale.json", stale)
        f = check_file(p, ("trends", "prices"), today)
        results.append(("stale trends RED", len(f) == 1 and "STALE INPUT" in f[0]))

        # long weekend: Friday prices seen on Tuesday after a Monday holiday
        # (4 calendar days) must NOT fire — the bound covers the design gap.
        holiday = {"trends": fresh["trends"],
                   "prices": {"source": "p", "last_record_date": "2026-08-13", "record_count": 100}}
        p = write(d, "holiday.json", holiday)
        results.append(("4d price gap (long weekend) passes",
                        check_file(p, ("trends", "prices"), today) == []))

        p = os.path.join(d, "nowm.json")
        json.dump({"updated": "x"}, open(p, "w"))
        f = check_file(p, ("trends",), today)
        results.append(("missing watermark RED", len(f) == 1 and "no input_watermark" in f[0]))

        # fundamentals false-RED guard: a quarterly as_of 60d old is normal.
        fund = {"edgar": {"source": "SEC EDGAR companyfacts",
                          "last_record_date": "2026-06-27", "record_count": 81}}
        p = write(d, "fund_ok.json", fund)
        results.append(("fundamentals 51d-old as_of passes (no false RED)",
                        check_file(p, ("edgar",), today) == []))

        dead = {"edgar": {"source": "SEC EDGAR companyfacts",
                          "last_record_date": "2026-03-31", "record_count": 81}}
        p = write(d, "fund_dead.json", dead)
        f = check_file(p, ("edgar",), today)
        results.append(("fundamentals 139d-old as_of RED", len(f) == 1 and "STALE INPUT" in f[0]))

        empty = {"trends": {"source": "t", "last_record_date": "2026-08-17", "record_count": 0},
                 "prices": fresh["prices"]}
        p = write(d, "empty.json", empty)
        f = check_file(p, ("trends", "prices"), today)
        results.append(("record_count=0 RED", any("record_count=0" in x for x in f)))

    ok = True
    for name, passed in results:
        print(("PASS  " if passed else "FAIL  ") + name)
        ok = ok and passed
    print(f"\n{sum(p for _, p in results)}/{len(results)} self-tests passed")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="tree to gate (for testing a doctored copy)")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        return self_test()
    return run_gate(args.root)


if __name__ == "__main__":
    sys.exit(main())
