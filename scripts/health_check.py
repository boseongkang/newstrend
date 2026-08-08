#!/usr/bin/env python3
"""Fail CI if sentiment_per_day is stale relative to today (UTC).

Reads data/sentiment_per_day/sentiment_YYYY-MM-DD.json (restored from
origin/data-cache by trend-site.yml), takes the max date, and compares to
today. If the gap exceeds STALE_DAYS, emits a GitHub Actions ::error:: line
and exits 1 — so the workflow turns red and the mobile app pushes a
notification. Independent of the local M3 launchd state.
"""
import datetime as dt
import glob
import os
import re
import sys

# 3 (2026-08-08): sentiment lags D-2 BY DESIGN since the 2026-08-05 clamp
# (news_archive stores only complete days ≤ D-2; same-day partial files are
# no longer produced). Healthy steady state is therefore gap=2, and gap=3 is
# normal early in day D before that day's archive→local-score→push cycle
# completes (local slots: 09/13/18/22 PT). gap>3 means the local runner
# missed all 4 slots for over a day — the real incident case. The previous
# value (1) assumed the pre-clamp same-day partial pushes and turned the
# gate permanently RED from 2026-08-07 on.
STALE_DAYS = 3
SENTIMENT_DIR = "data/sentiment_per_day"
PAT = re.compile(r"sentiment_(\d{4}-\d{2}-\d{2})\.json$")


def main() -> int:
    files = glob.glob(os.path.join(SENTIMENT_DIR, "sentiment_*.json"))
    dates = []
    for f in files:
        m = PAT.search(f)
        if not m:
            continue
        try:
            dates.append(dt.date.fromisoformat(m.group(1)))
        except ValueError:
            continue

    if not dates:
        print(f"::error title=Sentiment health::no sentiment_per_day files in {SENTIMENT_DIR}")
        return 1

    latest = max(dates)
    today = dt.datetime.now(dt.timezone.utc).date()
    gap = (today - latest).days

    print(f"latest sentiment date: {latest}  today (UTC): {today}  gap: {gap}d")

    if gap > STALE_DAYS:
        print(
            f"::error title=Sentiment stale::sentiment_per_day latest={latest} "
            f"is {gap}d behind today ({today}); with the D-2 design anything "
            f">{STALE_DAYS}d means the local FinBERT launchd has missed a full "
            f"day. Check ~/Library/Logs/newstrend-finbert.log "
            f"or run: launchctl kickstart -k gui/$(id -u)/com.newstrend.finbert"
        )
        return 1

    print(f"OK — within {STALE_DAYS}d window")
    return 0


if __name__ == "__main__":
    sys.exit(main())
