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

STALE_DAYS = 2
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
            f"is {gap}d behind today ({today}) — local FinBERT launchd has not "
            f"pushed in >{STALE_DAYS}d. Check ~/Library/Logs/newstrend-finbert.log "
            f"or run: launchctl kickstart -k gui/$(id -u)/com.newstrend.finbert"
        )
        return 1

    print(f"OK — within {STALE_DAYS}d window")
    return 0


if __name__ == "__main__":
    sys.exit(main())
