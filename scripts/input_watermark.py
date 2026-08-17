"""Input watermarks — D-1 (ci_softfail_audit_2026-08-05.md §D), 2026-08-17.

The end-of-job freshness gate can only see "was this output rewritten this
run" — a generator that runs fine on STALE inputs restamps its output fresh
and sails through (the sentiment pillar died for 95 days exactly this way).
Fix: every output embeds a watermark describing the DATA it consumed, so a
gate can check "was the input recent" instead of "was the file rewritten".

Shape (one entry per input source):

    "input_watermark": {
        "trends": {"source": "site/data/trends.json",
                   "last_record_date": "2026-08-17", "record_count": 180},
        ...
    }

`last_record_date` is the newest DATA date inside the input (not an mtime,
not an `updated` stamp — those lie when the generator restamps). The gate
(scripts/input_watermark_gate.py) owns the per-source allowed-lag policy.
"""
from __future__ import annotations


def trends_watermark(trends: dict, source_path: str) -> dict:
    """trends.json: top-level `dates` is the per-day token calendar."""
    dates = trends.get("dates") or []
    return {
        "source": source_path,
        "last_record_date": dates[-1] if dates else None,
        "record_count": len(dates),
    }


def prices_watermark(prices: dict, source_path: str) -> dict:
    """prices.json: {tickers: {T: {dates: [...], ...}}}.

    last_record_date = newest trading day across the universe (max, not min:
    one delisted/broken ticker must not mask a globally-fresh feed; a
    globally-dead feed drags the max down and is exactly what we detect).
    record_count = total price rows across all tickers.
    """
    last = None
    count = 0
    for tdata in (prices.get("tickers") or {}).values():
        dates = tdata.get("dates") or []
        count += len(dates)
        if dates and (last is None or dates[-1] > last):
            last = dates[-1]
    return {"source": source_path, "last_record_date": last, "record_count": count}
