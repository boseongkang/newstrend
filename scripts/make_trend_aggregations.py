"""Aggregate trends.json into day-of-week, weekly, and monthly views.

Reads `site/data/trends.json` (produced by scripts/make_trends_json.py)
and writes three sibling JSON files used by the dashboard:

  site/data/trends_dow.json      — Mon..Sun totals per term + peak_day/ratio
  site/data/trends_weekly.json   — ISO-week (YYYY-Www) totals per term
  site/data/trends_monthly.json  — YYYY-MM totals per term

Zero-volume days (the dashboard placeholder days where ingestion produced
no docs) are excluded from DOW averages so peak_ratio is not inflated by
a long quiet weekend.
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

DOW_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def parse_iso_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def iso_week_key(d: datetime) -> str:
    y, w, _ = d.isocalendar()
    return f"{y:04d}-W{w:02d}"


def month_key(d: datetime) -> str:
    return d.strftime("%Y-%m")


def build_dow(dates_dt, series, top_terms, day_totals):
    """Day-of-week aggregation. Returns dict suitable for JSON dump."""
    n_days_per_dow: dict[int, int] = defaultdict(int)
    vol_per_dow: dict[int, int] = defaultdict(int)
    for d, total in zip(dates_dt, day_totals):
        if total <= 0:
            # Skip empty-ingestion days so they don't drag the weekday average.
            continue
        dow = d.weekday()
        n_days_per_dow[dow] += 1
        vol_per_dow[dow] += total

    terms_out: dict[str, dict] = {}
    for term in top_terms:
        ser = series[term]
        by_dow_sum: dict[int, int] = defaultdict(int)
        by_dow_n: dict[int, int] = defaultdict(int)
        for d, v, total in zip(dates_dt, ser, day_totals):
            if total <= 0:
                continue
            dow = d.weekday()
            by_dow_sum[dow] += int(v)
            by_dow_n[dow] += 1
        avgs = {}
        for dow in range(7):
            n = by_dow_n.get(dow, 0)
            avgs[dow] = (by_dow_sum.get(dow, 0) / n) if n else 0.0
        overall_avg = sum(avgs.values()) / 7 if any(avgs.values()) else 0.0
        peak_dow = max(range(7), key=lambda x: avgs[x]) if overall_avg > 0 else 0
        peak_ratio = (avgs[peak_dow] / overall_avg) if overall_avg > 0 else 0.0
        terms_out[term] = {
            "avg_by_day": {DOW_NAMES[i]: round(avgs[i], 2) for i in range(7)},
            "total_by_day": {DOW_NAMES[i]: by_dow_sum.get(i, 0) for i in range(7)},
            "peak_day": DOW_NAMES[peak_dow],
            "peak_ratio": round(peak_ratio, 3),
        }

    return {
        "day_counts": {DOW_NAMES[i]: n_days_per_dow.get(i, 0) for i in range(7)},
        "volume_by_day": {DOW_NAMES[i]: vol_per_dow.get(i, 0) for i in range(7)},
        "avg_volume_by_day": {
            DOW_NAMES[i]: round(vol_per_dow.get(i, 0) / n_days_per_dow.get(i, 1), 1)
            if n_days_per_dow.get(i, 0)
            else 0.0
            for i in range(7)
        },
        "terms": terms_out,
    }


def build_bucketed(dates_dt, series, top_terms, key_fn):
    """Generic bucketed aggregation by (date → bucket key)."""
    bucket_order: list[str] = []
    bucket_seen: set[str] = set()
    for d in dates_dt:
        k = key_fn(d)
        if k not in bucket_seen:
            bucket_seen.add(k)
            bucket_order.append(k)

    out_series: dict[str, list[int]] = {t: [0] * len(bucket_order) for t in top_terms}
    bucket_idx = {k: i for i, k in enumerate(bucket_order)}
    for term in top_terms:
        ser = series[term]
        for d, v in zip(dates_dt, ser):
            out_series[term][bucket_idx[key_fn(d)]] += int(v)
    return bucket_order, out_series


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trends", default="site/data/trends.json")
    ap.add_argument("--outdir", default="site/data")
    ap.add_argument(
        "--topk",
        type=int,
        default=200,
        help="Number of terms to include (ranked by total volume across the window).",
    )
    args = ap.parse_args()

    src = Path(args.trends)
    data = json.loads(src.read_text(encoding="utf-8"))
    dates: list[str] = data["dates"]
    series: dict[str, list[int]] = data["series"]

    dates_dt = [parse_iso_date(s) for s in dates]
    day_totals = [sum(int(s[i]) for s in series.values()) for i in range(len(dates))]

    # Rank terms by total volume so the aggregations focus on signal-bearing words.
    term_totals = {t: sum(int(x) for x in s) for t, s in series.items()}
    top_terms = sorted(term_totals, key=lambda t: term_totals[t], reverse=True)[: args.topk]

    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    meta = {
        "generated_at": generated_at,
        "source": str(src),
        "n_dates": len(dates),
        "date_range": [dates[0], dates[-1]],
        "topk": args.topk,
        "terms": top_terms,
    }

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    dow_payload = {**meta, **build_dow(dates_dt, series, top_terms, day_totals)}
    (outdir / "trends_dow.json").write_text(
        json.dumps(dow_payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    weeks, weekly_series = build_bucketed(dates_dt, series, top_terms, iso_week_key)
    (outdir / "trends_weekly.json").write_text(
        json.dumps({**meta, "weeks": weeks, "series": weekly_series}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    months, monthly_series = build_bucketed(dates_dt, series, top_terms, month_key)
    (outdir / "trends_monthly.json").write_text(
        json.dumps({**meta, "months": months, "series": monthly_series}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(
        f"wrote dow ({len(top_terms)} terms), weekly ({len(weeks)} weeks), "
        f"monthly ({len(months)} months) → {outdir}"
    )


if __name__ == "__main__":
    main()
