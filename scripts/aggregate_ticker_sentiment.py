"""
Aggregate per-day FinBERT outputs into ticker_sentiment.json.

Reads files produced by sentiment_finbert.py (per-day, per-article scores +
ticker mapping). Groups by (ticker, date), counts bullish/bearish/neutral
articles with confidence threshold, computes score in [-1, +1].

Usage:
    python scripts/aggregate_ticker_sentiment.py \
        --inputs '/tmp/sentiment_2026-04-*.json' \
        --output /tmp/ticker_sentiment.json
"""
import argparse
import glob
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

CONF_THRESHOLD = 0.6
MIN_ARTICLES = 5  # filtered_score is null when daily total < this


def aggregate(per_day_files):
    by_ticker_date = defaultdict(lambda: defaultdict(
        lambda: {"bullish": 0, "bearish": 0, "neutral": 0, "total": 0}
    ))
    for fp in sorted(per_day_files):
        data = json.loads(Path(fp).read_text())
        # Derive date from file name (sentiment_YYYY-MM-DD.json or news_YYYY-MM-DD)
        m = re.search(r"(\d{4}-\d{2}-\d{2})", Path(fp).stem)
        if not m:
            print(f"  skip {fp}: no date in filename")
            continue
        date = m.group(1)
        n_with_tk = 0
        for r in data["results"]:
            tickers = r.get("tickers") or []
            if not tickers:
                continue
            n_with_tk += 1
            label = r["label"]
            conf = r["scores"][label]
            confident = conf >= CONF_THRESHOLD
            for tk in tickers:
                cell = by_ticker_date[tk][date]
                cell["total"] += 1
                if confident and label == "positive":
                    cell["bullish"] += 1
                elif confident and label == "negative":
                    cell["bearish"] += 1
                else:
                    cell["neutral"] += 1
        print(f"  {date}: {n_with_tk} articles with tickers")
    return by_ticker_date


def to_output(agg, min_articles):
    all_dates = sorted({d for tk in agg.values() for d in tk.keys()})
    tickers = {}
    for tk, by_date in agg.items():
        bullish = [by_date.get(d, {}).get("bullish", 0) for d in all_dates]
        bearish = [by_date.get(d, {}).get("bearish", 0) for d in all_dates]
        neutral = [by_date.get(d, {}).get("neutral", 0) for d in all_dates]
        total   = [by_date.get(d, {}).get("total",   0) for d in all_dates]
        score   = [round((b - bs) / max(t, 1), 3) for b, bs, t in zip(bullish, bearish, total)]
        filtered = [s if t >= min_articles else None
                    for s, t in zip(score, total)]
        tickers[tk] = {
            "bullish":        bullish,
            "bearish":        bearish,
            "neutral":        neutral,
            "total":          total,
            "score":          score,
            "filtered_score": filtered,
        }
    return {
        "model":        "ProsusAI/finbert",
        "generated":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "threshold":    CONF_THRESHOLD,
        "min_articles": min_articles,
        "dates":        all_dates,
        "tickers":      dict(sorted(tickers.items())),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", nargs="+", required=True,
                    help="Per-day FinBERT output JSONs (glob patterns ok)")
    ap.add_argument("--output", required=True)
    ap.add_argument("--min-articles", type=int, default=MIN_ARTICLES,
                    help="filtered_score=null when daily total < this (default 5)")
    args = ap.parse_args()

    files = []
    for pat in args.inputs:
        matched = glob.glob(pat)
        if matched:
            files.extend(matched)
        elif Path(pat).exists():
            files.append(pat)
    if not files:
        raise SystemExit(f"no input files matched: {args.inputs}")

    print(f"aggregating {len(files)} per-day files (min_articles={args.min_articles})...")
    agg = aggregate(files)
    out = to_output(agg, args.min_articles)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2))
    print(f"\ntickers={len(out['tickers'])}  dates={len(out['dates'])}")
    print(f"output: {out_path}")


if __name__ == "__main__":
    main()
