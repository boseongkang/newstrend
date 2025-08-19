import argparse
import os
from datetime import datetime
from .config import settings
from .ingest import fetch_newsapi
# from .ingest import fetch_rss, fetch_newsapi
from .utils import save_jsonl, load_jsonl
from .dedup import dedup_rows

def cmd_ingest(args):
    rows = []
    if args.rss:
        rows.extend(fetch_rss(country=args.country))
    if args.newsapi:
        outfile = fetch_newsapi(outdir=args.outdir, date=args.date)
        print(f"Saved newsapi data to {outfile}")
        return
    if rows:
        d = datetime.utcnow().date().isoformat() if args.date == "today" else args.date
        out = os.path.join(args.outdir, f"{d}.jsonl")
        save_jsonl(out, rows)
        print(f"[OK] saved {len(rows)} rows -> {out}")

def cmd_dedup(args):
    d = datetime.utcnow().date().isoformat() if args.date == "today" else args.date
    inpath = os.path.join(args.indir, f"{d}.jsonl")
    if not os.path.exists(inpath):
        raise SystemExit(f"raw file not found: {inpath}")
    rows = list(load_jsonl(inpath))
    cleaned = dedup_rows(rows)
    outpath = os.path.join(args.outdir, f"{d}.jsonl")
    save_jsonl(outpath, cleaned)
    print(f"[OK] {len(rows)} -> {len(cleaned)} after dedup -> {outpath}")

def build_parser():
    p = argparse.ArgumentParser(prog="newscli", description="News ingestion & dedup CLI")
    sub = p.add_subparsers(dest="command", required=True)

    pi = sub.add_parser("ingest", help="Ingest news")
    pi.add_argument("--country", default=settings.default_country)
    pi.add_argument("--rss", action="store_true", help="Use RSS feeds")
    pi.add_argument("--newsapi", action="store_true", help="Use NewsAPI (requires key)")
    pi.add_argument("--outdir", default="data/raw")
    pi.add_argument("--date", default="today", help='"YYYY-MM-DD" or "today"')
    pi.set_defaults(func=cmd_ingest)

    pd = sub.add_parser("dedup", help="Deduplicate by (publisher, normalized_title, date)")
    pd.add_argument("--date", default="today", help='"YYYY-MM-DD" or "today"')
    pd.add_argument("--indir", default="data/raw")
    pd.add_argument("--outdir", default="data/silver")
    pd.set_defaults(func=cmd_dedup)

    return p

def main():
    p = build_parser()
    args = p.parse_args()
    args.func(args)
