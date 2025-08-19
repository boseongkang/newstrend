from __future__ import annotations
import argparse
from datetime import datetime, timezone
from pathlib import Path

from .ingest import fetch_rss, fetch_newsapi, _parse_date_arg

def cmd_ingest(args: argparse.Namespace) -> None:
    target = _parse_date_arg(args.date)
    iso_date = target.isoformat()

    if args.newsapi:
        out = fetch_newsapi(outdir=args.outdir, date=iso_date)
        print(f"Saved newsapi data to {out}")

    if args.rss:
        out = fetch_rss(outdir=args.outdir, date=iso_date)
        print(f"Saved rss data to {out}")

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="newscli", description="News ingestion & dedup CLI")
    sub = p.add_subparsers(dest="command", required=True)

    pi = sub.add_parser("ingest", help="Ingest news")
    pi.add_argument("--rss", action="store_true")
    pi.add_argument("--newsapi", action="store_true")
    pi.add_argument("--outdir", default="data/raw")
    pi.add_argument("--date", default="today")    
    pi.set_defaults(func=cmd_ingest)
    return p

def main() -> None:
    p = build_parser()
    args = p.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
