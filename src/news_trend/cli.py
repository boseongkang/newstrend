from __future__ import annotations
import argparse
from pathlib import Path
from .ingest import fetch_newsapi, parse_date
from .utils import save_jsonl, load_jsonl
from .dedup import dedup_rows

def cmd_ingest(args):
    if args.newsapi:
        out = fetch_newsapi(outdir=args.outdir, date=args.date)
        print(f"Saved newsapi data to {out}")
    else:
        raise SystemExit("Use --newsapi")

def cmd_dedup(args):
    d = parse_date(args.date).isoformat()
    inpath = Path(args.indir) / f"{d}.jsonl"
    if not inpath.exists():
        raise SystemExit(f"raw file not found: {inpath}")
    rows = list(load_jsonl(inpath))
    cleaned = dedup_rows(rows)
    outpath = Path(args.outdir) / f"{d}.jsonl"
    save_jsonl(outpath, cleaned)
    print(f"[OK] {len(rows)} -> {len(cleaned)} after dedup -> {outpath}")

def build_parser():
    p = argparse.ArgumentParser(prog="newscli", description="News ingestion & dedup CLI")
    sub = p.add_subparsers(dest="command", required=True)

    pi = sub.add_parser("ingest")
    pi.add_argument("--newsapi", action="store_true")
    pi.add_argument("--outdir", default="data/raw")
    pi.add_argument("--date", default="yesterday")
    pi.set_defaults(func=cmd_ingest)

    pd = sub.add_parser("dedup")
    pd.add_argument("--date", default="yesterday")
    pd.add_argument("--indir", default="data/raw")
    pd.add_argument("--outdir", default="data/silver")
    pd.set_defaults(func=cmd_dedup)

    return p

def main():
    p = build_parser()
    args = p.parse_args()
    args.func(args)
