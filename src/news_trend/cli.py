from __future__ import annotations
import argparse
from pathlib import Path
from .ingest import fetch_newsapi, parse_date
from .utils import save_jsonl, load_jsonl
from .dedup import dedup_rows

def _locate_input(indir: str, in_kind: str, date_iso: str) -> Path:
    candidates = [
        Path(indir) / "raw_newsapi" / f"{date_iso}.jsonl",
        Path(indir) / "raw" / f"newsapi_{date_iso}.jsonl",
        Path(indir) / "raw" / f"{date_iso}.jsonl",
        Path(indir) / in_kind / f"{date_iso}.jsonl",
    ]
    for p in candidates:
        if p.exists():
            return p
    return candidates[0]

def cmd_ingest(args):
    d = parse_date(args.date)
    iso = d.isoformat()
    if args.newsapi:
        outfile = fetch_newsapi(
            query=args.query,
            hours_split=args.hours_split,
            max_pages_per_window=args.max_pages,
            outdir=args.outdir,
            date=iso,
            pause=args.pause,
        )
        print(f"Saved newsapi data to {outfile}")
    else:
        raise SystemExit("no source selected")

def cmd_dedup(args):
    d = parse_date(args.date)
    iso = d.isoformat()
    inpath = _locate_input(args.indir, args.in_kind, iso)
    if not inpath.exists():
        raise SystemExit(f"input not found: {inpath}")
    rows = list(load_jsonl(inpath))
    cleaned = dedup_rows(rows)
    outdir = Path(args.outdir) / args.out_kind
    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / f"{iso}.jsonl"
    save_jsonl(outpath, cleaned)
    print(f"[OK] {len(rows)} -> {len(cleaned)} after dedup -> {outpath}")

def build_parser():
    p = argparse.ArgumentParser(prog="newscli", description="News ingestion and dedup CLI")
    sub = p.add_subparsers(dest="command", required=True)

    pi = sub.add_parser("ingest", help="Ingest news")
    pi.add_argument("--newsapi", action="store_true")
    pi.add_argument("--query", default="news")
    pi.add_argument("--hours-split", type=int, default=2)
    pi.add_argument("--max-pages", type=int, default=8)
    pi.add_argument("--pause", type=float, default=0.25)
    pi.add_argument("--outdir", default="data/raw")
    pi.add_argument("--date", default="today")
    pi.set_defaults(func=cmd_ingest)

    pd = sub.add_parser("dedup", help="Deduplicate records")
    pd.add_argument("--date", default="today")
    pd.add_argument("--indir", default="data")
    pd.add_argument("--in-kind", default="raw_newsapi")
    pd.add_argument("--outdir", default="data")
    pd.add_argument("--out-kind", default="silver_newsapi")
    pd.set_defaults(func=cmd_dedup)

    return p

def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)
