import argparse, os
from datetime import datetime, timezone, timedelta, date as dtdate
from pathlib import Path
from .ingest import fetch_newsapi

def main():
    p = argparse.ArgumentParser(prog="newscli")
    sub = p.add_subparsers(dest="cmd", required=True)

    pi = sub.add_parser("ingest")
    pi.add_argument("--newsapi", action="store_true")
    pi.add_argument("--date", default="today")
    pi.add_argument("--outdir", default="data/raw")

    args = p.parse_args()

    if args.cmd == "ingest":
        outfile = None
        if args.newsapi:
            outfile = fetch_newsapi(outdir=args.outdir, date=args.date)
            print(f"Saved newsapi data to {outfile}")
        else:
            print("No source selected")
    else:
        raise SystemExit("unknown command")

if __name__ == "__main__":
    main()
