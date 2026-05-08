from pathlib import Path
from datetime import datetime, timezone, timedelta
import argparse
from news_trend.report import write_report

def resolve_date(date_arg: str, indir: str, kind: str) -> str:
    if date_arg.lower() in ("today",):
        return datetime.now(timezone.utc).date().isoformat()
    if date_arg.lower() in ("yesterday",):
        return (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
    if date_arg.lower() in ("auto", "latest"):
        folder = Path(indir) / kind
        files = sorted(folder.glob("*.jsonl"))
        if not files:
            raise FileNotFoundError(f"no jsonl files in {folder}")
        return files[-1].stem
    return date_arg

def main():
    ap = argparse.ArgumentParser(description="Generate daily news report HTML/CSV")
    ap.add_argument("--date", default="auto", help='"YYYY-MM-DD" or "yesterday" or "auto"')
    ap.add_argument("--kind", default="silver_newsapi", help="subfolder under --indir (e.g., raw, raw_newsapi, silver_newsapi)")
    ap.add_argument("--indir", default="data")
    ap.add_argument("--outdir", default="reports")
    ap.add_argument("--limit", type=int, default=50)
    args = ap.parse_args()

    d = resolve_date(args.date, args.indir, args.kind)
    out_dir = write_report(d, kind=args.kind, indir=args.indir, outdir=args.outdir, sample_limit=args.limit)
    print(f"[OK] wrote report -> {out_dir}/report.html")

if __name__ == "__main__":
    main()
