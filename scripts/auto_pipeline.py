from pathlib import Path
from datetime import datetime, timezone, timedelta
import argparse
from news_trend.ingest import fetch_newsapi
from news_trend.utils import load_jsonl, save_jsonl
from news_trend.dedup import dedup_rows
from news_trend.report import write_report

def resolve_date(s: str) -> str:
    if s.lower() == "today":
        return datetime.now(timezone.utc).date().isoformat()
    if s.lower() == "yesterday":
        return (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
    return s

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="yesterday")
    ap.add_argument("--rawdir", default="data/raw_newsapi")
    ap.add_argument("--silverdir", default="data/silver_newsapi")
    ap.add_argument("--reports", default="reports")
    ap.add_argument("--sample-limit", type=int, default=50)
    args = ap.parse_args()

    d = resolve_date(args.date)
    Path(args.rawdir).mkdir(parents=True, exist_ok=True)
    Path(args.silverdir).mkdir(parents=True, exist_ok=True)
    Path(args.reports).mkdir(parents=True, exist_ok=True)

    ret = fetch_newsapi(outdir=args.rawdir, date=d)
    raw_path = Path(ret) if ret else Path(args.rawdir) / f"{d}.jsonl"
    target_raw = Path(args.rawdir) / f"{d}.jsonl"
    if raw_path.exists() and raw_path != target_raw:
        raw_path.rename(target_raw)
    raw_path = target_raw
    rows = list(load_jsonl(str(raw_path)))
    cleaned = dedup_rows(rows)
    silver_path = Path(args.silverdir) / f"{d}.jsonl"
    save_jsonl(str(silver_path), cleaned)
    out_dir = write_report(d, kind=Path(args.silverdir).name, indir=str(Path(args.silverdir).parents[0]), outdir=args.reports, sample_limit=args.sample_limit)
    print(f"[OK] raw={raw_path} rows={len(rows)} | silver={silver_path} rows={len(cleaned)} | report={Path(out_dir)/'report.html'}")

if __name__ == "__main__":
    main()
