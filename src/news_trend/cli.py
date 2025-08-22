from __future__ import annotations
import argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone, date as ddate
from typing import Optional
from .hourly import ingest_newsapi_hourly, ingest_newsapi_recent


def _resolve_date_arg(s: Optional[str]) -> str:
    t = datetime.now(timezone.utc).date()
    if not s or s.lower() == "today":
        return t.isoformat()
    if s.lower() == "yesterday":
        return (t - timedelta(days=1)).isoformat()
    return ddate.fromisoformat(s).isoformat()


def cmd_ingest(args: argparse.Namespace) -> None:
    from .ingest import fetch_newsapi

    iso = _resolve_date_arg(args.date)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    outfile = fetch_newsapi(outdir=str(outdir), date=iso)
    print(f"Saved newsapi data to {outfile}")


def cmd_dedup(args: argparse.Namespace) -> None:
    from .utils import load_jsonl, save_jsonl
    from .dedup import dedup_rows

    iso = _resolve_date_arg(args.date)
    inpath = Path(args.indir) / f"{iso}.jsonl"
    rows = list(load_jsonl(str(inpath)))
    cleaned = dedup_rows(rows)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    out = outdir / f"{iso}.jsonl"
    save_jsonl(str(out), cleaned)
    print(f"[OK] {len(rows)} -> {len(cleaned)} -> {out}")


def cmd_report(args: argparse.Namespace) -> None:
    from .report import write_report

    iso = _resolve_date_arg(args.date)
    write_report(
        iso, kind=args.kind, indir=args.indir, outdir=args.outdir, sample_limit=args.top
    )


def cmd_ingest_hourly(args: argparse.Namespace) -> None:
    ingest_newsapi_hourly(
        query=args.query,
        hours_split=args.hours_split,
        max_pages_per_window=args.max_pages,
        outroot=args.outroot,
        date=args.date,
    )


def cmd_aggregate(args: argparse.Namespace) -> None:
    from .aggregate import aggregate_windows

    aggregate_windows(
        date=_resolve_date_arg(args.date),
        inroot=args.inroot,
        daily_outdir=args.daily_outdir,
        silver_outdir=args.silver_outdir,
    )


def cmd_analyze_hourly(args: argparse.Namespace) -> None:
    from .analyze_hourly import analyze_hourly

    analyze_hourly(
        date=_resolve_date_arg(args.date),
        indir=args.indir,
        outdir=args.outdir,
        top_k_publishers=args.top_publishers,
        top_k_words=args.top_words,
    )


def cmd_pipeline_day(args: argparse.Namespace) -> None:
    from .aggregate import aggregate_windows
    from .analyze_hourly import analyze_hourly

    d = _resolve_date_arg(args.date)
    ingest_newsapi_hourly(query=args.query, hours_split=args.hours_split, date=d)
    aggregate_windows(date=d)
    analyze_hourly(date=d)


def cmd_collect_live(args: argparse.Namespace) -> None:
    outfile = Path(args.outfile) if args.outfile else None
    ingest_newsapi_recent(
        query=args.query,
        recent_minutes=args.recent_minutes,
        pages=args.pages,
        outdir=args.outdir,
        outfile=outfile,
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="newscli", description="News ingestion, processing, and reports (NewsAPI-only)"
    )
    sub = p.add_subparsers(dest="command", required=True)

    pi = sub.add_parser("ingest", help="Ingest daily with NewsAPI (single file)")
    pi.add_argument("--outdir", default="data/raw")
    pi.add_argument("--date", default="today")
    pi.set_defaults(func=cmd_ingest)

    pd = sub.add_parser("dedup", help="Deduplicate a daily file")
    pd.add_argument("--date", default="today")
    pd.add_argument("--indir", default="data/raw_newsapi")
    pd.add_argument("--outdir", default="data/silver_newsapi")
    pd.set_defaults(func=cmd_dedup)

    pr = sub.add_parser("report", help="Generate HTML report")
    pr.add_argument("--date", required=True)
    pr.add_argument("--indir", default="data")
    pr.add_argument("--kind", default="silver_newsapi", choices=["raw", "raw_newsapi", "silver_newsapi"])
    pr.add_argument("--outdir", default="reports")
    pr.add_argument("--top", type=int, default=30)
    pr.set_defaults(func=cmd_report)

    ph = sub.add_parser("ingest-hourly", help="Ingest NewsAPI by time windows")
    ph.add_argument("--date", default="yesterday")
    ph.add_argument("--hours-split", type=int, default=2)
    ph.add_argument("--outroot", default="data/raw_windows")
    ph.add_argument("--max-pages", type=int, default=8)
    ph.add_argument("--query", default="news")
    ph.set_defaults(func=cmd_ingest_hourly)

    pa = sub.add_parser("aggregate", help="Merge hourly windows to daily raw/silver")
    pa.add_argument("--date", required=True)
    pa.add_argument("--inroot", default="data/raw_windows")
    pa.add_argument("--daily-outdir", default="data/raw_newsapi")
    pa.add_argument("--silver-outdir", default="data/silver_newsapi")
    pa.set_defaults(func=cmd_aggregate)

    pan = sub.add_parser("analyze-hourly", help="Analyze silver and build hourly report")
    pan.add_argument("--date", required=True)
    pan.add_argument("--indir", default="data/silver_newsapi")
    pan.add_argument("--outdir", default="reports/hourly")
    pan.add_argument("--top-publishers", type=int, default=10)
    pan.add_argument("--top-words", type=int, default=30)
    pan.set_defaults(func=cmd_analyze_hourly)

    pp = sub.add_parser("pipeline-day", help="Run hourly ingest -> aggregate -> analyze")
    pp.add_argument("--date", default="yesterday")
    pp.add_argument("--hours-split", type=int, default=2)
    pp.add_argument("--query", default="news")
    pp.set_defaults(func=cmd_pipeline_day)

    pc = sub.add_parser("collect-live", help="Collect recent minutes window (live)")
    pc.add_argument("--query", default="news")
    pc.add_argument("--recent-minutes", type=int, default=30)
    pc.add_argument("--pages", type=int, default=3)
    pc.add_argument("--outdir", default="data/live_newsapi")
    pc.add_argument("--outfile", default="")
    pc.set_defaults(func=cmd_collect_live)

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
