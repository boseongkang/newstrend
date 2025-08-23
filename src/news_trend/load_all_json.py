from __future__ import annotations
import argparse, re
from pathlib import Path
from datetime import datetime, timezone, date as ddate, time as dtime
import pandas as pd

LIVE_RE = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}-\d{2})Z\.jsonl$")
DAILY_RE = re.compile(r"(\d{4}-\d{2}-\d{2})\.jsonl$")

def parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    t = s.strip().replace("Z", "")
    if "T" in t:
        return datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
    return datetime.combine(ddate.fromisoformat(t), dtime(0,0,tzinfo=timezone.utc))

def infer_file_dt(p: Path) -> datetime | None:
    m = LIVE_RE.search(p.name)
    if m:
        return datetime.fromisoformat(m.group(1).replace("-", ":", 1).replace("-", ":", 1)).replace(tzinfo=timezone.utc)
    m = DAILY_RE.search(p.name)
    if m:
        return datetime.combine(ddate.fromisoformat(m.group(1)), dtime(0,0,tzinfo=timezone.utc))
    return None

def discover_files(roots: list[str], pattern: str, since: datetime | None, until: datetime | None, limit: int | None) -> list[Path]:
    out: list[Path] = []
    for r in roots:
        base = Path(r)
        if not base.exists():
            continue
        for p in sorted(base.rglob(pattern)):
            dt = infer_file_dt(p)
            if since and dt and dt < since:
                continue
            if until and dt and dt >= until:
                continue
            out.append(p)
            if limit and len(out) >= limit:
                return out
    return out

def read_jsonl(paths: list[Path]) -> pd.DataFrame:
    dfs = []
    for p in paths:
        try:
            df = pd.read_json(p, lines=True)
            df["_file"] = str(p)
            dfs.append(df)
        except Exception:
            continue
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

def dedup_df(df: pd.DataFrame, key: str | None = None) -> pd.DataFrame:
    if df.empty:
        return df
    keys = []
    if key and key in df.columns:
        keys = [key]
    else:
        for k in ["article_id","url","title"]:
            if k in df.columns:
                keys.append(k)
    if "published_at" in df.columns:
        try:
            df["_ts"] = pd.to_datetime(df["published_at"], utc=True, errors="coerce")
            df = df.sort_values("_ts", ascending=False, kind="stable")
        except Exception:
            pass
    if keys:
        df = df.drop_duplicates(subset=keys, keep="first")
    if "_ts" in df.columns:
        df = df.drop(columns=["_ts"])
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--roots", default="data/live_newsapi,data/raw_newsapi,data/silver_newsapi")
    ap.add_argument("--pattern", default="*.jsonl")
    ap.add_argument("--since")
    ap.add_argument("--until")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--no-dedup", action="store_true")
    ap.add_argument("--dedup-key", default="")
    ap.add_argument("--out")
    ap.add_argument("--top", type=int, default=15)
    args = ap.parse_args()

    roots = [s.strip() for s in args.roots.split(",") if s.strip()]
    since = parse_dt(args.since)
    until = parse_dt(args.until)
    paths = discover_files(roots, args.pattern, since, until, args.limit)
    df = read_jsonl(paths)
    if not args.no_dedup:
        df = dedup_df(df, key=(args.dedup_key or None))

    print(f"files={len(paths)} rows={len(df)}")
    if "publisher" in df.columns:
        vc = df["publisher"].fillna("").value_counts().head(args.top)
        print("\nTop publishers:")
        for k, v in vc.items():
            print(f"{k}\t{v}")
    if "title" in df.columns:
        print("\nSample titles:")
        for t in df["title"].dropna().head(min(args.top, 10)).tolist():
            print(f"- {t}")

    if args.out:
        outp = Path(args.out)
        outp.parent.mkdir(parents=True, exist_ok=True)
        if outp.suffix.lower() == ".csv":
            df.to_csv(outp, index=False)
        elif outp.suffix.lower() in [".parquet", ".pq"]:
            df.to_parquet(outp, index=False)
        else:
            df.to_json(outp, orient="records", lines=True, force_ascii=False)
        print(f"saved -> {outp}")

if __name__ == "__main__":
    main()
