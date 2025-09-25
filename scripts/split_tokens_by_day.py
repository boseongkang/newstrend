import argparse
from pathlib import Path
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--in", dest="inp", required=True, help="cleaned tokens_by_day.csv (must have date,term,count)")
ap.add_argument("--outdir", required=True, help="output directory for per-day files")
ap.add_argument("--min-count", type=int, default=0, help="drop terms with total < min-count before splitting")
args = ap.parse_args()

inp = Path(args.inp)
out = Path(args.outdir)
out.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(inp)
cols = {c.lower(): c for c in df.columns}
date_col = cols.get("date") or "date"
term_col = cols.get("term") or "term"
count_col = cols.get("count") or "count"

df = df.rename(columns={date_col:"date", term_col:"term", count_col:"count"})[["date","term","count"]]
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df.dropna(subset=["date"])
df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)

if args.min_count > 0:
    keep = (df.groupby("term")["count"].sum() >= args.min_count)
    df = df[df["term"].isin(keep[keep].index)]

for day, g in df.groupby(df["date"].dt.date, sort=True):
    small = g[["term","count"]].groupby("term", as_index=False)["count"].sum().sort_values("count", ascending=False)
    a = out / f"{day.isoformat()}.tokens.csv"
    small.to_csv(a, index=False)
    b = out / f"tokens_{day.isoformat()}.csv"
    small.to_csv(b, index=False)

print(f"wrote per-day tokens into: {out}")