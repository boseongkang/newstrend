import argparse, shutil
from pathlib import Path
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--cleaned", required=True, help="cleaned tokens_by_day CSV path")
ap.add_argument("--aggregate-dir", required=True, help="original aggregate dir (to copy articles_by_day.csv if present)")
ap.add_argument("--outdir", required=True, help="output folder to act as --daily-dir for trending_terms.py")
ap.add_argument("--min-count", type=int, default=5, help="OPTIONAL: drop terms whose TOTAL < min-count")
args = ap.parse_args()

cleaned = Path(args.cleaned)
aggdir = Path(args.aggregate_dir)
outdir = Path(args.outdir)
outdir.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(cleaned)
if args.min_count > 0 and {"term","count"}.issubset(df.columns):
    keep = (df.groupby("term")["count"].sum() >= args.min_count)
    df = df[df["term"].isin(keep[keep].index)]
df.to_csv(outdir/"tokens_by_day.csv", index=False)

src_art = aggdir/"articles_by_day.csv"
if src_art.exists():
    shutil.copy2(src_art, outdir/"articles_by_day.csv")

print(f"prepared daily-dir -> {outdir}")