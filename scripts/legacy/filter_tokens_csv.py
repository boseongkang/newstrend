import argparse, pandas as pd
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("--in", dest="inp", required=True, help="tokens_by_day.csv")
ap.add_argument("--stop-file", required=True, help="config/extra_noise.txt")
ap.add_argument("--min-len", type=int, default=3)
ap.add_argument("--out", required=True)
args = ap.parse_args()

stop = {ln.strip().lower() for ln in Path(args.stop_file).read_text(encoding="utf-8").splitlines() if ln.strip()}
df = pd.read_csv(args.inp)
df["term"] = df["term"].astype(str).str.lower()
mask = (~df["term"].isin(stop)) & (df["term"].str.len() >= args.min_len) & (~df["term"].str.isnumeric())
df = df[mask].copy()
Path(args.out).parent.mkdir(parents=True, exist_ok=True)
df.to_csv(args.out, index=False)
print(f"saved -> {args.out} (rows={len(df)})")