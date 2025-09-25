import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trend-csv", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--topk", type=int, default=10)
    args = ap.parse_args()

    out = Path(args.outdir); out.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.trend_csv)
    cols = {c.lower():c for c in df.columns}
    date_col = cols.get("date") or df.columns[0]
    token_col= cols.get("token") or df.columns[1]
    count_col= cols.get("count") or df.columns[2]
    df = df.rename(columns={date_col:"date", token_col:"token", count_col:"count"})[["date","token","count"]]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0)

    dmin, dmax = df["date"].min().date(), df["date"].max().date()
    all_days = pd.date_range(dmin, dmax, freq="D")
    top_tokens = (df.groupby("token")["count"].sum().sort_values(ascending=False).head(args.topk).index.tolist())

    piv = (df[df["token"].isin(top_tokens)]
           .pivot_table(index="date", columns="token", values="count", aggfunc="sum", fill_value=0)
           .reindex(all_days)
           .fillna(0)
           .sort_index())

    sma7 = piv.rolling(7, min_periods=3).mean().add_suffix("_sma7")
    std7 = piv.rolling(7, min_periods=3).std(ddof=0)
    z = ((piv - piv.rolling(7, min_periods=3).mean()) / std7.replace(0, np.nan)).add_suffix("_z")

    x = np.arange(len(piv.index))
    slopes = {}
    for c in piv.columns:
        y = piv[c].values.astype(float)
        m = y.mean()
        slopes[c] = 0.0 if m==0 else float(np.polyfit(x, y, 1)[0]/(m+1e-9))
    slopes = pd.Series(slopes, name="norm_slope").sort_values(ascending=False)

    piv.to_csv(out/"pivot_counts.csv")
    sma7.to_csv(out/"pivot_sma7.csv")
    z.to_csv(out/"pivot_z.csv")
    slopes.to_csv(out/"pivot_norm_slope.csv", header=True)

    plt.figure(figsize=(10,4))
    for c in slopes.head(5).index.tolist():
        plt.plot(piv.index, piv[c], label=c)
    plt.title("Top 5 rising tokens (counts)")
    plt.xlabel("date"); plt.ylabel("count"); plt.legend(); plt.tight_layout()
    plt.savefig(out/"trend_top5_counts.png", dpi=150); plt.close()

if __name__ == "__main__":
    main()