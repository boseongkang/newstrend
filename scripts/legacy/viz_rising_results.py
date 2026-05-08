import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rising-dir", required=True)  # reports/.../rising_csv
    ap.add_argument("--topn", type=int, default=10)
    args = ap.parse_args()

    d = Path(args.rising_dir)
    rising = pd.read_csv(d/"rising_terms_top.csv")
    bursty = pd.read_csv(d/"bursty_terms_top.csv")
    ts = pd.read_csv(d/"trend_selected_timeseries.csv", parse_dates=["date"])

    out = d/"figs"; out.mkdir(parents=True, exist_ok=True)

    top_rise = rising.head(args.topn)["term"].tolist()
    for term in top_rise:
        cols = [c for c in ts.columns if c==term or c==f"{term}_sma7"]
        if not cols: continue
        sub = ts[["date"]+cols].copy().sort_values("date")
        plt.figure(figsize=(10,4))
        for c in cols:
            plt.plot(sub["date"], sub[c], label=c)
        plt.title(f"Rising · {term}")
        plt.xlabel("date"); plt.ylabel("count")
        plt.legend(); plt.tight_layout()
        plt.savefig(out/f"rising_{term}.png", dpi=150); plt.close()

    top_burst = bursty.head(args.topn)["term"].tolist()
    for term in top_burst:
        cols = [c for c in ts.columns if c==term or c==f"{term}_sma7" or c==f"{term}_z"]
        if not cols: continue
        sub = ts[["date"]+cols].copy().sort_values("date")
        plt.figure(figsize=(10,4))
        for c in cols:
            plt.plot(sub["date"], sub[c], label=c)
        plt.title(f"Burst · {term}")
        plt.xlabel("date"); plt.ylabel("value")
        plt.legend(); plt.tight_layout()
        plt.savefig(out/f"burst_{term}.png", dpi=150); plt.close()

    print(f"saved figures -> {out}")

if __name__ == "__main__":
    main()