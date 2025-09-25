import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tokens-csv", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--start", default="")
    ap.add_argument("--end", default="")
    ap.add_argument("--window", type=int, default=7, help="rolling window for SMA/Z")
    ap.add_argument("--min-total", type=int, default=50, help="min total count across the windowed period")
    ap.add_argument("--topk", type=int, default=200)
    return ap.parse_args()

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    date_col  = cols.get("date")  or df.columns[0]
    token_col = cols.get("term")  or cols.get("token") or df.columns[1]
    count_col = cols.get("count") or df.columns[2]
    df = df.rename(columns={date_col:"date", token_col:"term", count_col:"count"})[["date","term","count"]]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).copy()
    df["term"] = df["term"].astype(str).str.strip()
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(float)
    return df

def main():
    args = parse_args()
    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(args.tokens_csv)
    df = normalize_columns(df)

    if args.start:
        df = df[df["date"] >= pd.to_datetime(args.start)]
    if args.end:
        df = df[df["date"] <= pd.to_datetime(args.end)]

    if df.empty:
        raise SystemExit("No rows after filtering dates. Check --start/--end or input CSV.")

    dmin, dmax = df["date"].min().date(), df["date"].max().date()
    all_days = pd.date_range(dmin, dmax, freq="D")
    piv = (df.pivot_table(index="date", columns="term", values="count", aggfunc="sum", fill_value=0)
             .reindex(all_days).fillna(0).sort_index())

    totals = piv.sum(axis=0)
    piv = piv.loc[:, totals >= max(1, min(args.min_total, totals.max()))]
    if piv.shape[1] == 0:
        raise SystemExit("After min-total filter, no terms remain. Try lowering --min-total.")

    w = max(3, args.window)
    sma = piv.rolling(w, min_periods=max(2, w//2)).mean()
    std = piv.rolling(w, min_periods=max(2, w//2)).std(ddof=0).replace(0, np.nan)
    z = (piv - sma) / std

    x = np.arange(len(piv.index), dtype=float)
    slopes = {}
    for c in piv.columns:
        y = piv[c].values.astype(float)
        m = y.mean()
        if m <= 0:
            slopes[c] = 0.0
            continue
        try:
            slopes[c] = float(np.polyfit(x, y, 1)[0] / (m + 1e-9))
        except Exception:
            slopes[c] = 0.0
    s = pd.Series(slopes, name="norm_slope").sort_values(ascending=False)

    maxz = z.max(axis=0).rename("max_z").sort_values(ascending=False)

    last_counts = piv.iloc[-1]
    rising = (pd.concat([s, last_counts.rename("last_count")], axis=1)
                .join(piv.mean(axis=0).rename("mean"))
                .sort_values("norm_slope", ascending=False)
                .head(args.topk)
                .reset_index()
                .rename(columns={"index":"term"}))
    bursty = (pd.concat([maxz, last_counts.rename("last_count")], axis=1)
                .sort_values("max_z", ascending=False)
                .head(args.topk)
                .reset_index()
                .rename(columns={"index":"term"}))

    sel_terms = set(rising["term"].head(min(30, args.topk)).tolist())
    ts = piv.loc[:, list(sel_terms)].copy()
    ts_sma = sma.loc[:, list(sel_terms)].add_suffix("_sma7")
    ts_z = z.loc[:, list(sel_terms)].add_suffix("_z")
    long = (pd.concat([ts, ts_sma, ts_z], axis=1)
              .reset_index().rename(columns={"index":"date"}))
    long["date"] = long["date"].dt.strftime("%Y-%m-%d")

    rising.to_csv(outdir/"rising_terms_top.csv", index=False)
    bursty.to_csv(outdir/"bursty_terms_top.csv", index=False)
    long.to_csv(outdir/"trend_selected_timeseries.csv", index=False)
    (outdir/"summary.json").write_text(json.dumps({
        "input": Path(args.tokens_csv).as_posix(),
        "start": args.start, "end": args.end,
        "window": w, "min_total": args.min_total, "topk": args.topk,
        "dates": {"min": str(dmin), "max": str(dmax)},
        "n_terms_after_filter": int(piv.shape[1]),
    }, indent=2), encoding="utf-8")
    print("done:", outdir)

if __name__ == "__main__":
    main()