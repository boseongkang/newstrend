import argparse, json, sys
from pathlib import Path
import pandas as pd
import numpy as np

def log(*a): print(*a, file=sys.stderr, flush=True)

def load_terms(terms_csv: Path) -> pd.DataFrame:
    log(f"[load_terms] {terms_csv}")
    df = pd.read_csv(terms_csv)
    need = {"date","term","count"}
    if not need.issubset(df.columns):
        raise ValueError(f"terms CSV needs columns {need}, got {list(df.columns)}")
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None).dt.normalize()
    df = df.dropna(subset=["date"])
    df["term"] = df["term"].astype(str).str.lower()
    df = df.groupby(["date","term"], as_index=False)["count"].sum()
    log(f"[load_terms] rows={len(df)}, days={df['date'].nunique()}, terms={df['term'].nunique()}")
    return df

def load_alias_map(alias_json: Path) -> pd.DataFrame:
    log(f"[load_alias_map] {alias_json}")
    m = json.loads(Path(alias_json).read_text(encoding="utf-8"))
    rows=[]
    for ticker, aliases in m.items():
        for a in aliases:
            rows.append({"ticker": str(ticker).upper(), "term": str(a).lower()})
    alias_df = pd.DataFrame(rows).drop_duplicates()
    if alias_df.empty:
        raise ValueError("Alias map is empty.")
    log(f"[load_alias_map] tickers={alias_df['ticker'].nunique()}, alias_rows={len(alias_df)}")
    return alias_df

def build_ticker_counts(terms_df: pd.DataFrame, alias_df: pd.DataFrame) -> pd.DataFrame:
    m = terms_df.merge(alias_df, on="term", how="inner")
    counts = (m.groupby(["date","ticker"], as_index=False)["count"]
                .sum().sort_values(["ticker","date"]))
    log(f"[build_ticker_counts] matched_tickers={counts['ticker'].nunique()}, rows={len(counts)}")
    return counts

def fetch_prices(tickers, start, end) -> pd.DataFrame:
    import yfinance as yf
    end_plus = pd.to_datetime(end) + pd.Timedelta(days=1)  # yfinance end-exclusive
    log(f"[fetch_prices] tickers={len(tickers)} range={start}..{end} (+1d)")
    data = yf.download(tickers, start=pd.to_datetime(start), end=end_plus,
                       auto_adjust=True, progress=False)
    if data is None or len(data) == 0:
        raise ValueError("Empty price frame from yfinance.")
    # pick Adj Close or Close
    if isinstance(data.columns, pd.MultiIndex):
        lvl0 = data.columns.get_level_values(0)
        if "Adj Close" in set(lvl0):
            px = data["Adj Close"].copy()
        elif "Close" in set(lvl0):
            px = data["Close"].copy()
        else:
            raise ValueError(f"Unexpected columns: {data.columns[:5]}")
    else:
        px = data
    px = px.rename_axis("date").reset_index()
    long = px.melt(id_vars="date", var_name="ticker", value_name="price")
    long["ticker"] = long["ticker"].astype(str).str.upper()
    long["date"] = pd.to_datetime(long["date"]).dt.tz_localize(None).dt.normalize()
    long = long.sort_values(["ticker","date"])
    long["ret"] = long.groupby("ticker")["price"].pct_change()
    log(f"[fetch_prices] rows={len(long)}, days={long['date'].nunique()}")
    return long[["date","ticker","price","ret"]]

def compute_corr(daily: pd.DataFrame) -> pd.DataFrame:
    out=[]
    for t, g in daily.sort_values("date").groupby("ticker"):
        s_cnt = g["count"].astype(float)
        s_ret = g["ret"].astype(float)
        r0      = s_cnt.corr(s_ret)
        r_lead1 = s_cnt.corr(s_ret.shift(-1))
        r_lag1  = s_cnt.corr(s_ret.shift( 1))
        out.append({
            "ticker": t,
            "n_obs": int(s_ret.notna().sum()),
            "corr_same_day": np.nan if pd.isna(r0) else float(r0),
            "corr_lead1_next_return": np.nan if pd.isna(r_lead1) else float(r_lead1),
            "corr_lag1_prev_return":  np.nan if pd.isna(r_lag1) else float(r_lag1),
            "avg_count": float(s_cnt.mean())
        })
    corr = pd.DataFrame(out).sort_values("corr_lead1_next_return", ascending=False)
    log(f"[compute_corr] tickers={len(corr)}")
    return corr

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--terms", required=True)
    ap.add_argument("--map", required=True)
    ap.add_argument("--start", default="")
    ap.add_argument("--end",   default="")
    ap.add_argument("--out", required=True)
    ap.add_argument("--min-days", type=int, default=3, help="require >= this many trading days per ticker")
    args = ap.parse_args()

    outdir = Path(args.out); outdir.mkdir(parents=True, exist_ok=True)

    terms_df = load_terms(Path(args.terms))
    alias_df = load_alias_map(Path(args.map))
    counts   = build_ticker_counts(terms_df, alias_df)

    if counts.empty:
        log("[warn] no alias matched terms. Writing empty shells.")
        pd.DataFrame(columns=["date","ticker","count","price","ret"]).to_csv(outdir/"ticker_daily.csv", index=False)
        pd.DataFrame(columns=["ticker","n_obs","corr_same_day","corr_lead1_next_return","corr_lag1_prev_return","avg_count"]).to_csv(outdir/"ticker_corr.csv", index=False)
        sys.exit(0)

    start = args.start or counts["date"].min().strftime("%Y-%m-%d")
    end   = args.end   or counts["date"].max().strftime("%Y-%m-%d")
    log(f"[range] {start}..{end}")

    tickers = sorted(counts["ticker"].unique().tolist())
    prices  = fetch_prices(tickers, start, end)

    daily = counts.merge(prices, on=["date","ticker"], how="left")
    keep = (daily.groupby("ticker")["price"].apply(lambda s: s.notna().sum()) >= args.min_days)
    keep_tickers = set(keep[keep].index)
    daily = daily[daily["ticker"].isin(keep_tickers)].copy()

    daily.sort_values(["ticker","date"]).to_csv(outdir/"ticker_daily.csv", index=False)
    corr = compute_corr(daily) if not daily.empty else pd.DataFrame(columns=["ticker","n_obs","corr_same_day","corr_lead1_next_return","corr_lag1_prev_return","avg_count"])
    corr.to_csv(outdir/"ticker_corr.csv", index=False)

    log(f"[saved] {outdir/'ticker_daily.csv'} rows={len(daily)}")
    log(f"[saved] {outdir/'ticker_corr.csv'} rows={len(corr)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[error] {e.__class__.__name__}: {e}")
        sys.exit(2)