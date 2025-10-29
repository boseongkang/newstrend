import json
from pathlib import Path
import argparse
import pandas as pd
import numpy as np

def ensure(p): p.mkdir(parents=True, exist_ok=True); return p

def to_iso(d):
    dt = pd.to_datetime(d, errors="coerce")
    if isinstance(dt, pd.Series):
        if hasattr(dt, "dt"):
            try:
                dt = dt.dt.tz_localize(None)
            except TypeError:
                dt = dt.dt.tz_convert(None)
    elif isinstance(dt, pd.DatetimeIndex):
        try:
            dt = dt.tz_localize(None)
        except TypeError:
            dt = dt.tz_convert(None)
    return [x.strftime("%Y-%m-%d") for x in dt]

def load_terms_matrix(tokens_csv, top_terms_csv=None, keep_top=100):
    df = pd.read_csv(tokens_csv)
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    base = df.groupby(["date","term"])["count"].sum().reset_index()
    if top_terms_csv and Path(top_terms_csv).exists():
        tops = pd.read_csv(top_terms_csv)
        tops = [str(x) for x in tops["term"].astype(str).tolist()[:50]]
    else:
        tops = []
    totals = base.groupby("term")["count"].sum().sort_values(ascending=False)
    head = [str(x) for x in totals.head(keep_top).index.tolist()]
    want = list(dict.fromkeys(tops + head))
    mat = (
        base[base["term"].isin(want)]
        .pivot(index="date", columns="term", values="count")
        .sort_index()
        .fillna(0.0)
    )
    return mat

def load_prices(price_csv):
    pth = Path(price_csv)
    if not pth.exists():
        return None
    p = pd.read_csv(pth)
    norm = {c.lower().strip(): c for c in p.columns}
    def pick(cands):
        for k in cands:
            if k in norm:
                return norm[k]
        return None
    date_col   = pick(["date", "datetime", "day"])
    ticker_col = pick(["ticker", "symbol"])
    price_col  = pick(["close", "adj_close", "adj close", "adjclose",
                       "price", "close_price", "closing_price"])

    if not date_col or not ticker_col or not price_col:
        raise ValueError(f"price csv columns not found. got={list(p.columns)} "
                         f"need one of date:[date|datetime], ticker:[ticker|symbol], "
                         f"price:[close|adj_close|price...]")

    p[date_col] = pd.to_datetime(p[date_col]).dt.tz_localize(None)
    g = (
        p.groupby([date_col, ticker_col])[price_col]
         .last()
         .reset_index()
    )
    piv = (
        g.pivot(index=date_col, columns=ticker_col, values=price_col)
         .sort_index()
         .astype(float)
         .ffill()
    )
    return piv

def write_json(obj, path): Path(path).write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")

def main(run_dir, out_dir):
    run = Path(run_dir)
    out = ensure(Path(out_dir))
    data = ensure(out / "data")

    tokens_csv = run / "tokens_by_day.cleaned.csv"
    rising_top_csv = run / "rising_csv" / "rising_terms_top.csv"
    prices_csv = run / "prices_join" / "ticker_daily.csv"
    art_csv = run / "aggregate" / "articles_by_day.csv"

    terms_mat = load_terms_matrix(tokens_csv, rising_top_csv, keep_top=120)
    dates = to_iso(terms_mat.index)
    series = {c: [float(x) for x in terms_mat[c].to_numpy()] for c in terms_mat.columns}
    top_terms = list(terms_mat.sum().sort_values(ascending=False).head(20).index)

    trends_json = {"dates": dates, "terms": list(terms_mat.columns), "series": series, "top": top_terms}
    write_json(trends_json, data / "trends.json")

    prices_mat = load_prices(prices_csv)
    if prices_mat is not None:
        p_json = {
            "dates": to_iso(prices_mat.index),
            "tickers": list(prices_mat.columns),
            "close": {c: [float(x) for x in prices_mat[c].to_numpy()] for c in prices_mat.columns},
        }
        write_json(p_json, data / "prices.json")

    if Path(art_csv).exists():
        ad = pd.read_csv(art_csv)
        ad["date"] = pd.to_datetime(ad["date"]).dt.tz_localize(None)
        ad = ad.groupby("date")["articles"].sum().reset_index().sort_values("date")
        write_json({"dates": to_iso(ad["date"]), "articles": [int(x) for x in ad["articles"].to_numpy()]}, data / "articles.json")

    html = (Path(__file__).parent / "static_dashboard.html").read_text(encoding="utf-8")
    (out / "index.html").write_text(html, encoding="utf-8")
    print(f"saved -> {out}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--out", default="site")
    args = ap.parse_args()
    main(args.run, args.out)