import json
from pathlib import Path
import argparse
import pandas as pd
import numpy as np

def ensure(p):
    p = Path(p)
    p.mkdir(parents=True, exist_ok=True)
    return p

def js_num(x):
    try:
        v = float(x)
    except Exception:
        return None
    if not np.isfinite(v):
        return None
    return v

def to_iso(obj):
    s = pd.to_datetime(obj, errors="coerce")
    if isinstance(s, pd.Series):
        s = s.dt.tz_localize(None)
        return [d.strftime("%Y-%m-%d") for d in s]
    if isinstance(s, pd.DatetimeIndex):
        s = s.tz_localize(None)
        return [d.strftime("%Y-%m-%d") for d in s]
    s = pd.to_datetime([obj], errors="coerce").tz_localize(None)
    return [s[0].strftime("%Y-%m-%d")]

def write_json(obj, path):
    Path(path).write_text(json.dumps(obj, ensure_ascii=False, allow_nan=False), encoding="utf-8")

def load_terms_matrix(tokens_csv, top_terms_csv=None, keep_top=100):
    df = pd.read_csv(tokens_csv)
    if "date" not in df.columns or "term" not in df.columns or "count" not in df.columns:
        raise ValueError("tokens csv must have columns: date, term, count")
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    base = df.groupby(["date", "term"])["count"].sum().reset_index()
    tops = []
    if top_terms_csv and Path(top_terms_csv).exists():
        tdf = pd.read_csv(top_terms_csv)
        if "term" in tdf.columns:
            tops = [str(x) for x in tdf["term"].astype(str).tolist()[:50]]
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
    def pick(opts):
        for k in opts:
            if k in norm:
                return norm[k]
        return None
    date_col = pick(["date", "datetime", "day"])
    ticker_col = pick(["ticker", "symbol"])
    price_col = pick(["close", "adj_close", "adj close", "adjclose", "price", "close_price", "closing_price"])
    if not date_col or not ticker_col or not price_col:
        raise ValueError(f"price csv columns not found: have={list(p.columns)}")
    p[date_col] = pd.to_datetime(p[date_col]).dt.tz_localize(None)
    g = p.groupby([date_col, ticker_col])[price_col].last().reset_index()
    piv = (
        g.pivot(index=date_col, columns=ticker_col, values=price_col)
        .sort_index()
        .astype(float)
        .ffill()
    )
    return piv

def load_articles(csv_path):
    pth = Path(csv_path)
    if not pth.exists():
        return None
    df = pd.read_csv(pth)
    norm = {c.lower().strip(): c for c in df.columns}
    date_col = None
    for k in ["date", "day", "ds"]:
        if k in norm:
            date_col = norm[k]
            break
    art_col = None
    for k in ["articles", "count", "n", "num"]:
        if k in norm:
            art_col = norm[k]
            break
    if not date_col or not art_col:
        return None
    df[date_col] = pd.to_datetime(df[date_col]).dt.tz_localize(None)
    out = df.groupby(date_col)[art_col].sum().reset_index().sort_values(date_col)
    out.columns = ["date", "articles"]
    return out

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
    series = {c: [js_num(x) for x in terms_mat[c].to_numpy(dtype=float)] for c in terms_mat.columns}
    top_terms = list(terms_mat.sum().sort_values(ascending=False).head(20).index)
    write_json({"dates": dates, "terms": list(terms_mat.columns), "series": series, "top": top_terms}, data / "trends.json")

    prices_mat = load_prices(prices_csv)
    if prices_mat is not None:
        p_json = {
            "dates": to_iso(prices_mat.index),
            "tickers": list(prices_mat.columns),
            "close": {c: [js_num(v) for v in prices_mat[c].to_numpy(dtype=float)] for c in prices_mat.columns},
        }
        write_json(p_json, data / "prices.json")

    ad = load_articles(art_csv)
    if ad is not None and len(ad):
        write_json({"dates": to_iso(ad["date"]), "articles": [int(x) if np.isfinite(x) else 0 for x in ad["articles"].to_numpy()]}, data / "articles.json")

    html_src = Path(__file__).parent / "static_dashboard.html"
    if html_src.exists():
        (out / "index.html").write_text(html_src.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"saved -> {out}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--out", default="site")
    args = ap.parse_args()
    main(args.run, args.out)