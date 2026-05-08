import json, os
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.io import to_html

def read_csv(p):
    p = Path(p)
    try:
        if p.exists() and p.stat().st_size > 0:
            df = pd.read_csv(p)
            if len(df):
                return df
    except Exception:
        pass
    return None

def safe_date_col(df):
    for c in ["date","Date","ds","time","timestamp","published_at"]:
        if c in df.columns:
            s = pd.to_datetime(df[c], errors="coerce")
            if s.notna().any():
                return s.dt.tz_localize(None)
    return pd.Series(pd.NaT, index=df.index)

def pick_col(df, targets):
    for c in targets:
        if c in df.columns:
            return c
    return None

def standardize_tokens(df):
    if df is None or df.empty:
        return None
    df = df.copy()
    c_date = pick_col(df, ["date","Date","ds"])
    c_term = pick_col(df, ["term","token","word","ngram","keyword"])
    c_cnt  = pick_col(df, ["count","freq","value","n"])
    if not c_date or not c_term or not c_cnt:
        return None
    df["date"] = pd.to_datetime(df[c_date], errors="coerce").dt.tz_localize(None)
    df["term"] = df[c_term].astype(str)
    df["count"] = pd.to_numeric(df[c_cnt], errors="coerce").fillna(0)
    df = df.dropna(subset=["date"])
    return df[["date","term","count"]]

def standardize_trend_ts(df):
    if df is None or df.empty:
        return None
    df = df.copy()
    c_date = pick_col(df, ["date","Date","ds"])
    c_term = pick_col(df, ["term","token","word","ngram","keyword","series"])
    c_cnt  = pick_col(df, ["count","freq","value","n","y"])
    if not c_date or not c_term or not c_cnt:
        return None
    df["date"] = pd.to_datetime(df[c_date], errors="coerce").dt.tz_localize(None)
    df["term"] = df[c_term].astype(str)
    df["count"] = pd.to_numeric(df[c_cnt], errors="coerce").fillna(0)
    df = df.dropna(subset=["date"])
    return df[["date","term","count"]]

def standardize_rising(df):
    if df is None or df.empty:
        return None
    df = df.copy()
    c_term = pick_col(df, ["term","token","word","ngram","keyword"])
    if not c_term:
        return None
    df["term"] = df[c_term].astype(str)
    return df

def standardize_articles(df):
    if df is None or df.empty:
        return None
    df = df.copy()
    c_date = pick_col(df, ["date","Date","ds"])
    c_cnt  = pick_col(df, ["articles","count","n"])
    if not c_date or not c_cnt:
        return None
    df["date"] = pd.to_datetime(df[c_date], errors="coerce").dt.tz_localize(None)
    df["articles"] = pd.to_numeric(df[c_cnt], errors="coerce").fillna(0)
    df = df.dropna(subset=["date"])
    return df[["date","articles"]]

def standardize_ticker_daily(df):
    if df is None or df.empty:
        return None
    df = df.copy()
    c_tk = pick_col(df, ["ticker","symbol"])
    c_date = pick_col(df, ["date","Date"])
    c_close = pick_col(df, ["close","Close","adj_close","Adj Close","AdjClose"])
    if not c_tk or not c_date or not c_close:
        return None
    df["ticker"] = df[c_tk].astype(str)
    df["date"] = pd.to_datetime(df[c_date], errors="coerce").dt.tz_localize(None)
    df["close"] = pd.to_numeric(df[c_close], errors="coerce")
    df = df.dropna(subset=["date","close"])
    return df[["date","ticker","close"]]

def fig_articles_by_day(art):
    art = art.sort_values("date")
    return px.bar(art, x="date", y="articles", title="Articles per day")

def fig_terms_heatmap(tokens, last_days=30, topn=30):
    if tokens is None or tokens.empty:
        return None
    t = tokens.copy()
    mx = t["date"].max()
    if pd.isna(mx):
        return None
    t = t[t["date"] >= mx - pd.Timedelta(days=last_days - 1)]
    totals = t.groupby("term")["count"].sum().sort_values(ascending=False).head(topn).index
    t = t[t["term"].isin(totals)]
    if t.empty:
        return None
    pvt = t.pivot_table(index="term", columns="date", values="count", aggfunc="sum", fill_value=0)
    pvt = pvt.loc[totals]
    fig = px.imshow(
        pvt.values,
        aspect="auto",
        labels=dict(x="date", y="term", color="count"),
        x=[d.strftime("%Y-%m-%d") for d in pvt.columns],
        y=pvt.index,
        title=f"Top {topn} terms heatmap (last {last_days} days)",
    )
    return fig

def fig_trend_lines(trend_ts, top_terms=None):
    if trend_ts is None or trend_ts.empty:
        return None
    df = trend_ts.copy()
    if top_terms:
        df = df[df["term"].isin(top_terms)]
    if df.empty:
        return None
    fig = go.Figure()
    for term, g in df.groupby("term"):
        g = g.sort_values("date")
        fig.add_trace(go.Scatter(x=g["date"], y=g["count"], mode="lines", name=str(term)))
    fig.update_layout(title="Selected term trends", xaxis_title="date", yaxis_title="count")
    return fig

def fig_prices_lines(ticker_daily):
    if ticker_daily is None or ticker_daily.empty:
        return None
    df = ticker_daily.sort_values(["ticker","date"])
    fig = go.Figure()
    for tk, g in df.groupby("ticker"):
        fig.add_trace(go.Scatter(x=g["date"], y=g["close"], mode="lines", name=str(tk)))
    fig.update_layout(title="Prices (close)", xaxis_title="date", yaxis_title="close")
    return fig

def build_headlines(master_path, start=None, end=None, max_days=14, per_day=20):
    p = Path(master_path)
    if not p.exists() or p.stat().st_size == 0:
        return "<p>No headlines: master.jsonl missing.</p>"
    rows = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
            except Exception:
                continue
            date = (row.get("date") or row.get("published_at") or row.get("published") or "")[:10]
            url = row.get("url") or ""
            title = row.get("title") or ""
            if not date or not title:
                continue
            rows.append((date, title, url))
    if not rows:
        return "<p>No headlines in master.</p>"
    df = pd.DataFrame(rows, columns=["date", "title", "url"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    if start:
        df = df[df["date"] >= pd.to_datetime(start)]
    if end:
        df = df[df["date"] <= pd.to_datetime(end)]
    if df.empty:
        return "<p>No headlines in window.</p>"
    mx = df["date"].max()
    df = df[df["date"] >= mx - pd.Timedelta(days=max_days - 1)]
    html = ["<h2>Daily headlines</h2>"]
    for d, g in df.sort_values(["date"]).groupby(df["date"].dt.date):
        html.append(f"<h3>{d}</h3><ul>")
        for _, r in g.head(per_day).iterrows():
            url = r["url"] if isinstance(r["url"], str) else ""
            ttl = str(r["title"])
            if url:
                html.append(f"<li><a href='{url}' target='_blank' rel='noopener'>{ttl}</a></li>")
            else:
                html.append(f"<li>{ttl}</li>")
        html.append("</ul>")
    return "\n".join(html)

def main(run_dir, master_path, outdir, last_days=30):
    run = Path(run_dir)
    out = Path(outdir)
    out.mkdir(parents=True, exist_ok=True)

    art_raw = read_csv(run/"aggregate/articles_by_day.csv")
    tok_raw = read_csv(run/"aggregate/tokens_by_day.csv")
    rising_raw = read_csv(run/"rising_csv/rising_terms_top.csv")
    trend_ts_raw = read_csv(run/"rising_csv/trend_selected_timeseries.csv")
    tkr_daily_raw = read_csv(run/"prices_join/ticker_daily.csv")
    tkr_corr = read_csv(run/"prices_join/ticker_corr.csv")

    art = standardize_articles(art_raw) if art_raw is not None else None
    tok = standardize_tokens(tok_raw) if tok_raw is not None else None
    rising = standardize_rising(rising_raw) if rising_raw is not None else None
    trend_ts = standardize_trend_ts(trend_ts_raw) if trend_ts_raw is not None else None
    tkr_daily = standardize_ticker_daily(tkr_daily_raw) if tkr_daily_raw is not None else None

    parts = []
    parts.append("<meta charset='utf-8'><style>body{font-family:system-ui,Arial,sans-serif;max-width:1100px;margin:24px auto;padding:0 10px}h1{margin-bottom:6px;color:#111}h2{margin-top:28px}table{border-collapse:collapse}td,th{border:1px solid #ddd;padding:6px 8px}</style>")
    parts.append("<h1>News Trends</h1>")

    if art is not None and len(art):
        parts.append(to_html(fig_articles_by_day(art), include_plotlyjs="inline", full_html=False))
    else:
        parts.append("<p>No articles_by_day.csv</p>")

    heat = fig_terms_heatmap(tok, last_days=last_days, topn=30) if tok is not None else None
    if heat:
        parts.append(to_html(heat, include_plotlyjs=False, full_html=False))

    top_terms = None
    if rising is not None and "term" in rising.columns and len(rising):
        top_terms = rising["term"].head(10).tolist()

    trend_fig = fig_trend_lines(trend_ts, top_terms=top_terms)
    if trend_fig:
        parts.append(to_html(trend_fig, include_plotlyjs=False, full_html=False))

    price_fig = fig_prices_lines(tkr_daily)
    if price_fig:
        parts.append(to_html(price_fig, include_plotlyjs=False, full_html=False))
    if tkr_corr is not None and len(tkr_corr):
        parts.append("<h2>Ticker correlation</h2>")
        parts.append(tkr_corr.to_html(index=False))

    parts.append(build_headlines(master_path, max_days=min(last_days, 14)))
    (out/"index.html").write_text("\n".join(parts), encoding="utf-8")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--master", required=True)
    ap.add_argument("--outdir", default="site")
    ap.add_argument("--last-days", type=int, default=30)
    a = ap.parse_args()
    main(a.run, a.master, a.outdir, a.last_days)