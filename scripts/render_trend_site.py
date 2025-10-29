import json, os
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.io import to_html

def read_csv(p):
    p = Path(p)
    return pd.read_csv(p) if p.exists() else None

def safe_date(s):
    try:
        return pd.to_datetime(s).dt.tz_localize(None)
    except Exception:
        return pd.to_datetime(s, errors="coerce")

def fig_articles_by_day(df):
    df = df.copy()
    df["date"] = safe_date(df["date"])
    df = df.sort_values("date")
    return px.bar(df, x="date", y="articles", title="Articles per day")

def fig_terms_heatmap(tokens, last_days=30, topn=30):
    t = tokens.copy()
    t["date"] = safe_date(t["date"])
    mx = t["date"].max()
    if pd.isna(mx):
        return None
    t = t[t["date"] >= mx - pd.Timedelta(days=last_days - 1)]
    totals = t.groupby("term")["count"].sum().sort_values(ascending=False).head(topn).index
    t = t[t["term"].isin(totals)]
    pvt = t.pivot_table(index="term", columns="date", values="count", aggfunc="sum", fill_value=0)
    pvt = pvt.loc[totals]
    fig = px.imshow(pvt.values, aspect="auto", labels=dict(x="date", y="term", color="count"),
                    x=[d.strftime("%Y-%m-%d") for d in pvt.columns], y=pvt.index,
                    title=f"Top {topn} terms heatmap (last {last_days} days)")
    return fig

def fig_trend_lines(trend_ts, top_terms=None):
    df = trend_ts.copy()
    df["date"] = safe_date(df["date"])
    if top_terms:
        df = df[df["term"].isin(top_terms)]
    fig = go.Figure()
    for term, g in df.groupby("term"):
        g = g.sort_values("date")
        fig.add_trace(go.Scatter(x=g["date"], y=g["count"], mode="lines", name=str(term)))
    fig.update_layout(title="Selected term trends", xaxis_title="date", yaxis_title="count")
    return fig

def fig_prices_lines(ticker_daily):
    df = ticker_daily.copy()
    df["date"] = safe_date(df["date"])
    fig = go.Figure()
    for tk, g in df.groupby("ticker"):
        g = g.sort_values("date")
        fig.add_trace(go.Scatter(x=g["date"], y=g["close"], mode="lines", name=str(tk)))
    fig.update_layout(title="Prices (close)", xaxis_title="date", yaxis_title="close")
    return fig

def build_headlines(master_path, start=None, end=None, max_days=14, per_day=20):
    p = Path(master_path)
    if not p.exists():
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
    df["date"] = pd.to_datetime(df["date"])
    if start:
        df = df[df["date"] >= pd.to_datetime(start)]
    if end:
        df = df[df["date"] <= pd.to_datetime(end)]
    mx = df["date"].max()
    if pd.isna(mx):
        return "<p>No headlines in window.</p>"
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

    art = read_csv(run/"aggregate/articles_by_day.csv")
    tok = read_csv(run/"aggregate/tokens_by_day.csv")
    rising = read_csv(run/"rising_csv/rising_terms_top.csv")
    trend_ts = read_csv(run/"rising_csv/trend_selected_timeseries.csv")
    tkr_daily = read_csv(run/"prices_join/ticker_daily.csv")
    tkr_corr = read_csv(run/"prices_join/ticker_corr.csv")

    parts = []
    parts.append("<meta charset='utf-8'><style>body{font-family:system-ui,Arial,sans-serif;max-width:1100px;margin:24px auto;padding:0 10px}h1{margin-bottom:6px;color:#111}h2{margin-top:28px}table{border-collapse:collapse}td,th{border:1px solid #ddd;padding:6px 8px}</style>")
    parts.append("<h1>News Trends</h1>")

    if art is not None and len(art):
        fig = fig_articles_by_day(art)
        parts.append(to_html(fig, include_plotlyjs="inline", full_html=False))
    else:
        parts.append("<p>No articles_by_day.csv</p>")

    if tok is not None and len(tok):
        fig = fig_terms_heatmap(tok, last_days=last_days, topn=30)
        if fig:
            parts.append(to_html(fig, include_plotlyjs=False, full_html=False))
    else:
        parts.append("<p>No tokens_by_day.csv</p>")

    if trend_ts is not None and len(trend_ts):
        top_terms = None
        if rising is not None and "term" in rising.columns and len(rising):
            top_terms = rising["term"].head(10).tolist()
        fig = fig_trend_lines(trend_ts, top_terms=top_terms)
        parts.append(to_html(fig, include_plotlyjs=False, full_html=False))
    elif rising is not None and len(rising):
        parts.append(rising.head(30).to_html(index=False))

    if tkr_daily is not None and len(tkr_daily):
        figp = fig_prices_lines(tkr_daily)
        parts.append(to_html(figp, include_plotlyjs=False, full_html=False))
    if tkr_corr is not None and len(tkr_corr):
        parts.append("<h2>Ticker correlation</h2>")
        parts.append(tkr_corr.to_html(index=False))

    parts.append(build_headlines(master_path, last_days=min(last_days, 14)))

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