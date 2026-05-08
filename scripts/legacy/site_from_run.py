import argparse, os
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def ensure(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p

def write_html(fig, path: Path, title: str):
    fig.update_layout(title=title, margin=dict(l=50,r=30,t=60,b=40))
    fig.write_html(str(path), include_plotlyjs="cdn", full_html=True)

def page_index(run: Path, out: Path, links: list[tuple[str,str]]):
    title = f"News Trends – {run.name}"
    idx = [
        "<!doctype html><meta charset='utf-8'>",
        f"<title>{title}</title>",
        "<style>body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;max-width:980px;margin:40px auto;padding:0 16px;}</style>",
        f"<h1>{title}</h1>",
        "<ul>",
        *[f"<li><a href='{href}'>{label}</a></li>" for label, href in links],
        "</ul>",
        "<hr/>",
        f"<p>Source run: <code>{run.as_posix()}</code></p>"
    ]
    (out / "index.html").write_text("\n".join(idx), encoding="utf-8")

def build_site(run_dir: str, out_dir: str):
    run = Path(run_dir)
    out = ensure(Path(out_dir))
    links = []

    art_csv = run / "aggregate" / "articles_by_day.csv"
    if art_csv.exists():
        df = pd.read_csv(art_csv, parse_dates=["date"])
        fig = px.bar(df, x="date", y="articles")
        write_html(fig, out / "articles_by_day.html", "Articles per Day")
        links.append(("Articles by Day", "articles_by_day.html"))

    rise_csv = run / "rising_csv" / "rising_terms_top.csv"
    if rise_csv.exists():
        r = pd.read_csv(rise_csv)
        r = r.head(50)
        fig = px.bar(r.sort_values("score", ascending=True),
                     x="score", y="term", orientation="h")
        write_html(fig, out / "rising_terms_top.html", "Rising Terms (Top)")
        links.append(("Rising Terms (Top 50)", "rising_terms_top.html"))

    corr_csv = run / "prices_join" / "ticker_corr.csv"
    daily_csv = run / "prices_join" / "ticker_daily.csv"

    if corr_csv.exists():
        c = pd.read_csv(corr_csv)
        c.to_csv(out / "ticker_corr.csv", index=False)
        fig = px.bar(c.sort_values("corr_lead1_next_return", ascending=False),
                     x="ticker", y="corr_lead1_next_return")
        write_html(fig, out / "ticker_corr.html", "News→Next-Day Return Corr (lead1)")
        links.append(("Ticker Corr (lead1)", "ticker_corr.html"))
        links.append(("Ticker Corr (CSV)", "ticker_corr.csv"))

    if daily_csv.exists():
        d = pd.read_csv(daily_csv, parse_dates=["date"])
        last = d["date"].max()
        top = (d[d["date"]==last]
               .sort_values("count", ascending=False)
               .head(30)[["ticker","count"]])
        top.to_csv(out / "latest_news_spike.csv", index=False)
        links.append(("Latest News Spike (CSV)", "latest_news_spike.csv"))

        pivot = d.pivot_table(index="ticker", columns="date", values="count", aggfunc="sum").fillna(0)
        if not pivot.empty:
            fig = px.imshow(pivot, aspect="auto", labels=dict(x="date", y="ticker", color="count"))
            write_html(fig, out / "ticker_heatmap.html", "Ticker x Date News Count")
            links.append(("Ticker Heatmap", "ticker_heatmap.html"))

        for tkr, g in d.sort_values("date").groupby("ticker"):
            if g["price"].notna().sum() < 2:
                continue
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Bar(x=g["date"], y=g["count"], name="news count", opacity=0.5), secondary_y=False)
            fig.add_trace(go.Scatter(x=g["date"], y=g["price"], name="price"), secondary_y=True)
            fig.update_yaxes(title_text="count", secondary_y=False)
            fig.update_yaxes(title_text="price", secondary_y=True)
            write_html(fig, out / f"ticker_{tkr}.html", f"{tkr}: News Count vs Price")

        tlinks = [f"<li><a href='ticker_{t}.html'>{t}</a></li>" for t in sorted(d['ticker'].unique())]
        (out/"tickers.html").write_text(
            "<!doctype html><meta charset='utf-8'><h1>Tickers</h1><ul>"+ "\n".join(tlinks)+"</ul>", encoding="utf-8")
        links.append(("Tickers (detail pages)", "tickers.html"))

    page_index(run, out, links)
    print(f"site -> {out}/index.html")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--out", default="site")
    args = ap.parse_args()
    build_site(args.run, args.out)