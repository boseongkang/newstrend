import os, sys, argparse
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams["figure.dpi"] = 150

def ensure_dirs(p):
    p = Path(p)
    p.mkdir(parents=True, exist_ok=True)
    return p

def write_placeholder_png(path, msg):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(8, 2))
    plt.axis("off")
    plt.text(0.01, 0.5, msg, va="center", ha="left")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()

def load_daily(csv_path):
    p = Path(csv_path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame(columns=["date","entity","count"])
    try:
        df = pd.read_csv(p)
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=["date","entity","count"])
    cols = {c.lower(): c for c in df.columns}
    if "entity" not in cols and "name" in cols:
        df = df.rename(columns={cols["name"]: "entity"})
        cols["entity"] = "entity"
    if "date" not in cols and "dt" in cols:
        df = df.rename(columns={cols["dt"]: "date"})
        cols["date"] = "date"
    if "count" not in cols and "n" in cols:
        df = df.rename(columns={cols["n"]: "count"})
        cols["count"] = "count"
    if not {"date","entity","count"}.issubset(set(k.lower() for k in df.columns)):
        return pd.DataFrame(columns=["date","entity","count"])
    df["date"] = pd.to_datetime(df[cols.get("date","date")], errors="coerce").dt.tz_localize(None)
    df["entity"] = df[cols.get("entity","entity")].astype(str)
    df["count"] = pd.to_numeric(df[cols.get("count","count")], errors="coerce").fillna(0).astype(int)
    df = df.dropna(subset=["date"])
    return df[["date","entity","count"]]

def safe_slope(series):
    y = pd.to_numeric(series, errors="coerce").astype(float).to_numpy()
    x = np.arange(len(y), dtype=float)
    msk = np.isfinite(y)
    x, y = x[msk], y[msk]
    if x.size < 2:
        return 0.0
    if np.allclose(y, y[0], equal_nan=False):
        return 0.0
    try:
        m = np.polyfit(x, y, 1)[0]
        if not np.isfinite(m):
            return 0.0
        return float(m)
    except Exception:
        return 0.0

def top_today(df, k):
    if df.empty:
        return pd.DataFrame(columns=["entity","count","date"])
    day = df["date"].max()
    t = (
        df[df["date"] == day]
        .groupby("entity")["count"]
        .sum()
        .sort_values(ascending=False)
        .head(k)
        .reset_index()
    )
    t["date"] = day
    return t

def build_matrix(df):
    if df.empty:
        return pd.DataFrame()
    g = (
        df.groupby(["date", "entity"])["count"]
        .sum()
        .reset_index()
        .pivot(index="date", columns="entity", values="count")
        .sort_index()
        .fillna(0)
    )
    return g

def slopes_table(df, days, topk):
    if df.empty:
        return pd.DataFrame(columns=["entity","norm_slope","last_count","mean"]), pd.DataFrame()
    mx = pd.to_datetime(df["date"]).max()
    base = df[pd.to_datetime(df["date"]) >= mx - pd.Timedelta(days=days - 1)]
    grid = build_matrix(base)
    if grid.empty:
        return pd.DataFrame(columns=["entity","norm_slope","last_count","mean"]), grid
    s = {c: safe_slope(grid[c]) for c in grid.columns}
    out = (
        pd.DataFrame({"entity": list(s.keys()), "norm_slope": list(s.values())})
        .sort_values("norm_slope", ascending=False)
        .head(topk)
        .reset_index(drop=True)
    )
    last = grid.iloc[-1] if len(grid) else pd.Series(dtype=float)
    out["last_count"] = last.reindex(out["entity"]).fillna(0).astype(int).to_list()
    out["mean"] = grid.reindex(columns=out["entity"]).mean().fillna(0).to_list()
    return out, grid

def trend_subset(grid, entities, days):
    if grid.empty:
        return grid
    g = grid.tail(days).copy()
    g = g.reindex(columns=[e for e in entities if e in g.columns]).fillna(0)
    return g

def plot_top_overall(df, out_png, topk, days):
    if df.empty:
        write_placeholder_png(out_png, "no data")
        return
    mx = pd.to_datetime(df["date"]).max()
    base = df[pd.to_datetime(df["date"]) >= mx - pd.Timedelta(days=days - 1)]
    if base.empty:
        write_placeholder_png(out_png, "no data")
        return
    s = base.groupby("entity")["count"].sum().sort_values(ascending=False).head(topk)
    if s.empty:
        write_placeholder_png(out_png, "no data")
        return
    plt.figure(figsize=(9, 5))
    s[::-1].plot(kind="barh")
    plt.title(f"Top entities by total count (last {days}d)")
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()

def plot_slopes(sl_df, out_png):
    if sl_df.empty:
        write_placeholder_png(out_png, "no data")
        return
    plt.figure(figsize=(9, 5))
    s = sl_df.set_index("entity")["norm_slope"].iloc[::-1]
    s.plot(kind="barh")
    plt.title("Rising entities (slope)")
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()

def plot_trend(g, out_png):
    if g.empty or g.shape[1] == 0:
        write_placeholder_png(out_png, "no data")
        return
    plt.figure(figsize=(10, 5))
    for c in g.columns:
        y = g[c].to_numpy(dtype=float)
        if y.size == 0:
            continue
        plt.plot(g.index, y, lw=1)
        sma = pd.Series(y, index=g.index).rolling(7, min_periods=1).mean()
        plt.plot(g.index, sma, lw=2, alpha=0.8)
    plt.legend([f"{c}" for c in g.columns], fontsize=8, ncol=2, loc="upper left", frameon=False)
    plt.title("Selected entities (daily counts, with 7d SMA)")
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()

def plot_heat(g, out_png):
    if g.empty or g.shape[1] == 0:
        write_placeholder_png(out_png, "no data")
        return
    vals = g.to_numpy(dtype=float).T
    plt.figure(figsize=(10, 6))
    plt.imshow(vals, aspect="auto", interpolation="nearest")
    plt.yticks(np.arange(len(g.columns)), g.columns)
    plt.xticks(np.arange(len(g.index)), [d.strftime("%m-%d") for d in g.index], rotation=90)
    plt.colorbar(label="count")
    plt.title("Heatmap (entities x date)")
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()

def save_tables(outdir, today_df, slopes_df):
    (outdir / "entities_top_today.csv").write_text("") if today_df.empty else today_df.to_csv(outdir / "entities_top_today.csv", index=False)
    (outdir / "entities_slopes.csv").write_text("")    if slopes_df.empty else slopes_df.to_csv(outdir / "entities_slopes.csv", index=False)

def make_html(outdir, meta, images, tables):
    html = []
    html.append("<html><head><meta charset='utf-8'><title>Entities report</title>")
    html.append("<style>body{font-family:system-ui,Arial,sans-serif;background:#111;color:#d9d9d9} h2{margin-top:28px} table{border-collapse:collapse} td,th{border:1px solid #333;padding:6px 8px} a{color:#7ec8ff} .wrap{max-width:1100px;margin:20px auto;padding:8px}</style>")
    html.append("</head><body><div class='wrap'>")
    html.append("<h1>Entities report</h1>")
    html.append(f"<p>generated_at: {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%SZ')}</p>")
    if meta:
        html.append(f"<p>rows: {meta.get('rows','0')} | range: {meta.get('min_date','-')} .. {meta.get('max_date','-')}</p>")
    html.append("<h2>Images</h2><ul>")
    for k, rel in images.items():
        if (outdir / rel).exists():
            html.append(f"<li>{k}: <br><img src='{rel}' style='max-width:100%'></li>")
        else:
            html.append(f"<li>{k}: (missing)</li>")
    html.append("</ul>")
    html.append("<h2>Tables</h2>")
    for name, df in tables.items():
        html.append(f"<h3>{name}</h3>")
        if isinstance(df, pd.DataFrame) and not df.empty:
            html.append(df.to_html(index=False))
        else:
            html.append("<p>no data</p>")
    html.append("</div></body></html>")
    (outdir / "report.html").write_text("\n".join(html), encoding="utf-8")

def main(daily_csv, outdir, topk, slope_days, trend_days, heat_top):
    outdir = ensure_dirs(outdir)
    df = load_daily(daily_csv)
    if df.empty:
        write_placeholder_png(outdir / "top_overall.png", "no data")
        write_placeholder_png(outdir / "slopes.png", "no data")
        write_placeholder_png(outdir / "trend.png", "no data")
        write_placeholder_png(outdir / "heat.png", "no data")
        save_tables(outdir, pd.DataFrame(columns=["entity","count","date"]), pd.DataFrame(columns=["entity","norm_slope","last_count","mean"]))
        make_html(outdir, {"rows": 0, "min_date": "-", "max_date": "-"}, {"top_overall": "top_overall.png", "slopes": "slopes.png", "trend": "trend.png", "heat": "heat.png"}, {"today": pd.DataFrame(), "slopes": pd.DataFrame()})
        print(f"saved -> {outdir}")
        return
    meta = {"rows": len(df), "min_date": str(pd.to_datetime(df["date"]).min().date()), "max_date": str(pd.to_datetime(df["date"]).max().date())}
    today = top_today(df, topk)
    sl, grid_all = slopes_table(df, slope_days, topk)
    plot_top_overall(df, outdir / "top_overall.png", topk, trend_days)
    plot_slopes(sl, outdir / "slopes.png")
    sub_trend = trend_subset(grid_all, sl["entity"].tolist()[:min(10, len(sl))], trend_days)
    plot_trend(sub_trend, outdir / "trend.png")
    top_for_heat = grid_all.sum().sort_values(ascending=False).head(heat_top).index.tolist() if not grid_all.empty else []
    heat_g = trend_subset(grid_all, top_for_heat, trend_days) if top_for_heat else pd.DataFrame()
    plot_heat(heat_g, outdir / "heat.png")
    save_tables(outdir, today, sl)
    make_html(outdir, meta, {"top_overall": "top_overall.png", "slopes": "slopes.png", "trend": "trend.png", "heat": "heat.png"}, {"today": today, "slopes": sl})
    print(f"saved -> {outdir}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--daily-csv", default="reports/entities/entities_daily.csv")
    ap.add_argument("--outdir", default="reports/entities")
    ap.add_argument("--topk", type=int, default=30)
    ap.add_argument("--slope-days", type=int, default=30)
    ap.add_argument("--trend-days", type=int, default=60)
    ap.add_argument("--heat-top", type=int, default=20)
    main(**vars(ap.parse_args()))