import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

plt.rcParams["figure.dpi"] = 150

def ensure_dirs(p):
    p = Path(p)
    p.mkdir(parents=True, exist_ok=True)
    return p

def load_daily(csv_path):
    df = pd.read_csv(csv_path)
    if "date" not in df.columns or "entity" not in df.columns or "count" not in df.columns:
        raise ValueError("CSV must have columns: date, entity, count")
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    df["entity"] = df["entity"].astype(str)
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    return df

def safe_slope(series):
    y = pd.to_numeric(series, errors="coerce").astype(float).to_numpy()
    x = np.arange(len(y), dtype=float)
    msk = np.isfinite(y) & np.isfinite(x)
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
    mx = pd.to_datetime(df["date"]).max()
    base = df[pd.to_datetime(df["date"]) >= mx - pd.Timedelta(days=days - 1)]
    grid = build_matrix(base)
    s = {c: safe_slope(grid[c]) for c in grid.columns}
    out = (
        pd.DataFrame({"entity": list(s.keys()), "norm_slope": list(s.values())})
        .sort_values("norm_slope", ascending=False)
        .head(topk)
        .reset_index(drop=True)
    )
    last = grid.iloc[-1] if len(grid) else pd.Series(dtype=float)
    out["last_count"] = last.reindex(out["entity"]).fillna(0).astype(int).to_list()
    out["mean"] = grid.reindex(columns=out["entity"]).mean().fillna(0).to_list() if len(grid) else 0
    return out, grid

def trend_subset(grid, entities, days):
    g = grid.tail(days).copy()
    g = g.reindex(columns=[e for e in entities if e in g.columns]).fillna(0)
    return g

def plot_top_overall(df, out_png, topk, days):
    mx = pd.to_datetime(df["date"]).max()
    base = df[pd.to_datetime(df["date"]) >= mx - pd.Timedelta(days=days - 1)]
    s = base.groupby("entity")["count"].sum().sort_values(ascending=False).head(topk)
    plt.figure(figsize=(9, 5))
    s[::-1].plot(kind="barh")
    plt.title(f"Top entities by total count (last {days}d)")
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()

def plot_slopes(sl_df, out_png):
    plt.figure(figsize=(9, 5))
    s = sl_df.set_index("entity")["norm_slope"].iloc[::-1]
    s.plot(kind="barh")
    plt.title("Rising entities (slope)")
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()

def plot_trend(g, out_png):
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
    if g.empty:
        Path(out_png).write_text("")
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
    today_df.to_csv(outdir / "entities_top_today.csv", index=False)
    slopes_df.to_csv(outdir / "entities_slopes.csv", index=False)

def make_html(outdir, meta, images, tables):
    html = []
    html.append("<html><head><meta charset='utf-8'><title>Entities report</title>")
    html.append("<style>body{font-family:system-ui,Arial,sans-serif;background:#111;color:#d9d9d9} h2{margin-top:28px} table{border-collapse:collapse} td,th{border:1px solid #333;padding:6px 8px} a{color:#7ec8ff} .wrap{max-width:1100px;margin:20px auto;padding:8px}</style>")
    html.append("</head><body><div class='wrap'>")
    html.append(f"<h1>Entities report</h1>")
    html.append(f"<p>generated_at: {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%SZ')}</p>")
    html.append("<h2>Images</h2><ul>")
    for k, rel in images.items():
        if (outdir / rel).exists():
            html.append(f"<li>{k}: <br><img src='{rel}' style='max-width:100%'></li>")
    html.append("</ul>")
    html.append("<h2>Tables</h2>")
    for name, df in tables.items():
        html.append(f"<h3>{name}</h3>")
        html.append(df.to_html(index=False))
    html.append("</div></body></html>")
    (outdir / "report.html").write_text("\n".join(html), encoding="utf-8")

def main(daily_csv, outdir, topk, slope_days, trend_days, heat_top):
    outdir = ensure_dirs(outdir)
    df = load_daily(daily_csv)
    meta = {"rows": len(df), "min_date": str(df["date"].min().date()), "max_date": str(df["date"].max().date())}
    today = top_today(df, topk)
    sl, grid_all = slopes_table(df, slope_days, topk)
    plot_top_overall(df, outdir / "top_overall.png", topk, trend_days)
    plot_slopes(sl, outdir / "slopes.png")
    sub_trend = trend_subset(grid_all, sl["entity"].tolist()[:min(10, len(sl))], trend_days)
    plot_trend(sub_trend, outdir / "trend.png")
    top_for_heat = grid_all.sum().sort_values(ascending=False).head(heat_top).index.tolist()
    heat_g = trend_subset(grid_all, top_for_heat, trend_days)
    plot_heat(heat_g, outdir / "heat.png")
    save_tables(outdir, today, sl)
    make_html(
        outdir,
        meta,
        {"top_overall": "top_overall.png", "slopes": "slopes.png", "trend": "trend.png", "heat": "heat.png"},
        {"today": today, "slopes": sl},
    )
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