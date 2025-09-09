import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

plt.rcParams["figure.dpi"]=150

def top_today(df, k):
    day=df["date"].max()
    t=df[df["date"]==day].groupby("entity")["count"].sum().sort_values(ascending=False).head(k).reset_index()
    t["date"]=day
    return t

def slopes(df, k_days, k_entities):
    mx=pd.to_datetime(df["date"]).max()
    base=df[pd.to_datetime(df["date"])>=mx-pd.Timedelta(days=k_days-1)]
    grid=(base.groupby(["date","entity"])["count"].sum().reset_index()
               .pivot(index="date", columns="entity", values="count").fillna(0))
    xs=np.arange(len(grid.index))
    res=[]
    for ent in grid.columns:
        y=grid[ent].values
        if np.count_nonzero(y)==0:
            continue
        m=np.polyfit(xs,y,1)[0]
        res.append((ent,float(m)))
    out=(pd.DataFrame(res, columns=["entity","slope"])
           .sort_values("slope", ascending=False).head(k_entities))
    return out

def barh(df, x, y, title, path):
    fig,ax=plt.subplots(figsize=(10,6))
    ax.barh(df[y], df[x])
    ax.set_title(title)
    ax.invert_yaxis()
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)

def trend(df, ents, days, path):
    mx=pd.to_datetime(df["date"]).max()
    base=df[pd.to_datetime(df["date"])>=mx-pd.Timedelta(days=days-1)]
    p=(base.groupby(["date","entity"])["count"].sum().reset_index()
          .pivot(index="date", columns="entity", values="count").fillna(0))
    keep=[e for e in ents if e in p.columns]
    fig,ax=plt.subplots(figsize=(10,5))
    p[keep].plot(ax=ax)
    ax.set_title("Top entities — trend")
    ax.set_ylabel("count")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)

def heatmap(df, entities, days, out_png):
    import numpy as np
    import matplotlib.pyplot as plt

    start = pd.to_datetime(df["date"]).max() - pd.Timedelta(days=days)
    dff = df[(pd.to_datetime(df["date"]) >= start) & (df["entity"].isin(entities))].copy()

    p = dff.pivot_table(index="entity", columns="date", values="count", aggfunc="sum", fill_value=0)

    cols_dt = pd.to_datetime(p.columns, errors="coerce")
    order = np.argsort(cols_dt.values.astype("datetime64[ns]"))
    p = p.iloc[:, order]
    cols_dt = cols_dt[order]

    fig, ax = plt.subplots(figsize=(12, 6))
    im = ax.imshow(p.values.astype(float), aspect="auto", cmap="viridis", interpolation="nearest")

    ax.set_yticks(np.arange(len(p.index)))
    ax.set_yticklabels(p.index, fontsize=8)

    ax.set_xticks(np.arange(len(cols_dt)))
    ax.set_xticklabels([d.strftime("%m-%d") if not pd.isna(d) else "" for d in cols_dt], rotation=90, fontsize=8)

    ax.set_xlabel("date")
    ax.set_ylabel("entity")
    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("count")

    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=150)
    plt.close(fig)


def make_html(outdir, meta, pngs, tables):
    html=[f"<html><head><meta charset='utf-8'><title>Entities Report</title></head><body>"]
    html.append(f"<h1>Entities Report</h1>")
    html.append(f"<p>Articles: {meta['articles']:,} | Entities(rows): {meta['rows']:,} | Updated: {meta['updated']}</p>")
    html.append("<table width='100%'><tr>")
    html.append(f"<td width='50%'><img src='{pngs['top_overall']}' width='100%'></td>")
    html.append(f"<td width='50%'><img src='{pngs['slopes']}' width='100%'></td>")
    html.append("</tr><tr>")
    html.append(f"<td width='50%'><img src='{pngs['trend']}' width='100%'></td>")
    html.append(f"<td width='50%'><img src='{pngs['heat']}' width='100%'></td>")
    html.append("</tr></table>")
    html.append("<h3>Top today</h3>")
    html.append(tables["today"].to_html(index=False))
    html.append("<h3>Strongest positive slopes</h3>")
    html.append(tables["slopes"].to_html(index=False))
    html.append("</body></html>")
    Path(outdir/"report.html").write_text("\n".join(html), encoding="utf-8")

def main(daily_csv, outdir, topk, slope_days, trend_days, heat_top):
    outdir=Path(outdir); outdir.mkdir(parents=True, exist_ok=True)
    df=pd.read_csv(daily_csv)
    df=df.dropna(subset=["date","entity","count"])
    df["date"]=pd.to_datetime(df["date"]).dt.date.astype(str)
    meta={"articles": int(df["count"].sum()), "rows": int(len(df)), "updated": pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M UTC")}
    overall=(df.groupby("entity")["count"].sum().sort_values(ascending=False).head(topk).reset_index())
    today=top_today(df, topk)
    sl=slopes(df, slope_days, topk)
    overall.to_csv(outdir/"entities_top_overall.csv", index=False)
    today.to_csv(outdir/"entities_top_today.csv", index=False)
    sl.to_csv(outdir/"entities_top_slopes.csv", index=False)
    barh(overall, "count", "entity", "Top entities (overall)", outdir/"top_overall.png")
    barh(sl, "slope", "entity", "Strongest positive slopes", outdir/"slopes.png")
    trend(df, list(overall["entity"]), trend_days, outdir/"trend.png")
    heatmap(df, list(overall["entity"].head(heat_top)), trend_days, outdir/"heat.png")
    make_html(outdir, meta,
              {"top_overall":"top_overall.png","slopes":"slopes.png","trend":"trend.png","heat":"heat.png"},
              {"today":today, "slopes":sl})
    print(f"png saved to: {outdir}")

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--daily-csv", default="reports/entities/entities_daily.csv")
    ap.add_argument("--outdir", default="reports/entities")
    ap.add_argument("--topk", type=int, default=30)
    ap.add_argument("--slope-days", type=int, default=30)
    ap.add_argument("--trend-days", type=int, default=60)
    ap.add_argument("--heat-top", type=int, default=20)
    main(**vars(ap.parse_args()))
