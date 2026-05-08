#!/usr/bin/env python3
import argparse, json, pathlib
from collections import Counter
import pandas as pd
import matplotlib.pyplot as plt

def load_json(p):
    return json.loads(pathlib.Path(p).read_text())

def count_lines(p):
    n = 0
    with open(p, "r", encoding="utf-8") as f:
        for _ in f: n += 1
    return n

def daily_counts(daily_dir):
    rows = []
    for fp in sorted(pathlib.Path(daily_dir).glob("*.jsonl")):
        rows.append({"date": fp.stem, "count": count_lines(fp)})
    return pd.DataFrame(rows)

def top_publishers(master_path, limit=15):
    c = Counter()
    with open(master_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                o = json.loads(line)
                pub = o.get("publisher") or o.get("source") or ""
                if pub: c[pub] += 1
            except Exception:
                pass
    items = c.most_common(limit)
    return pd.DataFrame(items, columns=["publisher", "count"])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metrics", default="data/metrics/warehouse_latest.json")
    ap.add_argument("--daily-dir", default=None)
    ap.add_argument("--master", default=None)
    ap.add_argument("--top-pubs", type=int, default=15)
    ap.add_argument("--outdir", default="reports/viz")
    args = ap.parse_args()

    outdir = pathlib.Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    m = load_json(args.metrics)

    master_path = args.master or m.get("master_path")
    master_rows = count_lines(master_path) if master_path and pathlib.Path(master_path).exists() else 0

    kpi = pd.Series({
        "files_seen": m.get("files_seen", 0),
        "files_processed": m.get("files_processed", 0),
        "new_accepted": m.get("new_accepted", 0),
        "master_rows": master_rows
    })
    ax = kpi.plot(kind="bar")
    ax.set_title(f"Warehouse KPIs\nupdated_at={m.get('updated_at','')}")
    ax.set_ylabel("count")
    plt.tight_layout()
    (outdir/"kpi.png").parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(outdir/"kpi.png"); plt.close()

    daily_dir = args.daily_dir or m.get("daily_dir")
    if daily_dir and pathlib.Path(daily_dir).exists():
        df = daily_counts(daily_dir)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date")
            ax = df.plot(x="date", y="count", kind="line", legend=False)
            ax.set_title("Daily accepted rows")
            ax.set_xlabel("date"); ax.set_ylabel("rows")
            plt.tight_layout()
            plt.savefig(outdir/"daily_trend.png"); plt.close()
            df.to_csv(outdir/"daily_counts.csv", index=False)

    if master_path and pathlib.Path(master_path).exists() and args.top_pubs > 0:
        dfp = top_publishers(master_path, args.top_pubs)
        if not dfp.empty:
            ax = dfp.plot(x="publisher", y="count", kind="barh", legend=False)
            ax.invert_yaxis()
            ax.set_title(f"Top {len(dfp)} publishers (all time)")
            ax.set_xlabel("rows")
            plt.tight_layout()
            plt.savefig(outdir/"top_publishers.png"); plt.close()
            dfp.to_csv(outdir/"top_publishers.csv", index=False)

    manifest = {
        "updated_at": m.get("updated_at"),
        "kpi_png": str(outdir/"kpi.png"),
        "daily_trend_png": str(outdir/"daily_trend.png"),
        "top_publishers_png": str(outdir/"top_publishers.png"),
        "outdir": str(outdir)
    }
    pathlib.Path(outdir/"manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
