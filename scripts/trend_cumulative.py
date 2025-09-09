import argparse, json, re, math
from pathlib import Path
from collections import Counter, defaultdict
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

BASIC_STOP = {
    "the","and","for","that","with","you","your","are","was","were","from","this","have","has","had","but","not",
    "they","their","our","his","her","its","into","about","over","after","before","more","most","than","then","too",
    "on","in","to","of","as","by","at","an","a","is","be","or","it","we","i","he","she","them","us","will","can",
    "may","might","just","also","said","says","new","one","two","three","week","month","year","day","days",
    "mr","mrs","dr","u","s","t","re","ll","ve"
}
TOKEN_RE = re.compile(r"[a-z]{3,}")

def load_daily(daily_dir: Path) -> pd.DataFrame:
    rows = []
    for p in sorted(daily_dir.glob("*.jsonl")):
        date = p.stem
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    o = json.loads(line)
                except Exception:
                    continue
                title = (o.get("title") or "")[:500]
                desc = (o.get("description") or "")[:1000]
                content = (o.get("content") or "")[:2000]
                url = o.get("url") or ""
                rows.append((date, url, " ".join([title, desc, content])))
    df = pd.DataFrame(rows, columns=["date","url","text"])
    if df.empty:
        return df
    df = df.drop_duplicates(["url"]).reset_index(drop=True)
    return df

def tokenize(text: str, stop: set):
    text = text.lower()
    toks = TOKEN_RE.findall(text)
    return [t for t in toks if t not in stop]

def build_counts_by_day(df: pd.DataFrame, stop: set) -> pd.DataFrame:
    counts_by_day = defaultdict(Counter)
    for d, txt in zip(df["date"], df["text"]):
        for t in tokenize(txt, stop):
            counts_by_day[d][t] += 1
    rows = []
    for d in sorted(counts_by_day.keys()):
        for w, c in counts_by_day[d].items():
            rows.append((d, w, c))
    out = pd.DataFrame(rows, columns=["date","word","count"])
    out["date"] = pd.to_datetime(out["date"])
    out = out.sort_values(["date","word"]).reset_index(drop=True)
    return out

def top_overall(df_counts: pd.DataFrame, top: int, min_count: int):
    s = df_counts.groupby("word")["count"].sum().sort_values(ascending=False)
    s = s[s >= min_count]
    return s.head(top).reset_index().rename(columns={"count":"total"})

def momentum_ranking(df_counts: pd.DataFrame, recent: int, past: int, top: int, min_recent_total: int):
    dates = np.sort(df_counts["date"].unique())
    if len(dates) < recent + max(1,past):
        return pd.DataFrame()
    recent_dates = dates[-recent:]
    past_pool = dates[: -recent]
    past_dates = past_pool[-past:] if len(past_pool) >= past else past_pool

    recent_sum = df_counts[df_counts["date"].isin(recent_dates)].groupby("word")["count"].sum()
    past_mean = df_counts[df_counts["date"].isin(past_dates)].groupby("word")["count"].mean()

    words = set(recent_sum.index) | set(past_mean.index)
    rows = []
    for w in words:
        r = float(recent_sum.get(w, 0.0))
        p = float(past_mean.get(w, 0.0))
        if r < min_recent_total:
            continue
        score = math.log((r/recent + 1.0) / (p + 1.0)) * r
        rows.append((w, r, p, score))
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows, columns=["word","recent_total","past_mean","score"]).sort_values("score", ascending=False)
    return out.head(top)

def slopes(df_counts: pd.DataFrame, min_days: int, min_total: int, top: int):
    rows = []
    by_w = df_counts.pivot_table(index="date", columns="word", values="count", aggfunc="sum").fillna(0.0)
    x = np.arange(len(by_w))
    for w in by_w.columns:
        y = by_w[w].values
        if (y>0).sum() < min_days or y.sum() < min_total:
            continue
        slope = np.polyfit(x, y, 1)[0]
        rows.append((w, float(slope), float(y.sum())))
    out = pd.DataFrame(rows, columns=["word","slope","total"]).sort_values("slope", ascending=False)
    return out.head(top)

def plot_bar(df, xcol, ycol, out_png, title):
    plt.figure(figsize=(12,7))
    plt.barh(df[xcol][::-1], df[ycol][::-1])
    plt.title(title)
    plt.xlabel(ycol)
    plt.tight_layout()
    Path(out_png).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png, dpi=160)
    plt.close()

def plot_timeseries(df_counts, words, out_png, title):
    plt.figure(figsize=(14,7))
    pivot = df_counts[df_counts["word"].isin(words)].pivot_table(index="date", columns="word", values="count", aggfunc="sum").fillna(0.0)
    for w in words:
        if w in pivot.columns:
            plt.plot(pivot.index, pivot[w].values, label=w)
    plt.legend(ncol=2)
    plt.title(title)
    plt.ylabel("count")
    plt.tight_layout()
    Path(out_png).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png, dpi=160)
    plt.close()

def plot_heatmap(df_counts, words, out_png, title):
    pivot = df_counts[df_counts["word"].isin(words)].pivot_table(index="word", columns="date", values="count", aggfunc="sum").fillna(0.0)
    plt.figure(figsize=(max(10, len(pivot.columns)*0.5), max(6, len(words)*0.35)))
    plt.imshow(pivot.values, aspect="auto")
    plt.yticks(range(len(pivot.index)), pivot.index)
    plt.xticks(range(len(pivot.columns)), [d.strftime("%m-%d") for d in pivot.columns], rotation=90)
    plt.title(title)
    plt.colorbar(label="count")
    plt.tight_layout()
    Path(out_png).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png, dpi=160)
    plt.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--daily-dir", default="data/warehouse/daily")
    ap.add_argument("--outdir", default="reports/trends_cumulative")
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--min-count", type=int, default=50)
    ap.add_argument("--recent", type=int, default=7)
    ap.add_argument("--past", type=int, default=14)
    ap.add_argument("--min-recent-total", type=int, default=40)
    ap.add_argument("--slope-min-days", type=int, default=5)
    ap.add_argument("--slope-min-total", type=int, default=80)
    ap.add_argument("--extra-stop", default="")
    args = ap.parse_args()

    stop = set(BASIC_STOP) | {"chars","nbsp","amp","apos","mdash","ndash","inc","com","report","reports","share","shares","according","company","market","news"}
    if args.extra_stop:
        stop |= {w.strip() for w in args.extra_stop.split(",") if w.strip()}

    daily_dir = Path(args.daily_dir)
    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    df = load_daily(daily_dir)
    if df.empty:
        print("no data")
        return
    counts = build_counts_by_day(df, stop)
    counts.to_csv(outdir/"counts_by_day.csv", index=False)

    top_all = top_overall(counts, args.top, args.min_count)
    top_all.to_csv(outdir/"top_overall.csv", index=False)
    plot_bar(top_all, "word", "total", outdir/"top_overall.png", "Top words (overall)")

    mom = momentum_ranking(counts, args.recent, args.past, args.top, args.min_recent_total)
    if not mom.empty:
        mom.to_csv(outdir/"top_momentum.csv", index=False)
        plot_bar(mom, "word", "recent_total", outdir/"top_momentum.png", f"Momentum (last {args.recent} days)")
        plot_timeseries(counts, list(mom["word"][:10]), outdir/"timeseries_momentum.png", "Time series — momentum words")

    sl = slopes(counts, args.slope_min_days, args.slope_min_total, args.top)
    if not sl.empty:
        sl.to_csv(outdir/"top_slopes.csv", index=False)
        plot_bar(sl, "word", "slope", outdir/"top_slopes.png", "Strongest positive slopes")

    heat_words = list(top_all["word"][:20])
    plot_heatmap(counts, heat_words, outdir/"heatmap_top_overall.png", "Heatmap — top overall")

if __name__ == "__main__":
    main()
