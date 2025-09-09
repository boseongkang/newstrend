import argparse, re, json, math
from pathlib import Path
from collections import Counter, defaultdict
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def load_daily(daily_dir: Path):
    rows = []
    for p in sorted(daily_dir.glob("*.jsonl")):
        date = p.stem  # YYYY-MM-DD
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                title = (obj.get("title") or "")[:500]
                desc = (obj.get("description") or "")[:1000]
                content = (obj.get("content") or "")[:2000]
                src = ""
                s = obj.get("source")
                if isinstance(s, dict):
                    src = s.get("name") or ""
                url = obj.get("url") or ""
                rows.append((date, src, url, " ".join([title, desc, content])))
    df = pd.DataFrame(rows, columns=["date","publisher","url","text"])
    if df.empty:
        return df
    df = df.drop_duplicates(["url"]).reset_index(drop=True)
    return df

BASIC_STOP = {
    "the","and","for","that","with","you","your","are","was","were","from","this","have","has","had","but","not",
    "they","their","our","his","her","its","into","about","over","after","before","more","most","than","then","too",
    "on","in","to","of","as","by","at","an","a","is","be","or","it","we","i","he","she","them","us","will","can",
    "may","might","just","also","said","says","new","one","two","three","week","month","year","day","days",
    "mr","mrs","dr","u","s","t","re","ll","ve"
}

TOKEN_RE = re.compile(r"[a-z]{3,}")

def tokenize(text: str, stop):
    text = text.lower()
    toks = TOKEN_RE.findall(text)
    return [t for t in toks if t not in stop]

def build_daily_counts(df, stop):
    counts_by_day = defaultdict(Counter)
    for d, txt in zip(df["date"], df["text"]):
        for t in tokenize(txt, stop):
            counts_by_day[d][t] += 1
    return counts_by_day

def trending_today(counts_by_day, window=7, top=50, min_count=30):
    days = sorted(counts_by_day.keys())
    if len(days) < 2:
        return pd.DataFrame()
    target = days[-1]
    base_days = days[-(window+1):-1] if len(days) > window else days[:-1]
    if not base_days:
        return pd.DataFrame()

    today = counts_by_day[target]
    baseline = Counter()
    for d in base_days:
        baseline.update(counts_by_day[d])

    base_avg = {w: baseline[w] / max(1, len(base_days)) for w in set(baseline)|set(today)}
    rows = []
    for w, c in today.items():
        if c < min_count:
            continue
        b = base_avg.get(w, 0.0)
        score = math.log((c + 1.0) / (b + 1.0)) * c
        rows.append((w, c, b, score))
    out = pd.DataFrame(rows, columns=["word","today","baseline_avg","score"]).sort_values("score", ascending=False)
    out["date"] = target
    return out.head(top)

def trend_plot(df, out_png, title="Top trending words"):
    plt.figure(figsize=(12,7))
    plt.barh(df["word"][::-1], df["today"][::-1])
    plt.title(title)
    plt.xlabel("count (today)")
    plt.tight_layout()
    Path(out_png).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png, dpi=160)
    plt.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--daily-dir", default="data/warehouse/daily")
    ap.add_argument("--outdir", default="reports/trends")
    ap.add_argument("--window", type=int, default=7)
    ap.add_argument("--top", type=int, default=50)
    ap.add_argument("--min-count", type=int, default=30)
    ap.add_argument("--min-len", type=int, default=3)
    ap.add_argument("--extra-stop", default="")
    args = ap.parse_args()

    stop = set(BASIC_STOP)
    if args.extra_stop:
        stop |= {w.strip() for w in args.extra_stop.split(",") if w.strip()}
    stop |= {"chars","nbsp","amp","apos","mdash","ndash","inc","com","report","reports","share","shares"}

    daily_dir = Path(args.daily_dir)
    df = load_daily(daily_dir)
    if df.empty:
        print("no data")
        return

    counts_by_day = build_daily_counts(df, stop)
    out = trending_today(counts_by_day, window=args.window, top=args.top, min_count=args.min_count)
    if out.empty:
        print("no trending")
        return

    out_dir = Path(args.outdir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv = out_dir / f"trending_{out['date'].iloc[0]}.csv"
    png = out_dir / f"trending_{out['date'].iloc[0]}.png"
    out.to_csv(csv, index=False)
    trend_plot(out, png, title=f"Top trending words — {out['date'].iloc[0]}")
    print(f"saved: {csv}")
    print(f"saved: {png}")

if __name__ == "__main__":
    main()
