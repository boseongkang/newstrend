from __future__ import annotations
import argparse, json, re, os
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import math

import pandas as pd
import matplotlib.pyplot as plt

TOKEN_RE = re.compile(r"[a-z][a-z']+")

DEFAULT_STOP = {
    "a","about","above","after","again","against","all","am","an","and","any","are","aren't","as","at","be","because","been","before","being","below","between","both","but","by","can","can't","cannot","could","couldn't","did","didn't","do","does","doesn't","doing","don't","down","during","each","few","for","from","further","had","hadn't","has","hasn't","have","haven't","having","he","he'd","he'll","he's","her","here","here's","hers","herself","him","himself","his","how","how's","i","i'd","i'll","i'm","i've","if","in","into","is","isn't","it","it's","its","itself","let's","me","more","most","mustn't","my","myself","no","nor","not","of","off","on","once","only","or","other","ought","our","ours","ourselves","out","over","own","same","shan't","she","she'd","she'll","she's","should","shouldn't","so","some","such","than","that","that's","the","their","theirs","them","themselves","then","there","there's","these","they","they'd","they'll","they're","they've","this","those","through","to","too","under","until","up","very","was","wasn't","we","we'd","we'll","we're","we've","were","weren't","what","what's","when","when's","where","where's","which","while","who","who's","whom","why","why's","with","won't","would","wouldn't","you","you'd","you'll","you're","you've","your","yours","yourself","yourselves",
    "say","says","said","will","new","news","time","year","years","week","today","yesterday","tomorrow","one","two","three","mr","ms","us","u.s","u.s.","—","–"
}

def iso_to_date(iso: str) -> datetime.date:
    s = iso.replace("Z","+00:00")
    return datetime.fromisoformat(s).astimezone(timezone.utc).date()

def iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue

def gather_sources(source: str, last_days: int|None) -> list[Path]:
    p = Path(source)
    if p.is_file():
        return [p]
    if p.is_dir():
        return sorted(p.glob("*.jsonl"))
    return [Path(x) for x in sorted(Path().glob(source))]

def normalize_text(s: str) -> str:
    s = s.lower()
    s = re.sub(r"http[s]?://\S+"," ",s)
    s = re.sub(r"[\d\W_]+"," ",s)
    return s

def tokenize(txt: str, min_len: int, stop: set[str]) -> list[str]:
    txt = normalize_text(txt)
    toks = [t for t in TOKEN_RE.findall(txt) if len(t)>=min_len and t not in stop]
    return toks

def topn_df(counter: Counter, top: int) -> pd.DataFrame:
    items = counter.most_common(top)
    total = sum(counter.values()) or 1
    df = pd.DataFrame(items, columns=["token","count"])
    df["share"] = df["count"]/total
    return df

def plot_bar(df: pd.DataFrame, title: str, outpath: Path):
    plt.figure(figsize=(10,6))
    ax = plt.gca()
    ax.barh(df["token"][::-1], df["count"][::-1])
    ax.set_title(title)
    ax.set_xlabel("count")
    plt.tight_layout()
    outpath.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(outpath, dpi=140)
    plt.close()

def plot_trend(trend_df: pd.DataFrame, title: str, outpath: Path):
    plt.figure(figsize=(11,6))
    ax = plt.gca()
    for tok, sub in trend_df.groupby("token"):
        ax.plot(sub["date"], sub["count"], label=tok, marker="o", linewidth=2)
    ax.set_title(title)
    ax.set_xlabel("date (UTC)")
    ax.set_ylabel("count")
    ax.legend(ncol=2, fontsize=9)
    plt.tight_layout()
    outpath.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(outpath, dpi=140)
    plt.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="data/warehouse/master.jsonl")
    ap.add_argument("--fields", default="title,description,content")
    ap.add_argument("--min-len", type=int, default=3)
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--last-days", type=int, default=14)
    ap.add_argument("--outdir", default="reports/words")
    ap.add_argument("--bigrams", action="store_true")
    ap.add_argument("--extra-stop", default="")
    ap.add_argument("--max-docs", type=int, default=0)
    args = ap.parse_args()

    stop = set(DEFAULT_STOP)
    if args.extra_stop:
        for t in args.extra_stop.split(","):
            t=t.strip().lower()
            if t:
                stop.add(t)

    fields = [x.strip() for x in args.fields.split(",") if x.strip()]
    since_date = None
    if args.last_days and args.last_days>0:
        since_date = (datetime.now(timezone.utc) - timedelta(days=args.last_days)).date()

    files = gather_sources(args.source, args.last_days)
    unigrams = Counter()
    bigrams = Counter()
    per_day = defaultdict(Counter)

    seen = 0
    for fp in files:
        for row in iter_jsonl(fp):
            d = None
            if "published_at" in row and row["published_at"]:
                try:
                    d = iso_to_date(row["published_at"])
                except Exception:
                    d = None
            if since_date and d and d < since_date:
                continue
            txts = []
            for f in fields:
                v = row.get(f)
                if isinstance(v,str) and v:
                    txts.append(v)
            if not txts:
                continue
            tokens = tokenize(" ".join(txts), args.min_len, stop)
            if not tokens:
                continue
            unigrams.update(tokens)
            if args.bigrams and len(tokens)>1:
                bigrams.update([" ".join(pair) for pair in zip(tokens,tokens[1:]) if pair[0] not in stop and pair[1] not in stop])
            if d:
                per_day[d].update(tokens)
            seen += 1
            if args.max_docs and seen>=args.max_docs:
                break
        if args.max_docs and seen>=args.max_docs:
            break

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    top_uni = topn_df(unigrams, args.top)
    top_uni.to_csv(outdir/"unigrams_top.csv", index=False)
    plot_bar(top_uni, f"Top {args.top} words", outdir/"top_words.png")

    if args.bigrams and bigrams:
        top_bi = topn_df(bigrams, args.top)
        top_bi.to_csv(outdir/"bigrams_top.csv", index=False)
        plot_bar(top_bi, f"Top {args.top} bigrams", outdir/"top_bigrams.png")

    if per_day:
        top_tokens = list(top_uni["token"])
        recs = []
        for d, ctr in per_day.items():
            for t in top_tokens:
                c = ctr.get(t,0)
                if c>0:
                    recs.append((d,t,c))
        if recs:
            trend = pd.DataFrame(recs, columns=["date","token","count"]).sort_values(["date","token"])
            trend.to_csv(outdir/"top_words_trend.csv", index=False)
            plot_trend(trend, f"Top words trend (last {args.last_days} days)", outdir/"top_words_trend.png")

    summary = {
        "source": args.source,
        "files": len(files),
        "docs_seen": seen,
        "unique_words": len(unigrams),
        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
        "top": args.top,
        "last_days": args.last_days,
        "min_len": args.min_len
    }
    with (outdir/"summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
