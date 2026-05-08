#!/usr/bin/env python3
import argparse, json, re
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import Counter, defaultdict
import pandas as pd
import matplotlib.pyplot as plt

def load_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def load_rows(master_path=None, inputs=None):
    if master_path:
        for row in load_jsonl(master_path):
            yield row
    if inputs:
        from glob import glob
        for pat in inputs:
            for fp in sorted(glob(pat)):
                for row in load_jsonl(fp):
                    yield row

_ws = re.compile(r"\s+")
_url = re.compile(r"https?://\S+")
_punct = re.compile(r"[^\w\s]")

def normalize_text(s):
    s = s.lower()
    s = _url.sub(" ", s)
    s = _punct.sub(" ", s)
    s = _ws.sub(" ", s).strip()
    return s

def safe_join(parts):
    out = []
    for x in parts:
        if x is None:
            continue
        if isinstance(x, (int, float)):
            x = str(x)
        elif not isinstance(x, str):
            continue
        if x:
            out.append(x)
    return " ".join(out)

def tokenize(text):
    return [t for t in text.split(" ") if t]

def parse_date(s):
    if not s or not isinstance(s, str):
        return datetime.now(timezone.utc).date()
    s = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s).astimezone(timezone.utc).date()
    except Exception:
        return datetime.now(timezone.utc).date()

def build_stopwords(extra=None, min_len=3):
    base = {
        "the","a","an","and","or","of","to","in","on","for","by","with","as","at","from",
        "that","this","it","its","is","are","was","were","be","been","being",
        "he","she","they","we","you","i","his","her","their","our","your","them",
        "will","would","can","could","should","may","might","must","do","did","does","done",
        "not","no","yes","but","if","than","then","there","here","about","over","under",
        "more","most","less","least","very","much","many","new","news","latest","today",
        "say","says","said","according","via","source","mr","ms","dr","u","us","uk",
        "nbsp","amp","apos","mdash","ndash"
    }
    if extra:
        for t in extra.split(","):
            t = t.strip().lower()
            if t:
                base.add(t)
    return base, min_len

def top_words(rows, stopwords, min_len=3, drop_content=False):
    total = Counter()
    daily = defaultdict(Counter)
    for r in rows:
        title = r.get("title")
        desc = r.get("description")
        content = r.get("content")
        text = safe_join([title, desc] if drop_content else [title, desc, content])
        text = normalize_text(text)
        if not text:
            continue
        toks = tokenize(text)
        toks = [t for t in toks if len(t) >= min_len and t not in stopwords and not t.isdigit()]
        if not toks:
            continue
        d = parse_date(r.get("published_at") or r.get("published") or r.get("date"))
        total.update(toks)
        daily[d.isoformat()].update(toks)
    return total, daily

def save_bar_chart(df, out_png, title):
    plt.figure(figsize=(12,6))
    plt.barh(df["token"], df["count"])
    plt.gca().invert_yaxis()
    plt.title(title)
    plt.xlabel("count")
    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png, dpi=150)
    plt.close()

def save_trend_chart(df, out_png, days):
    pivot = df.pivot_table(index="date", columns="token", values="count", fill_value=0)
    last_dates = sorted(pivot.index)[-days:] if days else sorted(pivot.index)
    pivot = pivot.loc[last_dates]
    plt.figure(figsize=(16,8))
    for col in pivot.columns[:30]:
        plt.plot(pivot.index, pivot[col], label=col)
    plt.title(f"Top words trend (last {len(last_dates)} days)")
    plt.xlabel("date (UTC)")
    plt.ylabel("count")
    plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=9)
    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png, dpi=150)
    plt.close()

def write_html(out_html, title, imgs):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    html = [
        "<!doctype html><meta charset='utf-8'>",
        f"<title>{title}</title>",
        f"<h1>{title}</h1>",
        f"<p>Updated: {ts}</p>",
    ]
    for src, caption in imgs:
        html.append(f"<h2>{caption}</h2>")
        html.append(f"<img src='{src}' style='max-width:100%;height:auto'/>")
    out_html.parent.mkdir(parents=True, exist_ok=True)
    out_html.write_text("\n".join(html), encoding="utf-8")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--master")
    p.add_argument("--inputs", nargs="*", default=None)
    p.add_argument("--outdir", default="reports/words")
    p.add_argument("--top", type=int, default=30)
    p.add_argument("--days", type=int, default=14)
    p.add_argument("--min-len", type=int, default=3)
    p.add_argument("--extra-stop", default="")
    p.add_argument("--drop-content", action="store_true")
    args = p.parse_args()

    rows = list(load_rows(args.master, args.inputs))
    stop, _ = build_stopwords(args.extra_stop, args.min_len)

    total, daily = top_words(rows, stop, args.min_len, args.drop_content)

    top_df = pd.DataFrame(total.most_common(args.top), columns=["token","count"])
    outdir = Path(args.outdir)
    top_csv = outdir / "top_words.csv"
    top_png = outdir / "top_words.png"
    top_df.to_csv(top_csv, index=False)
    save_bar_chart(top_df, top_png, "Top words")

    daily_rows = []
    for d, cnt in daily.items():
        for tok, c in cnt.items():
            daily_rows.append({"date": d, "token": tok, "count": c})
    trend_df = pd.DataFrame(daily_rows)
    if not trend_df.empty:
        trend_csv = outdir / "top_words_trend.csv"
        trend_png = outdir / "top_words_trend.png"
        trend_df.to_csv(trend_csv, index=False)
        trend_top = top_df["token"].tolist()
        trend_df = trend_df[trend_df["token"].isin(trend_top)]
        save_trend_chart(trend_df, trend_png, args.days)

    uni_csv = outdir / "unigrams_top.csv"
    pd.DataFrame(total.most_common(), columns=["token","count"]).to_csv(uni_csv, index=False)

    summary = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "inputs": args.inputs or [args.master],
        "rows": len(rows),
        "unique_tokens": len(total),
        "top": args.top,
        "days": args.days,
        "min_len": args.min_len,
        "extra_stop": args.extra_stop,
        "drop_content": args.drop_content,
    }
    (outdir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    imgs = []
    if top_png.exists(): imgs.append((top_png.name, "Top words"))
    trend_png = outdir / "top_words_trend.png"
    if trend_png.exists(): imgs.append((trend_png.name, "Top words trend"))
    write_html(outdir / "latest_words.html", "Top Words", imgs)

if __name__ == "__main__":
    main()
