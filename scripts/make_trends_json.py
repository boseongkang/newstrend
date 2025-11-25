import argparse, json, re
from pathlib import Path
from collections import Counter

WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9']+")

STOPWORDS = {
    "the","and","for","with","that","this","from","have","has","had","were","was",
    "been","will","would","could","should","about","into","over","after","before",
    "their","they","them","your","you","than","then","when","what","which","while",
    "just","like","also","more","most","some","such","only","other","many","very",
    "any","each","much","those","these","where","who","whom","whose","our","ours",
    "its","it's","not","are","but","can","all","out","his","her","him","she","himself",
    "herself","itself","ourselves","themselves","my","mine","me","i","of","in","on",
    "at","as","by","to","an","a","or","be","do","does","did","so","no","yes"
}

TEXT_FIELDS = (
    "title","headline","summary","description","content","text","body","snippet"
)

def extract_text(obj):
    parts = []
    for k in TEXT_FIELDS:
        v = obj.get(k)
        if isinstance(v, str):
            parts.append(v)
    return " ".join(parts)

def tokens_from_text(text, min_len):
    for m in WORD_RE.findall(text.lower()):
        if len(m) < min_len:
            continue
        if m in STOPWORDS:
            continue
        yield m

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-dir", default="data/silver")
    ap.add_argument("--out", default="site/data/trends.json")
    ap.add_argument("--last-days", type=int, default=90)
    ap.add_argument("--topk", type=int, default=200)
    ap.add_argument("--min-len", type=int, default=4)
    args = ap.parse_args()

    sd = Path(args.silver_dir)
    files = sorted(f for f in sd.glob("*.jsonl") if f.stem[:4].isdigit())
    if not files:
        raise SystemExit(f"no silver files in {sd}")

    dates = [f.stem for f in files]
    if args.last_days > 0 and len(dates) > args.last_days:
        files = files[-args.last_days:]
        dates = [f.stem for f in files]

    by_date = {}
    total = Counter()

    for f, d in zip(files, dates):
        cnt = Counter()
        with f.open("r", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                text = extract_text(obj)
                if not text:
                    continue
                for tok in tokens_from_text(text, args.min_len):
                    cnt[tok] += 1
        by_date[d] = cnt
        total.update(cnt)

    top_terms = [t for t, _ in total.most_common(args.topk)]

    series = {}
    for t in top_terms:
        series[t] = [int(by_date.get(d, {}).get(t, 0)) for d in dates]

    out = {
        "dates": dates,
        "terms": top_terms,
        "top": top_terms,
        "series": series
    }

    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print("wrote", outp, "dates", len(dates), "terms", len(top_terms))

if __name__ == "__main__":
    main()