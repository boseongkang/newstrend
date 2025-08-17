# src/news_trend/quickview.py
from __future__ import annotations
import os, re, json, shutil
from pathlib import Path
from collections import Counter
from datetime import datetime, timezone
import argparse

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INDIR = ROOT / "data"

EN_STOPWORDS = {
    "the","a","an","and","or","of","to","in","on","for","with","at","by","from","as",
    "is","are","was","were","be","been","being","it","its","this","that","these","those",
    "i","you","he","she","we","they","them","his","her","their","our","us",
    "but","if","so","not","no","yes","do","does","did","doing","done","can","could","should",
    "will","would","may","might","must","about","over","under","after","before","between",
    "than","then","there","here","when","where","why","how","into","out","up","down","new",
    "more","most","other","some","any","such","also","just","one","two","three","first","second",
    "said","says","say","mr","ms"
}
TOKEN_RE = re.compile(r"[A-Za-z']+")

def _load_jsonl(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def tokenize(text: str, min_len: int, stopwords: set[str]) -> list[str]:
    if not text: return []
    toks = [t.lower() for t in TOKEN_RE.findall(text)]
    return [t for t in toks if len(t) >= min_len and t not in stopwords and not t.startswith("'")]

def print_section(title: str):
    width = shutil.get_terminal_size((100, 20)).columns
    bar = "—" * min(width, max(20, len(title) + 4))
    print(f"\n{title}\n{bar}")

def fmt_table(rows: list[tuple], headers: tuple[str, ...], col_widths: tuple[int, ...] | None = None, max_rows: int | None = None):
    if max_rows is not None:
        rows = rows[:max_rows]
    if col_widths is None:
        col_widths = tuple(max(len(str(h)), 10) for h in headers)

    def _cut(s, w):
        s = str(s)
        return s if len(s) <= w else s[: w - 1] + "…"

    header_line = "  ".join(_cut(h, w) for h, w in zip(headers, col_widths))
    print(header_line)
    print("  ".join("-" * w for w in col_widths))
    for r in rows:
        print("  ".join(_cut(v, w) for v, w in zip(r, col_widths)))

def quickview(date_str: str, kind: str, indir: Path, top: int, sample: int, min_len: int, include_content: bool, extra_stopwords: list[str]):
    inpath = indir / kind / f"{date_str}.jsonl"
    if not inpath.exists():
        raise FileNotFoundError(f"input not found: {inpath}")

    rows = list(_load_jsonl(inpath))
    total = len(rows)
    by_source = Counter(r.get("raw_source", "unknown") for r in rows)
    by_pub = Counter(r.get("publisher", "") for r in rows)

    stop = set(EN_STOPWORDS) | {w.strip().lower() for w in extra_stopwords if w.strip()}
    uni, bi = Counter(), Counter()
    for r in rows:
        text = (r.get("title") or "")
        if include_content:
            text += " " + (r.get("content") or "")
        toks = tokenize(text, min_len=min_len, stopwords=stop)
        uni.update(toks)
        for a, b in zip(toks, toks[1:]):
            bi[(a, b)] += 1

    print_section(f"Quick View — {date_str} ({kind})")
    print(f"Total articles : {total}")
    print("By source      :", dict(by_source))
    print("Distinct pubs  :", len(by_pub))

    print_section("Top Publishers")
    fmt_table([(pub, c) for pub, c in by_pub.most_common(top)], headers=("Publisher", "Count"), col_widths=(40, 10))

    print_section(f"Top Words (min_len={min_len}, content={'on' if include_content else 'off'})")
    fmt_table([(w, c) for w, c in uni.most_common(top)], headers=("Word", "Count"), col_widths=(20, 10))

    print_section("Top Bigrams")
    fmt_table([(" ".join(k), c) for k, c in bi.most_common(top)], headers=("Bigram", "Count"), col_widths=(28, 10))

    print_section("Sample Articles")
    sample_rows = []
    for r in rows[:sample]:
        sample_rows.append((r.get("publisher","")[:36], r.get("raw_source",""), (r.get("title") or "")[:80]))
    fmt_table(sample_rows, headers=("Publisher", "Src", "Title"), col_widths=(36, 6, 80))

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Console quick view of daily news file (no HTML)")
    ap.add_argument("--date", default="today", help='"YYYY-MM-DD" or "today" (UTC)')
    ap.add_argument("--kind", choices=["raw","silver"], default="silver")
    ap.add_argument("--indir", default=str(DEFAULT_INDIR))
    ap.add_argument("--top", type=int, default=20, help="top-N for publishers/words/bigrams")
    ap.add_argument("--sample", type=int, default=10, help="sample article rows to show")
    ap.add_argument("--min-len", type=int, default=3, help="min token length")
    ap.add_argument("--no-content", action="store_true", help="use title only (ignore content)")
    ap.add_argument("--extra-stopwords", default="", help="comma-separated extra stopwords")
    args = ap.parse_args()

    d = datetime.now(timezone.utc).date().isoformat() if args.date == "today" else args.date
    extra = [s for s in args.extra_stopwords.split(",") if s]
    quickview(
        date_str=d,
        kind=args.kind,
        indir=Path(args.indir),
        top=args.top,
        sample=args.sample,
        min_len=args.min_len,
        include_content=not args.no_content,
        extra_stopwords=extra
    )
