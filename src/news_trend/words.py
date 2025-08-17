# src/news_trend/words.py
from __future__ import annotations
import os, re, json, csv
from pathlib import Path
from collections import Counter
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INDIR = ROOT / "data"
DEFAULT_OUTDIR = ROOT / "reports"

EN_STOPWORDS = {
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "with", "at", "by", "from", "as",
    "is", "are", "was", "were", "be", "been", "being", "it", "its", "this", "that", "these", "those",
    "i", "you", "he", "she", "we", "they", "them", "his", "her", "their", "our", "us",
    "but", "if", "so", "not", "no", "yes", "do", "does", "did", "doing", "done", "can", "could", "should",
    "will", "would", "may", "might", "must", "about", "over", "under", "after", "before", "between",
    "than", "then", "there", "here", "when", "where", "why", "how", "into", "out", "up", "down", "new",
    "more", "most", "other", "some", "any", "such", "also", "just", "one", "two", "three", "first", "second",
    "said", "says", "say", "mr", "ms"
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


def analyze(date_str: str, kind: str = "silver",
            indir: Path = DEFAULT_INDIR, outdir: Path = DEFAULT_OUTDIR,
            min_len: int = 3, top_k: int = 200, include_content: bool = True,
            extra_stopwords: list[str] | None = None) -> Path:
    indir = Path(indir);
    outdir = Path(outdir)
    inpath = indir / kind / f"{date_str}.jsonl"
    if not inpath.exists():
        raise FileNotFoundError(f"input not found: {inpath}")

    stop = set(EN_STOPWORDS)
    if extra_stopwords:
        stop |= {w.strip().lower() for w in extra_stopwords if w.strip()}

    uni = Counter()
    bi = Counter()

    for r in _load_jsonl(inpath):
        text = (r.get("title") or "")
        if include_content:
            text += " " + (r.get("content") or "")
        tokens = tokenize(text, min_len=min_len, stopwords=stop)
        uni.update(tokens)
        # bigrams
        for a, b in zip(tokens, tokens[1:]):
            bi[(a, b)] += 1

    out_base = outdir / date_str
    out_base.mkdir(parents=True, exist_ok=True)

    with open(out_base / "words.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f);
        w.writerow(["word", "count"])
        for word, c in uni.most_common(top_k):
            w.writerow([word, c])

    # CSV: bigrams
    with open(out_base / "bigrams.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f);
        w.writerow(["bigram", "count"])
        for (a, b), c in bi.most_common(top_k):
            w.writerow([f"{a} {b}", c])

    html = [
        "<html><head><meta charset='utf-8'><style>body{font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;padding:24px}table{border-collapse:collapse;width:100%}th,td{border-bottom:1px solid #eee;padding:8px 10px;text-align:left}h2{margin-top:28px}</style></head><body>"]
    html.append(f"<h1>Word Frequencies — {date_str} ({kind})</h1>")
    html.append("<h2>Top Words</h2><table><thead><tr><th>#</th><th>Word</th><th>Count</th></tr></thead><tbody>")
    for i, (wrd, cnt) in enumerate(uni.most_common(top_k), 1):
        html.append(f"<tr><td>{i}</td><td>{wrd}</td><td>{cnt}</td></tr>")
    html.append("</tbody></table>")
    html.append("<h2>Top Bigrams</h2><table><thead><tr><th>#</th><th>Bigram</th><th>Count</th></tr></thead><tbody>")
    for i, ((a, b), cnt) in enumerate(bi.most_common(top_k), 1):
        html.append(f"<tr><td>{i}</td><td>{a} {b}</td><td>{cnt}</td></tr>")
    html.append("</tbody></table></body></html>")
    (out_base / "words.html").write_text("\n".join(html), encoding="utf-8")

    (outdir / "latest_words.html").write_text("\n".join(html), encoding="utf-8")
    return out_base


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Make word-frequency report from daily news JSONL")
    ap.add_argument("--date", default="today", help='"YYYY-MM-DD" or "today" (UTC)')
    ap.add_argument("--kind", choices=["raw", "silver"], default="silver")
    ap.add_argument("--indir", default=str(DEFAULT_INDIR))
    ap.add_argument("--outdir", default=str(DEFAULT_OUTDIR))
    ap.add_argument("--min-len", type=int, default=3)
    ap.add_argument("--top", type=int, default=200)
    ap.add_argument("--no-content", action="store_true", help="use title only (ignore content)")
    ap.add_argument("--extra-stopwords", default="", help="comma-separated extra stopwords")
    args = ap.parse_args()

    d = datetime.now(timezone.utc).date().isoformat() if args.date == "today" else args.date
    out = analyze(
        date_str=d,
        kind=args.kind,
        indir=Path(args.indir),
        outdir=Path(args.outdir),
        min_len=args.min_len,
        top_k=args.top,
        include_content=not args.no_content,
        extra_stopwords=[s for s in args.extra_stopwords.split(",") if s]
    )
    print(f"[OK] word report -> {out}/words.html")
