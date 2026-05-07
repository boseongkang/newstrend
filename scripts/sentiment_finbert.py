"""
FinBERT per-article sentiment scoring (Phase 2-C).

Reads raw articles from a JSONL file, deduplicates, runs ProsusAI/finbert
on title+description, maps each article to tickers via config/ticker_aliases.json,
writes per-article scores + tickers. Aggregation to ticker_sentiment.json
is downstream (Phase 2-D).

Usage:
    python scripts/sentiment_finbert.py \
        --input data/warehouse/daily/2026-05-07.jsonl \
        --output /tmp/sentiment_test.json
"""
import argparse
import json
import re
import time
from collections import Counter
from pathlib import Path

import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

MODEL_NAME = "ProsusAI/finbert"
ALIASES_PATH = Path("config/ticker_aliases.json")


def load_ticker_aliases(path: Path = ALIASES_PATH) -> dict:
    return json.loads(path.read_text())


def build_ticker_lookup(aliases: dict):
    """Two patterns: symbol (case-sensitive uppercase) + alias (case-insensitive).
    Short aliases equal to their ticker symbol are skipped from alias matching
    so e.g. "ms" doesn't match "Ms." — only uppercase "MS" hits via symbol.
    """
    tickers = sorted(aliases.keys())
    symbol_pat = re.compile(r"\$?\b(" + "|".join(map(re.escape, tickers)) + r")\b")
    alias_pats = []
    for tk, names in aliases.items():
        for name in names:
            n = name.lower()
            if n == tk.lower() and len(n) <= 4:
                continue
            alias_pats.append((re.compile(r"\b" + re.escape(n) + r"\b", re.I), tk))
    return symbol_pat, alias_pats


def find_tickers(text: str, symbol_pat, alias_pats) -> set:
    if not text:
        return set()
    out = {m.group(1) for m in symbol_pat.finditer(text)}
    for pat, tk in alias_pats:
        if pat.search(text):
            out.add(tk)
    return out


def load_articles(path: Path, dedup: bool = True):
    seen = set()
    with path.open(encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if dedup:
                key = (obj.get("article_id") or obj.get("id") or obj.get("url")
                       or (obj.get("title") or "").strip().lower())
                if not key or key in seen:
                    continue
                seen.add(key)
            yield obj


def article_text(obj: dict) -> str:
    title = (obj.get("title") or "").strip()
    desc = (obj.get("description") or "").strip()
    if title and desc:
        return f"{title}. {desc}"
    return title or desc


def get_publisher(obj):
    p = obj.get("publisher")
    if p:
        return p if isinstance(p, str) else (p.get("name") if isinstance(p, dict) else None)
    src = obj.get("source")
    if isinstance(src, dict):
        return src.get("name")
    return src if isinstance(src, str) else None


def pick_device() -> torch.device:
    if torch.backends.mps.is_available():
        return torch.device("mps")
    if torch.cuda.is_available():
        return torch.device("cuda")
    return torch.device("cpu")


def score_batch(model, tokenizer, texts, device, max_length=128):
    inputs = tokenizer(
        texts, return_tensors="pt", padding=True, truncation=True,
        max_length=max_length,
    ).to(device)
    with torch.no_grad():
        logits = model(**inputs).logits
    probs = torch.softmax(logits, dim=-1).cpu().numpy()
    id2label = model.config.id2label
    return [{id2label[i]: float(p[i]) for i in range(len(id2label))}
            for p in probs]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Raw daily JSONL file")
    ap.add_argument("--output", required=True, help="Per-article sentiment JSON")
    ap.add_argument("--batch-size", type=int, default=32)
    ap.add_argument("--max-length", type=int, default=128)
    args = ap.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)
    if not in_path.exists():
        raise SystemExit(f"input not found: {in_path}")

    device = pick_device()
    print(f"device: {device}")
    print(f"loading {MODEL_NAME} ...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = (AutoModelForSequenceClassification
             .from_pretrained(MODEL_NAME).eval().to(device))

    aliases = load_ticker_aliases()
    symbol_pat, alias_pats = build_ticker_lookup(aliases)
    print(f"ticker aliases: {len(aliases)} tickers, {len(alias_pats)} alias patterns")

    results = []
    skipped_empty = 0
    batch_objs, batch_texts = [], []
    t0 = time.time()

    def flush():
        if not batch_texts:
            return
        scores = score_batch(model, tokenizer, batch_texts, device, args.max_length)
        for obj, text, sc in zip(batch_objs, batch_texts, scores):
            results.append({
                "article_id":   obj.get("article_id") or obj.get("id") or obj.get("url"),
                "url":          obj.get("url"),
                "publisher":    get_publisher(obj),
                "published_at": obj.get("published_at") or obj.get("publishedAt"),
                "text":         text[:300],
                "scores":       sc,
                "label":        max(sc, key=sc.get),
                "tickers":      sorted(find_tickers(text, symbol_pat, alias_pats)),
            })
        batch_objs.clear()
        batch_texts.clear()

    for obj in load_articles(in_path):
        text = article_text(obj)
        if not text:
            skipped_empty += 1
            continue
        batch_objs.append(obj)
        batch_texts.append(text)
        if len(batch_texts) >= args.batch_size:
            flush()
    flush()

    elapsed = time.time() - t0
    labels = Counter(r["label"] for r in results)
    n_with_tickers = sum(1 for r in results if r["tickers"])
    ticker_hits = Counter(t for r in results for t in r["tickers"])

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump({
            "input":               str(in_path),
            "model":               MODEL_NAME,
            "device":              str(device),
            "n_articles_scored":   len(results),
            "n_skipped_empty":     skipped_empty,
            "n_with_tickers":      n_with_tickers,
            "elapsed_seconds":     round(elapsed, 2),
            "throughput_per_min":  round(len(results) / elapsed * 60, 1) if elapsed > 0 else 0,
            "label_distribution":  dict(labels),
            "ticker_distribution": dict(ticker_hits.most_common()),
            "results":             results,
        }, f, indent=2, ensure_ascii=False)

    print(f"\nScored {len(results)} articles in {elapsed:.1f}s "
          f"({len(results)/elapsed*60:.0f}/min)")
    print(f"Skipped (empty title+desc): {skipped_empty}")
    print(f"With tickers: {n_with_tickers} ({n_with_tickers/len(results)*100:.1f}%)")
    print(f"Label distribution: {dict(labels)}")
    print(f"Top-10 tickers: {dict(ticker_hits.most_common(10))}")
    print(f"Output: {out_path}")


if __name__ == "__main__":
    main()
