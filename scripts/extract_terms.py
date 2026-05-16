import argparse, sys, glob, json, re
from pathlib import Path
from datetime import datetime, timezone
from collections import Counter, defaultdict
import unicodedata
import pandas as pd

try:
    from keybert import KeyBERT
    _HAS_KEYBERT = True
except Exception:
    _HAS_KEYBERT = False

_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_WS_RE = re.compile(r"\s+")
_NUMERIC_ONLY_RE = re.compile(r"^[0-9]+$")
_PUNCT_RE = re.compile(r"^[\W_]+$")
_TOKEN_RE = re.compile(r"[A-Za-z]+")

def parse_date_from_doc(obj):
    pu = obj.get("published_at") or obj.get("publishedAt") or obj.get("published")
    if not pu or not isinstance(pu, str):
        return None
    pu = pu.strip()
    if not pu:
        return None
    if pu.endswith("Z"):
        pu = pu[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(pu)
    except Exception:
        if len(pu) >= 10 and pu[4] == "-" and pu[7] == "-":
            return pu[:10]
        return None
    return dt.astimezone(timezone.utc).date().isoformat()


def doc_text(obj):
    parts = []
    for k in ("title", "description", "content", "text"):
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            parts.append(v.strip())
    return " ".join(parts)


def read_docs_with_date(patterns):
    """Yield (text, date_str_or_None) tuples from jsonl files."""
    files = []
    for p in patterns:
        files.extend(glob.glob(p, recursive=True))
    seen_files = set()
    for fp in files:
        p = Path(fp)
        rp = str(p.resolve())
        if rp in seen_files:
            continue
        seen_files.add(rp)
        if not p.exists() or p.stat().st_size == 0:
            continue
        if p.suffix.lower() == ".jsonl":
            with p.open("r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    t = doc_text(obj)
                    if not t:
                        continue
                    yield t, parse_date_from_doc(obj)
        else:
            try:
                txt = p.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                txt = ""
            if txt:
                yield txt, None


def read_docs(patterns):
    """Legacy: list of text only. Kept for backwards compat."""
    return [t for t, _d in read_docs_with_date(patterns)]

def normalize_text(s):
    s = unicodedata.normalize("NFKC", s)
    s = _URL_RE.sub(" ", s)
    s = _EMAIL_RE.sub(" ", s)
    s = s.replace("\u200b", " ")
    s = _WS_RE.sub(" ", s)
    return s.strip()

def load_stopwords(path):
    st = set()
    if not path:
        return st
    p = Path(path)
    if p.exists():
        for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
            w = line.strip().lower()
            if not w:
                continue
            st.add(w)
    return st

def load_alias(path):
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    try:
        import yaml
    except Exception:
        return {}
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8", errors="ignore")) or {}
    except Exception:
        data = {}
    mp = {}
    for k, v in data.items():
        canon = str(k).strip()
        if isinstance(v, list):
            for a in v:
                mp[str(a).strip().lower()] = canon
        else:
            mp[str(v).strip().lower()] = canon
    return mp

def tokenize_en(text, stop, minlen):
    text = normalize_text(text).lower()
    toks = _TOKEN_RE.findall(text)
    toks = [t for t in toks if len(t) >= minlen and t not in stop]
    return toks

def counts_ngrams(docs, stop, minlen, max_ngram):
    uni = Counter()
    bi = Counter()
    tri = Counter()
    for t in docs:
        words = tokenize_en(t, stop, minlen)
        for w in words:
            uni[w] += 1
        if max_ngram >= 2:
            for i in range(len(words)-1):
                bi[words[i] + " " + words[i+1]] += 1
        if max_ngram >= 3:
            for i in range(len(words)-2):
                tri[" ".join(words[i:i+3])] += 1
    return uni, bi, tri

def keybert_scores(docs, topk=50):
    if not _HAS_KEYBERT:
        return {}
    texts = [normalize_text(t) for t in docs if t]
    if not texts:
        return {}
    try:
        kb = KeyBERT()
        scores = defaultdict(float)
        n = min(len(texts), 100)
        for i in range(n):
            try:
                kws = kb.extract_keywords(texts[i], keyphrase_ngram_range=(1,3), stop_words=None, top_n=topk)
                for kw, sc in kws:
                    scores[kw.lower()] = max(scores[kw.lower()], float(sc))
            except Exception:
                continue
        return scores
    except Exception:
        return {}

def filter_and_alias(counter, stop, mincount, alias):
    out = Counter()
    for w, c in counter.items():
        if c < mincount:
            continue
        if _NUMERIC_ONLY_RE.match(w) or _PUNCT_RE.match(w):
            continue
        if any(part in stop for part in w.split()):
            continue
        canon = alias.get(w.lower(), w)
        out[canon] += c
    return out

def combine_scores(freq_uni, freq_bi, freq_tri, kbs):
    res = {}
    f_all = Counter()
    f_all.update(freq_uni)
    f_all.update(freq_bi)
    f_all.update(freq_tri)
    if not f_all:
        return res
    fmax = max(f_all.values())
    for w, c in f_all.items():
        fz = c / fmax
        kz = kbs.get(w.lower(), 0.0)
        score = 0.8*fz + 0.2*kz
        res[w] = score
    return res

def to_daily_csv(date_str, outcsv, terms, topk, raw_counts=None):
    """Write CSV with date, entity, count, score columns.

    `terms` is {term: normalized_score in [0,1]}.
    `raw_counts` is {term: int} — actual occurrence count for that day.
    For back-compat, if raw_counts is None we fall back to score*1000 in the count column.
    """
    date = date_str
    rank = sorted(terms.items(), key=lambda x: x[1], reverse=True)[:topk]
    rows = []
    for w, sc in rank:
        if raw_counts is not None:
            c = int(raw_counts.get(w, 0))
        else:
            c = int(sc * 1000)
        rows.append((date, w, c, round(float(sc), 6)))
    df = pd.DataFrame(rows, columns=["date", "entity", "count", "score"])
    p = Path(outcsv)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False)
    return p

def _build_one_day_csv(date_str, docs, args, alias, stop, outcsv):
    uni, bi, tri = counts_ngrams(docs, stop, args.minlen, args.max_ngram)
    filtered_uni = filter_and_alias(uni, stop, args.mincount, alias)
    filtered_bi = filter_and_alias(bi, stop, args.mincount, alias)
    filtered_tri = filter_and_alias(tri, stop, args.mincount, alias)
    kbs = keybert_scores(docs, topk=50) if args.use_keybert and _HAS_KEYBERT else {}
    scores = combine_scores(filtered_uni, filtered_bi, filtered_tri, kbs)
    # raw_counts spans all three n-gram tiers so the count column reflects what got scored.
    raw_counts = Counter()
    raw_counts.update(filtered_uni)
    raw_counts.update(filtered_bi)
    raw_counts.update(filtered_tri)
    return to_daily_csv(date_str, outcsv, scores, args.topk, raw_counts=raw_counts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", nargs="+", required=True)
    ap.add_argument("--outcsv", default=None,
                    help="Single-day mode output path. Required unless --multi-day is set.")
    ap.add_argument("--date", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    ap.add_argument("--date-filter", default=None,
                    help="If set, only include docs whose published_at matches this YYYY-MM-DD. "
                         "Docs with no parseable date are skipped.")
    ap.add_argument("--multi-day", action="store_true",
                    help="Emit one CSV per published_at date present in the inputs.")
    ap.add_argument("--outdir", default=None,
                    help="Output directory for --multi-day mode. CSVs named <date>.csv.")
    ap.add_argument("--csv-template", default="{date}.csv",
                    help="Filename template within --outdir for --multi-day mode.")
    ap.add_argument("--include-undated", action="store_true",
                    help="In --multi-day mode, include docs with no published_at under --date.")
    ap.add_argument("--topk", type=int, default=500)
    ap.add_argument("--minlen", type=int, default=2)
    ap.add_argument("--mincount", type=int, default=2)
    ap.add_argument("--max-ngram", type=int, default=3)
    ap.add_argument("--stop", default=None)
    ap.add_argument("--alias", default=None)
    ap.add_argument("--use-keybert", action="store_true")
    args = ap.parse_args()

    if args.multi_day and not args.outdir:
        ap.error("--multi-day requires --outdir")
    if not args.multi_day and not args.outcsv:
        ap.error("--outcsv is required (or use --multi-day with --outdir)")

    stop = load_stopwords(args.stop)
    alias = load_alias(args.alias)

    if args.multi_day:
        by_date: dict[str, list[str]] = defaultdict(list)
        n_skipped = 0
        for text, d in read_docs_with_date(args.inputs):
            if d is None:
                if args.include_undated:
                    by_date[args.date].append(text)
                else:
                    n_skipped += 1
                continue
            by_date[d].append(text)
        outdir = Path(args.outdir)
        outdir.mkdir(parents=True, exist_ok=True)
        for d in sorted(by_date.keys()):
            docs = by_date[d]
            outcsv = outdir / args.csv_template.format(date=d)
            _build_one_day_csv(d, docs, args, alias, stop, outcsv)
            print(f"saved {d} -> {outcsv}  (n_docs={len(docs)})")
        print(f"[multi-day] dates={len(by_date)}  skipped_undated={n_skipped}")
        return

    # Single-day mode
    docs: list[str] = []
    n_skipped = 0
    for text, d in read_docs_with_date(args.inputs):
        if args.date_filter is None:
            docs.append(text)
        elif d == args.date_filter:
            docs.append(text)
        else:
            n_skipped += 1
    outp = _build_one_day_csv(args.date, docs, args, alias, stop, args.outcsv)
    if args.date_filter is not None:
        print(f"[date-filter={args.date_filter}] kept={len(docs)} skipped={n_skipped}")
    print(f"saved -> {outp}")

if __name__ == "__main__":
    main()