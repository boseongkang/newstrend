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

def read_docs(patterns):
    files = []
    for p in patterns:
        files.extend(glob.glob(p, recursive=True))
    docs = []
    for fp in files:
        p = Path(fp)
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
                        t = obj.get("text") or obj.get("content") or ""
                    except Exception:
                        t = ""
                    if t:
                        docs.append(t)
        else:
            try:
                txt = p.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                txt = ""
            if txt:
                docs.append(txt)
    return docs

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

def to_daily_csv(date_str, outcsv, terms, topk):
    date = date_str
    rows = []
    rank = sorted(terms.items(), key=lambda x: x[1], reverse=True)[:topk]
    for w, sc in rank:
        rows.append((date, w, int(sc*1000)))
    df = pd.DataFrame(rows, columns=["date","entity","count"])
    p = Path(outcsv)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False)
    return p

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", nargs="+", required=True)
    ap.add_argument("--outcsv", required=True)
    ap.add_argument("--date", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    ap.add_argument("--topk", type=int, default=500)
    ap.add_argument("--minlen", type=int, default=2)
    ap.add_argument("--mincount", type=int, default=2)
    ap.add_argument("--max-ngram", type=int, default=3)
    ap.add_argument("--stop", default=None)
    ap.add_argument("--alias", default=None)
    ap.add_argument("--use-keybert", action="store_true")
    args = ap.parse_args()

    docs = read_docs(args.inputs)
    stop = load_stopwords(args.stop)
    alias = load_alias(args.alias)

    uni, bi, tri = counts_ngrams(docs, stop, args.minlen, args.max_ngram)
    filtered_uni = filter_and_alias(uni, stop, args.mincount, alias)
    filtered_bi = filter_and_alias(bi, stop, args.mincount, alias)
    filtered_tri = filter_and_alias(tri, stop, args.mincount, alias)
    kbs = keybert_scores(docs, topk=50) if args.use_keybert and _HAS_KEYBERT else {}
    scores = combine_scores(filtered_uni, filtered_bi, filtered_tri, kbs)
    outp = to_daily_csv(args.date, args.outcsv, scores, args.topk)
    print(f"saved -> {outp}")

if __name__ == "__main__":
    main()