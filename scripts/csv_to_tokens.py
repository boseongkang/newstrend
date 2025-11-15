import json, re
from pathlib import Path
from collections import Counter
import pandas as pd

ROOT = Path("data/warehouse/daily")
ROOT.mkdir(parents=True, exist_ok=True)

TOKEN_RE = re.compile(r"[A-Za-z]{2,}")
DEFAULT_STOP = {
    "the","a","an","of","and","or","to","in","for","on","at","by","with","from","as","that","this","is","are","was","were","be","been","being",
    "it","its","into","about","over","after","before","during","than","but","not","no","we","you","they","he","she","him","her","his","hers",
    "their","them","our","us","i","my","me"
}

def load_extra_stop():
    p = Path("config/extra_noise.txt")
    if p.exists():
        return {x.strip().lower() for x in p.read_text(encoding="utf-8", errors="ignore").splitlines() if x.strip()}
    return set()

STOP = DEFAULT_STOP | load_extra_stop()

def write_tokens(path, pairs):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as w:
        for tok, n in pairs:
            w.write(json.dumps({"tok": str(tok), "n": int(n)}, ensure_ascii=False) + "\n")

def tokens_from_csv(csv_path):
    try:
        df = pd.read_csv(csv_path)
    except Exception:
        return []
    cols = {c.lower(): c for c in df.columns}
    if {"entity","count"}.issubset({c.lower() for c in df.columns}):
        e = cols.get("entity","entity"); c = cols.get("count","count")
        return [(str(r[e]), int(r[c])) for _, r in df[[e,c]].fillna({"count":0}).iterrows()]
    return []

def detect_schema(obj):
    k = set(obj.keys())
    if {"tok","n"}.issubset(k): return "tok"
    if {"entity","count"}.issubset(k): return "entity"
    if any(x in k for x in ("text","content","title","description")): return "article"
    return "unknown"

def tokens_from_jsonl(jsonl_path, topk=None):
    cnt = Counter()
    schema = None
    with jsonl_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                continue
            if schema is None:
                schema = detect_schema(obj)
            if schema == "tok":
                tok = obj.get("tok"); n = obj.get("n", 1)
                if tok is None: continue
                try: n = int(n)
                except Exception: n = 1
                cnt[str(tok)] += n
            elif schema == "entity":
                tok = obj.get("entity"); n = obj.get("count", 1)
                if tok is None: continue
                try: n = int(n)
                except Exception: n = 1
                cnt[str(tok)] += n
            else:
                text = " ".join(str(obj.get(k,"")) for k in ("title","description","text","content"))
                for t in TOKEN_RE.findall(text.lower()):
                    if t in STOP: continue
                    cnt[t] += 1
    items = sorted(cnt.items(), key=lambda x: x[1], reverse=True)
    if topk: items = items[:topk]
    return items

dates = set(f.stem for f in ROOT.glob("*.csv"))
dates |= set(f.stem for f in ROOT.glob("*.jsonl") if not f.name.endswith("_tokens.jsonl"))

for d in sorted(dates):
    out = ROOT / f"{d}_tokens.jsonl"
    if out.exists() and out.stat().st_size > 0:
        continue
    pairs = []
    csvf = ROOT / f"{d}.csv"
    jsonlf = ROOT / f"{d}.jsonl"
    if csvf.exists():
        pairs = tokens_from_csv(csvf)
        if not pairs and jsonlf.exists():
            pairs = tokens_from_jsonl(jsonlf, topk=2000)
    elif jsonlf.exists():
        pairs = tokens_from_jsonl(jsonlf, topk=2000)
    else:
        out.write_text("", encoding="utf-8")
        print(f"skip {d} (no source)")
        continue
    write_tokens(out, pairs)
    print(f"built tokens {d} rows={len(pairs)}")