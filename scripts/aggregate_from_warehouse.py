import argparse, json, re, gzip, html
from pathlib import Path
from collections import Counter, defaultdict
import pandas as pd

FN_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")
TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9\-']{1,}", re.I)
URL_RE = re.compile(r"https?://\S+")
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")

def norm(s):
    if not s: return ""
    s = html.unescape(s)
    s = TAG_RE.sub(" ", s)
    s = URL_RE.sub(" ", s)
    s = s.replace("…"," ").replace("—","-").replace("–","-")
    return WS_RE.sub(" ", s).strip()

def s(x):
    if x is None: return ""
    if isinstance(x, str): return x
    if isinstance(x, dict):
        for k in ("name","title","source","publisher"):
            v = x.get(k)
            if isinstance(v, str) and v.strip():
                return v
        return ""
    return str(x)

def tokenize(text, min_len=3, stop=None):
    out=[]
    t=text.lower()
    for m in TOKEN_RE.finditer(t):
        w=m.group()
        if len(w)>=min_len and (not stop or w not in stop):
            out.append(w)
    return out

def read_stop(path):
    if not path: return set()
    p=Path(path)
    if not p.exists(): return set()
    rows=[x.strip().lower() for x in p.read_text(encoding="utf-8").splitlines() if x.strip()]
    return set(rows)

def iter_files(warehouse):
    files=[]
    for ext in ("*.jsonl","*.jsonl.gz"):
        files.extend(Path(warehouse).glob(ext))
    out=[]
    for f in files:
        m=FN_DATE_RE.search(f.name)
        if not m: continue
        out.append((pd.to_datetime(m.group(1)).date(), f))
    out.sort()
    return out

def open_jsonl(p):
    if str(p).endswith(".gz"):
        return gzip.open(p, "rt", encoding="utf-8", errors="ignore")
    return open(p, "r", encoding="utf-8", errors="ignore")

def main(warehouse, outdir, last_days, min_len, extra_stop):
    out=Path(outdir); (out/"aggregate").mkdir(parents=True, exist_ok=True)
    stop=read_stop(extra_stop)
    files=iter_files(warehouse)
    if not files:
        print(json.dumps({"rows":0,"days":0,"terms":0,"out":str(out)}))
        return
    dates=sorted({d for d,_ in files})
    if last_days>0 and len(dates)>last_days:
        keep=set(dates[-last_days:])
        files=[(d,f) for d,f in files if d in keep]

    per_day_count=Counter()
    per_day_pub=defaultdict(Counter)
    per_day_tok=defaultdict(Counter)
    rows_seen=0

    for d,fp in files:
        with open_jsonl(fp) as f:
            for line in f:
                try:
                    row=json.loads(line)
                except Exception:
                    continue
                date_raw=(row.get("date") or row.get("published_at") or row.get("published") or "")
                date_str=str(date_raw)[:10] if date_raw else ""
                if not date_str:
                    dd=d
                else:
                    try:
                        dd=pd.to_datetime(date_str).date()
                    except Exception:
                        dd=d

                pub = s(row.get("source")) or s(row.get("publisher"))
                pub = pub.strip()

                txt = " ".join([s(row.get("title")), s(row.get("description")), s(row.get("content"))])
                txt = norm(txt)
                if not txt: continue

                per_day_count[dd]+=1
                if pub: per_day_pub[dd][pub]+=1
                toks=tokenize(txt, min_len=min_len, stop=stop)
                if toks: per_day_tok[dd].update(toks)
                rows_seen+=1

    if rows_seen==0:
        print(json.dumps({"rows":0,"days":0,"terms":0,"out":str(out)}))
        return

    art_rows=[{"date":pd.to_datetime(d), "articles":c} for d,c in sorted(per_day_count.items())]
    pd.DataFrame(art_rows).to_csv(out/"aggregate"/"articles_by_day.csv", index=False)

    pub_rows=[]
    for d,cnt in per_day_pub.items():
        for pub,c in cnt.items():
            pub_rows.append({"date":pd.to_datetime(d),"publisher":pub,"count":c})
    pd.DataFrame(pub_rows).sort_values(["date","count"],ascending=[True,False]).to_csv(out/"aggregate"/"publisher_by_day.csv", index=False)

    tok_rows=[]
    for d,cnt in per_day_tok.items():
        for t,c in cnt.items():
            tok_rows.append({"date":pd.to_datetime(d),"term":t,"count":c})
    pd.DataFrame(tok_rows).sort_values(["date","count"],ascending=[True,False]).to_csv(out/"aggregate"/"tokens_by_day.csv", index=False)

    print(json.dumps({
        "rows":len(tok_rows),
        "days":len(per_day_count),
        "terms":len({t for _,c in per_day_tok.items() for t in c}),
        "out":str(out)
    }))

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--out", default="run")
    ap.add_argument("--last-days", type=int, default=30)
    ap.add_argument("--min-len", type=int, default=4)
    ap.add_argument("--extra-stop", default="config/extra_noise.txt")
    a=ap.parse_args()
    main(a.warehouse, a.out, a.last_days, a.min_len, a.extra_stop)