import argparse, json, re, html
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict
import pandas as pd

TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9\-']{1,}", re.I)
URL_RE = re.compile(r"https?://\S+")
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")

def norm(s:str)->str:
    if not s: return ""
    s = html.unescape(s)
    s = TAG_RE.sub(" ", s)
    s = URL_RE.sub(" ", s)
    s = s.replace("…"," ").replace("—","-").replace("–","-")
    return WS_RE.sub(" ", s).strip()

def tokenize(text:str, min_len:int=3):
    toks=[]
    for m in TOKEN_RE.finditer(text.lower()):
        t=m.group()
        if len(t)>=min_len: toks.append(t)
    return toks

def iter_docs_from_master(master:Path):
    with master.open("r",encoding="utf-8") as f:
        for line in f:
            try:
                row=json.loads(line)
            except Exception:
                continue
            date=(row.get("date") or row.get("published_at") or row.get("published") or "")[:10]
            if not date: continue
            pub=(row.get("source") or row.get("publisher") or "").strip()
            txt=" ".join([row.get("title") or "", row.get("description") or "", row.get("content") or ""])
            yield date, pub, norm(txt)

def iter_docs_from_root(root:Path, pattern:str):
    for p in root.rglob(pattern):
        try:
            with p.open("r",encoding="utf-8") as f:
                for line in f:
                    try:
                        row=json.loads(line)
                    except Exception:
                        continue
                    date=(row.get("date") or row.get("published_at") or row.get("published") or "")[:10]
                    if not date:
                        try:
                            ts=row.get("timestamp") or row.get("time") or ""
                            date=str(ts)[:10]
                        except Exception:
                            date=""
                    if not date: continue
                    pub=(row.get("source") or row.get("publisher") or "").strip()
                    txt=" ".join([row.get("title") or "", row.get("description") or "", row.get("content") or ""])
                    yield date, pub, norm(txt)
        except Exception:
            continue

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--master", default="")
    ap.add_argument("--root", default="data/live_newsapi")
    ap.add_argument("--pattern", default="*.jsonl")
    ap.add_argument("--outdir", default="reports/aggregate")
    ap.add_argument("--min-len", type=int, default=3)
    ap.add_argument("--top", type=int, default=0)
    ap.add_argument("--start", default="", help="inclusive start date YYYY-MM-DD")
    ap.add_argument("--end", default="", help="inclusive end date YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=0, help="if >0 keep last N days ending at --end or max date")
    args=ap.parse_args()

    out=Path(args.outdir); out.mkdir(parents=True, exist_ok=True)

    if args.master:
        docs=iter_docs_from_master(Path(args.master))
    else:
        docs=iter_docs_from_root(Path(args.root), args.pattern)

    items = []
    for date, pub, text in docs:
        try:
            d = pd.to_datetime(date).date()
        except Exception:
            continue
        items.append((d, pub, text))

    if not items:
        print("no documents found")
        return

    start_dt = pd.to_datetime(args.start).date() if args.start else None
    end_dt = pd.to_datetime(args.end).date() if args.end else None
    if args.days and args.days > 0:
        if not end_dt:
            end_dt = max(d for d, _, _ in items)
        start_dt = (pd.to_datetime(end_dt) - pd.Timedelta(days=args.days-1)).date()

    def in_range(d):
        if start_dt and d < start_dt:
            return False
        if end_dt and d > end_dt:
            return False
        return True

    per_day_count = Counter()
    per_day_pub = defaultdict(Counter)
    per_day_tok = defaultdict(Counter)

    for d, pub, text in items:
        if not in_range(d):
            continue
        per_day_count[d] += 1
        if pub:
            per_day_pub[d][pub] += 1
        toks = tokenize(text, args.min_len)
        per_day_tok[d].update(toks)

    rows=[]
    for d,c in sorted(per_day_count.items()):
        rows.append({"date":pd.to_datetime(d), "articles":c})
    pd.DataFrame(rows).to_csv(out/"articles_by_day.csv",index=False)

    pub_rows=[]
    for d,cnt in per_day_pub.items():
        for pub,c in cnt.items():
            pub_rows.append({"date":pd.to_datetime(d),"publisher":pub,"count":c})
    pd.DataFrame(pub_rows).sort_values(["date","count"],ascending=[True,False]).to_csv(out/"publisher_by_day.csv",index=False)

    tok_rows=[]
    for d,cnt in per_day_tok.items():
        if args.top and args.top>0:
            items=cnt.most_common(args.top)
        else:
            items=cnt.items()
        for t,c in items:
            tok_rows.append({"date":pd.to_datetime(d),"term":t,"count":c})
    pd.DataFrame(tok_rows).sort_values(["date","count"],ascending=[True,False]).to_csv(out/"tokens_by_day.csv",index=False)

    print(f"saved: {out/'articles_by_day.csv'}")
    print(f"saved: {out/'publisher_by_day.csv'}")
    print(f"saved: {out/'tokens_by_day.csv'}")

if __name__=="__main__":
    main()