import pathlib, json, gzip, sys, pandas as pd

def iter_jsonl(p):
    with open(p, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if s:
                yield s

def iter_jsonl_gz(p):
    with gzip.open(p, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if s:
                yield s

src_site = pathlib.Path("site/data/daily")
src_wh = pathlib.Path("data/warehouse/daily")
dst = pathlib.Path("site/data/entities_daily.jsonl")
dst.parent.mkdir(parents=True, exist_ok=True)

inputs = []
if src_site.is_dir():
    inputs += sorted(src_site.glob("*.jsonl"))
    inputs += sorted(src_site.glob("*.jsonl.gz"))
if not inputs and src_wh.is_dir():
    inputs += sorted(src_wh.glob("*.jsonl"))
    inputs += sorted(src_wh.glob("*.jsonl.gz"))

written = 0
with dst.open("w", encoding="utf-8") as w:
    for f in inputs:
        stem = pathlib.Path(f).stem
        if stem.endswith(".jsonl"):
            stem = stem[:-6]
        date = stem.replace("_tokens", "")
        it = iter_jsonl_gz(f) if str(f).endswith(".gz") else iter_jsonl(f)
        for s in it:
            try:
                o = json.loads(s)
            except Exception:
                continue
            tok = o.get("tok") or o.get("entity")
            n = o.get("n") if "n" in o else o.get("count")
            if tok is None or n is None:
                continue
            w.write(json.dumps({"date": date, "tok": str(tok), "n": int(n)}, ensure_ascii=False) + "\n")
            written += 1

# CSV까지 백업 입력으로 합치기(위에서 아무것도 못 썼을 때만)
if written == 0 and src_wh.is_dir():
    for f in sorted(src_wh.glob("*.csv")):
        date = f.stem
        try:
            df = pd.read_csv(f)
        except Exception:
            continue
        if not {"entity","count"}.issubset(df.columns):
            continue
        with dst.open("a", encoding="utf-8") as w:
            for r in df.itertuples(index=False):
                w.write(json.dumps({"date": date, "tok": str(r.entity), "n": int(r.count)}, ensure_ascii=False) + "\n")
                written += 1

print(f"wrote {dst} rows={written}")
sys.exit(0)