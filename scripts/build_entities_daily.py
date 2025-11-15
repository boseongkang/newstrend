import pathlib, json, gzip, sys

def iter_jsonl(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if s:
                yield s

def iter_jsonl_gz(path):
    with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if s:
                yield s

src_site = pathlib.Path("site/data/daily")
src_wh   = pathlib.Path("data/warehouse/daily")
dst      = pathlib.Path("site/data/entities_daily.jsonl")
dst.parent.mkdir(parents=True, exist_ok=True)

inputs = []
if src_site.is_dir():
    inputs.extend(sorted(src_site.glob("*.jsonl")))
    inputs.extend(sorted(src_site.glob("*.jsonl.gz")))
if not inputs and src_wh.is_dir():
    inputs.extend(sorted(src_wh.glob("*.jsonl")))
    inputs.extend(sorted(src_wh.glob("*.jsonl.gz")))

written = 0
with dst.open("w", encoding="utf-8") as w:
    for f in inputs:
        name = f.name
        stem = f.stem
        if stem.endswith(".jsonl"):
            stem = stem[:-6]
        date = stem.replace("_tokens", "")
        try:
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
                rec = {"date": date, "tok": str(tok), "n": int(n)}
                w.write(json.dumps(rec, ensure_ascii=False) + "\n")
                written += 1
        except Exception:
            continue

print(f"wrote {dst} rows={written}")
# 항상 정상 종료
sys.exit(0)