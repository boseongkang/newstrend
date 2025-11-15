import pathlib, json

src = pathlib.Path("data/warehouse/daily")
dst = pathlib.Path("site/data/entities_daily.jsonl")
dst.parent.mkdir(parents=True, exist_ok=True)

files = sorted(src.glob("*.jsonl"))
with dst.open("w", encoding="utf-8") as w:
    for f in files:
        date = f.stem.replace("_tokens", "")
        with f.open("r", encoding="utf-8", errors="ignore") as r:
            for line in r:
                line = line.strip()
                if not line:
                    continue
                try:
                    o = json.loads(line)
                except Exception:
                    continue
                tok = o.get("tok") or o.get("entity")
                n = o.get("n") if "n" in o else o.get("count")
                if tok is None or n is None:
                    continue
                w.write(json.dumps({"date": date, "tok": str(tok), "n": int(n)}, ensure_ascii=False) + "\n")

print("wrote", dst)