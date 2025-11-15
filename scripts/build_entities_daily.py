import pathlib, json

src = pathlib.Path("site/data/daily")
dst = pathlib.Path("site/data/entities_daily.jsonl")
dst.parent.mkdir(parents=True, exist_ok=True)

with dst.open("w", encoding="utf-8") as w:
    for f in sorted(src.glob("*.jsonl")):
        date = f.stem
        for line in f.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                o = json.loads(line)
            except Exception:
                continue
            o.setdefault("date", date)
            if "entity" in o and "tok" not in o:
                o["tok"] = o["entity"]
            if "count" in o and "n" not in o:
                o["n"] = o["count"]
            w.write(json.dumps(o, ensure_ascii=False) + "\n")

print("wrote", dst)