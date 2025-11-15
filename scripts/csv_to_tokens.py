import json
from pathlib import Path
from collections import Counter
import pandas as pd

root = Path("data/warehouse/daily")
root.mkdir(parents=True, exist_ok=True)

def write_tokens(out_path, items):
    with out_path.open("w", encoding="utf-8") as w:
        for tok, n in items:
            w.write(json.dumps({"tok": str(tok), "n": int(n)}, ensure_ascii=False) + "\n")

dates = set(f.stem for f in root.glob("*.csv"))
dates |= set(f.stem for f in root.glob("*.jsonl") if not f.name.endswith("_tokens.jsonl"))

for d in sorted(dates):
    out = root / f"{d}_tokens.jsonl"
    if out.exists() and out.stat().st_size > 0:
        continue

    csvf = root / f"{d}.csv"
    jsonlf = root / f"{d}.jsonl"

    if csvf.exists():
        try:
            df = pd.read_csv(csvf)
        except Exception:
            df = pd.DataFrame(columns=["entity","count"])
        cols = {c.lower(): c for c in df.columns}
        if {"entity","count"}.issubset(set(k.lower() for k in df.columns)):
            e = cols.get("entity","entity"); c = cols.get("count","count")
            items = [(r[e], int(r[c])) for _, r in df[[e,c]].fillna({"count":0}).iterrows()]
            write_tokens(out, items)
            print(f"csv->tokens {d} rows={len(items)}")
            continue

    if jsonlf.exists():
        cnt = Counter()
        with jsonlf.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try:
                    o = json.loads(line)
                except Exception:
                    continue
                tok = o.get("tok") or o.get("entity")
                n = o.get("n") if "n" in o else o.get("count", 1)
                if tok is None: continue
                try:
                    n = int(n)
                except Exception:
                    n = 1
                cnt[str(tok)] += n
        items = sorted(cnt.items(), key=lambda x: x[1], reverse=True)
        write_tokens(out, items)
        print(f"jsonl->tokens {d} rows={len(items)}")
        continue

    out.write_text("", encoding="utf-8")
    print(f"skip {d} (no source)")