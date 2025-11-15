import json, csv
from pathlib import Path

root = Path("data/warehouse/daily")
paths = sorted(root.glob("*_tokens.jsonl"))

for jf in paths:
    cf = jf.with_suffix("").with_suffix(".csv")
    rows = []
    with jf.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                o = json.loads(s)
            except Exception:
                continue
            tok = o.get("tok")
            n = o.get("n")
            if tok is None or n is None:
                continue
            try:
                n = int(n)
            except Exception:
                continue
            rows.append((tok, n))
    cf.parent.mkdir(parents=True, exist_ok=True)
    with cf.open("w", newline="", encoding="utf-8") as out:
        w = csv.writer(out)
        w.writerow(["tok", "n"])
        w.writerows(rows)
    print(f"wrote {cf} rows={len(rows)}")