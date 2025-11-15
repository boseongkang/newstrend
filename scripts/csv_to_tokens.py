import pandas as pd, json, pathlib

p = pathlib.Path("data/warehouse/daily")
p.mkdir(parents=True, exist_ok=True)

for f in sorted(p.glob("*.csv")):
    df = pd.read_csv(f)
    if df.empty or not {"entity","count"}.issubset(df.columns):
        continue
    j = p / (f.stem + "_tokens.jsonl")
    with j.open("w", encoding="utf-8") as w:
        for r in df.itertuples(index=False):
            w.write(json.dumps({"tok": str(r.entity), "n": int(r.count)}, ensure_ascii=False) + "\n")
print("csv -> *_tokens.jsonl done")