cd /Users/mymac/Desktop/newstrend
cat > scripts/make_trends_json.py <<'PY'
import argparse, json
from pathlib import Path
import pandas as pd

def read_tokens_csv(fp, min_len):
    try:
        df = pd.read_csv(fp)
    except Exception:
        return {}
    cols = {c.lower(): c for c in df.columns}
    if {"entity","count"}.issubset({c.lower() for c in df.columns}):
        e, c = cols.get("entity","entity"), cols.get("count","count")
    elif {"tok","n"}.issubset({c.lower() for c in df.columns}):
        e, c = cols.get("tok","tok"), cols.get("n","n")
    else:
        return {}
    df[e] = df[e].astype(str)
    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    df = df[df[e].str.len() >= min_len]
    return dict(df[[e, c]].values)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tokens-dir", default="data/warehouse/daily")
    ap.add_argument("--out", default="site/data/trends.json")
    ap.add_argument("--last-days", type=int, default=60)
    ap.add_argument("--topk", type=int, default=200)
    ap.add_argument("--min-len", type=int, default=4)
    args = ap.parse_args()

    td = Path(args.tokens_dir)
    files = sorted(td.glob("*_tokens.csv"))
    if not files:
        files = sorted(td.glob("*_tokens.jsonl"))

    def get_date(fp: Path) -> str:
        name = fp.name
        for suf in ("_tokens.csv", "_tokens.jsonl"):
            if name.endswith(suf):
                return name[:-len(suf)]
        return fp.stem

    dates = sorted({get_date(f) for f in files})
    if args.last_days > 0 and len(dates) > args.last_days:
        dates = dates[-args.last_days:]

    by_date = {}
    for d in dates:
        csvf = td / f"{d}_tokens.csv"
        jsonlf = td / f"{d}_tokens.jsonl"
        if csvf.exists():
            by_date[d] = read_tokens_csv(csvf, args.min_len)
        elif jsonlf.exists():
            rows = []
            with jsonlf.open("r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    s = line.strip()
                    if not s: continue
                    try:
                        o = json.loads(s)
                    except Exception:
                        continue
                    tok = o.get("tok") or o.get("entity")
                    n = o.get("n") if "n" in o else o.get("count")
                    if tok is None or n is None: continue
                    rows.append((str(tok), int(n)))
            if rows:
                import pandas as pd
                df = pd.DataFrame(rows, columns=["entity","count"])
                df = df[df["entity"].str.len() >= args.min_len]
                by_date[d] = dict(df.values)
            else:
                by_date[d] = {}
        else:
            by_date[d] = {}

    totals = {}
    for d in dates:
        for t, n in by_date.get(d, {}).items():
            totals[t] = totals.get(t, 0) + int(n)
    top_tokens = [t for t, _ in sorted(totals.items(), key=lambda x: x[1], reverse=True)[:args.topk]]

    series = []
    for t in top_tokens:
        series.append({"name": t, "data": [int(by_date.get(d, {}).get(t, 0)) for d in dates]})

    out = {"dates": dates, "x": dates, "tokens": top_tokens, "series": series}
    outp = Path(args.out); outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {outp} dates={len(dates)} tokens={len(top_tokens)}")

if __name__ == "__main__":
    main()
PY