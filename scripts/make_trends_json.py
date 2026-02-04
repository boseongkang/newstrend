import argparse
import json
from pathlib import Path

import pandas as pd


def read_tokens_csv(path: Path, min_len: int):
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}
    lower = {c.lower(): c for c in df.columns}
    if {"entity", "count"}.issubset(lower.keys()):
        e = lower["entity"]
        c = lower["count"]
    elif {"tok", "n"}.issubset(lower.keys()):
        e = lower["tok"]
        c = lower["n"]
    elif {"term", "n"}.issubset(lower.keys()):
        e = lower["term"]
        c = lower["n"]
    else:
        return {}
    df[e] = df[e].astype(str)
    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    df = df[df[e].str.len() >= min_len]
    return dict(df[[e, c]].values)


def read_tokens_jsonl(path: Path, min_len: int):
    rows = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            tok = obj.get("tok") or obj.get("term") or obj.get("entity")
            n = obj.get("n") if "n" in obj else obj.get("count")
            if tok is None or n is None:
                continue
            rows.append((str(tok), int(n)))
    if not rows:
        return {}
    df = pd.DataFrame(rows, columns=["tok", "n"])
    df = df[df["tok"].str.len() >= min_len]
    return dict(df.values)


def get_date_from_filename(path: Path) -> str:
    name = path.name
    for suffix in ("_tokens.csv", "_tokens.jsonl"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return path.stem


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--tokens-dir", default="data/warehouse/daily")
    p.add_argument("--out", default="site/data/trends.json")
    p.add_argument("--last-days", type=int, default=90)
    p.add_argument("--topk", type=int, default=200)
    p.add_argument("--min-len", type=int, default=4)
    args = p.parse_args()

    td = Path(args.tokens_dir)
    csv_files = sorted(td.glob("*_tokens.csv"))
    jsonl_files = sorted(td.glob("*_tokens.jsonl"))
    files = csv_files + jsonl_files
    if not files:
        raise SystemExit("no *_tokens.csv or *_tokens.jsonl files found")

    dates = sorted({get_date_from_filename(f) for f in files})
    if args.last_days > 0 and len(dates) > args.last_days:
        dates = dates[-args.last_days :]

    by_date = {}
    for d in dates:
        csv_path = td / f"{d}_tokens.csv"
        jsonl_path = td / f"{d}_tokens.jsonl"
        if csv_path.exists():
            by_date[d] = read_tokens_csv(csv_path, args.min_len)
        elif jsonl_path.exists():
            by_date[d] = read_tokens_jsonl(jsonl_path, args.min_len)
        else:
            by_date[d] = {}

    totals = {}
    for d in dates:
        for tok, n in by_date.get(d, {}).items():
            totals[tok] = totals.get(tok, 0) + int(n)

    top_tokens = [
        t for t, _ in sorted(totals.items(), key=lambda x: x[1], reverse=True)[: args.topk]
    ]

    series = {t: [int(by_date.get(d, {}).get(t, 0)) for d in dates] for t in top_tokens}

    out = {
        "dates": dates,
        "terms": top_tokens,
        "top": top_tokens,
        "series": series,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print("wrote", out_path, "dates", len(dates), "terms", len(top_tokens))


if __name__ == "__main__":
    main()