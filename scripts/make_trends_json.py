import argparse
import json
from pathlib import Path

import pandas as pd


def read_tokens_csv(path: Path, min_len: int):
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}
    cols_lower = {c.lower(): c for c in df.columns}
    if {"entity", "count"}.issubset(cols_lower.keys()):
        e_col = cols_lower["entity"]
        c_col = cols_lower["count"]
    elif {"tok", "n"}.issubset(cols_lower.keys()):
        e_col = cols_lower["tok"]
        c_col = cols_lower["n"]
    elif {"term", "n"}.issubset(cols_lower.keys()):
        e_col = cols_lower["term"]
        c_col = cols_lower["n"]
    else:
        return {}
    df[e_col] = df[e_col].astype(str)
    df[c_col] = pd.to_numeric(df[c_col], errors="coerce").fillna(0).astype(int)
    df = df[df[e_col].str.len() >= min_len]
    return dict(df[[e_col, c_col]].values)


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
    parser = argparse.ArgumentParser()
    parser.add_argument("--tokens-dir", default="data/warehouse/daily")
    parser.add_argument("--out", default="site/data/trends.json")
    parser.add_argument("--last-days", type=int, default=90)
    parser.add_argument("--topk", type=int, default=200)
    parser.add_argument("--min-len", type=int, default=4)
    args = parser.parse_args()

    tokens_dir = Path(args.tokens_dir)
    csv_files = sorted(tokens_dir.glob("*_tokens.csv"))
    jsonl_files = sorted(tokens_dir.glob("*_tokens.jsonl"))
    files = csv_files + jsonl_files

    if not files:
        raise SystemExit("no *_tokens.csv or *_tokens.jsonl files found")

    dates = sorted({get_date_from_filename(p) for p in files})
    if args.last_days > 0 and len(dates) > args.last_days:
        dates = dates[-args.last_days :]

    by_date = {}
    for d in dates:
        csv_path = tokens_dir / f"{d}_tokens.csv"
        jsonl_path = tokens_dir / f"{d}_tokens.jsonl"
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