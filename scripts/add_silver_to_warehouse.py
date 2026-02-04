import argparse
import json
import re
import shutil
from collections import Counter
from pathlib import Path

import pandas as pd


def tokenize(text, min_len):
    words = re.findall(r"[A-Za-z][A-Za-z0-9']*", text.lower())
    return [w for w in words if len(w) >= min_len]


def build_tokens_from_silver(fp: Path, min_len: int) -> Counter:
    c = Counter()
    with fp.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                continue
            parts = []
            for v in obj.values():
                if isinstance(v, str):
                    parts.append(v)
            if not parts:
                continue
            text = " ".join(parts)
            for tok in tokenize(text, min_len):
                c[tok] += 1
    return c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-dir", default="data/silver")
    ap.add_argument("--tokens-dir", default="data/warehouse/daily")
    ap.add_argument("--releases-dir", default="data/releases")
    ap.add_argument("--min-len", type=int, default=4)
    args = ap.parse_args()

    silver_dir = Path(args.silver_dir)
    tokens_dir = Path(args.tokens_dir)
    releases_dir = Path(args.releases_dir)

    tokens_dir.mkdir(parents=True, exist_ok=True)
    releases_dir.mkdir(parents=True, exist_ok=True)

    existing = []
    for f in tokens_dir.glob("*_tokens.csv"):
        name = f.name
        if name.endswith("_tokens.csv"):
            existing.append(name[: -len("_tokens.csv")])
    existing_dates = set(existing)
    min_existing = min(existing_dates) if existing_dates else None

    silver_files = sorted(silver_dir.glob("*.jsonl"))

    for sf in silver_files:
        d = sf.stem
        if min_existing and d < min_existing:
            continue
        token_path = tokens_dir / f"{d}_tokens.csv"
        if token_path.exists():
            continue
        counter = build_tokens_from_silver(sf, args.min_len)
        if counter:
            items = sorted(counter.items(), key=lambda x: x[1], reverse=True)
            df = pd.DataFrame(items, columns=["entity", "count"])
            df.to_csv(token_path, index=False)
            print("wrote tokens", token_path)
        rel_json = releases_dir / f"news_{d}.jsonl"
        rel_gz = releases_dir / f"news_{d}.jsonl.gz"
        if not rel_json.exists() and not rel_gz.exists():
            shutil.copy2(sf, rel_json)
            print("wrote release", rel_json)


if __name__ == "__main__":
    main()