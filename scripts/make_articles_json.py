import argparse, json, gzip
from pathlib import Path

def count_lines(fp):
    if not fp.exists():
        return 0
    if fp.suffix == ".gz":
        f = gzip.open(fp, "rt", encoding="utf-8", errors="ignore")
    else:
        f = fp.open("r", encoding="utf-8", errors="ignore")
    n = 0
    try:
        for line in f:
            if line.strip():
                n += 1
    finally:
        f.close()
    return n

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--releases-dir", default="data/releases")
    ap.add_argument("--out", default="site/data/articles.json")
    ap.add_argument("--last-days", type=int, default=90)
    args = ap.parse_args()

    rd = Path(args.releases_dir)
    files = sorted(rd.glob("news_*.jsonl")) + sorted(rd.glob("news_*.jsonl.gz"))

    dates = []
    counts = {}

    for fp in files:
        name = fp.name
        if not name.startswith("news_"):
            continue
        if name.endswith(".jsonl.gz"):
            d = name[len("news_"):-len(".jsonl.gz")]
        elif name.endswith(".jsonl"):
            d = name[len("news_"):-len(".jsonl")]
        else:
            continue
        dates.append(d)

    dates = sorted(set(dates))
    if args.last_days > 0 and len(dates) > args.last_days:
        dates = dates[-args.last_days:]

    for d in dates:
        j = rd / f"news_{d}.jsonl"
        gz = rd / f"news_{d}.jsonl.gz"
        n = 0
        if j.exists() or gz.exists():
            n = count_lines(j if j.exists() else gz)
        counts[d] = n

    out = {
        "dates": dates,
        "articles": [counts.get(d, 0) for d in dates]
    }

    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print("wrote", outp, "days", len(dates))

if __name__ == "__main__":
    main()
