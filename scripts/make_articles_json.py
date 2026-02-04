import argparse
import json
import gzip
from pathlib import Path


def count_lines(path: Path) -> int:
    if not path.exists():
        return 0
    if path.suffix == ".gz":
        f = gzip.open(path, "rt", encoding="utf-8", errors="ignore")
    else:
        f = path.open("r", encoding="utf-8", errors="ignore")
    n = 0
    try:
        for line in f:
            if line.strip():
                n += 1
    finally:
        f.close()
    return n


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--releases-dir", default="data/releases")
    p.add_argument("--silver-dir", default="data/silver")
    p.add_argument("--out", default="site/data/articles.json")
    p.add_argument("--last-days", type=int, default=90)
    args = p.parse_args()

    rd = Path(args.releases_dir)
    sd = Path(args.silver_dir)

    dates_set = set()

    rel_files = sorted(rd.glob("news_*.jsonl")) + sorted(rd.glob("news_*.jsonl.gz"))
    for fp in rel_files:
        name = fp.name
        if not name.startswith("news_"):
            continue
        if name.endswith(".jsonl.gz"):
            d = name[len("news_") : -len(".jsonl.gz")]
        elif name.endswith(".jsonl"):
            d = name[len("news_") : -len(".jsonl")]
        else:
            continue
        dates_set.add(d)

    sil_files = sorted(sd.glob("*.jsonl")) + sorted(sd.glob("*.jsonl.gz"))
    for fp in sil_files:
        name = fp.name
        if name.endswith(".jsonl.gz"):
            d = name[: -len(".jsonl.gz")]
        elif name.endswith(".jsonl"):
            d = name[: -len(".jsonl")]
        else:
            continue
        if len(d) == 10 and d[4] == "-" and d[7] == "-":
            dates_set.add(d)

    dates = sorted(dates_set)
    if args.last_days > 0 and len(dates) > args.last_days:
        dates = dates[-args.last_days :]

    counts = {}
    for d in dates:
        jrel = rd / f"news_{d}.jsonl"
        grel = rd / f"news_{d}.jsonl.gz"
        jsil = sd / f"{d}.jsonl"
        gsil = sd / f"{d}.jsonl.gz"
        if jrel.exists() or grel.exists():
            counts[d] = count_lines(jrel if jrel.exists() else grel)
        elif jsil.exists() or gsil.exists():
            counts[d] = count_lines(jsil if jsil.exists() else gsil)
        else:
            counts[d] = 0

    out = {
        "dates": dates,
        "articles": [counts[d] for d in dates],
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print("wrote", out_path, "days", len(dates))


if __name__ == "__main__":
    main()