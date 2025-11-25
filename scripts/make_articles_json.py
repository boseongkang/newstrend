import argparse, json
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-dir", default="data/silver")
    ap.add_argument("--out", default="site/data/articles.json")
    ap.add_argument("--last-days", type=int, default=90)
    args = ap.parse_args()

    sd = Path(args.silver_dir)
    files = sorted(f for f in sd.glob("*.jsonl") if f.stem[:4].isdigit())
    if not files:
        raise SystemExit(f"no silver files in {sd}")

    dates = [f.stem for f in files]
    if args.last_days > 0 and len(dates) > args.last_days:
        files = files[-args.last_days:]
        dates = [f.stem for f in files]

    counts = []
    for f in files:
        n = 0
        with f.open("r", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                if line.strip():
                    n += 1
        counts.append(int(n))

    out = {"dates": dates, "articles": counts}

    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print("wrote", outp, "days", len(dates))

if __name__ == "__main__":
    main()