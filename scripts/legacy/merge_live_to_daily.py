from pathlib import Path
import argparse, json

def dedup_key(a: dict) -> str:
    return a.get("url") or f"{a.get('source',{}).get('name','')}|{a.get('title','')}|{a.get('publishedAt','')}"

def merge_date(date_str: str) -> Path:
    day_dir = Path("data/live_newsapi") / date_str
    files = sorted(day_dir.glob("*.jsonl"))
    if not files:
        raise FileNotFoundError(f"no live files for {date_str} under {day_dir}")

    out_dir = Path("data/silver_newsapi")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{date_str}.jsonl"

    seen = set()
    written = 0
    with out_path.open("w", encoding="utf-8") as w:
        for f in files:
            with f.open("r", encoding="utf-8") as r:
                for line in r:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    k = dedup_key(obj)
                    if k in seen:
                        continue
                    seen.add(k)
                    w.write(json.dumps(obj, ensure_ascii=False) + "\n")
                    written += 1
    print(f"[merge] {date_str}: {len(files)} files -> {written} articles -> {out_path}")
    return out_path

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD (UTC)")
    args = ap.parse_args()
    merge_date(args.date)
