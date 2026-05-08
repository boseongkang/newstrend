from pathlib import Path
import shutil

root = Path("data/live_newsapi")
for p in sorted(root.glob("*.jsonl")):
    stem = p.stem
    if "T" not in stem:
        continue
    day = stem.split("T", 1)[0]
    d = root / day
    d.mkdir(parents=True, exist_ok=True)
    dst = d / p.name
    if not dst.exists():
        shutil.move(str(p), str(dst))
        print(dst)
