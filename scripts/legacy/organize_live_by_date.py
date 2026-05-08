from pathlib import Path
import re, shutil

ROOT = Path("data/live_newsapi")
pat = re.compile(r"(\d{4}-\d{2}-\d{2})T\d{2}-\d{2}Z\.jsonl$")

moved = 0
for p in ROOT.glob("*.jsonl"):
    m = pat.search(p.name)
    if not m:
        continue
    d = m.group(1)
    outdir = ROOT / d
    outdir.mkdir(parents=True, exist_ok=True)
    dest = outdir / p.name
    if dest.exists():
        continue
    shutil.move(str(p), str(dest))
    moved += 1

print(f"moved {moved} files.")
