from pathlib import Path
import re, shutil

root = Path("data/live_newsapi")
pat = re.compile(r"(\d{4}-\d{2}-\d{2})T\d{2}-\d{2}Z\.jsonl$")

moved = 0
for p in sorted(root.rglob("*.jsonl")):
    rel = p.relative_to(root)
    if len(rel.parts) >= 3:
        y, m, d = rel.parts[0], rel.parts[1], rel.parts[2]
        if len(y) == 4 and len(m) == 2 and len(d) == 2:
            continue
    m = pat.search(p.name)
    if not m:
        continue
    d = m.group(1)
    y, mth, day = d.split("-")
    outdir = root / y / mth / day
    outdir.mkdir(parents=True, exist_ok=True)
    dest = outdir / p.name
    if dest.resolve() == p.resolve():
        continue
    if dest.exists():
        continue
    shutil.move(str(p), str(dest))
    moved += 1

print(moved)
