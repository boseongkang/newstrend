from __future__ import annotations
from pathlib import Path
import json, os
from datetime import datetime, timedelta, timezone, date as ddate
from typing import Iterable
from .dedup import dedup_rows

def _iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def aggregate_windows(
    date: str,
    inroot: str = "data/raw_windows",
    daily_outdir: str = "data/raw_newsapi",
    silver_outdir: str = "data/silver_newsapi",
) -> tuple[Path, Path]:
    day = ddate.fromisoformat(date)
    windows_dir = Path(inroot) / day.isoformat()
    assert windows_dir.exists(), f"missing directory: {windows_dir}"

    daily_out = Path(daily_outdir); daily_out.mkdir(parents=True, exist_ok=True)
    silver_out = Path(silver_outdir); silver_out.mkdir(parents=True, exist_ok=True)

    daily_path = daily_out / f"{day.isoformat()}.jsonl"
    rows = []
    for p in sorted(windows_dir.glob("*.jsonl")):
        if p.name.startswith("_"):
            continue
        rows.extend(_iter_jsonl(p))
    with daily_path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"[OK] merged {len(rows)} rows -> {daily_path}")

    cleaned = list(dedup_rows(rows))
    silver_path = silver_out / f"{day.isoformat()}.jsonl"
    with silver_path.open("w", encoding="utf-8") as f:
        for r in cleaned:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"[OK] dedup {len(rows)} -> {len(cleaned)} -> {silver_path}")
    return daily_path, silver_path
