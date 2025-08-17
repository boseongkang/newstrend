# src/news_trend/quickview_today.py
from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path
from news_trend.quickview import quickview  # 앞서 만든 quickview.py를 사용

ROOT = Path(__file__).resolve().parents[2]
INDIR = ROOT / "data"
DATE_UTC = datetime.now(timezone.utc).date().isoformat()

def main():
    jsonl = INDIR / "silver" / f"{DATE_UTC}.jsonl"
    if not jsonl.exists():
        jsonl_raw = INDIR / "raw" / f"{DATE_UTC}.jsonl"
        if jsonl_raw.exists():
            kind = "raw"
        else:
            raise SystemExit(f"No input File: {jsonl} or {jsonl_raw}")
    else:
        kind = "silver"

    quickview(
        date_str=DATE_UTC,
        kind=kind,
        indir=INDIR,
        top=20,
        sample=10,
        min_len=3,       
        include_content=True,
        extra_stopwords=[]
    )

if __name__ == "__main__":
    main()
