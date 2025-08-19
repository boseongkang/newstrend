import os, json, time
from pathlib import Path
from typing import Optional, List, Dict
import requests
from datetime import datetime, date as ddate, time as dtime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("NEWSAPI_KEY")
BASE_URL = "https://newsapi.org/v2/everything"
PAGE_SIZE = 100

def _parse_date_arg(d: Optional[str]) -> ddate:
    today = datetime.now(timezone.utc).date()
    if not d or d.lower() == "today":
        return today
    if d.lower() == "yesterday":
        return today - timedelta(days=1)
    return ddate.fromisoformat(d)

def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def fetch_newsapi(
    query: Optional[str] = None,
    hours_split: int = 2,
    max_pages_per_window: int = 8,
    outdir: str = "data/raw",
    date: Optional[str] = None,
    pause: float = 0.25,
) -> Path:
    assert API_KEY, "NEWSAPI_KEY is missing"
    assert hours_split > 0, "hours_split must be > 0"

    target_day = _parse_date_arg(date)
    start = datetime.combine(target_day, dtime(0, 0, 0, tzinfo=timezone.utc))
    end = start + timedelta(days=1)
    delta = timedelta(hours=hours_split)

    outdir_p = Path(outdir); outdir_p.mkdir(parents=True, exist_ok=True)
    outfile = outdir_p / f"newsapi_{target_day.isoformat()}.jsonl"

    rows_count = 0
    with outfile.open("w", encoding="utf-8") as f:
        window_start = start
        while window_start < end:
            window_end = min(window_start + delta, end)

            for page in range(1, max_pages_per_window + 1):
                params = {
                    "from": _iso_utc(window_start),
                    "to": _iso_utc(window_end),
                    "pageSize": PAGE_SIZE,
                    "page": page,
                    "apiKey": API_KEY,
                    "language": "en",
                    "sortBy": "publishedAt",
                }
                if query and query.strip():
                    params["q"] = query.strip()
                else:
                    params["q"] = "news"

                r = requests.get(BASE_URL, params=params, timeout=40)
                try:
                    r.raise_for_status()
                except requests.HTTPError as e:
                    break

                payload = r.json()
                articles = payload.get("articles") or []
                if not articles:
                    break

                for a in articles:
                    row = {
                        "article_id": f"newsapi:{a.get('url')}",
                        "title": a.get("title"),
                        "url": a.get("url"),
                        "publisher": (a.get("source") or {}).get("name"),
                        "published_at": a.get("publishedAt"),
                        "description": a.get("description"),
                        "content": a.get("content"),
                        "raw_source": "newsapi",
                    }
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
                    rows_count += 1

                if len(articles) < PAGE_SIZE:
                    break

                time.sleep(pause)

            window_start = window_end

    print(f"[OK] NewsAPI -> {outfile} ({rows_count} rows)")
    return outfile
