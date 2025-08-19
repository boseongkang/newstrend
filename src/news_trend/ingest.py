import os, json, time
from pathlib import Path
import requests
from datetime import datetime, date as ddate, time as dtime, timedelta, timezone
 
API_KEY = os.getenv("NEWSAPI_KEY")
BASE_URL = "https://newsapi.org/v2/everything"
PAGE_SIZE = 100

def _parse_date_arg(d: str | None) -> ddate:
    today = datetime.now(timezone.utc).date()
    if not d or d.lower() == "today":
        return today
    if d.lower() == "yesterday":
        return today - timedelta(days=1)
    return ddate.fromisoformat(d)

def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def fetch_newsapi(query: str | None = None,
                  hours_split: int = 2,
                  max_pages_per_window: int = 8,
                  outdir: str = "data/raw",
                  date: str | None = None,
                  pause: float = 0.25) -> Path:
    assert API_KEY, "NEWSAPI_KEY is missing"
    target_day = _parse_date_arg(date)
    start = datetime.combine(target_day, dtime(0, 0, 0, tzinfo=timezone.utc))
    end = start + timedelta(days=1)
    delta = timedelta(hours=hours_split)

    outdir_p = Path(outdir); outdir_p.mkdir(parents=True, exist_ok=True)
    outfile = outdir_p / f"newsapi_{target_day.isoformat()}.jsonl"

    rows = 0
    with outfile.open("w", encoding="utf-8") as f:
        w = start
        while w < end:
            w2 = min(w + delta, end)
            for page in range(1, max_pages_per_window + 1):
                params = {
                    "from": _iso_utc(w),
                    "to": _iso_utc(w2),
                    "pageSize": PAGE_SIZE,
                    "page": page,
                    "apiKey": API_KEY,
                    "language": "en",
                    "sortBy": "publishedAt",
                    "q": (query or "news").strip()
                }
                r = requests.get(BASE_URL, params=params, timeout=40)
                try:
                    r.raise_for_status()
                except requests.HTTPError:
                    break
                arts = (r.json() or {}).get("articles") or []
                if not arts:
                    break
                for a in arts:
                    f.write(json.dumps({
                        "article_id": f"newsapi:{a.get('url')}",
                        "title": a.get("title"),
                        "url": a.get("url"),
                        "publisher": (a.get("source") or {}).get("name"),
                        "published_at": a.get("publishedAt"),
                        "description": a.get("description"),
                        "content": a.get("content"),
                        "raw_source": "newsapi",
                    }, ensure_ascii=False) + "\n")
                    rows += 1
                if len(arts) < PAGE_SIZE:
                    break
                time.sleep(pause)
            w = w2
    print(f"[OK] NewsAPI -> {outfile} ({rows} rows)")
    return outfile
