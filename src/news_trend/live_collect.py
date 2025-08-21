import os, json, time
from pathlib import Path
from datetime import datetime, timedelta, timezone
import requests

API_KEY = os.getenv("NEWSAPI_KEY")
BASE_URL = "https://newsapi.org/v2/everything"
PAGE_SIZE = 100

def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def ingest_newsapi_recent(query: str, recent_minutes: int, max_pages: int, outdir: str = "data/raw_newsapi") -> tuple[str, int]:
    assert API_KEY, "NEWSAPI_KEY is missing"
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=recent_minutes)
    params_common = {
        "from": _iso_utc(start),
        "to": _iso_utc(now),
        "pageSize": PAGE_SIZE,
        "apiKey": API_KEY,
        "language": "en",
        "sortBy": "publishedAt",
        "q": (query or "news").strip()
    }
    Path(outdir).mkdir(parents=True, exist_ok=True)
    daily_path = Path(outdir) / f"{now.date().isoformat()}.jsonl"
    written = 0
    with daily_path.open("a", encoding="utf-8") as f:
        for page in range(1, max_pages + 1):
            p = dict(params_common)
            p["page"] = page
            r = requests.get(BASE_URL, params=p, timeout=40)
            try:
                r.raise_for_status()
            except requests.HTTPError:
                break
            data = r.json() or {}
            arts = data.get("articles") or []
            if not arts:
                break
            for a in arts:
                rec = {
                    "article_id": f"newsapi:{a.get('url')}",
                    "title": a.get("title"),
                    "url": a.get("url"),
                    "publisher": (a.get("source") or {}).get("name"),
                    "published_at": a.get("publishedAt"),
                    "description": a.get("description"),
                    "content": a.get("content"),
                    "raw_source": "newsapi",
                }
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                written += 1
            if len(arts) < PAGE_SIZE:
                break
            time.sleep(0.2)
    print(f"[OK] recent {recent_minutes}m -> {daily_path} (+{written})")
    return str(daily_path), written
