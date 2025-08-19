import os, json, time
from pathlib import Path
from typing import Iterable, Optional
import requests
from datetime import datetime, date as ddate, time as dtime, timedelta, timezone

API_KEY = os.getenv("NEWSAPI_KEY")
BASE_URL = "https://newsapi.org/v2/everything"
PAGE_SIZE = 100

def _parse_date_arg(d: Optional[str]) -> ddate:
    today = datetime.now(timezone.utc).date()
    if d is None or d.strip().lower() == "today":
        return today
    if d.strip().lower() == "yesterday":
        return today - timedelta(days=1)
    return ddate.fromisoformat(d.strip())

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
    target_day = _parse_date_arg(date)
    start = datetime.combine(target_day, dtime(0, 0, tzinfo=timezone.utc))
    end = start + timedelta(days=1)
    delta = timedelta(hours=hours_split)

    outdir_p = Path(outdir)
    outdir_p.mkdir(parents=True, exist_ok=True)
    outfile = outdir_p / f"newsapi_{target_day.isoformat()}.jsonl"

    rows = 0
    q = (query or "news").strip()
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
                    "q": q,
                }
                r = requests.get(BASE_URL, params=params, timeout=40)
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
                    rows += 1
                if len(arts) < PAGE_SIZE:
                    break
                time.sleep(pause)
            w = w2
    print(f"[OK] NewsAPI -> {outfile} ({rows} rows)")
    return outfile

def fetch_rss(urls: Optional[Iterable[str]] = None, outdir: str = "data/raw", date: Optional[str] = None) -> Path:
    import feedparser
    target_day = _parse_date_arg(date)
    outdir_p = Path(outdir)
    outdir_p.mkdir(parents=True, exist_ok=True)
    outfile = outdir_p / f"rss_{target_day.isoformat()}.jsonl"

    if not urls:
        urls = []

    rows = 0
    with outfile.open("w", encoding="utf-8") as f:
        for u in urls:
            feed = feedparser.parse(u)
            for e in feed.entries:
                pub = None
                for key in ("published_parsed", "updated_parsed"):
                    if getattr(e, key, None):
                        pub = datetime(*getattr(e, key)[:6], tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
                        break
                rec = {
                    "article_id": f"rss:{getattr(e, 'link', '')}",
                    "title": getattr(e, "title", None),
                    "url": getattr(e, "link", None),
                    "publisher": getattr(feed.feed, "title", None),
                    "published_at": pub,
                    "description": getattr(e, "summary", None),
                    "content": None,
                    "raw_source": "rss",
                }
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                rows += 1
    print(f"[OK] RSS -> {outfile} ({rows} rows)")
    return outfile

__all__ = ["fetch_newsapi", "fetch_rss"]
