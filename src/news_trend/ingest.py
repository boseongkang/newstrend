from datetime import datetime, timezone
from dateutil import parser as dateparser
import requests, feedparser
from .config import settings
from .utils import normalize_title, make_id

RSS_FEEDS = [
    "https://rss.nytimes.com/services/xml/rss/nyt/US.xml",
    "https://feeds.a.dj.com/rss/RSSUSNews.xml",
    "https://www.npr.org/rss/rss.php?id=1001",
    "https://feeds.bbci.co.uk/news/world/us_and_canada/rss.xml",
    "https://rss.cnn.com/rss/edition_us.rss",
]

def _iso(dt):
    if isinstance(dt, str):
        try:
            return dateparser.parse(dt).astimezone(timezone.utc).isoformat()
        except Exception:
            return None
    return dt.astimezone(timezone.utc).isoformat()

def fetch_rss():
    rows = []
    for url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
            publisher = feed.feed.get("title", "RSS")
            for e in feed.entries:
                title = e.get("title")
                link = e.get("link")
                published = e.get("published") or e.get("updated") or datetime.utcnow().isoformat()
                title_norm = normalize_title(title or "")
                aid = make_id(publisher, title_norm, str(published)[:10])
                rows.append({
                    "article_id": aid,
                    "url": link,
                    "publisher": publisher,
                    "title": title,
                    "published_at": _iso(published),
                    "ingested_at": _iso(datetime.utcnow()),
                    "language": "en",
                    "country": "us",
                    "content": e.get("summary"),
                    "raw_source": "rss",
                })
        except Exception as err:
            print(f"[WARN] RSS error {url}: {err}")
    return rows

def fetch_newsapi(country="us", page_size=100):
    if not settings.newsapi_key:
        return []
    url = "https://newsapi.org/v2/top-headlines"
    params = {"country": country, "pageSize": page_size}
    headers = {"X-Api-Key": settings.newsapi_key}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()
        rows = []
        for a in data.get("articles", []):
            publisher = (a.get("source") or {}).get("name") or "NewsAPI"
            title = a.get("title")
            published = a.get("publishedAt") or datetime.utcnow().isoformat()
            title_norm = normalize_title(title or "")
            aid = make_id(publisher, title_norm, str(published)[:10])
            rows.append({
                "article_id": aid,
                "url": a.get("url"),
                "publisher": publisher,
                "title": title,
                "published_at": _iso(published),
                "ingested_at": _iso(datetime.utcnow()),
                "language": "en",
                "country": country,
                "content": a.get("content"),
                "raw_source": "newsapi",
            })
        return rows
    except Exception as err:
        print(f"[WARN] NewsAPI error: {err}")
        return []
