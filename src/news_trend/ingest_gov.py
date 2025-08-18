from __future__ import annotations
import json, time, hashlib, argparse
from pathlib import Path
from datetime import datetime, timezone

import feedparser
import trafilatura

GOV_FEEDS = [
    "https://www.whitehouse.gov/briefing-room/feed/",
    "https://www.justice.gov/opa/press-releases.xml",
    "https://www.dhs.gov/news-releases/feed",
    "https://www.sec.gov/news/pressreleases.rss",
    "https://www.cdc.gov/media/rss.htm",
]

def _iso(dt=None):
    if dt is None: dt = datetime.now(timezone.utc)
    if isinstance(dt, str): return dt
    return dt.astimezone(timezone.utc).isoformat()

def _id(prefix: str, url: str) -> str:
    return f"{prefix}:{hashlib.sha1(url.encode()).hexdigest()[:16]}"

def _extract(url: str) -> str | None:
    try:
        html = trafilatura.fetch_url(url, timeout=25)
        if not html: return None
        return trafilatura.extract(html, include_tables=False, include_comments=False)
    except Exception:
        return None

def save_jsonl(path: str | Path, rows: list[dict]):
    p = Path(path); p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for r in rows: f.write(json.dumps(r, ensure_ascii=False) + "\n")

def fetch_gov(max_items_per_feed: int = 100, extract_body: bool = True, delay: float = 0.2) -> list[dict]:
    rows: list[dict] = []
    for feed_url in GOV_FEEDS:
        feed = feedparser.parse(feed_url)
        for e in feed.entries[:max_items_per_feed]:
            url = e.get("link")
            if not url: continue
            content = _extract(url) if extract_body else None
            pub = getattr(getattr(e, "source", None), "title", None) or feed.feed.get("title")
            rows.append({
                "article_id": _id("gov", url),
                "title": e.get("title"),
                "url": url,
                "publisher": pub,
                "published_at": getattr(e, "published", None),
                "ingested_at": _iso(),
                "content": content,
                "content_source": "extracted" if content else "none",
                "raw_source": "gov_rss",
                "source_type": "gov_release",
                "language": "en",
            })
        time.sleep(delay)
    return rows

def ingest_gov(outdir="data/raw_gov", date_str="today", **kw):
    d = datetime.now(timezone.utc).date().isoformat() if date_str=="today" else date_str
    rows = fetch_gov(**kw)
    out = Path(outdir) / f"{d}.jsonl"
    save_jsonl(out, rows)
    print(f"[OK] GOV -> {out} ({len(rows)} rows)")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Ingest US federal government press releases into data/raw_gov/")
    ap.add_argument("--outdir", default="data/raw_gov")
    ap.add_argument("--date", default="today")
    ap.add_argument("--max-items-per-feed", type=int, default=100)
    ap.add_argument("--no-extract", action="store_true", help="do not fetch full text")
    ap.add_argument("--delay", type=float, default=0.2)
    args = ap.parse_args()
    ingest_gov(outdir=args.outdir, date_str=args.date,
               max_items_per_feed=args.max_items_per_feed,
               extract_body=not args.no_extract,
               delay=args.delay)
