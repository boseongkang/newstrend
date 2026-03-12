"""
collect_rss.py
RSS 피드에서 금융/경제/지정학 뉴스 수집 → JSONL 저장
사용법: python scripts/collect_rss.py --outdir data/live_newsapi --hours 6
"""
import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError
import xml.etree.ElementTree as ET

# ── 수집 대상 RSS 피드 ────────────────────────────────────────────
RSS_FEEDS = [
    # 거시경제 / 금융
    {"url": "https://feeds.reuters.com/reuters/businessNews",       "source": "Reuters Business",    "category": "macro"},
    {"url": "https://feeds.reuters.com/reuters/topNews",            "source": "Reuters Top",         "category": "macro"},
    {"url": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",       "source": "WSJ Markets",         "category": "macro"},
    {"url": "https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml",     "source": "WSJ Business",        "category": "macro"},
    {"url": "https://www.cnbc.com/id/100003114/device/rss/rss.html","source": "CNBC Markets",        "category": "macro"},
    {"url": "https://www.cnbc.com/id/20910258/device/rss/rss.html", "source": "CNBC Economy",       "category": "macro"},
    {"url": "https://feeds.marketwatch.com/marketwatch/topstories", "source": "MarketWatch Top",     "category": "macro"},
    {"url": "https://feeds.marketwatch.com/marketwatch/marketpulse","source": "MarketWatch Pulse",   "category": "macro"},
    {"url": "https://finance.yahoo.com/news/rssindex",              "source": "Yahoo Finance",       "category": "macro"},
    # 지정학 / 정치
    {"url": "https://feeds.bbci.co.uk/news/world/rss.xml",         "source": "BBC World",           "category": "geopolitical"},
    {"url": "https://feeds.bbci.co.uk/news/business/rss.xml",      "source": "BBC Business",        "category": "macro"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml","source": "NYT World",         "category": "geopolitical"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml","source": "NYT Business",   "category": "macro"},
    {"url": "https://foreignpolicy.com/feed/",                     "source": "Foreign Policy",      "category": "geopolitical"},
    # 에너지 / 원자재
    {"url": "https://oilprice.com/rss/main",                       "source": "OilPrice",            "category": "energy"},
    # 중앙은행 공식 발표
    {"url": "https://www.federalreserve.gov/feeds/press_all.xml",  "source": "Federal Reserve",     "category": "macro"},
    {"url": "https://www.ecb.europa.eu/rss/press.html",            "source": "ECB",                 "category": "macro"},
    # 기술 / 반도체
    {"url": "https://techcrunch.com/feed/",                        "source": "TechCrunch",          "category": "tech"},
    {"url": "https://feeds.arstechnica.com/arstechnica/technology", "source": "Ars Technica",       "category": "tech"},
]

# ── XML 네임스페이스 ──────────────────────────────────────────────
NS = {
    "dc":      "http://purl.org/dc/elements/1.1/",
    "content": "http://purl.org/rss/1.0/modules/content/",
    "atom":    "http://www.w3.org/2005/Atom",
    "media":   "http://search.yahoo.com/mrss/",
}

_WS_RE   = re.compile(r"\s+")
_TAG_RE  = re.compile(r"<[^>]+>")
_URL_RE  = re.compile(r"https?://\S+")


def clean_text(s) -> str:
    if not s:
        return ""
    s = _TAG_RE.sub(" ", s)
    s = _URL_RE.sub(" ", s)
    s = s.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">") \
         .replace("&quot;", '"').replace("&#39;", "'").replace("&nbsp;", " ")
    return _WS_RE.sub(" ", s).strip()


def parse_date(s) -> str:
    """RFC 2822 또는 ISO 8601 → UTC ISO 8601 문자열"""
    if not s:
        return None
    s = s.strip()
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S GMT",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
    return None


def article_id(url: str, title: str) -> str:
    key = (url or title or "").encode("utf-8")
    return hashlib.sha1(key).hexdigest()[:16]


def fetch_feed(feed_cfg: dict, cutoff: datetime, timeout: int = 15) -> list[dict]:
    url     = feed_cfg["url"]
    source  = feed_cfg["source"]
    category = feed_cfg.get("category", "general")

    headers = {
        "User-Agent": "newstrend-rss-bot/1.0 (+https://github.com/boseongkang/newstrend)"
    }
    try:
        req  = Request(url, headers=headers)
        resp = urlopen(req, timeout=timeout)
        raw  = resp.read()
    except URLError as e:
        print(f"  [SKIP] {source}: {e}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"  [ERR]  {source}: {e}", file=sys.stderr)
        return []

    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        print(f"  [XML ERR] {source}: {e}", file=sys.stderr)
        return []

    # RSS 2.0 vs Atom
    items = root.findall(".//item") or root.findall(f".//{{{NS['atom']}}}entry")
    results = []

    for item in items:
        def _t(tag, ns_key=None):
            el = item.find(f"{{{NS[ns_key]}}}{tag}" if ns_key else tag)
            return (el.text or "").strip() if el is not None else ""

        title       = clean_text(_t("title"))
        link        = _t("link") or _t("link", "atom")
        description = clean_text(_t("description") or _t("summary", "atom"))
        pub_date    = parse_date(_t("pubDate") or _t("published", "atom") or _t("updated", "atom"))

        if not title:
            continue

        # 시간 필터
        if pub_date:
            try:
                dt = datetime.strptime(pub_date, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                if dt < cutoff:
                    continue
            except ValueError:
                pass

        results.append({
            "id":          article_id(link, title),
            "title":       title,
            "description": description,
            "url":         link,
            "published_at": pub_date,
            "source":      source,
            "category":    category,
            "collected_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        })

    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir",  default="data/live_newsapi",
                    help="output directory (appends to existing daily JSONL)")
    ap.add_argument("--hours",   type=int, default=6,
                    help="collect articles published in last N hours")
    ap.add_argument("--feeds",   default=None,
                    help="path to custom feeds JSON (optional)")
    ap.add_argument("--dry-run", action="store_true",
                    help="print counts only, don't write files")
    args = ap.parse_args()

    cutoff  = datetime.now(timezone.utc) - timedelta(hours=args.hours)
    out_dir = Path(args.outdir)
    out_dir.mkdir(parents=True, exist_ok=True)

    feeds = RSS_FEEDS
    if args.feeds:
        feeds = json.loads(Path(args.feeds).read_text())

    today    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_path = out_dir / f"{today}.jsonl"

    # 기존 ID 로드 (중복 방지)
    seen_ids: set[str] = set()
    if out_path.exists():
        with out_path.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    if "id" in obj:
                        seen_ids.add(obj["id"])
                except Exception:
                    pass

    total_new = 0
    with out_path.open("a", encoding="utf-8") as fout:
        for feed_cfg in feeds:
            articles = fetch_feed(feed_cfg, cutoff)
            new_count = 0
            for art in articles:
                if art["id"] in seen_ids:
                    continue
                seen_ids.add(art["id"])
                new_count += 1
                if not args.dry_run:
                    fout.write(json.dumps(art, ensure_ascii=False) + "\n")
            print(f"  {feed_cfg['source']:<30} +{new_count:>3} new  ({len(articles)} fetched)")
            total_new += new_count
            time.sleep(0.3)  # 서버 부하 방지

    print(f"\nTotal new articles: {total_new} → {out_path}")


if __name__ == "__main__":
    main()