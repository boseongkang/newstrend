from __future__ import annotations
import os, json, time, itertools, argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone
import requests
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

API = "https://newsapi.org/v2"

def _iso(dt=None):
    if dt is None: dt = datetime.now(timezone.utc)
    if isinstance(dt, str): return dt
    return dt.astimezone(timezone.utc).isoformat()

def save_jsonl(path: str | Path, rows: list[dict]):
    p = Path(path); p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def chunk(lst, n):
    it = iter(lst)
    while True:
        block = list(itertools.islice(it, n))
        if not block: break
        yield block

def get_us_sources(api_key: str) -> list[str]:
    r = requests.get(f"{API}/top-headlines/sources",
                     params={"country": "us", "apiKey": api_key}, timeout=30)
    r.raise_for_status()
    return [s["id"] for s in r.json().get("sources", [])]

def fetch_everything(api_key: str, *,
                     sources: list[str] | None = None,
                     q: str | None = None,
                     from_dt: datetime,
                     to_dt: datetime,
                     pages: int = 1,
                     page_size: int = 100,
                     delay: float = 0.25) -> tuple[list[dict], int]:
    """
    returns (rows, requests_used)
    """
    base = {
        "apiKey": api_key,
        "language": "en",
        "sortBy": "publishedAt",
        "from": from_dt.isoformat().replace("+00:00", "Z"),
        "to":   to_dt.isoformat().replace("+00:00", "Z"),
        "pageSize": page_size,
    }
    if q: base["q"] = q
    if sources: base["sources"] = ",".join(sources) #max20

    rows, used = [], 0
    for p in range(1, pages + 1):
        params = dict(base, page=p)
        r = None
        try:
            r = requests.get(f"{API}/everything", params=params, timeout=40)
            used += 1
            r.raise_for_status()
        except requests.HTTPError:
            if r is not None and r.status_code == 426:
                print("[newsapi] 426 Upgrade Required, free plan cant page>1")
                break
            raise

        items = r.json().get("articles", []) or []
        for a in items:
            rows.append({
                "article_id": f"newsapi:{a.get('url')}",
                "title": a.get("title"),
                "url": a.get("url"),
                "publisher": (a.get("source") or {}).get("name"),
                "published_at": a.get("publishedAt"),
                "description": a.get("description"),
                "content": a.get("content"),
                "raw_source": "newsapi",
            })
        if len(items) < page_size:
            break
        time.sleep(delay)
    return rows, used

def ingest_newsapi_bulk(outdir: str = "data/raw_newsapi",
                        date_str: str = "yesterday",
                        max_requests: int = 90,
                        pages_per_group: int = 1,
                        page_size: int = 100,
                        group_limit: int | None = None,
                        topics: list[str] = ("economy","politics","technology"),
                        time_slices: int = 6,
                        delay: float = 0.25):
    key = os.getenv("NEWSAPI_KEY")
    assert key, "NEWSAPI_KEY missing (put api key .env)"

    d = (datetime.now(timezone.utc) - timedelta(days=1)).date() if date_str=="yesterday" \
        else datetime.fromisoformat(date_str).date()
    day_start = datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc)
    day_end   = datetime.combine(d, datetime.max.time(), tzinfo=timezone.utc)
    slice_span = (day_end - day_start) / max(1, time_slices)

    def slice_bounds(i: int) -> tuple[datetime, datetime]:
        s = day_start + i * slice_span
        e = day_start + (i + 1) * slice_span
        return s, e

    out = Path(outdir) / f"{d.isoformat()}.jsonl"

    all_sources = get_us_sources(key)
    groups = list(chunk(all_sources, 20))
    if group_limit is not None:
        groups = groups[:group_limit]

    requests_left = max_requests
    rows_all: list[dict] = []

    for i in range(time_slices):
        if requests_left <= 0: break
        fdt, tdt = slice_bounds(i)
        print(f"[slice {i+1}/{time_slices}] {fdt.isoformat()} → {tdt.isoformat()}")

        for idx, g in enumerate(groups, 1):
            if requests_left <= 0: break
            pages = min(pages_per_group, requests_left)
            if pages <= 0: break
            batch, used = fetch_everything(key, sources=g,
                                           from_dt=fdt, to_dt=tdt,
                                           pages=pages, page_size=page_size, delay=delay)
            rows_all.extend(batch); requests_left -= used
            print(f"  [sources {idx}/{len(groups)}] rows={len(batch)} req_used={used} left={requests_left}")

        for q in topics:
            if requests_left <= 0: break
            batch, used = fetch_everything(key, q=q,
                                           from_dt=fdt, to_dt=tdt,
                                           pages=1, page_size=page_size, delay=delay)
            rows_all.extend(batch); requests_left -= used
            print(f"  [topic '{q}'] rows={len(batch)} req_used={used} left={requests_left}")

    save_jsonl(out, rows_all)
    print(f"[OK] NewsAPI -> {out} ({len(rows_all)} rows, requests_used={max_requests-requests_left}/{max_requests})")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Bulk ingest from NewsAPI into data/raw_newsapi/")
    ap.add_argument("--outdir", default="data/raw_newsapi")
    ap.add_argument("--date", default="yesterday", help='"YYYY-MM-DD" or "yesterday"')
    ap.add_argument("--max-requests", type=int, default=90)
    ap.add_argument("--pages-per-group", type=int, default=1)
    ap.add_argument("--page-size", type=int, default=100)
    ap.add_argument("--group-limit", type=int)
    ap.add_argument("--topics", default="economy,politics,technology")
    ap.add_argument("--time-slices", type=int, default=6)
    ap.add_argument("--delay", type=float, default=0.25)
    args = ap.parse_args()
    topics = [t.strip() for t in args.topics.split(",") if t.strip()]
    ingest_newsapi_bulk(outdir=args.outdir, date_str=args.date,
                        max_requests=args.max_requests,
                        pages_per_group=args.pages_per_group,
                        page_size=args.page_size,
                        group_limit=args.group_limit,
                        topics=topics,
                        time_slices=args.time_slices,
                        delay=args.delay)
