from __future__ import annotations
import os
import json
import time
from pathlib import Path
from datetime import datetime, date as ddate, time as dtime, timedelta, timezone
from typing import Optional
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("NEWSAPI_KEY")
BASE_URL = "https://newsapi.org/v2/everything"
PAGE_SIZE = 100


def _parse_date_arg(d: Optional[str]) -> ddate:
    t = datetime.now(timezone.utc).date()
    if not d or d.lower() == "today":
        return t
    if d.lower() == "yesterday":
        return t - timedelta(days=1)
    return ddate.fromisoformat(d)


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def ingest_newsapi_hourly(
    query: Optional[str] = None,
    hours_split: int = 2,
    max_pages_per_window: int = 8,
    outroot: str = "data/raw_windows",
    date: Optional[str] = None,
    pause: float = 0.25,
) -> Path:
    assert API_KEY, "NEWSAPI_KEY is missing"
    day = _parse_date_arg(date)
    start = datetime.combine(day, dtime(0, 0, 0, tzinfo=timezone.utc))
    end = start + timedelta(days=1)
    delta = timedelta(hours=hours_split)

    outdir = Path(outroot) / day.isoformat()
    outdir.mkdir(parents=True, exist_ok=True)
    index_path = outdir / "_index.jsonl"

    written = 0
    with index_path.open("w", encoding="utf-8") as idx:
        w = start
        while w < end:
            w2 = min(w + delta, end)
            fname = f"{w.strftime('%H-%M')}__{w2.strftime('%H-%M')}.jsonl"
            fpath = outdir / fname
            n = 0
            with fpath.open("w", encoding="utf-8") as f:
                for page in range(1, max_pages_per_window + 1):
                    params = {
                        "from": _iso_utc(w),
                        "to": _iso_utc(w2),
                        "pageSize": PAGE_SIZE,
                        "page": page,
                        "apiKey": API_KEY,
                        "language": "en",
                        "sortBy": "publishedAt",
                        "q": (query or "news").strip(),
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
                        f.write(
                            json.dumps(
                                {
                                    "article_id": f"newsapi:{a.get('url')}",
                                    "title": a.get("title"),
                                    "url": a.get("url"),
                                    "publisher": (a.get("source") or {}).get("name"),
                                    "published_at": a.get("publishedAt"),
                                    "description": a.get("description"),
                                    "content": a.get("content"),
                                    "raw_source": "newsapi",
                                },
                                ensure_ascii=False,
                            )
                            + "\n"
                        )
                        n += 1
                    if len(arts) < PAGE_SIZE:
                        break
                    time.sleep(pause)
            idx.write(
                json.dumps(
                    {
                        "window_start": w.isoformat(),
                        "window_end": w2.isoformat(),
                        "rows": n,
                        "path": str(fpath),
                    }
                )
                + "\n"
            )
            written += n
            w = w2

    print(f"[OK] hourly ingest -> {outdir} ({written} rows)")
    return outdir


def ingest_newsapi_recent(
    query: Optional[str] = None,
    recent_minutes: int = 30,
    pages: int = 3,
    outdir: str = "data/live_newsapi",
    outfile: Optional[Path] = None,
    pause: float = 0.2,
) -> Path:
    assert API_KEY, "NEWSAPI_KEY is missing"
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=recent_minutes)

    if outfile is not None:
        fpath = Path(outfile)
        fpath.parent.mkdir(parents=True, exist_ok=True)
    else:
        outp = Path(outdir)
        outp.mkdir(parents=True, exist_ok=True)
        fname = now.strftime("%Y-%m-%dT%H-%MZ") + ".jsonl"
        fpath = outp / fname

    rows = 0
    with fpath.open("w", encoding="utf-8") as f:
        for page in range(1, int(pages) + 1):
            params = {
                "from": _iso_utc(start),
                "to": _iso_utc(now),
                "pageSize": PAGE_SIZE,
                "page": page,
                "apiKey": API_KEY,
                "language": "en",
                "sortBy": "publishedAt",
                "q": (query or "news").strip(),
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
                f.write(
                    json.dumps(
                        {
                            "article_id": f"newsapi:{a.get('url')}",
                            "title": a.get("title"),
                            "url": a.get("url"),
                            "publisher": (a.get("source") or {}).get("name"),
                            "published_at": a.get("publishedAt"),
                            "description": a.get("description"),
                            "content": a.get("content"),
                            "raw_source": "newsapi",
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                rows += 1
            if len(arts) < PAGE_SIZE:
                break
            time.sleep(pause)

    print(f"[LIVE] NewsAPI -> {fpath} ({rows} rows)")
    return fpath
