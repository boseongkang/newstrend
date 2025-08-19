import os
import argparse
import requests
import json
from datetime import datetime, timedelta
from pathlib import Path

NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
BASE_URL = "https://newsapi.org/v2/everything"


def fetch_news(query="*", from_date=None, to_date=None, page=1, page_size=100, language="en"):
    """Fetch a single page of news articles from NewsAPI"""
    params = {
        "q": query,
        "from": from_date,
        "to": to_date,
        "sortBy": "publishedAt",
        "page": page,
        "pageSize": page_size,
        "language": language,
        "apiKey": NEWSAPI_KEY,
    }
    resp = requests.get(BASE_URL, params=params)
    resp.raise_for_status()
    return resp.json()


def ingest_newsapi(date, outdir="data/raw_newsapi", max_requests=90, time_split=1):
    """Ingest news articles from NewsAPI with optional time split"""
    Path(outdir).mkdir(parents=True, exist_ok=True)
    outpath = Path(outdir) / f"{date}.jsonl"

    start_date = datetime.strptime(date, "%Y-%m-%d")
    end_date = start_date + timedelta(days=1)

    delta = (end_date - start_date) / time_split
    intervals = [(start_date + i * delta, start_date + (i + 1) * delta) for i in range(time_split)]

    total_saved, requests_used = 0, 0
    with open(outpath, "w", encoding="utf-8") as f:
        for interval_start, interval_end in intervals:
            from_date = interval_start.strftime("%Y-%m-%dT%H:%M:%S")
            to_date = interval_end.strftime("%Y-%m-%dT%H:%M:%S")

            page = 1
            while requests_used < max_requests:
                data = fetch_news(from_date=from_date, to_date=to_date, page=page)
                articles = data.get("articles", [])
                if not articles:
                    break
                for a in articles:
                    f.write(json.dumps(a, ensure_ascii=False) + "\n")
                total_saved += len(articles)
                requests_used += 1
                page += 1

                # NewsAPI only allows up to 100 results per query
                if len(articles) < 100:
                    break

    print(f"✅ Saved {total_saved} articles into {outpath}")
    print(f"Requests used: {requests_used}/{max_requests}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--outdir", type=str, default="data/raw_newsapi")
    parser.add_argument("--max-requests", type=int, default=90)
    parser.add_argument("--time-split", type=int, default=1, help="Number of splits per day (e.g. 4 → 6h chunks)")
    args = parser.parse_args()

    ingest_newsapi(args.date, args.outdir, args.max_requests, args.time_split)
