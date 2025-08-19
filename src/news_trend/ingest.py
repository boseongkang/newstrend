import os
import json
import datetime
import requests
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()
API_KEY = os.getenv("NEWSAPI_KEY")
BASE_URL = "https://newsapi.org/v2/everything"

def fetch_newsapi(query="*", hours_split=2, max_pages=8, outdir="data/raw", date=None):
    today = datetime.date.today()
    target_date = today - datetime.timedelta(days=1) if date is None else datetime.date.fromisoformat(date)
    start = datetime.datetime.combine(target_date, datetime.time(0,0,0))
    end = start + datetime.timedelta(days=1)
    delta = datetime.timedelta(hours=hours_split)
    results = []
    Path(outdir).mkdir(parents=True, exist_ok=True)
    outfile = Path(outdir) / f"newsapi_{target_date}.jsonl"
    with open(outfile, "w", encoding="utf-8") as f:
        while start < end:
            window_end = start + delta
            for page in range(1, max_pages+1):
                params = {
                    "q": query,
                    "from": start.isoformat(),
                    "to": window_end.isoformat(),
                    "pageSize": 100,
                    "page": page,
                    "apiKey": API_KEY,
                    "language": "en"
                }
                r = requests.get(BASE_URL, params=params)
                data = r.json()
                if "articles" not in data:
                    break
                for a in data["articles"]:
                    f.write(json.dumps(a, ensure_ascii=False) + "\n")
                    results.append(a)
            start = window_end
    return outfile
