import json, argparse, pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pathlib import Path

def stream(master):
    with open(master) as f:
        for line in f:
            r=json.loads(line)
            d=str(r.get("publishedAt") or r.get("date"))[:10]
            pub=(r.get("source") or r.get("publisher") or {}).get("name") if isinstance(r.get("source"),dict) else (r.get("source") or r.get("publisher") or "unknown")
            txt=" ".join([r.get("title") or "", r.get("description") or ""])
            yield d, pub or "unknown", txt

def main(master, outdir):
    Path(outdir).mkdir(parents=True, exist_ok=True)
    vs=SentimentIntensityAnalyzer()
    rows=[]
    for d,p,t in stream(master):
        if not t: continue
        s=vs.polarity_scores(t)["compound"]
        rows.append((d,p,s))
    df=pd.DataFrame(rows, columns=["date","publisher","sent"])
    daily=df.groupby(["date","publisher"])["sent"].mean().unstack(fill_value=0)
    daily.tail(14).to_csv(f"{outdir}/publisher_sentiment_recent.csv")
    print("saved:", f"{outdir}/publisher_sentiment_recent.csv")

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--master", default="data/warehouse/master.jsonl")
    ap.add_argument("--outdir", default="reports/publisher")
    main(**vars(ap.parse_args()))
