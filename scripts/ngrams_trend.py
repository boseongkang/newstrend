import json, re, argparse, collections, datetime as dt
from pathlib import Path
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np
import pandas as pd

def rows(path):
    with open(path) as f:
        for line in f:
            r=json.loads(line)
            d=r.get("publishedAt") or r.get("published_at") or r.get("date")
            d=str(d)[:10]
            txt=" ".join([r.get("title") or "", r.get("description") or ""])
            yield d, txt

def main(master, outdir, ngram_max=3, min_df=5, days=30):
    out=Path(outdir); out.mkdir(parents=True, exist_ok=True)
    data=list(rows(master))
    df=pd.DataFrame(data, columns=["date","text"])
    df=df[df["text"].astype(bool)]
    df=df[df["date"]>=str((pd.to_datetime(df["date"]).max()-pd.Timedelta(days=days)).date())]

    vect=CountVectorizer(ngram_range=(2, ngram_max), min_df=min_df, token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z\-]+\b")
    X=vect.fit_transform(df["text"].str.lower())
    df["date"]=pd.to_datetime(df["date"])
    feats=np.array(vect.get_feature_names_out())

    daily=pd.DataFrame(X.toarray(), columns=feats).groupby(df["date"]).sum().sort_index()
    t=np.arange(len(daily))
    def slope(y):
        if y.sum()==0: return 0.0
        return np.polyfit(t, y, 1)[0]
    sl=daily.apply(slope, axis=0).sort_values(ascending=False)
    top=sl.head(50).rename("slope").to_frame()
    top.to_csv(out/"ngrams_top_slopes.csv")
    print("saved:", out/"ngrams_top_slopes.csv")

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--master", default="data/warehouse/master.jsonl")
    ap.add_argument("--outdir", default="reports/words")
    ap.add_argument("--ngram-max", type=int, default=3)
    ap.add_argument("--min-df", type=int, default=5)
    ap.add_argument("--days", type=int, default=30)
    main(**vars(ap.parse_args()))
