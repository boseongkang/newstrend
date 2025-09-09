import json, argparse, pandas as pd, numpy as np
from pathlib import Path
from sklearn.feature_extraction.text import CountVectorizer

def stream_rows(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                r = json.loads(line)
            except Exception:
                continue
            d = r.get("publishedAt") or r.get("published_at") or r.get("date")
            if not d:
                continue
            d = str(d)[:10]
            if not d or d.lower() == "none":
                continue
            txt = " ".join([(r.get("title") or ""), (r.get("description") or "")]).strip()
            if not txt:
                continue
            yield d, txt

def main(master, outdir, vocab, win, z):
    Path(outdir).mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(stream_rows(master), columns=["date", "text"])
    if df.empty:
        print("no usable rows")
        return

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    if df.empty:
        print("no parsable dates after cleaning")
        return

    vect = CountVectorizer(max_features=vocab, token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z\-]+\b")
    X = vect.fit_transform(df["text"].str.lower())
    feats = vect.get_feature_names_out()

    daily = (
        pd.DataFrame(X.toarray(), columns=feats)
        .groupby(df["date"])
        .sum()
        .sort_index()
    )
    if daily.empty:
        print("daily matrix empty")
        return

    mu = daily.rolling(win, min_periods=1).mean()
    sd = daily.rolling(win, min_periods=1).std().replace(0, 1)
    zsc = (daily - mu) / sd

    today = daily.index.max()
    alerts = zsc.loc[today].sort_values(ascending=False)
    picks = alerts[alerts > z].rename("z").to_frame()

    outpath = Path(outdir) / f"bursts_{today.date()}.csv"
    picks.to_csv(outpath)
    print("saved:", outpath)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="data/warehouse/master.jsonl")
    ap.add_argument("--outdir", default="reports/bursts")
    ap.add_argument("--vocab", type=int, default=400)
    ap.add_argument("--win", type=int, default=14)
    ap.add_argument("--z", type=float, default=3.0)
    main(**vars(ap.parse_args()))
