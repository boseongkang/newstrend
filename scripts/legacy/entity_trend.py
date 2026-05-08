import os, json, argparse, warnings
from pathlib import Path
import pandas as pd
import numpy as np

import urllib3
warnings.filterwarnings("ignore", category=urllib3.exceptions.NotOpenSSLWarning)

import spacy
nlp = spacy.load("en_core_web_sm", disable=["tagger","parser","lemmatizer"])
KEEP = {"ORG","PERSON","GPE","PRODUCT"}

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

def extract_entities(rows, min_len):
    out = []
    for d, txt in rows:
        doc = nlp(txt)
        for e in doc.ents:
            if e.label_ in KEEP:
                s = e.text.strip()
                if len(s) >= min_len:
                    out.append((d, s.lower()))
    return out

def plot_bar(df, title, fname, xlabel="count"):
    import matplotlib.pyplot as plt
    plt.figure(figsize=(12,7))
    df.iloc[::-1].plot(kind="barh", legend=False)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.tight_layout()
    plt.savefig(fname, dpi=150)
    plt.close()

def plot_heatmap(piv, topn, out_png):
    import matplotlib.pyplot as plt
    import numpy as np
    # 상위 단어 고르고 heatmap
    totals = piv.sum(axis=0).sort_values(ascending=False).head(topn).index
    sub = piv[totals]
    plt.figure(figsize=(14,8))
    plt.imshow(sub.T, aspect="auto", cmap="viridis")
    plt.colorbar(label="count")
    plt.yticks(np.arange(len(sub.columns)), sub.columns)
    plt.xticks(np.arange(len(sub.index)), [d.strftime("%m-%d") for d in sub.index], rotation=90)
    plt.title("Entity heatmap (top overall)")
    plt.tight_layout()
    plt.savefig(out_png, dpi=150)
    plt.close()

def main(master, outdir, days, min_len, min_df, sample, top, heat_top):
    out = Path(outdir); out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(stream_rows(master), columns=["date","text"])
    if df.empty:
        print("no usable rows"); return

    # 날짜 정리
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")

    if days > 0:
        cutoff = df["date"].max() - pd.Timedelta(days=days)
        df = df[df["date"] >= cutoff]

    if sample and sample > 0:
        df = df.tail(sample)  # 최신 n개만

    if df.empty:
        print("no rows after filtering"); return

    # 엔터티 추출
    ents = extract_entities(df.itertuples(index=False, name=None), min_len)
    if not ents:
        print("no entities extracted"); return

    edf = pd.DataFrame(ents, columns=["date","ent"])
    # 최소 등장일수 필터
    keep = edf.groupby("ent")["date"].nunique()
    keep = keep[keep >= max(1, min_df)].index
    edf = edf[edf["ent"].isin(keep)]

    daily = edf.groupby(["date","ent"]).size().unstack(fill_value=0).sort_index()
    if daily.empty:
        print("daily pivot empty"); return

    # CSV 저장
    daily_csv = out / "entities_daily.csv"
    daily.to_csv(daily_csv)
    print("saved:", daily_csv)

    # 오늘/최근의 상위 엔터티
    today = daily.index.max()
    top_today = daily.loc[today].sort_values(ascending=False).head(top)
    top_today_csv = out / "entities_top_today.csv"
    top_today.to_frame("count").to_csv(top_today_csv)
    print("saved:", top_today_csv)

    # 추세(기울기) 상위
    t = np.arange(len(daily))
    def slope(y):
        arr = y.to_numpy(dtype=float)
        if arr.sum() == 0 or len(arr) < 2: return 0.0
        return np.polyfit(t, arr, 1)[0]
    slopes = daily.apply(slope, axis=0).sort_values(ascending=False)
    top_slopes = slopes[slopes > 0].head(top)
    top_slopes_csv = out / "entities_top_slopes.csv"
    top_slopes.rename("slope").to_csv(top_slopes_csv)
    print("saved:", top_slopes_csv)

    # 시각화(바, 히트맵)
    plot_bar(top_today, f"Top entities — {today.date()}", out / "entities_top_today.png", "count (today)")
    plot_bar(top_slopes, "Strongest positive slopes", out / "entities_top_slopes.png", "slope")
    plot_heatmap(daily, heat_top, out / "entities_heatmap.png")
    print("png saved to:", out)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="data/warehouse/master.jsonl")
    ap.add_argument("--outdir", default="reports/entities")
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--min-len", type=int, default=3)
    ap.add_argument("--min-df", type=int, default=3)
    ap.add_argument("--sample", type=int, default=0, help="use last N docs (0=all)")
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--heat-top", type=int, default=20)
    main(**vars(ap.parse_args()))
