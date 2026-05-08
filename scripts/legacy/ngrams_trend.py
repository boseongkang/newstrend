import argparse
from pathlib import Path
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer

def load_master(path, days):
    df = pd.read_json(path, lines=True)
    if days and "date" in df.columns:
        df = df.sort_values("date").tail(days)
    text_col = "text" if "text" in df.columns else None
    if text_col is None:
        for c in ("content", "description", "title"):
            if c in df.columns:
                text_col = c
                break
    if text_col is None:
        return pd.DataFrame()
    df = df[[text_col]].rename(columns={text_col: "text"})
    df["text"] = df["text"].astype(str)
    df = df[df["text"].str.strip().ne("")]
    return df

def main(master, outdir, ngram_max=2, min_df=25, days=30, max_features=None, token_pattern=None):
    out = Path(outdir); out.mkdir(parents=True, exist_ok=True)
    df = load_master(master, days)
    if df.empty:
        print("no documents"); return
    n_docs = len(df)
    target_min_df = min_df
    if isinstance(min_df, int) and (min_df >= max(2, n_docs // 2) or min_df > n_docs):
        target_min_df = max(1, n_docs // 20) or 1
    vect = CountVectorizer(
        ngram_range=(1, ngram_max),
        min_df=target_min_df,
        lowercase=True,
        max_features=None if max_features in (None, 0) else max_features,
        token_pattern=token_pattern if token_pattern else r"(?u)\b\w\w+\b",
    )
    try:
        X = vect.fit_transform(df["text"].str.lower())
    except ValueError as e:
        if "After pruning" in str(e):
            target_min_df = 1
            vect.set_params(min_df=1)
            X = vect.fit_transform(df["text"].str.lower())
        else:
            raise
    terms = vect.get_feature_names_out()
    counts = X.toarray().sum(axis=0)
    top = pd.DataFrame({"term": terms, "count": counts}).sort_values("count", ascending=False)
    top.to_csv(out / "ngrams_top_slopes.csv", index=False)
    pd.DataFrame(
        [{"docs": int(n_docs), "min_df_used": int(target_min_df), "ngram_max": int(ngram_max), "rows": int(top.shape[0])}]
    ).to_json(out / "meta.json", orient="records", lines=False)
    print(f"saved: {out/'ngrams_top_slopes.csv'} (docs={n_docs}, min_df={target_min_df})")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--ngram-max", type=int, default=2)
    ap.add_argument("--min-df", type=int, default=25)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--max-features", type=int, default=None)
    ap.add_argument("--token-pattern", default=None)
    args = ap.parse_args()
    main(
        args.master,
        args.outdir,
        ngram_max=args.ngram_max,
        min_df=args.min_df,
        days=args.days,
        max_features=args.max_features,
        token_pattern=args.token_pattern,
        )