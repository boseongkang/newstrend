from pathlib import Path
import json
import pandas as pd

def _load_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def _append_dedupe_csv(csv_path: Path, df_new: pd.DataFrame, keys, sort_cols=None, sort_ascending=True):
    if csv_path.exists():
        df_old = pd.read_csv(csv_path)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new.copy()
    df_all = df_all.drop_duplicates(subset=keys, keep="last")
    if sort_cols:
        df_all = df_all.sort_values(by=sort_cols, ascending=sort_ascending)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_csv(csv_path, index=False)

def append_metrics(date: str, indir: str = "data", kind: str = "raw_newsapi", metrics_dir: str = "data/metrics"):
    base = Path(indir)
    inpath = {
        "raw_newsapi": base / "raw_newsapi" / f"{date}.jsonl",
        "raw":         base / "raw"         / f"{date}.jsonl",
    }.get(kind, base / kind / f"{date}.jsonl")

    if not inpath.exists():
        raise FileNotFoundError(f"input not found: {inpath}")

    rows = list(_load_jsonl(inpath))
    df = pd.DataFrame(rows)

    if "published_at" in df.columns:
        ts_col = "published_at"
    elif "publishedAt" in df.columns:
        ts_col = "publishedAt"
    else:
        raise ValueError("no timestamp column in input")

    if "publisher" not in df.columns and "source" in df.columns and isinstance(df["source"].iloc[0], dict):
        df["publisher"] = df["source"].apply(lambda s: (s or {}).get("name"))

    df["ts"] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])
    df["date"] = df["ts"].dt.date.astype(str)
    df["hour"] = df["ts"].dt.strftime("%H:00")

    mdir = Path(metrics_dir); mdir.mkdir(parents=True, exist_ok=True)

    g_hour = df.groupby(["date", "hour"]).size().reset_index(name="count")
    hourly_csv = mdir / "articles_by_hour.csv"
    _append_dedupe_csv(hourly_csv, g_hour, keys=["date", "hour"], sort_cols=["date", "hour"])

    pub_csv = None
    if "publisher" in df.columns:
        g_pub = df.groupby(["date", "publisher"]).size().reset_index(name="count")
        pub_csv = mdir / "publisher_counts.csv"
        _append_dedupe_csv(pub_csv, g_pub, keys=["date", "publisher"], sort_cols=["date", "count"], sort_ascending=[True, False])

    return {"hourly": str(hourly_csv), "publishers": str(pub_csv) if pub_csv else None}
