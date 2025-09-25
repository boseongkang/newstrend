import argparse, json, re
from pathlib import Path
from typing import Dict, List, Optional, Iterable, Tuple
import numpy as np
import pandas as pd

def ensure_outdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def load_lexicon(path: Path) -> Dict[str, Dict[str, list]]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in (".yml", ".yaml"):
        import yaml  # pip install pyyaml
        data = yaml.safe_load(text)
    else:
        data = json.loads(text)
    return data.get("groups", {})

def zscore(s: pd.Series) -> pd.Series:
    m, sd = s.mean(), s.std(ddof=0)
    if not sd or np.isnan(sd):
        return pd.Series(np.zeros(len(s)), index=s.index)
    return (s - m) / sd

def norm_slope(y: np.ndarray) -> float:
    y = np.asarray(y, dtype=float)
    m = y.mean()
    if m == 0:
        return 0.0
    x = np.arange(len(y))
    return float(np.polyfit(x, y, 1)[0] / (m + 1e-9))

def ts_from_unigrams(tokens_csv: Path, groups: Dict[str, Dict[str, list]],
                     start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    df = pd.read_csv(tokens_csv, parse_dates=["date"]).sort_values("date")
    df["date"] = df["date"].dt.date
    if start: df = df[df["date"] >= pd.to_datetime(start).date()]
    if end:   df = df[df["date"] <= pd.to_datetime(end).date()]
    df["term_lc"] = df["term"].astype(str).str.lower()

    rows = []
    for gname, cfg in groups.items():
        terms = {str(t).lower() for t in cfg.get("terms", []) if t and " " not in str(t)}
        if not terms:
            continue
        sub = df[df["term_lc"].isin(terms)]
        agg = sub.groupby("date")["count"].sum().reset_index()
        agg["group"] = gname; agg["source"] = "unigram"
        rows.append(agg[["date","group","source","count"]])
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=["date","group","source","count"])

def iter_daily_bigrams(reports_dir: Path) -> Iterable[Tuple[str, pd.DataFrame]]:
    if not reports_dir.exists():
        return
    for day_dir in sorted(reports_dir.iterdir()):
        if not day_dir.is_dir():
            continue
        try:
            day = pd.to_datetime(day_dir.name).date()
        except Exception:
            continue
        f = day_dir / "top_bigrams.csv"
        if not f.exists():
            continue
        df = pd.read_csv(f)
        cols = {c.lower(): c for c in df.columns}
        name_col = cols.get("bigram") or cols.get("term") or list(df.columns)[0]
        cnt_col  = cols.get("count")  or list(df.columns)[-1]
        yield (day.isoformat(), df.rename(columns={name_col: "phrase", cnt_col: "count"})[["phrase","count"]])

def ts_from_bigrams(reports_dir: Path, groups: Dict[str, Dict[str, list]],
                    start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    rows = []
    start_d = pd.to_datetime(start).date() if start else None
    end_d   = pd.to_datetime(end).date()   if end   else None

    group_phrases = {g: {str(t).lower() for t in cfg.get("terms", []) if len(str(t).split()) == 2}
                     for g, cfg in groups.items()}

    for day_str, df in iter_daily_bigrams(reports_dir):
        d = pd.to_datetime(day_str).date()
        if start_d and d < start_d: continue
        if end_d and d > end_d:   continue
        df["phrase_lc"] = df["phrase"].astype(str).str.lower()
        for gname, phrases in group_phrases.items():
            if not phrases: continue
            cnt = int(df[df["phrase_lc"].isin(phrases)]["count"].sum())
            rows.append({"date": d, "group": gname, "source": "phrase", "count": cnt})
    return pd.DataFrame(rows, columns=["date","group","source","count"]) if rows else \
           pd.DataFrame(columns=["date","group","source","count"])

def compile_group_patterns(groups: Dict[str, Dict[str, list]]) -> Dict[str, List[re.Pattern]]:
    out: Dict[str, List[re.Pattern]] = {}
    for gname, cfg in groups.items():
        pats: List[str] = []
        for t in cfg.get("terms", []) or []:
            toks = [re.escape(x) for x in str(t).split() if x]
            if not toks:
                continue
            pat = r"\b" + r"\s*[- ]\s*".join(toks) + r"\b" if len(toks) > 1 else r"\b" + toks[0] + r"\b"
            pats.append(pat)
        for rpat in cfg.get("regex", []) or []:
            pats.append(str(rpat))
        out[gname] = [re.compile(p, re.IGNORECASE) for p in pats]
    return out

def ts_from_master(master_jsonl: Path, groups: Dict[str, Dict[str, list]],
                   start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    if not master_jsonl or not master_jsonl.exists():
        return pd.DataFrame(columns=["date","group","source","count"])
    start_d = pd.to_datetime(start).date() if start else None
    end_d   = pd.to_datetime(end).date()   if end   else None
    patterns = compile_group_patterns(groups)

    counts: Dict[Tuple[str,str], int] = {}
    with master_jsonl.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            d_raw = obj.get("publishedAt") or obj.get("date") or obj.get("published_at")
            try:
                d = pd.to_datetime(d_raw).date()
            except Exception:
                continue
            if start_d and d < start_d: continue
            if end_d   and d > end_d:   continue
            text = " ".join([str(obj.get("title") or ""), str(obj.get("description") or ""), str(obj.get("content") or "")])
            for gname, pats in patterns.items():
                if any(p.search(text) for p in pats):
                    k = (d.isoformat(), gname)
                    counts[k] = counts.get(k, 0) + 1
    rows = [{"date": pd.to_datetime(d).date(), "group": g, "source": "regex", "count": c}
            for (d, g), c in counts.items()]
    return pd.DataFrame(rows, columns=["date","group","source","count"])

def finalize_timeseries(ts: pd.DataFrame, start: Optional[str], end: Optional[str],
                        articles_by_day: Optional[Path]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if ts.empty:
        return ts, pd.DataFrame()
    ts = ts.copy()
    ts["date"] = pd.to_datetime(ts["date"]).dt.date

    all_days = pd.date_range(start or ts["date"].min(), end or ts["date"].max(), freq="D").date
    filled = []
    for (g, s), gdf in ts.groupby(["group","source"]):
        gdf = gdf.set_index("date").reindex(all_days, fill_value=0).reset_index().rename(columns={"index":"date"})
        gdf["group"] = g; gdf["source"] = s
        filled.append(gdf)
    ts_full = pd.concat(filled, ignore_index=True).sort_values(["group","source","date"])

    if articles_by_day and Path(articles_by_day).exists():
        a = pd.read_csv(articles_by_day, parse_dates=["date"])
        a["date"] = a["date"].dt.date
        ts_full = ts_full.merge(a[["date","articles"]], on="date", how="left")
        ts_full["count_norm"] = ts_full["count"] / ts_full["articles"].replace({0: np.nan})
    else:
        ts_full["count_norm"] = np.nan

    ts_full["sma7"] = ts_full.groupby(["group","source"])["count"] \
                             .transform(lambda s: pd.Series(s).rolling(7, min_periods=3).mean())
    ts_full["z"] = ts_full.groupby(["group","source"])["count"].transform(zscore)

    union = ts_full.groupby(["date","group"])["count"].sum().reset_index()
    slopes = (union.sort_values(["group","date"]).groupby("group")["count"]
              .apply(lambda s: norm_slope(s.values.astype(float))).reset_index(name="norm_slope"))
    burst  = ts_full.groupby("group")["z"].max().reset_index(name="max_burst_z")
    totals = ts_full.groupby("group")["count"].sum().reset_index(name="total")
    summary = totals.merge(slopes, on="group", how="left").merge(burst, on="group", how="left")
    return ts_full, summary

def plot_groups(ts_full: pd.DataFrame, outdir: Path) -> None:
    import matplotlib.pyplot as plt
    for g, gdf in ts_full.groupby("group"):
        gdf = gdf.sort_values("date")
        plt.figure(figsize=(9,4))
        for s, sdf in gdf.groupby("source"):
            plt.plot(pd.to_datetime(sdf["date"]), sdf["count"], label=s)
        plt.title(f"Trend: {g}"); plt.xlabel("Date"); plt.ylabel("Count"); plt.legend(); plt.tight_layout()
        plt.savefig(outdir/f"trend_{g}.png"); plt.close()

def main():
    p = argparse.ArgumentParser(description="Keyword trend visualizer")
    p.add_argument("--tokens", type=Path, help="Path to tokens_by_day.csv")
    p.add_argument("--articles", type=Path, help="Path to articles_by_day.csv (정규화용)")
    p.add_argument("--reports-dir", type=Path, help="Path to reports/ (YYYY-MM-DD/top_bigrams.csv)")
    p.add_argument("--master", type=Path, help="Path to data/warehouse/master.jsonl (regex)")
    p.add_argument("--lexicon", type=Path, required=True, help="YAML/JSON lexicon")
    p.add_argument("--mode", nargs="+", default=["unigram"], choices=["unigram","phrase","regex"])
    p.add_argument("--start", type=str, default=None)
    p.add_argument("--end", type=str, default=None)
    p.add_argument("--outdir", type=Path, required=True)
    p.add_argument("--charts", action="store_true")
    args = p.parse_args()

    ensure_outdir(args.outdir)
    groups = load_lexicon(args.lexicon)

    frames = []
    if "unigram" in args.mode:
        if not args.tokens: raise SystemExit("--tokens is required for unigram mode")
        frames.append(ts_from_unigrams(args.tokens, groups, args.start, args.end))
    if "phrase" in args.mode:
        if not args.reports_dir: raise SystemExit("--reports-dir is required for phrase mode")
        frames.append(ts_from_bigrams(args.reports_dir, groups, args.start, args.end))
    if "regex" in args.mode:
        if not args.master: raise SystemExit("--master is required for regex mode")
        frames.append(ts_from_master(args.master, groups, args.start, args.end))

    ts = pd.concat([f for f in frames if f is not None and not f.empty], ignore_index=True) \
         if frames else pd.DataFrame(columns=["date","group","source","count"])
    ts_final, summary = finalize_timeseries(ts, args.start, args.end, args.articles)

    ts_final.to_csv(args.outdir/"group_timeseries.csv", index=False)
    summary.to_csv(args.outdir/"group_summary.csv", index=False)

    if args.charts and not ts_final.empty:
        plot_groups(ts_final, args.outdir)

if __name__ == "__main__":
    main()