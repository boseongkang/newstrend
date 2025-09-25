import argparse, json, re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd

def ensure_outdir(p: Path): p.mkdir(parents=True, exist_ok=True)

def load_lexicon(path: Path) -> Dict[str, Dict[str, list]]:
    txt = path.read_text(encoding="utf-8")
    if path.suffix.lower() in (".yml",".yaml"):
        import yaml; data = yaml.safe_load(txt)
    else:
        data = json.loads(txt)
    return data.get("groups", {})

def load_publisher_sets(blacklist: Optional[Path], weights: Optional[Path]):
    bl = set()
    if blacklist and blacklist.exists():
        bl = {line.strip() for line in blacklist.read_text(encoding="utf-8").splitlines() if line.strip()}
    w = {}
    if weights and weights.exists():
        w = json.loads(weights.read_text(encoding="utf-8"))
    return bl, w

def compile_patterns(groups: Dict[str, Dict[str, list]]) -> Dict[str, List[re.Pattern]]:
    out={}
    for g,cfg in groups.items():
        pats=[]
        for t in (cfg.get("terms") or []):
            toks=[re.escape(x) for x in str(t).split() if x]
            if not toks: continue
            pat=r"\b"+r"\s*[- ]\s*".join(toks)+r"\b" if len(toks)>1 else r"\b"+toks[0]+r"\b"
            pats.append(pat)
        for rp in (cfg.get("regex") or []):
            pats.append(str(rp))
        out[g]=[re.compile(p, re.IGNORECASE) for p in pats]
    return out

def zscore(s: pd.Series)->pd.Series:
    mu, sd = s.mean(), s.std(ddof=0)
    if not sd or np.isnan(sd): return pd.Series(np.zeros(len(s)), index=s.index)
    return (s-mu)/sd

def norm_slope(y: np.ndarray)->float:
    m=float(np.mean(y))
    if m==0: return 0.0
    x=np.arange(len(y))
    return float(np.polyfit(x,y,1)[0]/(m+1e-9))

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--lexicon", required=True)
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--publisher-blacklist", default="")
    ap.add_argument("--publisher-weights", default="")
    ap.add_argument("--charts", action="store_true")
    args=ap.parse_args()

    master=Path(args.master); ensure_outdir(Path(args.outdir))
    groups=load_lexicon(Path(args.lexicon))
    pats=compile_patterns(groups)
    bl, w = load_publisher_sets(Path(args.publisher_blacklist) if args.publisher_blacklist else None,
                                Path(args.publisher_weights) if args.publisher_weights else None)

    start_d=pd.to_datetime(args.start).date() if args.start else None
    end_d  =pd.to_datetime(args.end).date()   if args.end   else None

    counts = {}
    art_per_day = {}
    with master.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            try: obj=json.loads(line)
            except Exception: continue
            d_raw = obj.get("publishedAt") or obj.get("date") or obj.get("published_at")
            try: d = pd.to_datetime(d_raw).date()
            except Exception: continue
            if start_d and d < start_d: continue
            if end_d   and d > end_d:   continue

            src = obj.get("source") or {}
            pub = src.get("name") or obj.get("source_name") or ""
            if pub in bl:
                continue

            text = " ".join([str(obj.get("title") or ""), str(obj.get("description") or ""), str(obj.get("content") or "")])
            matched=False
            for g, plist in pats.items():
                if any(p.search(text) for p in plist):
                    weight=float(w.get(pub, 1.0))
                    key=(d,g)
                    counts[key]=counts.get(key,0.0)+weight
                    matched=True
            art_per_day[d]=art_per_day.get(d,0)+1

    if not counts:
        Path(args.outdir,"group_timeseries.csv").write_text("", encoding="utf-8")
        Path(args.outdir,"group_summary.csv").write_text("", encoding="utf-8")
        return

    rows=[{"date":k[0], "group":k[1], "count_weighted":v} for k,v in counts.items()]
    ts=pd.DataFrame(rows).sort_values("date")
    ts["date"]=pd.to_datetime(ts["date"])

    all_days=pd.date_range(ts["date"].min(), ts["date"].max(), freq="D")
    filled=[]
    for g, gdf in ts.groupby("group"):
        gdf=gdf.set_index("date").reindex(all_days, fill_value=0).rename_axis("date").reset_index()
        gdf["group"]=g
        filled.append(gdf)
    tsf=pd.concat(filled, ignore_index=True).sort_values(["group","date"])

    art_df=pd.DataFrame([{"date":pd.to_datetime(d), "articles":n} for d,n in art_per_day.items()])
    tsf=tsf.merge(art_df, on="date", how="left")
    tsf["count_norm"]=tsf["count_weighted"]/tsf["articles"].replace({0:np.nan})
    tsf["sma7"]=tsf.groupby("group")["count_weighted"].transform(lambda s: s.rolling(7, min_periods=3).mean())
    tsf["z"]=tsf.groupby("group")["count_weighted"].transform(zscore)

    union=tsf.groupby(["date","group"])["count_weighted"].sum().reset_index().sort_values(["group","date"])
    slopes=union.groupby("group")["count_weighted"].apply(lambda s: norm_slope(s.values)).rename("norm_slope")
    burst =tsf.groupby("group")["z"].max().rename("max_burst_z")
    total =tsf.groupby("group")["count_weighted"].sum().rename("total_weighted")
    summary=pd.concat([total, slopes, burst], axis=1).reset_index()

    tsf.to_csv(Path(args.outdir)/"group_timeseries.csv", index=False)
    summary.to_csv(Path(args.outdir)/"group_summary.csv", index=False)

    if args.charts:
        import matplotlib.pyplot as plt
        for g,gdf in tsf.groupby("group"):
            gdf=gdf.sort_values("date")
            plt.figure(figsize=(9,4))
            plt.plot(gdf["date"], gdf["count_weighted"])
            plt.title(f"Trend (weighted): {g}"); plt.xlabel("Date"); plt.ylabel("Weighted count"); plt.tight_layout()
            plt.savefig(Path(args.outdir)/f"trend_{g}.png"); plt.close()

if __name__ == "__main__":
    main()