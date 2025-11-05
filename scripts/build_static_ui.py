#!/usr/bin/env python3
import json, pathlib, re, datetime as dt
from collections import defaultdict, Counter

ROOT = pathlib.Path(__file__).resolve().parent

def load_range(run_dir: pathlib.Path, wh_dir: pathlib.Path):
    rng = run_dir / "date_range.txt"
    if rng.exists():
        s, e = open(rng).read().strip().split()
    else:
        files = sorted(p for p in wh_dir.glob("*.jsonl") if re.match(r"\d{4}-\d{2}-\d{2}\.jsonl$", p.name))
        if not files:
            raise SystemExit("no warehouse daily files")
        s = files[0].name[:10]
        e = files[-1].name[:10]
    return dt.date.fromisoformat(s), dt.date.fromisoformat(e)

def build_basic_jsons(wh_daily: pathlib.Path, out_data: pathlib.Path, s: dt.date, e: dt.date):
    import orjson
    pubs = Counter()
    day_keys = defaultdict(set)
    for f in sorted(wh_daily.glob("*.jsonl")):
        m = re.match(r"(\d{4}-\d{2}-\d{2})\.jsonl", f.name)
        if not m: continue
        d = dt.date.fromisoformat(m.group(1))
        if d < s or d > e: continue
        with open(f, "rb") as fh:
            for line in fh:
                try:
                    o = orjson.loads(line)
                except:
                    continue
                v = o.get("publisher") or o.get("source") or o.get("source_name") or o.get("site") or o.get("domain") or ""
                if isinstance(v, dict): v = v.get("name") or v.get("id") or ""
                if isinstance(v, list): v = v[0] if v else ""
                v = str(v).strip()
                if v: pubs[v] += 1
                url = (o.get("url") or o.get("link") or "").strip()
                title = (o.get("title") or "").strip()
                key = url or (title, v)
                if key: day_keys[d].add(key)

    top_pubs = pubs.most_common(50)
    (out_data / "publishers.json").write_text(json.dumps({
        "labels":[k for k,_ in top_pubs],
        "counts":[int(v) for _,v in top_pubs]
    }))

    dates, counts = [], []
    d = s
    while d <= e:
        dates.append(d.isoformat())
        counts.append(len(day_keys.get(d, set())))
        d += dt.timedelta(days=1)
    (out_data / "articles.json").write_text(json.dumps({"dates":dates,"articles":counts}))

def main(run: str, out_dir: str):
    out = pathlib.Path(out_dir); (out / "data").mkdir(parents=True, exist_ok=True)
    wh = pathlib.Path("data/warehouse/daily")
    s, e = load_range(pathlib.Path(run), wh)
    build_basic_jsons(wh, out/"data", s, e)

    trends = pathlib.Path(run) / "tokens_by_day.cleaned.csv"
    if trends.exists():
        import pandas as pd
        df = pd.read_csv(trends)
        dates = sorted(df["date"].unique().tolist())
        top_terms = df.groupby("term")["count"].sum().sort_values(ascending=False).head(50).index.tolist()
        terms = sorted(df["term"].unique().tolist())
        series = {}
        for t in top_terms:
            sub = df[df["term"]==t].set_index("date")["count"]
            series[t] = [int(sub.get(d, 0)) for d in dates]
        (out/"data"/"trends.json").write_text(json.dumps({"dates":dates,"terms":terms,"top":top_terms,"series":series}))

    sectors = ROOT.parent / "config" / "ticker_sectors.json"
    if sectors.exists():
        (out/"data"/"tickers.json").write_text(sectors.read_text(encoding="utf-8"))

    idx = (ROOT / "static_dashboard.html").read_text(encoding="utf-8")
    rpt = (ROOT / "report.html").read_text(encoding="utf-8")
    ris = (ROOT / "rising.html").read_text(encoding="utf-8")
    (out/"index.html").write_text(idx, encoding="utf-8")
    (out/"report.html").write_text(rpt, encoding="utf-8")
    (out/"rising.html").write_text(ris, encoding="utf-8")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    main(a.run, a.out)