import argparse, json, csv, re
from collections import Counter, defaultdict
from pathlib import Path

try:
    import orjson as fastjson
except Exception:
    fastjson = None

def dumps(obj):
    if fastjson:
        return fastjson.dumps(obj).decode()
    return json.dumps(obj, ensure_ascii=False)

def read_jsonl_lines(path):
    loads = fastjson.loads if fastjson else json.loads
    with open(path, 'rb') as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield loads(line)
            except Exception:
                continue

def safe_pub(o):
    v = o.get("publisher") or o.get("source") or o.get("source_name") or o.get("site") or o.get("domain") or ""
    if isinstance(v, dict):
        v = v.get("name") or v.get("id") or ""
    if isinstance(v, list):
        v = v[0] if v else ""
    return str(v).strip()

def day_from_name(p):
    return p.name[:10]

def build_from_warehouse(wh_dir):
    wh = Path(wh_dir)
    files = sorted([p for p in wh.glob("*.jsonl") if re.match(r"\d{4}-\d{2}-\d{2}\.jsonl$", p.name)])
    pubs = Counter()
    day_keys = defaultdict(set)
    for f in files:
        d = day_from_name(f)
        for o in read_jsonl_lines(f):
            pub = safe_pub(o)
            if pub:
                pubs[pub] += 1
            url = (o.get("url") or o.get("link") or "").strip()
            title = (o.get("title") or "").strip()
            key = url or (title, pub)
            if key:
                day_keys[d].add(key)
    dates = [day_from_name(f) for f in files]
    articles = [len(day_keys.get(d, set())) for d in dates]
    return pubs, dates, articles

def build_words(run_dir, stop_path, topn):
    stop = set()
    if stop_path and Path(stop_path).exists():
        for line in open(stop_path, 'r', encoding='utf-8'):
            w = line.strip()
            if w:
                stop.add(w.lower())
    counts = Counter()
    tbd = Path(run_dir) / "tokens_by_day.cleaned.csv"
    if tbd.exists():
        with open(tbd, newline='', encoding='utf-8') as fh:
            reader = csv.DictReader(fh)
            cols = [c.lower() for c in reader.fieldnames]
            tok_col = "token" if "token" in cols else ("term" if "term" in cols else None)
            cnt_col = "count" if "count" in cols else ("n" if "n" in cols else None)
            for row in reader:
                t = row.get(tok_col) if tok_col else None
                c = row.get(cnt_col) if cnt_col else None
                if not t or not c:
                    continue
                t = t.strip().lower()
                if t and t not in stop:
                    try:
                        counts[t] += int(float(c))
                    except:
                        continue
        top = counts.most_common(topn)
        return [w for w, _ in top], [int(v) for _, v in top]
    return [], []

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", default="data/warehouse/daily")
    ap.add_argument("--run", default="run")
    ap.add_argument("--out", default="site/data")
    ap.add_argument("--top-publishers", type=int, default=50)
    ap.add_argument("--top-words", type=int, default=50)
    ap.add_argument("--extra-stop", default="config/extra_noise.txt")
    args = ap.parse_args()

    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)

    pubs, dates, articles = build_from_warehouse(args.warehouse)
    top_pubs = pubs.most_common(args.top_publishers)
    (out/"publishers.json").write_text(dumps({"labels":[k for k,_ in top_pubs],
                                              "counts":[int(v) for _,v in top_pubs]}), encoding="utf-8")
    (out/"articles.json").write_text(dumps({"dates":dates, "articles":[int(x) for x in articles]}), encoding="utf-8")

    labels, counts = build_words(args.run, args.extra_stop, args.top_words)
    (out/"words.json").write_text(dumps({"labels":labels, "counts":[int(x) for x in counts]}), encoding="utf-8")

if __name__ == "__main__":
    main()