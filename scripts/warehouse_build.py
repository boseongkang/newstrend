from __future__ import annotations
import argparse, os, glob, json, hashlib
from pathlib import Path
from datetime import datetime, timezone

def iso_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

def _first10(s):
    return s[:10] if isinstance(s,str) and len(s)>=10 else None

def parse_date(row, src_path):
    cands = [
        row.get("published_at"),
        row.get("publishedAt"),
        row.get("date"),
        row.get("pubDate"),
    ]
    for v in cands:
        if isinstance(v,str) and len(v)>=10:
            d = _first10(v)
            if d: return d
    name = os.path.basename(src_path)
    d = _first10(name)
    if d and d[4]=="-" and d[7]=="-": return d
    return datetime.now(timezone.utc).date().isoformat()

def pick_publisher(row):
    cand = [row.get("publisher")]
    src = row.get("source")
    if isinstance(src, dict): cand.append(src.get("name"))
    elif isinstance(src, str): cand.append(src)
    cand.append(row.get("source_name"))
    cand.append(row.get("publisher_name"))
    for v in cand:
        if v:
            v = str(v).strip()
            if v and v.lower()!="none": return v
    return "unknown"

def article_key(row):
    k = row.get("article_id") or row.get("id") or row.get("url") or ""
    if k: return k
    base = (row.get("title") or "") + "|" + (row.get("description") or "")
    return "h:" + hashlib.sha1(base.encode("utf-8")).hexdigest()

def load_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            try:
                yield json.loads(line)
            except Exception:
                continue

def save_jsonl(path, rows):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def normalize(row, src_path):
    out = {}
    out["article_id"] = article_key(row)
    out["title"] = row.get("title")
    out["url"] = row.get("url")
    out["publisher"] = pick_publisher(row)
    out["published_at"] = row.get("published_at") or row.get("publishedAt")
    if not out["published_at"]:
        d = parse_date(row, src_path)
        out["published_at"] = d + "T00:00:00Z"
    out["description"] = row.get("description")
    out["content"] = row.get("content")
    out["raw_source"] = row.get("raw_source") or "newsapi"
    return out

def build(master_path, daily_dir, metrics_path, inputs):
    files = []
    for pat in inputs:
        files.extend(glob.glob(pat))
    files = sorted(set(files))
    seen_ids = set()
    master = []
    if os.path.exists(master_path):
        for r in load_jsonl(master_path):
            k = article_key(r)
            if k in seen_ids: continue
            seen_ids.add(k)
            master.append(r)
    before = len(seen_ids)
    new_rows = []
    per_day = {}
    processed = 0
    for fp in files:
        processed += 1
        for row in load_jsonl(fp):
            n = normalize(row, fp)
            k = article_key(n)
            if k in seen_ids: continue
            seen_ids.add(k)
            new_rows.append(n)
            d = n["published_at"][:10]
            per_day.setdefault(d, []).append(n)
    if new_rows:
        master.extend(new_rows)
        save_jsonl(master_path, master)
    for d, rows in per_day.items():
        day_path = os.path.join(daily_dir, f"{d}.jsonl")
        if os.path.exists(day_path):
            exist_ids = set()
            exist_rows = []
            for r in load_jsonl(day_path):
                k = article_key(r)
                if k in exist_ids: continue
                exist_ids.add(k)
                exist_rows.append(r)
            for r in rows:
                k = article_key(r)
                if k not in exist_ids:
                    exist_ids.add(k)
                    exist_rows.append(r)
            save_jsonl(day_path, exist_rows)
        else:
            save_jsonl(day_path, rows)
    metrics = {
        "updated_at": iso_now(),
        "inputs": inputs,
        "files_seen": len(files),
        "files_processed": processed,
        "new_accepted": len(new_rows),
        "master_path": master_path,
        "daily_dir": daily_dir,
        "master_count": len(master),
    }
    Path(metrics_path).parent.mkdir(parents=True, exist_ok=True)
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    print(json.dumps(metrics, ensure_ascii=False))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="data/warehouse/master.jsonl")
    ap.add_argument("--daily-dir", default="data/warehouse/daily")
    ap.add_argument("--metrics", default="data/metrics/warehouse_latest.json")
    ap.add_argument("--inputs", default="data/live_newsapi/*.jsonl,data/raw_newsapi/*.jsonl,data/silver_newsapi/*.jsonl")
    args = ap.parse_args()
    inputs = [s.strip() for s in args.inputs.split(",") if s.strip()]
    build(args.master, args.daily_dir, args.metrics, inputs)
if __name__ == "__main__":
    main()
