from __future__ import annotations
import argparse, json, os, hashlib, time
from pathlib import Path
from glob import glob
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from datetime import datetime, timezone

def norm_url(u: str | None) -> str:
    if not u: return ""
    s = urlsplit(u.strip())
    netloc = s.netloc.lower().removeprefix("www.")
    qs = [(k,v) for k,v in parse_qsl(s.query, keep_blank_values=True) if not k.lower().startswith("utm_")]
    path = s.path.rstrip("/")
    return urlunsplit((s.scheme.lower(), netloc, path, urlencode(qs), ""))

def parse_ts(x: str | None):
    if not x: return None
    x = x.strip()
    if x.endswith("Z"): x = x[:-1] + "+00:00"
    try: return datetime.fromisoformat(x)
    except Exception: return None

def iso_utc(dt: datetime | None) -> str:
    if not dt: return ""
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00","Z")

def read_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            try: yield json.loads(line)
            except Exception: continue

def write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False)+"\n")

def sha256_path(path: Path) -> str:
    h=hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda:f.read(1024*1024), b""):
            h.update(b)
    return h.hexdigest()

def load_master_keys(master_jsonl: Path) -> set[str]:
    keys=set()
    if master_jsonl.exists():
        for r in read_jsonl(master_jsonl):
            k=r.get("norm_url") or ""
            if k: keys.add(k)
    return keys

def load_daily_keys_file(path: Path) -> set[str]:
    if not path.exists(): return set()
    with path.open("r", encoding="utf-8") as f:
        return set(x.strip() for x in f if x.strip())

def append_daily_keys_file(path: Path, keys: list[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for k in keys: f.write(k+"\n")

def normalize_row(row: dict, src: str):
    url=row.get("url") or row.get("link")
    pu=row.get("published_at") or row.get("publishedAt") or row.get("published")
    return {
        "article_id": row.get("article_id") or (f"newsapi:{url}" if url else None),
        "url": url,
        "norm_url": norm_url(url),
        "title": row.get("title"),
        "publisher": (row.get("source") or {}).get("name") if isinstance(row.get("source"), dict) else row.get("publisher"),
        "published_at": pu,
        "description": row.get("description"),
        "content": row.get("content"),
        "raw_source": row.get("raw_source") or "newsapi",
        "_src": src,
    }

def decide_date_str(published_at: str | None, fallback_dt: datetime) -> str:
    dt=parse_ts(published_at) or fallback_dt
    return dt.date().isoformat()

def now_utc(): return datetime.now(timezone.utc)

def safe_lock(lock_path: Path, stale_seconds: int = 7200):
    try:
        if lock_path.exists():
            if time.time() - lock_path.stat().st_mtime > stale_seconds:
                lock_path.unlink(missing_ok=True)
        fd=os.open(str(lock_path), os.O_CREAT|os.O_EXCL|os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        return True
    except FileExistsError:
        return False

def release_lock(lock_path: Path):
    try: lock_path.unlink(missing_ok=True)
    except Exception: pass

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--inputs", nargs="*", default=[
        "data/live_newsapi/*.jsonl",
        "data/raw_newsapi/*.jsonl",
        "data/silver_newsapi/*.jsonl",
    ])
    ap.add_argument("--warehouse", default="data/warehouse")
    ap.add_argument("--master", default="data/warehouse/master.jsonl")
    ap.add_argument("--daily-dir", default="data/warehouse/daily")
    ap.add_argument("--index", default="data/warehouse/index.jsonl")
    ap.add_argument("--metrics", default="data/metrics/warehouse_latest.json")
    ap.add_argument("--force-rebuild", action="store_true")
    ap.add_argument("--since", default="")
    ap.add_argument("--until", default="")
    args=ap.parse_args()

    wh=Path(args.warehouse); wh.mkdir(parents=True, exist_ok=True)
    lock=wh/".lock"
    if not safe_lock(lock):
        print("busy"); return 0

    try:
        master_path=Path(args.master)
        daily_dir=Path(args.daily_dir)
        index_path=Path(args.index)
        metrics_path=Path(args.metrics)

        globs=args.inputs or []
        files=[]
        for g in globs: files.extend(glob(g))
        files=sorted({str(Path(p)) for p in files})
        indexed={}
        if index_path.exists() and not args.force_rebuild:
            for r in read_jsonl(index_path):
                indexed[r.get("path")]=r

        master_keys=set()
        if not args.force_rebuild:
            master_keys=load_master_keys(master_path)

        all_new=[]
        processed=0
        accepted=0
        now=now_utc()
        t0=parse_ts(args.since if args.since else None) if args.since else None
        t1=parse_ts(args.until if args.until else None) if args.until else None

        for pstr in files:
            p=Path(pstr)
            if not p.exists(): continue
            sh=sha256_path(p)
            rec=indexed.get(pstr)
            if rec and rec.get("sha256")==sh and rec.get("applied") and not args.force_rebuild:
                continue
            rows=list(read_jsonl(p))
            out_rows=[]
            for r in rows:
                nr=normalize_row(r, pstr)
                k=nr["norm_url"]
                if not k: continue
                if t0 or t1:
                    dt=parse_ts(nr["published_at"])
                    if not dt: dt=now
                    if t0 and dt<t0: continue
                    if t1 and dt>t1: continue
                if k in master_keys: continue
                out_rows.append(nr)
                master_keys.add(k)
            if out_rows:
                write_jsonl(master_path, out_rows)
                groups={}
                for r in out_rows:
                    d=decide_date_str(r.get("published_at"), now)
                    groups.setdefault(d, []).append(r)
                for d,rows_d in groups.items():
                    daily_file=daily_dir/f"{d}.jsonl"
                    keys_file=daily_dir/".keys"/f"{d}.txt"
                    seen=load_daily_keys_file(keys_file)
                    new_for_day=[r for r in rows_d if (r["norm_url"] not in seen)]
                    if new_for_day:
                        write_jsonl(daily_file, new_for_day)
                        append_daily_keys_file(keys_file, [r["norm_url"] for r in new_for_day])
                accepted+=len(out_rows)
            processed+=1
            idx_row={"path":pstr,"sha256":sh,"bytes":p.stat().st_size,"rows":len(rows),"applied":True,"updated_at":iso_utc(now)}
            write_jsonl(index_path, [idx_row])

        meta={
            "updated_at": iso_utc(now),
            "inputs": globs,
            "files_seen": len(files),
            "files_processed": processed,
            "new_accepted": accepted,
            "master_path": str(master_path),
            "daily_dir": str(daily_dir),
        }
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        metrics_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(meta))
        return 0
    finally:
        release_lock(lock)

if __name__=="__main__":
    raise SystemExit(main())
