from __future__ import annotations
import json, hashlib, argparse
from pathlib import Path
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from datetime import datetime, timezone, timedelta

TRACKING_PARAMS = {
    "utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "gclid","fbclid","mc_cid","mc_eid","_hsenc","_hsmi"
}

def _resolve_date(s: str) -> str:
    s = (s or "").strip().lower()
    today = datetime.now(timezone.utc).date()
    if s in ("", "today"): return today.isoformat()
    if s == "yesterday":   return (today - timedelta(days=1)).isoformat()
    if s.startswith(("+","-")):
        try:
            return (today + timedelta(days=int(s))).isoformat()
        except ValueError:
            pass
    return s  # assume YYYY-MM-DD

def load_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def save_jsonl(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def normalize_url(u: str | None) -> str | None:
    if not u: return None
    try:
        p = urlparse(u)
        # lowercase scheme/host; drop fragment; strip default ports; trim trailing slash
        scheme = (p.scheme or "https").lower()
        netloc = (p.netloc or "").lower()
        if netloc.endswith(":80") and scheme == "http":
            netloc = netloc[:-3]
        if netloc.endswith(":443") and scheme == "https":
            netloc = netloc[:-4]
        # remove tracking params and keep stable order
        q = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True) if k not in TRACKING_PARAMS]
        query = urlencode(q, doseq=True)
        path = p.path.rstrip("/") or "/"
        cleaned = urlunparse((scheme, netloc, path, "", query, ""))
        return cleaned
    except Exception:
        return u

def dedupe_rows(rows: list[dict], key_mode: str = "url") -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for r in rows:
        url = normalize_url(r.get("url"))
        title = (r.get("title") or "").strip().lower()
        if key_mode == "url":
            k = url or title
        elif key_mode == "title":
            k = title or url or ""
        else:  # url_or_title
            k = url or title
        if not k:
            # fallback hash on entire record to avoid dropping everything
            k = "rec:" + hashlib.sha1(json.dumps(r, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        h = hashlib.sha1(k.encode()).hexdigest()
        if h in seen:
            continue
        seen.add(h)
        out.append(r)
    return out

def main():
    ap = argparse.ArgumentParser(description="Deduplicate a daily JSONL into a 'silver' folder.")
    ap.add_argument("--date", default="yesterday", help='"YYYY-MM-DD", "today", "yesterday", or +/-N days')
    ap.add_argument("--indir", default="data", help="input root directory")
    ap.add_argument("--in-kind", default="raw_newsapi", help="subfolder under --indir for input")
    ap.add_argument("--outdir", default="data", help="output root directory")
    ap.add_argument("--out-kind", default="silver_newsapi", help="subfolder under --outdir for output")
    ap.add_argument("--key-mode", choices=["url","title","url_or_title"], default="url",
                    help="how to compute the dedup key")
    args = ap.parse_args()

    d = _resolve_date(args.date)
    inp = Path(args.indir) / args.in_kind / f"{d}.jsonl"
    if not inp.exists():
        raise FileNotFoundError(f"input not found: {inp}")
    rows = list(load_jsonl(inp))
    kept = dedupe_rows(rows, key_mode=args.key_mode)

    out = Path(args.outdir) / args.out_kind / f"{d}.jsonl"
    save_jsonl(out, kept)
    print(f"[OK] dedup -> {out} (input={len(rows)} kept={len(kept)} removed={len(rows)-len(kept)})")

if __name__ == "__main__":
    main()
