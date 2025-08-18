from __future__ import annotations
import os, json, csv
from collections import Counter
from pathlib import Path
from datetime import datetime, timezone, timedelta
import argparse

def _load_jsonl(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)

def _resolve_date(s: str) -> str:
    """
    Return ISO date for 'today'/'yesterday'/'+N'/'-N' or a literal YYYY-MM-DD.
    """
    s = (s or "").strip().lower()
    today = datetime.now(timezone.utc).date()
    if s in ("", "today"):
        return today.isoformat()
    if s == "yesterday":
        return (today - timedelta(days=1)).isoformat()
    if s.startswith(("+", "-")):
        try:
            n = int(s)
            return (today + timedelta(days=n)).isoformat()
        except ValueError:
            pass
    return s  # assume already YYYY-MM-DD

def write_report(
    date_str: str,
    kind: str = "raw",
    indir: str = "data",
    outdir: str = "reports",
    sample_limit: int = 50
) -> str:
    # input
    base_in = os.path.join(indir, kind)
    inpath = os.path.join(base_in, f"{date_str}.jsonl")
    if not os.path.exists(inpath):
        raise FileNotFoundError(f"input not found: {inpath}")

    rows = list(_load_jsonl(inpath))
    total = len(rows)
    by_source = Counter(r.get("raw_source", "unknown") for r in rows)
    by_pub = Counter(r.get("publisher", "") for r in rows)

    # output folders
    out_base = os.path.join(outdir, date_str)
    Path(out_base).mkdir(parents=True, exist_ok=True)

    # publishers.csv
    with open(os.path.join(out_base, "publishers.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["publisher", "count"])
        for pub, c in by_pub.most_common():
            w.writerow([pub, c])

    # summary.csv
    with open(os.path.join(out_base, "summary.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["total", "newsapi", "rss", "unknown"])
        w.writerow([
            total,
            by_source.get("newsapi", 0),
            by_source.get("rss", 0),
            by_source.get("unknown", 0),
        ])

    # articles_sample.csv
    fields = ["publisher", "title", "url", "published_at", "raw_source"]
    with open(os.path.join(out_base, "articles_sample.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(fields)
        for r in rows[:sample_limit]:
            w.writerow([r.get(k, "") for k in fields])

    # summary.json (optional)
    with open(os.path.join(out_base, "summary.json"), "w", encoding="utf-8") as f:
        json.dump({
            "date": date_str,
            "kind": kind,
            "total": total,
            "by_source": dict(by_source),
            "top_publishers": by_pub.most_common(20),
        }, f, ensure_ascii=False)

    # HTML report
    html = f"""<html><head><meta charset='utf-8'>
<style>
body{{font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;padding:24px}}
.kpi{{display:flex;gap:16px;margin:12px 0 24px}}
.card{{border:1px solid #eee;border-radius:12px;padding:12px 16px}}
table{{border-collapse:collapse;width:100%}}
th,td{{border-bottom:1px solid #eee;padding:8px 10px;text-align:left}}
h2{{margin-top:28px}}
</style></head><body>
<h1>News Report — {date_str} ({kind})</h1>
<div class="kpi">
  <div class="card"><b>Total</b><div>{total}</div></div>
  <div class="card"><b>newsapi</b><div>{by_source.get('newsapi',0)}</div></div>
  <div class="card"><b>rss</b><div>{by_source.get('rss',0)}</div></div>
</div>
<h2>Top Publishers</h2>
<table><thead><tr><th>#</th><th>Publisher</th><th>Count</th></tr></thead><tbody>
"""
    for i, (pub, c) in enumerate(by_pub.most_common(20), 1):
        html += f"<tr><td>{i}</td><td>{pub}</td><td>{c}</td></tr>"
    html += "</tbody></table>"

    html += "<h2>Sample Articles</h2><table><thead><tr><th>Publisher</th><th>Title</th><th>Source</th><th>Time</th></tr></thead><tbody>"
    for r in rows[:min(sample_limit, 50)]:
        pub = (r.get("publisher") or "").replace("&", "&amp;")
        title = (r.get("title") or "").replace("&", "&amp;")
        src = r.get("raw_source", "")
        t = r.get("published_at", "") or ""
        html += f"<tr><td>{pub}</td><td>{title}</td><td>{src}</td><td>{t}</td></tr>"
    html += "</tbody></table></body></html>"

    with open(os.path.join(out_base, "report.html"), "w", encoding="utf-8") as f:
        f.write(html)

    # quick preview copy
    with open(os.path.join(outdir, "latest.html"), "w", encoding="utf-8") as f:
        f.write(html)

    return out_base

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Write HTML/CSV report for a given day")
    ap.add_argument("--date", default="today",
                    help='"YYYY-MM-DD", "today", "yesterday", or +/-N days')
    ap.add_argument("--kind", default="raw",
                    help="subfolder under --indir (e.g., raw, silver, raw_newsapi, raw_gov)")
    ap.add_argument("--indir", default="data", help="input root directory")
    ap.add_argument("--outdir", default="reports", help="output root directory")
    ap.add_argument("--limit", type=int, default=50, help="article sample size in the HTML")
    args = ap.parse_args()

    d = _resolve_date(args.date)
    out = write_report(d, kind=args.kind, indir=args.indir, outdir=args.outdir, sample_limit=args.limit)
    print(f"[OK] wrote report -> {out}/report.html")
