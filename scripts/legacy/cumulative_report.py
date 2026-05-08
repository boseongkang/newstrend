import argparse, glob, json, re, html, pathlib, datetime
from collections import Counter, defaultdict

NOISE = {
    "chars","href","blank","return","false","window","open","shar",
    "inc","llc","plc","ltd","co","com",
}
STOP = NOISE | {
    "the","a","an","and","or","of","to","in","for","on","by","with","as","at","from",
    "that","this","it","its","is","are","was","were","be","been","but","not","you",
    "we","they","their","your","our","will","would","can","could","should","may",
    "more","most","other","over","into","about","after","before","than","up","down",
    "new","news","via","amp"
}

TOKEN_RE = re.compile(r"[A-Za-z]+")
BRACKET_CHARS_RE = re.compile(r"\[\+\d+\s+chars\]")  # e.g. [... +123 chars]

def load_jsonl(paths):
    for p in paths:
        for line in open(p, "r", encoding="utf-8", errors="ignore"):
            line = line.strip()
            if not line: continue
            try:
                yield json.loads(line)
            except Exception:
                continue

def pick_publisher(row):
    cand = []

    cand.append(row.get("publisher"))

    src = row.get("source")
    if isinstance(src, dict):
        cand.append(src.get("name"))
    else:
        cand.append(src)

    cand.append(row.get("source_name"))
    cand.append(row.get("publisher_name"))

    for v in cand:
        if v:
            v = str(v).strip()
            if v and v.lower() != "none":
                return v
    return "unknown"


def norm_title(t):
    if not t: return ""
    t = BRACKET_CHARS_RE.sub("", t)
    t = re.sub(r"\s+", " ", t).strip().lower()
    return t

def get_source(rec):
    if isinstance(rec.get("source"), dict):
        nm = rec["source"].get("name") or ""
    else:
        nm = rec.get("source") or ""
    return (nm or "unknown").strip()

def get_when(rec):
    ts = rec.get("publishedAt") or rec.get("published_at") or ""
    return ts

def make_text(rec):
    parts = [rec.get("title") or "", rec.get("description") or "", rec.get("content") or ""]
    txt = " ".join(parts)
    txt = BRACKET_CHARS_RE.sub("", txt)
    return txt

def tokenize(text, min_len=3):
    words = []
    for w in TOKEN_RE.findall(text.lower()):
        if len(w) < min_len: continue
        if w in STOP: continue
        words.append(w)
    return words

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", nargs="*", default=[
        "data/live_newsapi/*.jsonl",
        "data/raw_newsapi/*.jsonl",
        "data/silver_newsapi/*.jsonl",
    ], help="glob patterns")
    ap.add_argument("--out", default="reports/cumulative.html")
    ap.add_argument("--top", type=int, default=30)
    args = ap.parse_args()

    files = []
    for pat in args.inputs:
        files.extend(sorted(glob.glob(pat)))
    if not files:
        raise SystemExit("No input files found.")

    seen_urls = set()
    seen_titles = set()
    publishers = Counter()
    words = Counter()
    sample_rows = []

    for rec in load_jsonl(files):
        url = rec.get("url") or ""
        title = rec.get("title") or ""
        nt = norm_title(title)

        key = url or nt
        if not key or key in seen_urls or nt in seen_titles:
            continue
        seen_urls.add(key)
        if nt: seen_titles.add(nt)

        pub = get_source(rec)
        publishers[pub] += 1

        toks = tokenize(make_text(rec))
        words.update(toks)

        if len(sample_rows) < 20:
            sample_rows.append((
                pub, rec.get("title") or "", get_when(rec)
            ))

    total_docs = len(seen_urls)
    top_pubs = publishers.most_common(20)
    top_words = words.most_common(args.top)

    out = pathlib.Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    def bar_svg(items, width=900):
        if not items: return ""
        maxv = max(v for _, v in items) or 1
        row_h, pad, lab_x, bar_x = 24, 12, 12, 252
        h = pad + len(items)*24 + pad
        svg = [f"<svg viewBox='0 0 {width} {h}' class='barchart' preserveAspectRatio='xMinYMin meet'>"]
        y = pad+12
        for k, v in items:
            w = int((width-bar_x-60) * (v/maxv))
            svg.append(f"<text x='{lab_x}' y='{y}' class='svglab'>{html.escape(str(k))}</text>")
            svg.append(f"<rect x='{bar_x}' y='{y-12}' width='{w}' height='18' class='svgbar' rx='3'/>")
            svg.append(f"<text x='{bar_x+w+8}' y='{y}' class='svgval'>{v}</text>")
            y += row_h
        svg.append("</svg>")
        return "\n".join(svg)

    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    html_head = """<head><meta charset='utf-8'>
<style>
body{font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;padding:24px;max-width:1100px;margin:0 auto}
.kpi{display:flex;gap:16px;margin:12px 0 24px;flex-wrap:wrap}
.card{border:1px solid #eee;border-radius:12px;padding:12px 16px;min-width:140px}
table{border-collapse:collapse;width:100%}th,td{border-bottom:1px solid #eee;padding:8px 10px;text-align:left}
h2{margin-top:28px}.small{color:#666;font-size:12px}
.barchart{width:100%;height:auto}.svglab{font-size:12px;fill:#222}.svgval{font-size:12px;fill:#444}.svgbar{fill:#7ea6e0}
</style></head>"""

    rows = "\n".join(
        f"<tr><td>{i}</td><td>{html.escape(pub)}</td><td>{cnt}</td></tr>"
        for i,(pub,cnt) in enumerate(top_pubs,1)
    )
    samples = "\n".join(
        f"<tr><td>{html.escape(pub)}</td><td>{html.escape(title)}</td><td>{html.escape(ts or '')}</td></tr>"
        for pub,title,ts in sample_rows
    )
    word_items = [(k,v) for k,v in top_words]

    html_doc = f"""<!doctype html><html>{html_head}
<body>
<h1>Cumulative News Report (ALL files)</h1>
<div class="small">Updated: {now} | Files scanned: {len(files)} | Articles (deduped): {total_docs}</div>

<div class="kpi">
  <div class="card"><b>Files</b><div>{len(files)}</div></div>
  <div class="card"><b>Articles</b><div>{total_docs}</div></div>
</div>

<h2>Top Publishers</h2>
<table><thead><tr><th>#</th><th>Publisher</th><th>Count</th></tr></thead><tbody>
{rows}
</tbody></table>

<h2>Top Words</h2>
{bar_svg(word_items)}

<h2>Sample Articles</h2>
<table><thead><tr><th>Publisher</th><th>Title</th><th>PublishedAt</th></tr></thead><tbody>
{samples}
</tbody></table>

</body></html>"""

    out.write_text(html_doc, encoding="utf-8")
    print(f"[OK] wrote {out} (files={len(files)}, docs={total_docs})")

if __name__ == "__main__":
    main()
