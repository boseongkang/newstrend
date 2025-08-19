from __future__ import annotations
import os, json, csv, re, html
from collections import Counter, defaultdict
from pathlib import Path
from datetime import datetime, timezone, timedelta
import argparse

TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z']+")
TAIL_RE = re.compile(r"\s*\[\+\d+\s+chars\]\s*$")
URL_RE = re.compile(r"https?://\S+")
EMAIL_RE = re.compile(r"\S+@\S+")
PROPN_SEQ_RE = re.compile(r"\b([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){0,2})\b")
ACRO_RE = re.compile(r"\b[A-Z]{2,}\b")
EN_STOPWORDS = {
    "the","a","an","and","or","of","to","in","on","for","with","at","by","from","as",
    "is","are","was","were","be","been","being","it","its","this","that","these","those",
    "i","you","he","she","we","they","them","his","her","their","our","us",
    "but","if","so","not","no","yes","do","does","did","doing","done","can","could","should",
    "will","would","may","might","must","about","over","under","after","before","between",
    "than","then","there","here","when","where","why","how","into","out","up","down","new",
    "more","most","other","some","any","such","also","just","one","two","three","first","second",
    "said","says","say","mr","ms",
    "chars","https","http","amp","nbsp"
}
CAP_STOP = {"The","A","An","And","Or","Of","To","In","On","For","With","At","By","From","As","Mr","Ms","Dr","President","Vice","Gov","Sen","Rep","U","US","U.S","U.S.","USA","NATO","UN"}
CAP_MONTHS = {"January","February","March","April","May","June","July","August","September","October","November","December"}
IRREG = {
    "has":"have","having":"have","had":"have",
    "does":"do","did":"do","done":"do",
    "is":"be","am":"be","are":"be","was":"be","were":"be","been":"be","being":"be",
    "says":"say","said":"say",
    "goes":"go","went":"go","gone":"go","going":"go",
    "came":"come","coming":"come",
    "made":"make","makes":"make","making":"make",
    "took":"take","taken":"take","taking":"take",
    "got":"get","getting":"get","gotten":"get",
    "children":"child","people":"person","men":"man","women":"woman","mice":"mouse","geese":"goose",
    "better":"good","best":"good","worse":"bad","worst":"bad"
}

def _load_jsonl(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)

def _resolve_date(s: str) -> str:
    s = (s or "").strip().lower()
    today = datetime.now(timezone.utc).date()
    if s in ("", "today"): return today.isoformat()
    if s == "yesterday": return (today - timedelta(days=1)).isoformat()
    if s.startswith(("+","-")):
        try: return (today + timedelta(days=int(s))).isoformat()
        except: pass
    return s

def _lemmatize(t: str) -> str:
    if t in IRREG: return IRREG[t]
    if len(t) > 4 and t.endswith("ies"): return t[:-3] + "y"
    if len(t) > 3 and t.endswith("es") and not t.endswith("ses") and not t.endswith("xes"): return t[:-2]
    if len(t) > 3 and t.endswith("s") and not t.endswith("ss"): return t[:-1]
    if len(t) > 5 and t.endswith("ing"):
        base = t[:-3]
        if base.endswith("e"): return base
        return base
    if len(t) > 4 and t.endswith("ied"): return t[:-3] + "y"
    if len(t) > 4 and t.endswith("ed"): return t[:-2]
    if len(t) > 4 and t.endswith("er"): return t[:-2]
    if len(t) > 5 and t.endswith("est"): return t[:-3]
    return t

def _clean_text(s: str) -> str:
    s = html.unescape(s or "")
    s = URL_RE.sub("", s)
    s = EMAIL_RE.sub("", s)
    s = TAIL_RE.sub("", s)
    return s

def tokenize(text: str, min_len: int = 3) -> list[str]:
    text = _clean_text(text)
    toks = []
    for tok in TOKEN_RE.findall(text.lower()):
        if tok in EN_STOPWORDS: continue
        lem = _lemmatize(tok)
        if lem in EN_STOPWORDS: continue
        if len(lem) < min_len: continue
        if lem.startswith("'"): continue
        toks.append(lem)
    return toks

def extract_proper_nouns(raw_texts: list[str], top: int = 30) -> list[tuple[str,int]]:
    c = Counter()
    for t in raw_texts:
        s = _clean_text(t or "")
        for m in PROPN_SEQ_RE.findall(s):
            if any(w in CAP_STOP or w in CAP_MONTHS for w in m.split()):
                pass
            c[m] += 1
        for m in ACRO_RE.findall(s):
            if m in CAP_STOP:
                pass
            c[m] += 1
    items = [(k, v) for k, v in c.items() if not k.isdigit()]
    items.sort(key=lambda x: x[1], reverse=True)
    return items[:top]

def _svg_bar_chart(items: list[tuple[str,int]], width: int = 900, bar_h: int = 20, gap: int = 6, label_w: int = 260, pad: int = 12) -> str:
    if not items: return "<div class='small'>No data</div>"
    max_c = max(c for _, c in items) or 1
    chart_w = width - label_w - pad*2 - 50
    h = pad*2 + len(items)*(bar_h+gap) - gap
    def esc(s: str) -> str: return html.escape(s, quote=True)
    rows, y = [], pad
    for lab, c in items:
        rows.append(f"<text x='{pad}' y='{y+bar_h-6}' class='svglab'>{esc(lab)}</text>")
        bar_x = pad + label_w
        bar_w = int(chart_w * (c/max_c))
        rows.append(f"<rect x='{bar_x}' y='{y}' width='{bar_w}' height='{bar_h}' class='svgbar' rx='3'/>")
        rows.append(f"<text x='{bar_x+bar_w+6}' y='{y+bar_h-6}' class='svgval'>{c}</text>")
        y += bar_h + gap
    return f"<svg viewBox='0 0 {width} {h}' class='barchart' preserveAspectRatio='xMinYMin meet'>" + "".join(rows) + "</svg>"

def _tfidf_by_publisher(rows: list[dict], top_publishers: int = 8, top_terms: int = 6) -> list[tuple[str, list[tuple[str, float]]]]:
    pubs = Counter(r.get("publisher","") for r in rows)
    pub_list = [p for p,_ in pubs.most_common(top_publishers) if p]
    docs = {}
    for p in pub_list:
        toks = []
        for r in rows:
            if r.get("publisher","") == p:
                text = (r.get("title") or "") + " " + (r.get("content") or "")
                toks.extend(tokenize(text, 3))
        docs[p] = toks
    df = Counter()
    for p,toks in docs.items():
        df.update(set(toks))
    N = len(docs) or 1
    out = []
    for p,toks in docs.items():
        tf = Counter(toks)
        scores = []
        for w,c in tf.items():
            idf = 1.0 + (0 if df[w]==0 else (max(0.0, (N)/(df[w]))))
            scores.append((w, c*idf))
        scores.sort(key=lambda x: x[1], reverse=True)
        out.append((p, scores[:top_terms]))
    return out

def write_report(date_str: str, kind: str = "raw", indir: str = "data", outdir: str = "reports", sample_limit: int = 50, top_k: int = 30) -> str:
    base_in = os.path.join(indir, kind)
    inpath = os.path.join(base_in, f"{date_str}.jsonl")
    if not os.path.exists(inpath):
        raise FileNotFoundError(f"input not found: {inpath}")

    rows = list(_load_jsonl(inpath))
    total = len(rows)
    by_source = Counter(r.get("raw_source", "unknown") for r in rows)
    by_pub = Counter(r.get("publisher", "") for r in rows)

    uni, bi = Counter(), Counter()
    raw_texts = []
    for r in rows:
        title = r.get("title") or ""
        content = r.get("content") or ""
        raw_texts.append(title + " " + content)
        toks = tokenize(title + " " + content, 3)
        uni.update(toks)
        for a, b in zip(toks, toks[1:]):
            bi[(a, b)] += 1

    top_words = [(w, c) for w, c in uni.most_common(top_k)]
    top_bigrams = [(" ".join(k), c) for k, c in bi.most_common(top_k)]
    top_props = extract_proper_nouns(raw_texts, top_k)
    tfidf_pub = _tfidf_by_publisher(rows, top_publishers=8, top_terms=6)

    out_base = os.path.join(outdir, date_str)
    Path(out_base).mkdir(parents=True, exist_ok=True)

    with open(os.path.join(out_base, "publishers.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["publisher", "count"])
        for pub, c in by_pub.most_common(): w.writerow([pub, c])

    with open(os.path.join(out_base, "summary.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["total", "newsapi", "rss", "unknown"])
        w.writerow([total, by_source.get("newsapi", 0), by_source.get("rss", 0), by_source.get("unknown", 0)])

    fields = ["publisher", "title", "url", "published_at", "raw_source"]
    with open(os.path.join(out_base, "articles_sample.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(fields)
        for r in rows[:sample_limit]: w.writerow([r.get(k, "") for k in fields])

    with open(os.path.join(out_base, "top_words.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["word", "count"])
        for wd, c in top_words: w.writerow([wd, c])

    with open(os.path.join(out_base, "top_bigrams.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["bigram", "count"])
        for bg, c in top_bigrams: w.writerow([bg, c])

    with open(os.path.join(out_base, "top_proper_nouns.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["proper_noun", "count"])
        for pn, c in top_props: w.writerow([pn, c])

    with open(os.path.join(out_base, "tfidf_by_publisher.json"), "w", encoding="utf-8") as f:
        json.dump({p: terms for p, terms in tfidf_pub}, f, ensure_ascii=False, indent=2)

    words_svg = _svg_bar_chart(top_words, width=900, bar_h=18, gap=6, label_w=240, pad=12)
    bigrams_svg = _svg_bar_chart(top_bigrams, width=900, bar_h=18, gap=6, label_w=300, pad=12)
    props_svg = _svg_bar_chart(top_props, width=900, bar_h=18, gap=6, label_w=320, pad=12)

    html_doc = f"""<html><head><meta charset='utf-8'>
<style>
body{{font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;padding:24px;max-width:1100px;margin:0 auto}}
.kpi{{display:flex;gap:16px;margin:12px 0 24px;flex-wrap:wrap}}
.card{{border:1px solid #eee;border-radius:12px;padding:12px 16px;min-width:120px}}
table{{border-collapse:collapse;width:100%}}
th,td{{border-bottom:1px solid #eee;padding:8px 10px;text-align:left;vertical-align:top}}
h2{{margin-top:28px}}
.col2{{display:grid;grid-template-columns:1fr 1fr;gap:24px}}
.small{{font-size:12px;color:#666}}
.barchart{{width:100%;height:auto}}
.svglab{{font-size:12px;fill:#222}}
.svgval{{font-size:12px;fill:#444}}
.svgbar{{fill:#7ea6e0}}
.kw{{display:inline-block;background:#f5f7fb;border:1px solid #e5e9f2;border-radius:10px;padding:2px 8px;margin:2px;font-size:12px}}
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
    i = 1
    for pub, c in by_pub.most_common(20):
        html_doc += f"<tr><td>{i}</td><td>{html.escape(pub or '')}</td><td>{c}</td></tr>"
        i += 1
    html_doc += "</tbody></table>"

    html_doc += "<h2>Top Words & Bigrams</h2><div class='col2'>"
    html_doc += f"<div><h3>Top Words</h3>{words_svg}</div>"
    html_doc += f"<div><h3>Top Bigrams</h3>{bigrams_svg}</div>"
    html_doc += "</div>"

    html_doc += "<h2>Top Proper Nouns</h2>"
    html_doc += props_svg

    html_doc += "<h2>Distinctive Keywords by Publisher</h2><table><thead><tr><th>Publisher</th><th>Keywords</th></tr></thead><tbody>"
    for p, terms in tfidf_pub:
        kws = " ".join(f"<span class='kw'>{html.escape(w)}</span>" for w,_ in terms)
        html_doc += f"<tr><td>{html.escape(p)}</td><td>{kws}</td></tr>"
    html_doc += "</tbody></table>"

    html_doc += "<h2>Sample Articles</h2><table><thead><tr><th>Publisher</th><th>Title</th><th>Source</th><th>Time</th></tr></thead><tbody>"
    for r in rows[:min(sample_limit, 50)]:
        pub = html.escape(r.get("publisher") or "")
        title = html.escape(r.get("title") or "")
        src = r.get("raw_source", "")
        t = r.get("published_at", "") or ""
        html_doc += f"<tr><td>{pub}</td><td>{title}</td><td>{src}</td><td>{t}</td></tr>"
    html_doc += "</tbody></table><p class='small'>Tokens: lemmatized, min length 3, custom stopwords applied; title+content used; NewsAPI tails removed; proper nouns via capitalization heuristics.</p>"

    html_doc += "</body></html>"

    with open(os.path.join(out_base, "report.html"), "w", encoding="utf-8") as f:
        f.write(html_doc)
    with open(os.path.join(outdir, "latest.html"), "w", encoding="utf-8") as f:
        f.write(html_doc)
    return out_base

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Write HTML/CSV report for a given day")
    ap.add_argument("--date", default="today", help='"YYYY-MM-DD", "today", "yesterday", or +/-N days')
    ap.add_argument("--kind", default="raw", help="subfolder under --indir (e.g., raw, silver_newsapi, raw_newsapi)")
    ap.add_argument("--indir", default="data", help="input root directory")
    ap.add_argument("--outdir", default="reports", help="output root directory")
    ap.add_argument("--limit", type=int, default=50, help="HTML sample size")
    ap.add_argument("--top", type=int, default=30, help="top K words/bigrams")
    args = ap.parse_args()
    d = _resolve_date(args.date)
    out = write_report(d, kind=args.kind, indir=args.indir, outdir=args.outdir, sample_limit=args.limit, top_k=args.top)
    print(f"[OK] wrote report -> {out}/report.html")
