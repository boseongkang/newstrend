import argparse, re, json, html, math
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

DEF_STOP = {
    "the","a","an","and","or","but","if","then","else","when","while","for","to","of","in","on","at","by","as","from",
    "with","without","about","into","over","after","before","during","between","through","within","across","per",
    "is","am","are","was","were","be","been","being","do","does","did","done","doing","have","has","had",
    "will","would","should","could","may","might","must","can","just","also","not","no","nor","only","own",
    "same","so","than","too","very","such","any","each","both","more","most","other","some","few","many","much",
    "i","you","he","she","it","we","they","me","him","her","them","my","your","his","its","our","their","this","that",
    "these","those","there","here","where","who","whom","whose","which","what","why","how",
    "s","t","d","ll","m","re","ve","y","u","us",
    "said","says","say","via","news","story","update","latest","new","today","yesterday","monday","tuesday",
    "wednesday","thursday","friday","saturday","sunday","week","weeks","month","months","year","years","day","days",
    "mr","mrs","ms","dr","co","com","www","http","https","rt","img","src","amp","nbsp","apos","mdash","ndash","chars"
}

NOISE_PAT = re.compile(
    r"(?i)(window\.open\(.*?\)|href\s*=\s*['\"][^'\"]*['\"]|target\s*=\s*['\"]_blank['\"]|return\s+false|javascript:|onclick=[\"'][^\"']*[\"'])"
)
URL_PAT = re.compile(r"https?://\S+|www\.\S+")
TAG_PAT = re.compile(r"<[^>]+>")
NUM_PAT = re.compile(r"\b\d[\d,.\-/%]*\b")
PUNC_PAT = re.compile(r"[^a-z\s]+")

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--min-len", type=int, default=3)
    ap.add_argument("--extra-stop", type=str, default="")
    ap.add_argument("--use-ner", action="store_true")
    ap.add_argument("--model", type=str, default="en_core_web_sm")
    ap.add_argument("--max-docs", type=int, default=0)
    ap.add_argument("--nprocs", type=int, default=1)
    return ap.parse_args()

def build_stop(extra: str):
    s = set(DEF_STOP)
    if extra:
        s.update([w.strip().lower() for w in re.split(r"[,\s]+", extra) if w.strip()])
    s.update({"nasdaq","nyse","report","reports","free","get","inc","llc","corp","company","holdings","holding","quarter","q1","q2","q3","q4","etf","href","blank","return","false","open","thefly","thefly.com","globenewswire","prnewswire"})
    return s

def as_date(v):
    if not v:
        return None
    if isinstance(v, (int, float)):
        try:
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except Exception:
            return None
    for k in ("%Y-%m-%dT%H:%M:%SZ","%Y-%m-%d %H:%M:%S%z","%Y-%m-%dT%H:%M:%S%z","%Y-%m-%d"):
        try:
            dt = datetime.strptime(str(v), k)
            if not dt.tzinfo: dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue
    try:
        dt = pd.to_datetime(v, utc=True)
        return dt.to_pydatetime()
    except Exception:
        return None

def normalize_text(txt: str):
    if not txt: return ""
    t = html.unescape(txt)
    t = NOISE_PAT.sub(" ", t)
    t = URL_PAT.sub(" ", t)
    t = TAG_PAT.sub(" ", t)
    t = NUM_PAT.sub(" ", t)
    t = t.replace("\u00a0"," ")
    t = t.replace("\u200b"," ")
    t = t.lower()
    t = PUNC_PAT.sub(" ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def tokenize_simple(t: str, stop: set, min_len: int):
    toks = [w for w in t.split() if len(w) >= min_len and w not in stop]
    return toks

def load_spacy(model):
    try:
        import spacy
        nlp = spacy.load(model, disable=["textcat","textcat_multilabel"])
        return nlp
    except Exception:
        return None

def tokenize_ner(nlp, t: str, stop: set, min_len: int):
    doc = nlp(t)
    toks = []
    propn = []
    for ent in doc.ents:
        s = ent.text.strip()
        if 2 <= len(s) <= 80:
            propn.append(s)
    for tok in doc:
        if tok.is_space: continue
        if tok.like_num: continue
        w = tok.lemma_.lower().strip() if tok.lemma_ else tok.text.lower().strip()
        if len(w) < min_len: continue
        if w in stop: continue
        if tok.pos_ == "PROPN": continue
        if re.fullmatch(PUNC_PAT, w): continue
        toks.append(w)
    return toks, propn

def topn(counter: Counter, k: int):
    return pd.DataFrame(counter.most_common(k), columns=["key","count"])

def plot_barh(df, title, path):
    if df.empty:
        fig, ax = plt.subplots(figsize=(6,2))
        ax.text(0.5,0.5,"no data",ha="center",va="center"); ax.axis("off")
        fig.savefig(path, bbox_inches="tight"); plt.close(fig); return
    fig, ax = plt.subplots(figsize=(8,6))
    d = df.iloc[::-1]
    ax.barh(d.iloc[:,0], d.iloc[:,1])
    ax.set_title(title)
    ax.set_xlabel("count")
    fig.savefig(path, bbox_inches="tight"); plt.close(fig)

def plot_heatmap(df_counts: pd.DataFrame, vocab: list[str], path: Path, win: int):
    if df_counts.empty or not vocab:
        fig, ax = plt.subplots(figsize=(6,2))
        ax.text(0.5,0.5,"no data",ha="center",va="center"); ax.axis("off")
        fig.savefig(path, bbox_inches="tight"); plt.close(fig); return
    df = df_counts.copy()
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_convert(None).dt.normalize()
    last = df["date"].max()
    start = last - pd.Timedelta(days=max(win-1,0))
    df = df[df["date"].between(start,last)]
    if df.empty:
        fig, ax = plt.subplots(figsize=(6,2))
        ax.text(0.5,0.5,"no data",ha="center",va="center"); ax.axis("off")
        fig.savefig(path, bbox_inches="tight"); plt.close(fig); return
    p = (
        df.groupby(["token","date"], as_index=False)["count"].sum()
          .pivot_table(index="token", columns="date", values="count", aggfunc="sum", fill_value=0)
    )
    keep = [t for t in vocab if t in p.index]
    if not keep:
        fig, ax = plt.subplots(figsize=(6,2))
        ax.text(0.5,0.5,"no data",ha="center",va="center"); ax.axis("off")
        fig.savefig(path, bbox_inches="tight"); plt.close(fig); return
    p = p.loc[keep]
    fig, ax = plt.subplots(figsize=(10,7))
    im = ax.imshow(p.values, aspect="auto", cmap="viridis")
    ax.set_yticks(range(len(p.index)))
    ax.set_yticklabels(p.index)
    xs = list(p.columns)
    ax.set_xticks(range(len(xs)))
    ax.set_xticklabels([pd.to_datetime(x).strftime("%m-%d") for x in xs], rotation=90, fontsize=8)
    fig.colorbar(im, ax=ax, label="count")
    ax.set_title("Heatmap — top words")
    fig.savefig(path, bbox_inches="tight"); plt.close(fig)

def main():
    args = parse_args()
    stop = build_stop(args.extra_stop)
    nlp = load_spacy(args.model) if args.use_ner else None
    out = Path(args.outdir); out.mkdir(parents=True, exist_ok=True)
    docs = 0
    total_counter = Counter()
    bigram_counter = Counter()
    proper_counter = Counter()
    per_day = defaultdict(int)
    day_token = defaultdict(int)
    path = Path(args.master)
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if args.max_docs and docs >= args.max_docs: break
            line = line.strip()
            if not line: continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            dt = as_date(row.get("date") or row.get("publishedAt") or row.get("published_at"))
            if not dt: continue
            txt = " ".join([str(row.get(k) or "") for k in ("title","description","content")])
            t = normalize_text(txt)
            if not t: continue
            if nlp:
                toks, propn = tokenize_ner(nlp, t, stop, args.min_len)
                for p in propn:
                    proper_counter[p] += 1
            else:
                toks = tokenize_simple(t, stop, args.min_len)
            if not toks: continue
            dkey = dt.date().isoformat()
            per_day[dkey] += 1
            for w in toks:
                total_counter[w] += 1
                day_token[(dkey,w)] += 1
            for a,b in zip(toks, toks[1:]):
                bigram = f"{a} {b}"
                bigram_counter[bigram] += 1
            docs += 1
    df_counts = pd.DataFrame([(d,t,c) for (d,t),c in day_token.items()], columns=["date","token","count"])
    df_counts.sort_values(["date","count"], ascending=[True,False], inplace=True)
    df_top = topn(total_counter, args.top)
    df_bi = topn(bigram_counter, args.top)
    df_prop = topn(proper_counter, args.top)
    df_counts.to_csv(out/"words_daily.csv", index=False)
    df_top.to_csv(out/"words_top.csv", index=False)
    df_bi.to_csv(out/"bigrams_top.csv", index=False)
    df_prop.to_csv(out/"proper_nouns_top.csv", index=False)
    plot_barh(df_top.rename(columns={"key":"word"}), "Top words", out/"top_words.png")
    plot_barh(df_bi.rename(columns={"key":"bigram"}), "Top bigrams", out/"top_bigrams.png")
    plot_barh(df_prop.rename(columns={"key":"proper noun"}), "Top proper nouns", out/"proper_nouns.png")
    vocab = list(df_top["key"])
    plot_heatmap(df_counts, vocab, out/"heatmap.png", args.days)
    html_path = out/"index.html"
    stats = f"Docs: {docs} | Window: last {args.days} days in heatmap"
    html_path.write_text(
        f"""<!doctype html><meta charset="utf-8"><title>Clean Keywords Report</title>
        <style>body{{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif;padding:20px}}
        h1{{margin-top:0}} img{{max-width:100%}} .row{{display:grid;grid-template-columns:1fr 1fr;gap:24px}}</style>
        <h1>Clean Keywords Report</h1>
        <div>{stats}</div>
        <div class="row">
          <div><h2>Top Words</h2><img src="top_words.png"></div>
          <div><h2>Top Bigrams</h2><img src="top_bigrams.png"></div>
          <div><h2>Proper Nouns</h2><img src="proper_nouns.png"></div>
          <div><h2>Heatmap</h2><img src="heatmap.png"></div>
        </div>
        """,
        encoding="utf-8",
    )
    print(f"docs={docs}")
    print(f"saved: {html_path}")

if __name__ == "__main__":
    main()
