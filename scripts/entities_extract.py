import json, re, sys, argparse
from pathlib import Path
import pandas as pd
import spacy

def load_aliases(p):
    if not p or not Path(p).exists(): return {}
    with open(p, "r", encoding="utf-8") as f: return json.load(f)

def load_stopwords(p):
    base = {
        "inc","corp","co","ltd","plc","group","company","companies","reuters","news","update",
        "breaking","amp","nbsp","mdash","ndash","jan","feb","mar","apr","may","jun","jul",
        "aug","sep","sept","oct","nov","dec","monday","tuesday","wednesday","thursday",
        "friday","saturday","sunday","mon","tue","wed","thu","fri","sat","sun"
    }
    if p and Path(p).exists():
        base |= {x.strip().lower() for x in Path(p).read_text(encoding="utf-8").splitlines() if x.strip()}
    return base

def get_date(rec):
    for k in ("date","published_at","publishedAt","published"):
        if k in rec and rec[k]:
            s=str(rec[k])
            if len(s)>=10: return s[:10]
    return None

def normalize(txt):
    t = re.sub(r"\s+", " ", txt or "").strip()
    return t.lower()

def main(master, outdir, aliases, stopwords, model, min_len, days):
    outdir = Path(outdir); outdir.mkdir(parents=True, exist_ok=True)
    nlp = spacy.load(model, disable=["parser","lemmatizer","attribute_ruler"])
    alias = load_aliases(aliases)
    stop = load_stopwords(stopwords)
    rows=[]
    with open(master, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec=json.loads(line)
            except:
                continue
            d=get_date(rec)
            if not d:
                continue
            if days>0:
                pass
            text=" ".join([str(rec.get("title","")), str(rec.get("description","")), str(rec.get("content",""))]).strip()
            if not text:
                continue
            doc=nlp(text)
            for ent in doc.ents:
                if ent.label_ not in {"ORG","PERSON","GPE","PRODUCT","EVENT","WORK_OF_ART","NORP","FAC"}:
                    continue
                e=normalize(ent.text)
                if len(e)<min_len or e in stop:
                    continue
                e=alias.get(e,e)
                rows.append((d,e))
    if not rows:
        Path(outdir/"entities_daily.csv").write_text("", encoding="utf-8")
        return
    df=pd.DataFrame(rows, columns=["date","entity"]).value_counts().reset_index(name="count")
    df=df.sort_values(["date","count"], ascending=[True,False])
    if days>0:
        mx=pd.to_datetime(df["date"]).max()
        df=df[pd.to_datetime(df["date"])>=mx-pd.Timedelta(days=days)]
    df.to_csv(outdir/"entities_daily.csv", index=False)
    print(f"saved: {outdir/'entities_daily.csv'}")

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--master", default="data/warehouse/master.jsonl")
    ap.add_argument("--outdir", default="reports/entities")
    ap.add_argument("--aliases", default="config/entities_aliases.json")
    ap.add_argument("--stopwords", default="config/entities_stopwords.txt")
    ap.add_argument("--model", default="en_core_web_sm")
    ap.add_argument("--min-len", type=int, default=3)
    ap.add_argument("--days", type=int, default=0)
    main(**vars(ap.parse_args()))
