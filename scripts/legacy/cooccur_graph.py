import json, re, argparse, collections, itertools, math
from pathlib import Path
import networkx as nx

tok=re.compile(r"[A-Za-z][A-Za-z\-]+")

def docs(path):
    with open(path) as f:
        for line in f:
            r=json.loads(line)
            txt=" ".join([r.get("title") or "", r.get("description") or ""]).lower()
            yield set(w for w in tok.findall(txt) if len(w)>=3)

def pmi_graph(master, out, top=200, min_pair=20):
    out=Path(out); out.mkdir(parents=True, exist_ok=True)
    D=list(docs(master)); N=len(D)
    df=collections.Counter()
    for ws in D:
        for w in ws: df[w]+=1
    vocab=[w for w,c in df.most_common(top)]
    pair=collections.Counter()
    for ws in D:
        kws=[w for w in ws if w in vocab]
        for a,b in itertools.combinations(sorted(kws),2): pair[(a,b)]+=1
    G=nx.Graph()
    for (a,b),c in pair.items():
        if c<min_pair: continue
        p_ab=c/N; p_a=df[a]/N; p_b=df[b]/N
        pmi=math.log(p_ab/(p_a*p_b)+1e-9)
        if pmi>0: G.add_edge(a,b,weight=pmi,co=c)
    nx.write_graphml(G, out/"cooccur.graphml")
    print("nodes:",G.number_of_nodes(),"edges:",G.number_of_edges(),"->",out/"cooccur.graphml")

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--master", default="data/warehouse/master.jsonl")
    ap.add_argument("--outdir", default="reports/graph")
    ap.add_argument("--top", type=int, default=200)
    ap.add_argument("--min-pair", type=int, default=20)
    args=ap.parse_args()
    pmi_graph(args.master, args.outdir, args.top, args.min_pair)
