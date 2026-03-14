"""
make_trends_json.py  v4
Reads daily *_tokens.csv / *_tokens.jsonl from warehouse
→ site/data/trends.json

New in v4:
  - Extended STOP_WORDS (noise words confirmed from signal output)
  - calc_consistency: penalises single-day spikes
  - calc_source_proxy: fraction of days a word appeared (breadth proxy)
  - Outputs consistency, source_proxy, n_days_active per term
  - Improved summary print
"""

import argparse
import json
import math
from pathlib import Path

import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# STOP WORDS  (function words + boilerplate + confirmed spurious signal words)
# ─────────────────────────────────────────────────────────────────────────────
STOP_WORDS = {
    # ── Core English function words ──
    "the","a","an","and","or","but","in","on","at","to","for","of","with","by",
    "from","up","about","into","through","during","before","after","above","below",
    "between","out","off","over","under","again","further","then","once","here",
    "there","when","where","why","how","all","both","each","few","more","most",
    "other","some","such","no","nor","not","only","own","same","so","than","too",
    "very","can","will","just","now","is","are","was","were","be","been","being",
    "have","has","had","having","do","does","did","doing","would","could","should",
    "may","might","shall","must","need","it","its","itself","he","she","they",
    "we","you","i","me","him","her","us","them","what","which","who","this","that",
    "these","those","am","also","get","got","one","two","three","said","says",
    "make","know","take","see","come","think","look","want","give","use","find",
    "tell","ask","seem","feel","try","call","keep","let","show","hear","run",
    "move","write","read","become","include","continue","set","lead","change",
    "spend","open","stop","create","expect","build","stay","fall","cut","reach",
    "report","decide","raise","pass","require","remain","suggest",
    # ── Temporal ──
    "monday","tuesday","wednesday","thursday","friday","saturday","sunday",
    "january","february","march","april","june","july","august","september",
    "october","november","december","jan","feb","mar","apr","jun","jul","aug",
    "sep","oct","nov","dec","week","month","year","today","yesterday","tomorrow",
    "time","day","hour","minute","night","morning","evening","afternoon",
    # ── News boilerplate ──
    "news","reuters","bloomberg","update","breaking","report","reports","reported",
    "statement","according","sources","source","told","said","says","announced",
    "announcement","release","press","media","outlet","article","story","coverage",
    "interview","spokesperson","official","officials","percent","pct",
    "billion","million","thousand","share","shares","quarter","annual","fiscal",
    "record","high","low","data","information","analysis","result","results",
    "number","numbers","market","markets","stock","stocks","price","prices",
    "rate","rates","index","etf","fund","funds","asset","assets","portfolio",
    "investor","investors","investment","investments","trading","trader","traders",
    "company","companies","firm","firms","group","corp","inc","ltd","plc","llc",
    "business","businesses","industry","sector","segment","division",
    "government","federal","national","international","global","local","public",
    "president","chairman","chief","executive","officer","director","manager",
    "ceo","cfo","coo","vp","svp","evp","head","lead","senior","vice",
    "first","second","third","last","next","new","old","big","large","small",
    "good","bad","best","worst","great","major","key","main","top","leading",
    "strong","weak","positive","negative","current","former",
    "right","left","long","short","early","late","recent","latest","previous",
    "nbsp","mdash","ndash","amp","quot","apos","copy","reg",
    "http","https","www","com","net","org","html","php","aspx",
    "read","more","click","here","free","biztoc","thefly","daily",
    "blank","will","have","that","this","with","from","they","been","were",
    "their","when","there","what","into","your","than","then","them","these",
    "some","would","which","about","could","after","other","over","also","back",
    "only","just","like","well","even","most","many","does","made","each","both",
    "while","where","those","because","through","being","since","either","whether",
    "between","during","against","without","within","under","across","along",
    "toward","upon","among","around","behind","beyond","beside","although",
    "however","therefore","moreover","furthermore","nevertheless","nonetheless",
    "meanwhile","otherwise","already","still","always","never","often","usually",
    "sometimes","perhaps","maybe","probably","likely","recently","currently",
    "quickly","slowly","easily","simply","directly","specifically","particularly",
    "generally","especially","approximately","nearly","almost","quite","rather",
    "very","much","little","few","several","many","enough","every","any",
    # ── Vague / confirmed spurious signal words ──
    "giants","general","future","power","order","plan","claims","meeting",
    "total","level","levels","point","points","basis","scale",
    "deal","deals","part","parts","side","areas","area","place","case",
    "people","person","thing","things","way","ways","fact","facts",
    "kind","type","types","form","forms","process","step","steps",
    "issue","issues","matter","matters","question","situation","condition",
    "conditions","position","positions","role","roles","impact","impacts",
    "effect","effects","affect","affects","influence","attempt","attempts",
    "effort","efforts","action","actions","measure","measures","approach",
    "approaches","response","responses","reaction","decision","decisions",
    "choice","choices","option","options","solution","statement","statements",
    "comment","comments","view","views","opinion","focus","signal","signals",
    "trend","attention","concern","concerns","risk","risks","challenge",
    "challenges","opportunity","possibility","potential","prospect","prospects",
    "outlook","scenario","factor","factors","aspect","aspects","element",
    "elements","component","structure","framework","system","systems","model",
    "models","method","program","programs","project","projects","initiative",
    "initiatives","event","events","moment","period","phase","stage","round",
    "session","term","terms","item","items","detail","details","note","notes",
    "base","based","background","context","overview","summary",
    "increase","increases","decrease","decreases","growth","decline",
    "rise","fall","change","changes","shift","shifts","movement",
    "amount","value","values","volume","average","sum","count","trade",
    # ── Round-3 confirmed from output ──
    "test","ended","expected","fired","videos","announces","performance",
    "announced","america","banks","supply","korea","economy","futures",
    "summit","election","jobs","white","house","ended","appointed",
    "approved","confirmed","signed","passed","launched","released",
    "extended","expanded","raised","lowered","revised","upgraded",
    "downgraded","suspended","halted","resumed","paused","delayed",
    "canceled","rejected","denied","withdrawn","proposed","requested",
    "approved","rejected","passed","failed","won","lost","gained","fell",
    "climbed","dropped","jumped","slipped","surged","tumbled","recovered",
    "rebounded","retreated","advanced","declined","rose","fell","closed",
    "opened","ended","started","began","continued","remained","held",
    "reported","posted","recorded","achieved","missed","beat","topped",
    "exceeded","matched","met","fell short","came in","showed","revealed",
    "indicated","suggested","pointed","highlighted","noted","cited","added",
    "warned","cautioned","predicted","forecast","projected","estimated",
    "expected","anticipated","planned","targeted","aimed","intended",
    "designed","built","made","created","developed","introduced","unveiled",
    # ── Round-2 confirmed from output ──
    "their","reportedly","party","class","experts","plans","russia",
    "india","ukraine","asia","economic","argentina","actually","already",
    "despite","across","within","around","whether","although",
    "regarding","including","following","toward","broadly","officially",
    "apparently","allegedly","substantially","considerably","relatively",
    "potentially","effectively","successfully","immediately","suddenly",
    "slightly","significantly","eventually","finally","initially","previously",
    "actually","seriously","directly","certainly","strongly","clearly",
    "deeply","widely","largely","mainly","mostly","purely","simply","truly",
    "fully","highly","heavily","closely","openly","properly","quickly",
    "easily","nearly","almost","barely","merely","only","very","quite",
    "rather","fairly","somewhat","largely","mainly","mostly","really",
}

# Short words that are genuinely meaningful macro signals
SHORT_WHITELIST = {
    "war","fed","oil","gas","tax","gdp","cpi","ppi","ipo","sec","fda",
    "wto","imf","ecb","boe","pboc","rba","rbi","fed","usd","eur","jpy",
    "cny","gbp","spx","vix","spac","etf","m&a","esg","ebit","eps","roe",
}


def is_meaningful(tok: str, min_len: int) -> bool:
    t = tok.strip().lower()
    if t in SHORT_WHITELIST:
        return True
    if len(t) < min_len:
        return False
    if t in STOP_WORDS:
        return False
    if t.replace('.','').replace('-','').replace(',','').isdigit():
        return False
    if len(set(t)) == 1:
        return False
    if not any(c.isalpha() for c in t):
        return False
    # Block n-grams (spaces)
    if ' ' in t:
        return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
# CSV / JSONL readers
# ─────────────────────────────────────────────────────────────────────────────

def read_tokens_csv(path: Path, min_len: int) -> dict:
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return {}
    df.columns = df.columns.str.strip()
    lower = {col.lower(): col for col in df.columns}
    if {"entity","count"}.issubset(lower):
        col_e, col_n = lower["entity"], lower["count"]
    elif {"tok","n"}.issubset(lower):
        col_e, col_n = lower["tok"], lower["n"]
    elif {"term","n"}.issubset(lower):
        col_e, col_n = lower["term"], lower["n"]
    elif {"word","count"}.issubset(lower):
        col_e, col_n = lower["word"], lower["count"]
    else:
        print(f"[WARN] unknown columns in {path.name}: {list(df.columns)}")
        return {}
    df[col_e] = df[col_e].astype(str)
    df[col_n] = pd.to_numeric(df[col_n], errors="coerce").fillna(0).astype(int)
    result = {}
    for tok, n in zip(df[col_e], df[col_n]):
        if is_meaningful(tok, min_len):
            result[str(tok).lower()] = result.get(str(tok).lower(), 0) + int(n)
    return result


def read_tokens_jsonl(path: Path, min_len: int) -> dict:
    result = {}
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            tok = obj.get("tok") or obj.get("term") or obj.get("entity")
            n   = obj.get("n") if "n" in obj else obj.get("count")
            if tok is None or n is None:
                continue
            tok = str(tok).lower()
            if is_meaningful(tok, min_len):
                result[tok] = result.get(tok, 0) + int(n)
    return result


def get_date_from_filename(path: Path) -> str:
    for suffix in ("_tokens.csv", "_tokens.jsonl"):
        if path.name.endswith(suffix):
            return path.name[: -len(suffix)]
    return path.stem


# ─────────────────────────────────────────────────────────────────────────────
# Statistical helpers
# ─────────────────────────────────────────────────────────────────────────────

def calc_zscore(counts: list, window: int = 28) -> float:
    if len(counts) < 3:
        return 0.0
    hist = counts[max(0, len(counts) - window - 1): len(counts) - 1]
    if not hist:
        return 0.0
    mean = sum(hist) / len(hist)
    std  = math.sqrt(sum((x - mean) ** 2 for x in hist) / len(hist))
    if std < 0.5:
        return 0.0
    return round((counts[-1] - mean) / std, 3)


def calc_slope(counts: list, window: int = 14) -> float:
    w = counts[-window:] if len(counts) >= window else counts
    n = len(w)
    if n < 3:
        return 0.0
    xs = list(range(n))
    mx = sum(xs) / n
    my = sum(w) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, w))
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0:
        return 0.0
    return round((num / den) / (my + 1e-9), 4)


def calc_burst(counts: list, window: int = 7) -> float:
    if len(counts) < window + 2:
        return 0.0
    hist = counts[-(window + 1): -1]
    mean = sum(hist) / len(hist)
    std  = math.sqrt(sum((x - mean) ** 2 for x in hist) / len(hist))
    if std < 0.5:
        return 0.0
    return round((counts[-1] - mean) / std, 3)


def calc_consistency(counts: list) -> float:
    """CV 역수 기반 일관성 0~1. 고르게 분포할수록 높음, 단일 spike는 낮음."""
    nonzero = [c for c in counts if c > 0]
    if len(nonzero) < 3:
        return 0.0
    mean = sum(nonzero) / len(nonzero)
    if mean < 0.5:
        return 0.0
    std = math.sqrt(sum((x - mean) ** 2 for x in nonzero) / len(nonzero))
    cv  = std / mean
    return round(max(0.0, min(1.0, 1.0 - cv / 3.0)), 3)


def calc_source_proxy(counts: list) -> float:
    """n_active_days / total_days. 높을수록 여러 날에 걸쳐 지속 언급."""
    if not counts:
        return 0.0
    return round(sum(1 for c in counts if c > 0) / len(counts), 3)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--tokens-dir",    default="data/warehouse/daily")
    p.add_argument("--out",           default="site/data/trends.json")
    p.add_argument("--last-days",     type=int,   default=90)
    p.add_argument("--topk",          type=int,   default=300)
    p.add_argument("--min-len",       type=int,   default=4)
    p.add_argument("--zscore-window", type=int,   default=28)
    p.add_argument("--slope-window",  type=int,   default=14)
    args = p.parse_args()

    td    = Path(args.tokens_dir)
    files = sorted(td.glob("*_tokens.csv")) + sorted(td.glob("*_tokens.jsonl"))
    if not files:
        raise SystemExit("no *_tokens.csv or *_tokens.jsonl files found")

    dates = sorted({get_date_from_filename(f) for f in files})
    if args.last_days > 0 and len(dates) > args.last_days:
        dates = dates[-args.last_days:]

    by_date: dict[str, dict] = {}
    for d in dates:
        csv_p   = td / f"{d}_tokens.csv"
        jsonl_p = td / f"{d}_tokens.jsonl"
        if csv_p.exists():
            by_date[d] = read_tokens_csv(csv_p, args.min_len)
        elif jsonl_p.exists():
            by_date[d] = read_tokens_jsonl(jsonl_p, args.min_len)
        else:
            by_date[d] = {}

    # Frequency totals
    totals: dict[str, int] = {}
    for d in dates:
        for tok, n in by_date[d].items():
            totals[tok] = totals.get(tok, 0) + n

    top_tokens = [
        t for t, _ in sorted(totals.items(), key=lambda x: x[1], reverse=True)[: args.topk]
    ]

    # Time series
    series = {
        t: [int(by_date.get(d, {}).get(t, 0)) for d in dates]
        for t in top_tokens
    }

    # Per-term stats
    zscores:       dict[str, float] = {}
    slopes:        dict[str, float] = {}
    bursts:        dict[str, float] = {}
    today_counts:  dict[str, int]   = {}
    avg7:          dict[str, float] = {}
    consistency:   dict[str, float] = {}
    source_proxy:  dict[str, float] = {}
    n_days_active: dict[str, int]   = {}

    for t in top_tokens:
        counts = series[t]
        zscores[t]       = calc_zscore(counts, args.zscore_window)
        slopes[t]        = calc_slope(counts, args.slope_window)
        bursts[t]        = calc_burst(counts)
        today_counts[t]  = counts[-1] if counts else 0
        last7            = counts[-7:] if len(counts) >= 7 else counts
        avg7[t]          = round(sum(last7) / len(last7), 1) if last7 else 0.0
        consistency[t]   = calc_consistency(counts)
        source_proxy[t]  = calc_source_proxy(counts)
        n_days_active[t] = sum(1 for c in counts if c > 0)

    top_by_z = sorted(top_tokens, key=lambda t: zscores[t], reverse=True)

    out = {
        "dates":         dates,
        "terms":         top_tokens,
        "top":           top_by_z,
        "series":        series,
        "zscores":       zscores,
        "slopes":        slopes,
        "bursts":        bursts,
        "today":         today_counts,
        "avg7":          avg7,
        "consistency":   consistency,
        "source_proxy":  source_proxy,
        "n_days_active": n_days_active,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")

    hot    = sum(1 for z in zscores.values() if z >= 2.0)
    noisy  = sum(1 for t in top_tokens if consistency.get(t, 1) < 0.2)
    sparse = sum(1 for t in top_tokens if source_proxy.get(t, 1) < 0.1)

    print(f"wrote {out_path}")
    print(f"  dates        : {len(dates)}")
    print(f"  terms        : {len(top_tokens)}")
    print(f"  hot (z≥2)   : {hot}")
    print(f"  noisy (<0.2 consistency): {noisy}")
    print(f"  sparse (<10% days active): {sparse}")
    print()
    print("  Top 10 by z-score:")
    for t in top_by_z[:10]:
        print(f"    {t:<20} z={zscores[t]:+.2f}  "
              f"consistency={consistency.get(t,0):.2f}  "
              f"active={n_days_active.get(t,0)}d/{len(dates)}d")


if __name__ == "__main__":
    main()