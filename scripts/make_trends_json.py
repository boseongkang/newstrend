import argparse
import json
import math
from pathlib import Path

import pandas as pd

# ── Comprehensive English stop-words (general + financial news boilerplate) ──
STOP_WORDS = {
    # Common English
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
    # Temporal / generic
    "monday","tuesday","wednesday","thursday","friday","saturday","sunday",
    "january","february","march","april","june","july","august","september",
    "october","november","december","jan","feb","mar","apr","jun","jul","aug",
    "sep","oct","nov","dec","week","month","year","today","yesterday","tomorrow",
    "time","day","hour","minute","night","morning","evening","afternoon",
    # News boilerplate
    "news","reuters","bloomberg","update","breaking","report","reports","reported",
    "statement","according","sources","source","told","said","says","announced",
    "announced","announcement","release","press","media","outlet","article",
    "story","coverage","interview","spokesperson","official","officials",
    "percent","pct","billion","million","thousand","share","shares",
    "quarter","annual","fiscal","record","high","low","rise","fall","gain","loss",
    "data","information","analysis","result","results","number","numbers",
    "market","markets","stock","stocks","price","prices","rate","rates","index",
    "etf","fund","funds","asset","assets","portfolio","investor","investors",
    "investment","investments","trading","trade","trader","traders",
    "company","companies","firm","firms","group","corp","inc","ltd","plc","llc",
    "business","businesses","industry","sector","segment","division",
    "government","federal","national","international","global","local","public",
    "president","chairman","chief","executive","officer","director","manager",
    "ceo","cfo","coo","vp","svp","evp","head","lead","senior","vice",
    "first","second","third","last","next","new","old","big","large","small",
    "high","low","good","bad","best","worst","great","major","key","main",
    "top","leading","strong","weak","positive","negative","current","former",
    "right","left","long","short","early","late","recent","latest","previous",
    "nbsp","mdash","ndash","amp","quot","apos","copy","reg","trade",
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
    "very","much","little","few","several","many","enough","enough","every","any",
}

def is_meaningful(tok: str, min_len: int) -> bool:
    t = tok.strip().lower()
    if len(t) < min_len:
        return False
    if t in STOP_WORDS:
        return False
    # Pure numbers / punctuation
    if t.replace('.','').replace('-','').replace(',','').isdigit():
        return False
    # Single repeating char
    if len(set(t)) == 1:
        return False
    # URL fragments
    if '/' in t or '\\' in t or t.startswith('http'):
        return False
    return True

def read_tokens_csv(path: Path, min_len: int):
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")  # BOM 자동 처리
    except Exception:
        return {}
    df.columns = df.columns.str.strip()
    lower = {col.lower(): col for col in df.columns}
    if {"entity", "count"}.issubset(lower):
        col_e, col_n = lower["entity"], lower["count"]
    elif {"tok", "n"}.issubset(lower):
        col_e, col_n = lower["tok"], lower["n"]
    elif {"term", "n"}.issubset(lower):
        col_e, col_n = lower["term"], lower["n"]
    elif {"word", "count"}.issubset(lower):
        col_e, col_n = lower["word"], lower["count"]
    else:
        print(f"[WARN] unknown columns in {path.name}: {list(df.columns)}")
        return {}
    df[col_e] = df[col_e].astype(str)
    df[col_n] = pd.to_numeric(df[col_n], errors="coerce").fillna(0).astype(int)
    df = df[df[col_e].str.len() >= min_len]
    # df = df[df[col_e].apply(lambda x: is_meaningful(x, min_len))]
    return dict(zip(df[col_e], df[col_n]))

def read_tokens_jsonl(path: Path, min_len: int):
    rows = []
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
            n = obj.get("n") if "n" in obj else obj.get("count")
            if tok is None or n is None:
                continue
            rows.append((str(tok), int(n)))
    if not rows:
        return {}
    result = {}
    for tok, n in rows:
        if len(tok) >= min_len:
            result[tok] = result.get(tok, 0) + n
    return result

def get_date_from_filename(path: Path) -> str:
    name = path.name
    for suffix in ("_tokens.csv", "_tokens.jsonl"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return path.stem


# ── Statistical helpers ────────────────────────────────────────────────────────

def calc_zscore(counts: list, window: int = 28) -> float:
    """Rolling Z-score: today vs past `window` days."""
    if len(counts) < 3:
        return 0.0
    hist = counts[max(0, len(counts) - window - 1): len(counts) - 1]
    if not hist:
        return 0.0
    n = len(hist)
    mean = sum(hist) / n
    variance = sum((x - mean) ** 2 for x in hist) / n
    std = math.sqrt(variance)
    if std < 0.5:
        return 0.0
    return round((counts[-1] - mean) / std, 3)


def calc_slope(counts: list, window: int = 14) -> float:
    """Normalized linear slope over last `window` days."""
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
    slope = num / den
    return round(slope / (my + 1e-9), 4)


def calc_burst(counts: list, window: int = 7) -> float:
    """7-day rolling Z-score for burst detection."""
    if len(counts) < window + 2:
        return 0.0
    hist = counts[-(window + 1): -1]
    mean = sum(hist) / len(hist)
    std = math.sqrt(sum((x - mean) ** 2 for x in hist) / len(hist))
    if std < 0.5:
        return 0.0
    return round((counts[-1] - mean) / std, 3)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--tokens-dir", default="data/warehouse/daily")
    p.add_argument("--out", default="site/data/trends.json")
    p.add_argument("--last-days", type=int, default=90)
    p.add_argument("--topk", type=int, default=300)
    p.add_argument("--min-len", type=int, default=4)
    p.add_argument("--zscore-window", type=int, default=28)
    p.add_argument("--slope-window", type=int, default=14)
    args = p.parse_args()

    td = Path(args.tokens_dir)
    files = sorted(td.glob("*_tokens.csv")) + sorted(td.glob("*_tokens.jsonl"))
    if not files:
        raise SystemExit("no *_tokens.csv or *_tokens.jsonl files found")

    dates = sorted({get_date_from_filename(f) for f in files})
    if args.last_days > 0 and len(dates) > args.last_days:
        dates = dates[-args.last_days:]

    by_date = {}
    for d in dates:
        csv_path = td / f"{d}_tokens.csv"
        jsonl_path = td / f"{d}_tokens.jsonl"
        if csv_path.exists():
            by_date[d] = read_tokens_csv(csv_path, args.min_len)
        elif jsonl_path.exists():
            by_date[d] = read_tokens_jsonl(jsonl_path, args.min_len)
        else:
            by_date[d] = {}

    # Total frequency across all dates (for topk selection)
    totals: dict[str, int] = {}
    for d in dates:
        for tok, n in by_date.get(d, {}).items():
            totals[tok] = totals.get(tok, 0) + int(n)

    top_tokens = [
        t for t, _ in sorted(totals.items(), key=lambda x: x[1], reverse=True)[: args.topk]
    ]

    # Build time series per term
    series = {
        t: [int(by_date.get(d, {}).get(t, 0)) for d in dates]
        for t in top_tokens
    }

    # Pre-compute Z-scores and slopes for today
    zscores: dict[str, float] = {}
    slopes: dict[str, float] = {}
    bursts: dict[str, float] = {}
    today_counts: dict[str, int] = {}
    avg7: dict[str, float] = {}

    for t in top_tokens:
        counts = series[t]
        zscores[t] = calc_zscore(counts, args.zscore_window)
        slopes[t] = calc_slope(counts, args.slope_window)
        bursts[t] = calc_burst(counts)
        today_counts[t] = counts[-1] if counts else 0
        last7 = counts[-7:] if len(counts) >= 7 else counts
        avg7[t] = round(sum(last7) / len(last7), 1) if last7 else 0.0

    # Sort top list by today's Z-score for the dashboard default view
    top_by_z = sorted(top_tokens, key=lambda t: zscores[t], reverse=True)

    out = {
        "dates": dates,
        "terms": top_tokens,          # original freq-sorted (for compat)
        "top": top_by_z,              # z-score sorted (new default)
        "series": series,
        "zscores": zscores,           # NEW: pre-computed z-scores
        "slopes": slopes,             # NEW: momentum slopes
        "bursts": bursts,             # NEW: 7-day burst z-scores
        "today": today_counts,        # NEW: today's raw counts
        "avg7": avg7,                 # NEW: 7-day averages
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")

    # Summary
    hot = sum(1 for z in zscores.values() if z >= 2.0)
    print(f"wrote {out_path}")
    print(f"  dates: {len(dates)}  terms: {len(top_tokens)}  hot signals (z≥2): {hot}")


if __name__ == "__main__":
    main()