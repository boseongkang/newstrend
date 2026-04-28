"""
macro_themes.py — 매크로 뉴스 테마 분석
========================================
trends.json의 단어들을 카테고리별로 그룹핑하여 일별/주별 트렌드 산출.

카테고리:
  - GEOPOLITICS: 전쟁, 외교, 제재 (war, sanctions, iran, russia, ukraine, china, korea)
  - POLITICS: 정치 (trump, biden, congress, election, vote, party)
  - MACRO: 경제 정책 (fed, rate, inflation, gdp, cpi, unemployment, recession)
  - TRADE: 무역 (tariff, trade, deal, import, export, supply)
  - TECH: 기술 (ai, chip, semiconductor, software, cloud, cyber)
  - ENERGY: 에너지 (oil, gas, opec, energy, solar, nuclear)
  - FINANCE: 금융 (bank, debt, bond, yield, dollar, currency)
  - CRYPTO: 암호화폐 (bitcoin, crypto, ethereum, blockchain)
  - HEALTH: 보건 (pandemic, virus, vaccine, drug, health)
  - DISASTER: 재난 (earthquake, flood, hurricane, fire, climate)

출력: site/data/macro_themes.json
{
  "updated": "...",
  "categories": {
    "GEOPOLITICS": {
      "today_score":    3.2,        # 오늘 Z-score 합
      "week_avg":       2.8,
      "trend":          "rising",   # rising/falling/stable
      "active_words":   [{"word":"iran", "z":2.1, "count":45}, ...],
      "daily":          [{"date":"...", "score":1.5, "top_word":"war"}, ...],
    },
    ...
  },
  "top_themes_today":   ["GEOPOLITICS", "POLITICS"],
  "rising_themes":      ["TRADE"],   # 일주일 새 급상승
}
"""

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path


# ── 카테고리 정의 ────────────────────────────────────────────────────────────

CATEGORIES = {
    "GEOPOLITICS": [
        # 전쟁/군사
        "war", "wars", "warfare", "military", "army", "navy", "marines",
        "attack", "attacks", "missile", "missiles", "weapon", "weapons",
        "drone", "drones", "ammunition", "tank", "tanks", "soldier", "soldiers",
        "troops", "deployment", "invasion", "strike", "strikes", "bomb", "bombing",
        "rocket", "rockets", "airstrike",
        # 국가/지역
        "iran", "iranian", "russia", "russian", "moscow", "kremlin",
        "ukraine", "ukrainian", "kyiv", "china", "chinese", "beijing",
        "korea", "korean", "pyongyang", "seoul", "north", "south",
        "israel", "israeli", "tel", "aviv", "jerusalem", "gaza", "palestinian",
        "syria", "syrian", "damascus", "iraq", "iraqi", "baghdad",
        "afghanistan", "kabul", "yemen", "yemeni", "lebanon", "hezbollah",
        "taiwan", "taiwanese", "venezuela", "cuba", "saudi", "arabia",
        "turkey", "turkish", "egypt", "qatar", "uae",
        # 인물
        "putin", "zelensky", "netanyahu", "khamenei", "kim", "jong",
        "erdogan", "macron", "scholz",
        # 외교/제재
        "sanctions", "sanction", "embargo", "diplomatic", "diplomacy",
        "embassy", "consulate", "ambassador", "hostage", "captive",
        "ceasefire", "armistice", "treaty", "alliance", "nato", "pact",
        "border", "borders", "refugee", "refugees", "asylum", "exile",
        "summit", "meeting", "negotiation", "negotiations", "talks",
        "intelligence", "espionage", "spy", "spies",
    ],
    "POLITICS": [
        # 미국 정치인
        "trump", "donald", "melania", "ivanka", "vance",
        "biden", "joe", "harris", "kamala", "obama", "barack", "michelle",
        "clinton", "hillary", "pence", "desantis", "ramaswamy",
        "newsom", "abbott", "whitmer",
        # 정부/기관
        "congress", "senate", "house", "speaker", "majority", "minority",
        "republican", "democrat", "democratic", "republicans", "democrats",
        "gop", "dnc", "rnc", "filibuster", "caucus",
        # 선거
        "election", "elections", "vote", "votes", "voted", "voter", "voters",
        "voting", "ballot", "ballots", "campaign", "campaigning", "primary",
        "primaries", "presidential", "midterm", "midterms",
        "polls", "polling", "candidate", "candidates", "incumbent",
        # 입법/사법
        "law", "laws", "bill", "bills", "legislation", "legislative",
        "act", "acts", "amendment", "amendments", "veto", "executive",
        "order", "orders", "ruling", "rulings", "verdict", "verdicts",
        "supreme", "court", "justice", "justices", "scotus",
        # 스캔들/조사
        "impeachment", "impeach", "investigation", "investigations", "probe",
        "indictment", "indicted", "subpoena", "subpoenaed", "testimony",
        "hearing", "hearings", "scandal", "controversy", "leaked",
        "whistleblower", "perjury",
        # 정책
        "policy", "policies", "regulation", "regulations", "regulatory",
        "deregulation", "reform", "reforms", "mandate", "mandates",
    ],
    "MACRO": [
        # 중앙은행
        "fed", "federal", "reserve", "fomc", "powell", "yellen",
        "ecb", "boe", "boj", "pboc", "lagarde", "ueda",
        # 금리
        "rate", "rates", "hike", "hikes", "cut", "cuts", "raise",
        "tightening", "easing", "dovish", "hawkish", "neutral",
        "yield", "yields", "spread", "curve", "inverted",
        # 인플레이션
        "inflation", "deflation", "disinflation", "stagflation",
        "cpi", "ppi", "pce", "core",
        # 경제 지표
        "gdp", "gnp", "pmi", "ism", "ifo", "consumer", "confidence",
        "sentiment", "retail", "production", "manufacturing", "services",
        "unemployment", "employment", "jobs", "payroll", "payrolls",
        "nonfarm", "claims", "jobless", "hiring", "wages", "earnings",
        "labor", "workforce",
        # 경기
        "recession", "depression", "expansion", "contraction", "growth",
        "slowdown", "rebound", "recovery", "boom", "bust", "downturn",
        # 국채/부채
        "treasury", "treasuries", "bond", "bonds", "deficit", "surplus",
        "debt", "borrowing", "issuance", "auction", "stimulus", "relief",
    ],
    "TRADE": [
        # 관세/무역
        "tariff", "tariffs", "duty", "duties", "trade", "trading",
        "deal", "deals", "agreement", "agreements", "pact", "treaty",
        "import", "imports", "importing", "export", "exports", "exporting",
        "shipment", "shipments", "shipping", "container", "containers",
        # 공급망
        "supply", "supplies", "chain", "chains", "logistics", "warehouse",
        "manufacturing", "manufacturer", "manufacturers", "factory",
        "factories", "production", "assembly", "components", "parts",
        "materials", "raw", "inputs",
        # 기관/협정
        "wto", "imf", "worldbank", "nafta", "usmca", "tpp", "rcep",
        "negotiation", "negotiations", "deal", "deals",
        # 통상 분쟁
        "dispute", "retaliation", "retaliatory", "subsidy", "subsidies",
        "dumping", "antidumping", "quota", "quotas", "ban", "banned",
        "restriction", "restrictions", "blacklist",
    ],
    "TECH": [
        # AI/ML
        "ai", "artificial", "intelligence", "machine", "learning",
        "model", "models", "llm", "gpt", "chatbot", "chatgpt",
        "openai", "anthropic", "claude", "gemini", "copilot", "generative",
        "neural", "algorithm", "algorithms", "training", "inference",
        # 반도체
        "chip", "chips", "semiconductor", "semiconductors", "fab", "foundry",
        "wafer", "wafers", "node", "lithography", "tsmc", "asml", "samsung",
        # 기업
        "google", "alphabet", "microsoft", "apple", "nvidia", "meta",
        "tesla", "amazon", "netflix", "uber", "airbnb", "openai", "anthropic",
        "spacex", "tiktok", "bytedance", "tencent", "alibaba",
        # 인물
        "musk", "zuckerberg", "bezos", "pichai", "nadella", "altman",
        "huang", "cook", "tim",
        # 기술 분야
        "software", "hardware", "cloud", "computing", "saas", "iaas",
        "cybersecurity", "cyber", "hack", "hacked", "hacking", "breach",
        "ransomware", "phishing", "malware",
        "data", "database", "analytics", "platform", "api", "open", "source",
        "blockchain", "metaverse", "vr", "ar", "robotics", "robot", "automation",
        "5g", "6g", "broadband", "satellite", "starlink",
        # 칩 관련 단어
        "gpu", "cpu", "tpu", "accelerator", "transistor", "memory",
        "ddr", "hbm", "nand", "dram",
    ],
    "ENERGY": [
        # 화석 연료
        "oil", "crude", "petroleum", "barrel", "barrels", "wti", "brent",
        "gas", "gasoline", "diesel", "lng", "natgas", "natural",
        "coal", "fossil", "fuel", "fuels", "refinery", "refining",
        "drilling", "rig", "rigs", "shale", "fracking", "pipeline", "pipelines",
        # OPEC/생산국
        "opec", "saudi", "venezuela", "iran", "iraq", "russia", "uae",
        "kuwait", "qatar", "production", "output", "quota", "quotas",
        # 재생/원전
        "solar", "wind", "hydro", "geothermal", "renewable", "renewables",
        "clean", "green", "carbon", "emission", "emissions", "co2",
        "nuclear", "uranium", "reactor", "reactors", "plant", "plants",
        "battery", "batteries", "lithium", "cobalt", "ev", "electric",
        # 기업
        "exxon", "exxonmobil", "chevron", "bp", "shell", "totalenergies",
        "aramco", "occidental", "conocophillips",
        # 인프라
        "grid", "power", "electricity", "utility", "utilities", "transmission",
        "storage", "capacity", "generation",
    ],
    "FINANCE": [
        # 은행/금융기관
        "bank", "banks", "banking", "lender", "lenders", "lending",
        "jpmorgan", "chase", "citigroup", "citi", "wells", "fargo",
        "goldman", "sachs", "morgan", "stanley", "blackrock", "vanguard",
        "bofa", "bank of america", "ubs", "credit", "suisse", "deutsche",
        # 신용/대출
        "loan", "loans", "mortgage", "mortgages", "credit", "creditcard",
        "default", "defaults", "delinquency", "foreclosure",
        # 채권/통화
        "bond", "bonds", "yield", "yields", "treasury", "treasuries",
        "junk", "investment", "grade", "high", "yield",
        "dollar", "dollars", "euro", "yen", "yuan", "pound", "currency",
        "currencies", "fx", "forex", "exchange",
        # 투자/거래
        "investor", "investors", "investment", "investments",
        "ipo", "listing", "spac", "merger", "mergers", "acquisition",
        "acquisitions", "buyback", "buybacks", "dividend", "dividends",
        "earnings", "revenue", "revenues", "profit", "profits", "loss", "losses",
        "guidance", "outlook", "forecast",
        # 증시
        "stock", "stocks", "market", "markets", "index", "indices",
        "rally", "selloff", "correction", "bubble", "crash", "volatility",
        "vix", "options", "futures", "derivative", "hedge",
        "sec", "regulator", "regulators", "compliance",
    ],
    "CRYPTO": [
        "bitcoin", "btc", "satoshi", "halving", "crypto", "cryptocurrency",
        "cryptocurrencies", "ethereum", "eth", "vitalik", "buterin",
        "blockchain", "ledger", "wallet", "wallets", "exchange", "exchanges",
        "binance", "coinbase", "kraken", "bitfinex", "huobi", "okx",
        "stablecoin", "stablecoins", "tether", "usdt", "usdc", "dai",
        "defi", "decentralized", "dex", "yield", "farming", "staking",
        "nft", "nfts", "token", "tokens", "tokenization", "ico",
        "mining", "miner", "miners", "hashrate", "proof", "stake", "work",
        "altcoin", "altcoins", "solana", "cardano", "polkadot", "ripple",
        "xrp", "doge", "dogecoin", "shiba", "memecoin",
        "regulation", "etf", "spot", "futures", "halving",
        "bull", "bear", "moon", "pump", "dump", "fud", "hodl",
    ],
    "HEALTH": [
        # 감염병
        "pandemic", "epidemic", "outbreak", "virus", "viral", "infection",
        "covid", "coronavirus", "sars", "flu", "influenza", "rsv",
        "monkeypox", "ebola", "measles", "polio", "tuberculosis",
        # 백신/치료
        "vaccine", "vaccines", "vaccination", "booster", "shot", "shots",
        "jab", "jabs", "immunity", "antibody", "antibodies",
        "drug", "drugs", "pharmaceutical", "pharmaceuticals", "pharma",
        "treatment", "therapy", "therapies", "trial", "trials", "clinical",
        # 기관/규제
        "fda", "cdc", "who", "nih", "ema", "mhra",
        "approval", "approved", "rejected", "recall", "warning",
        # 의료
        "hospital", "hospitals", "clinic", "clinics", "doctor", "doctors",
        "physician", "nurse", "patient", "patients", "icu", "emergency",
        "surgery", "surgeon", "diagnosis", "diagnostic",
        "disease", "diseases", "cancer", "diabetes", "obesity", "alzheimer",
        "mental", "depression", "anxiety", "addiction", "opioid",
        "insurance", "medicare", "medicaid", "insulin", "obamacare",
    ],
    "DISASTER": [
        # 자연재해
        "earthquake", "earthquakes", "quake", "tremor", "aftershock",
        "tsunami", "tsunamis", "flood", "floods", "flooding",
        "hurricane", "hurricanes", "typhoon", "cyclone", "storm", "storms",
        "tornado", "tornados", "tornadoes",
        "fire", "fires", "wildfire", "wildfires", "blaze",
        "drought", "droughts", "famine",
        "volcano", "eruption", "lava", "ash",
        "landslide", "avalanche", "mudslide",
        # 기후
        "climate", "warming", "global", "heat", "heatwave", "cold",
        "freeze", "blizzard", "snowstorm",
        "carbon", "co2", "emission", "emissions", "greenhouse",
        "ipcc", "cop", "kyoto", "paris", "agreement",
        # 인공/기술 재난
        "explosion", "explosions", "blast", "spill", "spills", "leak",
        "contamination", "radiation", "nuclear", "chemical",
        "crash", "crashes", "collision", "derailment",
        "evacuation", "evacuated", "rescue", "casualty", "casualties",
        "damage", "destruction", "victim", "victims",
        "fema", "disaster", "emergency", "alert",
    ],
}

def zscore_at(counts, i, window=28):
    if i < 3 or i >= len(counts):
        return 0
    hist = counts[max(0, i - window): i]
    if not hist:
        return 0
    mean = sum(hist) / len(hist)
    std  = math.sqrt(sum((x - mean) ** 2 for x in hist) / len(hist))
    return (counts[i] - mean) / std if std >= 0.5 else 0


def analyze_category(category: str, words: list, trends: dict) -> dict:
    """카테고리의 일별 활성도 시계열 산출."""
    dates = trends["dates"]
    series = trends.get("series", {})

    # 각 날짜별 카테고리 점수 (단어 Z-score 합)
    daily_scores = []
    for i, d in enumerate(dates):
        day_score = 0
        word_zs = []
        for w in words:
            if w not in series:
                continue
            z = zscore_at(series[w], i)
            if z >= 0.5:  # noise 필터
                day_score += z
                word_zs.append((w, z, series[w][i] if i < len(series[w]) else 0))

        # 상위 단어
        word_zs.sort(key=lambda x: -x[1])
        top_word = word_zs[0][0] if word_zs else None

        daily_scores.append({
            "date":      d,
            "score":     round(day_score, 2),
            "top_word":  top_word,
            "n_active":  len(word_zs),
        })

    # 오늘 점수
    today = daily_scores[-1] if daily_scores else {"score": 0, "top_word": None}

    # 주간 평균 (지난 7일)
    week_scores = [d["score"] for d in daily_scores[-7:]]
    week_avg = sum(week_scores) / len(week_scores) if week_scores else 0

    # 트렌드 (최근 3일 vs 이전 4일)
    if len(daily_scores) >= 7:
        recent = sum(d["score"] for d in daily_scores[-3:]) / 3
        older  = sum(d["score"] for d in daily_scores[-7:-3]) / 4
        if recent > older * 1.3:
            trend = "rising"
        elif recent < older * 0.7:
            trend = "falling"
        else:
            trend = "stable"
    else:
        trend = "stable"

    # 오늘의 활성 단어들
    active_words = []
    last_idx = len(dates) - 1
    for w in words:
        if w not in series:
            continue
        z = zscore_at(series[w], last_idx)
        if z >= 1.0:
            cnt = series[w][last_idx] if last_idx < len(series[w]) else 0
            active_words.append({"word": w, "z": round(z, 2), "count": cnt})
    active_words.sort(key=lambda x: -x["z"])

    return {
        "today_score":  round(today["score"], 2),
        "today_top":    today["top_word"],
        "week_avg":     round(week_avg, 2),
        "trend":        trend,
        "active_words": active_words[:8],
        "daily":        daily_scores[-30:],  # 최근 30일만
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trends", default="site/data/trends.json")
    ap.add_argument("--out",    default="site/data/macro_themes.json")
    args = ap.parse_args()

    trends = json.loads(Path(args.trends).read_text())
    print(f"Loaded {len(trends['dates'])} days, {len(trends.get('series', {}))} terms")

    categories_data = {}
    for cat, words in CATEGORIES.items():
        result = analyze_category(cat, words, trends)
        categories_data[cat] = result

    # 오늘 가장 핫한 테마 (today_score 기준)
    sorted_today = sorted(categories_data.items(),
                          key=lambda x: -x[1]["today_score"])
    top_today = [{
        "category": c,
        "score":    d["today_score"],
        "top_word": d["today_top"],
    } for c, d in sorted_today[:5]]

    # 급상승 테마 (rising)
    rising = [c for c, d in categories_data.items() if d["trend"] == "rising"]

    # 약세 테마 (falling)
    falling = [c for c, d in categories_data.items() if d["trend"] == "falling"]

    output = {
        "updated":           datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "categories":        categories_data,
        "top_themes_today":  top_today,
        "rising_themes":     rising,
        "falling_themes":    falling,
    }

    Path(args.out).write_text(json.dumps(output, ensure_ascii=False, separators=(",",":")))

    # 콘솔 출력
    print(f"\n→ {args.out}")
    print(f"\n📊 Today's Top Themes:")
    for t in top_today:
        emoji = {"GEOPOLITICS":"🌍", "POLITICS":"🏛️", "MACRO":"📈", "TRADE":"📦",
                 "TECH":"💻", "ENERGY":"⚡", "FINANCE":"💰", "CRYPTO":"₿",
                 "HEALTH":"🏥", "DISASTER":"🌪️"}.get(t["category"], "")
        print(f"  {emoji} {t['category']:<13}  score {t['score']:.1f}  → {t['top_word']}")

    if rising:
        print(f"\n🔥 Rising themes (last 3 days): {', '.join(rising)}")
    if falling:
        print(f"❄️  Falling themes:               {', '.join(falling)}")


if __name__ == "__main__":
    main()