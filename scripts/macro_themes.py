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
        "war", "wars", "military", "attack", "missile", "weapon", "weapons",
        "iran", "russia", "ukraine", "china", "chinese", "korea", "korean",
        "israel", "gaza", "syria", "iraq", "afghanistan", "putin", "xi",
        "sanctions", "sanction", "diplomatic", "embassy", "hostage",
        "ceasefire", "treaty", "border",
    ],
    "POLITICS": [
        "trump", "donald", "biden", "harris", "obama", "kamala",
        "congress", "senate", "house", "speaker", "republican", "democrat",
        "democratic", "republicans", "democrats", "election", "vote", "votes",
        "voter", "voters", "campaign", "primary", "presidential",
        "impeachment", "investigation", "indictment", "subpoena",
    ],
    "MACRO": [
        "fed", "federal", "powell", "rate", "rates", "inflation", "deflation",
        "gdp", "cpi", "ppi", "pce", "unemployment", "jobs", "payroll", "payrolls",
        "recession", "growth", "stagflation", "yield", "yields", "treasury",
        "treasuries", "balance", "stimulus", "deficit", "debt",
    ],
    "TRADE": [
        "tariff", "tariffs", "trade", "deal", "deals", "import", "imports",
        "export", "exports", "supply", "chain", "manufacturing", "factory",
        "factories", "wto", "agreement", "negotiation", "negotiations",
    ],
    "TECH": [
        "ai", "artificial", "intelligence", "chip", "chips", "semiconductor",
        "semiconductors", "software", "cloud", "cyber", "hack", "hacked",
        "data", "algorithm", "model", "models", "openai", "google", "microsoft",
        "apple", "nvidia", "meta", "tesla", "amazon", "tech", "startup",
    ],
    "ENERGY": [
        "oil", "gas", "opec", "energy", "solar", "nuclear", "power",
        "electricity", "grid", "renewable", "battery", "lithium", "barrel",
        "barrels", "drilling", "pipeline", "exxon", "chevron", "saudi",
    ],
    "FINANCE": [
        "bank", "banks", "banking", "financial", "credit", "loan", "loans",
        "mortgage", "bond", "bonds", "dollar", "dollars", "currency", "fx",
        "forex", "investor", "investors", "investment", "ipo", "merger",
        "acquisition", "buyback", "dividend", "earnings", "revenue", "profit",
    ],
    "CRYPTO": [
        "bitcoin", "btc", "crypto", "cryptocurrency", "ethereum", "eth",
        "blockchain", "wallet", "exchange", "binance", "coinbase", "stablecoin",
        "defi", "nft", "mining",
    ],
    "HEALTH": [
        "pandemic", "virus", "covid", "vaccine", "vaccines", "health",
        "drug", "drugs", "fda", "medical", "hospital", "patient", "patients",
        "outbreak", "disease", "treatment",
    ],
    "DISASTER": [
        "earthquake", "flood", "floods", "hurricane", "tornado", "fire",
        "fires", "wildfire", "climate", "warming", "drought", "tsunami",
        "disaster", "evacuation", "damage",
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