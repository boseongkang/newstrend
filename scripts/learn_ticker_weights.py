"""
learn_ticker_weights.py — 종목별 신호 소스 가중치 학습
=========================================================
과거 데이터에서 각 종목이 어떤 신호 소스에 더 민감한지 학습하여
predict.py가 종목별로 다른 가중치를 사용하도록 함.

각 종목에 대해:
  1. 과거 N일 동안 신호 소스별 예측 적중률 측정
     - TA only (RSI, BB, MACD)
     - 뉴스 단어 신호
     - 시장 regime
  2. 각 소스의 hit rate를 가중치로 변환
  3. 종목별 가중치 JSON 저장

출력: site/data/ticker_weights.json
{
  "AAPL": {
    "ta_weight":     0.40,    # TA 적중률 기반
    "news_weight":   0.50,    # 뉴스 적중률 (높음 = 정치 민감)
    "regime_weight": 0.10,
    "ta_hit_rate":   0.58,
    "news_hit_rate": 0.65,
    "n_samples":     85
  },
  "NVDA": {
    "ta_weight":     0.70,
    "news_weight":   0.20,    # 뉴스 영향 작음
    "regime_weight": 0.10,
    ...
  }
}
"""

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path


def compute_ta_signal(ta_data: dict, ticker: str, date: str) -> str:
    """RSI + BB%를 기반으로 TA 신호 생성. BUY/SELL/HOLD."""
    t = ta_data.get("tickers", {}).get(ticker, {})
    if not t:
        return None

    # 키 이름이 'date' 또는 'dates'일 수 있음
    dates = t.get("date") or t.get("dates", [])
    if not dates or date not in dates:
        return None

    i = dates.index(date)

    # RSI 키도 다양 (rsi14, rsi_14, rsi)
    rsi_arr = t.get("rsi14") or t.get("rsi_14") or t.get("rsi", [])
    if i >= len(rsi_arr) or rsi_arr[i] is None:
        return None

    r = rsi_arr[i]

    # BB% 추가 (보너스)
    bb_pct = t.get("bb_pct", [])
    bb = bb_pct[i] if i < len(bb_pct) and bb_pct[i] is not None else 0.5

    # 종합 판단
    if r < 30 and bb < 0.2:    return "BUY"
    if r < 35:                 return "WATCH"
    if r > 70 and bb > 0.8:    return "SELL"
    if r > 65:                 return "REDUCE"
    return "HOLD"


def compute_news_signal(ticker_analysis: dict, trends: dict, date: str) -> str:
    """ticker_analysis의 검증된 단어들을 사용한 뉴스 신호."""
    bull_words = ticker_analysis.get("analysis", {}).get("bullish_words", [])
    bear_words = ticker_analysis.get("analysis", {}).get("bearish_words", [])

    if not bull_words and not bear_words:
        return None

    t_dates  = trends.get("dates", [])
    t_series = trends.get("series", {})

    if date not in t_dates:
        return None
    di = t_dates.index(date)

    def zscore_at(counts, i, win=28):
        if i < 3: return 0
        hist = counts[max(0,i-win):i]
        if not hist: return 0
        m = sum(hist)/len(hist)
        s = math.sqrt(sum((x-m)**2 for x in hist)/len(hist))
        return (counts[i]-m)/s if s >= 0.5 else 0

    bull_score = 0
    bear_score = 0

    for w in bull_words[:10]:
        word = w["word"]
        lag  = abs(w.get("lead_days", 0))
        if word in t_series and di - lag >= 0:
            z = zscore_at(t_series[word], di - lag)
            if z >= 1.0:
                bull_score += w["hit_rate"] * abs(w.get("avg_ret_1d", 0.5))

    for w in bear_words[:10]:
        word = w["word"]
        lag  = abs(w.get("lead_days", 0))
        if word in t_series and di - lag >= 0:
            z = zscore_at(t_series[word], di - lag)
            if z >= 1.0:
                bear_score += (1 - w["hit_rate"]) * abs(w.get("avg_ret_1d", 0.5))

    net = bull_score - bear_score
    if net >  2: return "BUY"
    if net >  1: return "WATCH"
    if net < -2: return "SELL"
    if net < -1: return "REDUCE"
    return "HOLD"


def evaluate_signal(signal: str, ret_pct: float) -> bool:
    """신호와 실제 수익률 적중 여부."""
    if signal is None:
        return None
    if signal in ("BUY", "WATCH"):
        return ret_pct > 0
    if signal in ("SELL", "REDUCE"):
        return ret_pct < 0
    if signal == "HOLD":
        return abs(ret_pct) < 2.0
    return None


def learn_for_ticker(ticker: str, prices: dict, ta_data: dict,
                      ticker_analysis: dict, trends: dict, hold_days: int) -> dict:
    """ticker에 대해 과거 신호 소스별 적중률 측정."""

    pdata = prices.get("tickers", {}).get(ticker)
    if not pdata:
        return None

    dates  = pdata["dates"]
    closes = pdata["closes"]

    # 충분한 데이터 필요
    if len(dates) < hold_days + 30:
        return None

    ta_results, news_results = [], []

    # 마지막 hold_days만큼은 평가 불가 (미래 수익률 필요)
    for i in range(30, len(dates) - hold_days):
        d = dates[i]
        end_close = closes[i + hold_days]
        cur_close = closes[i]
        if cur_close is None or end_close is None or cur_close == 0:
            continue
        ret = (end_close / cur_close - 1) * 100

        ta_sig   = compute_ta_signal(ta_data, ticker, d)
        news_sig = compute_news_signal(ticker_analysis, trends, d)

        ta_hit   = evaluate_signal(ta_sig, ret)
        news_hit = evaluate_signal(news_sig, ret)

        if ta_hit is not None and ta_sig != "HOLD":
            ta_results.append((ta_sig, ret, ta_hit))
        if news_hit is not None and news_sig != "HOLD":
            news_results.append((news_sig, ret, news_hit))

    # 적중률 계산
    def hit_rate(results):
        if not results: return 0.5, 0
        hits = sum(1 for _,_,h in results if h)
        return hits / len(results), len(results)

    ta_hr, ta_n     = hit_rate(ta_results)
    news_hr, news_n = hit_rate(news_results)

    # 가중치: hit rate가 0.5 이상일수록 신뢰
    # ta_weight = (ta_hr - 0.5) * 2 등으로 0~1 정규화
    def to_weight(hr, n):
        if n < 5:
            return 0.0
        # 0.5 = random = 가중치 0
        # 0.8 = 강한 신호 = 가중치 1
        return max(0, min(1, (hr - 0.5) * 3.33))

    ta_w   = to_weight(ta_hr, ta_n)
    news_w = to_weight(news_hr, news_n)

    # regime은 별도 기능 → 일단 기본 0.1
    regime_w = 0.1

    # 정규화
    total = ta_w + news_w + regime_w
    if total < 0.01:
        # 둘 다 의미없음 → TA 기본
        ta_w, news_w, regime_w = 0.6, 0.3, 0.1
    else:
        ta_w     = ta_w / total
        news_w   = news_w / total
        regime_w = regime_w / total

    return {
        "ta_weight":      round(ta_w, 3),
        "news_weight":    round(news_w, 3),
        "regime_weight":  round(regime_w, 3),
        "ta_hit_rate":    round(ta_hr, 3),
        "news_hit_rate":  round(news_hr, 3),
        "ta_n":           ta_n,
        "news_n":         news_n,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prices",        default="site/data/prices.json")
    ap.add_argument("--ta",            default="site/data/technical_analysis.json")
    ap.add_argument("--trends",        default="site/data/trends.json")
    ap.add_argument("--analysis-dir",  default="site/data/ticker_analysis")
    ap.add_argument("--out",           default="site/data/ticker_weights.json")
    ap.add_argument("--hold-days",     type=int, default=5)
    ap.add_argument("--tickers",       default=None)
    args = ap.parse_args()

    prices = json.loads(Path(args.prices).read_text())
    ta     = json.loads(Path(args.ta).read_text())
    trends = json.loads(Path(args.trends).read_text())

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]
    else:
        tickers = list(prices.get("tickers", {}).keys())

    weights = {}
    print(f"Learning weights for {len(tickers)} tickers (hold_days={args.hold_days})\n")

    for ticker in sorted(tickers):
        ta_path = Path(args.analysis_dir) / f"{ticker}.json"
        if not ta_path.exists():
            ticker_analysis = {}
        else:
            ticker_analysis = json.loads(ta_path.read_text())

        result = learn_for_ticker(ticker, prices, ta, ticker_analysis, trends, args.hold_days)
        if result is None:
            print(f"  {ticker:<6}  insufficient data")
            continue

        weights[ticker] = result
        primary = "TA" if result["ta_weight"] > result["news_weight"] else "NEWS"
        print(f"  {ticker:<6}  TA {result['ta_weight']:.0%} (hit {result['ta_hit_rate']:.0%}, n={result['ta_n']}) | "
              f"NEWS {result['news_weight']:.0%} (hit {result['news_hit_rate']:.0%}, n={result['news_n']}) "
              f"→ {primary}-driven")

    output = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "hold_days": args.hold_days,
        "weights": weights,
    }
    Path(args.out).write_text(json.dumps(output, ensure_ascii=False, separators=(",",":")))

    # 분류 요약
    ta_driven   = [t for t,w in weights.items() if w["ta_weight"]   > 0.5]
    news_driven = [t for t,w in weights.items() if w["news_weight"] > 0.5]
    balanced    = [t for t,w in weights.items() if abs(w["ta_weight"] - w["news_weight"]) < 0.1]

    print(f"\n=== Summary ===")
    print(f"  TA-driven   ({len(ta_driven)}):   {', '.join(sorted(ta_driven))}")
    print(f"  News-driven ({len(news_driven)}): {', '.join(sorted(news_driven))}")
    print(f"  Balanced    ({len(balanced)}):    {', '.join(sorted(balanced))}")
    print(f"\n→ {args.out}")


if __name__ == "__main__":
    main()