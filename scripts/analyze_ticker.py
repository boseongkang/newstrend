"""
analyze_ticker.py — Per-ticker news-price deep analysis
=========================================================
특정 종목에 대해:
  1. 주가 상승일 / 하락일을 분류
  2. 각 날 전날/당일 뉴스에서 어떤 단어가 많았는지 집계
  3. 단어별로 "이 단어가 많은 날 → 다음날 수익률" 상관 계산
  4. BUY/SELL 신호 단어 목록 + 과거 성공률 출력

출력: site/data/ticker_analysis/<TICKER>.json
{
  "ticker": "MU",
  "price_days": 258,
  "news_days": 165,
  "analysis": {
    "bullish_words": [
      {
        "word": "memory",
        "lead_days": -1,          // -1 = 전날 뉴스, 0 = 당일
        "corr": 0.71,
        "hit_rate": 0.78,         // 이 단어 급등 다음날 주가 상승 비율
        "avg_ret_1d": +2.3,       // 평균 익일 수익률(%)
        "avg_ret_3d": +3.8,
        "n_events": 18,
        "examples": [             // 실제 사례
          {"date": "2026-01-15", "word_z": 3.2, "ret_1d": +4.1}
        ]
      }
    ],
    "bearish_words": [...],
    "neutral_words": [...]
  },
  "today_signal": {               // 오늘 Z>=1.5 단어 중 이 종목과 관련된 것
    "action": "BUY",
    "score": 2.4,
    "active_bullish": ["memory", "chip"],
    "active_bearish": [],
    "summary": "2 bullish signals active today"
  }
}
"""

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path


# ── 통계 헬퍼 ──────────────────────────────────────────────────────────────────

def pearson(xs, ys):
    n = len(xs)
    if n < 5:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((a - mx) * (b - my) for a, b in zip(xs, ys))
    dx  = math.sqrt(sum((a - mx) ** 2 for a in xs))
    dy  = math.sqrt(sum((b - my) ** 2 for b in ys))
    if dx < 1e-9 or dy < 1e-9:
        return None
    return round(num / (dx * dy), 4)


def zscore_series(counts, window=28):
    out = []
    for i, c in enumerate(counts):
        if i < 3:
            out.append(0.0)
            continue
        hist = counts[max(0, i - window): i]
        mean = sum(hist) / len(hist)
        std  = math.sqrt(sum((x - mean) ** 2 for x in hist) / len(hist))
        out.append(round((c - mean) / std, 3) if std >= 0.5 else 0.0)
    return out


# ── 메인 분석 ─────────────────────────────────────────────────────────────────

def analyze(ticker: str, T: dict, P_data: dict,
            min_events: int, z_thresh: float, lag_range: int) -> dict:

    t_dates  = T["dates"]
    t_series = T["series"]
    zscores_today = T.get("zscores", {})

    p_tickers = P_data.get("tickers", {})
    if ticker not in p_tickers:
        return {"error": f"{ticker} not in prices.json"}

    pdata    = p_tickers[ticker]
    p_dates  = pdata["dates"]
    p_closes = [float(v) if v is not None else None for v in pdata["closes"]]
    p_rets   = pdata.get("returns", [None] * len(p_dates))
    p_highs  = [float(v) if v is not None else None for v in pdata.get("highs", p_closes)]
    p_lows   = [float(v) if v is not None else None for v in pdata.get("lows",  p_closes)]

    p_date_idx = {d: i for i, d in enumerate(p_dates)}
    t_date_idx = {d: i for i, d in enumerate(t_dates)}

    common_dates = sorted(set(t_dates) & set(p_dates))
    if len(common_dates) < 10:
        return {"error": "insufficient overlap between news and price data"}

    print(f"  {ticker}: {len(common_dates)} overlapping dates")

    # ── 가격 방향 분류 ────────────────────────────────────────────────────────
    # 각 날짜별 익일 수익률
    date_ret = {}
    for d in common_dates:
        pi = p_date_idx.get(d)
        if pi is None or pi + 1 >= len(p_rets):
            continue
        r = p_rets[pi + 1]
        if r is not None:
            date_ret[d] = r

    up_dates   = {d for d, r in date_ret.items() if r > 0.005}   # +0.5% 이상
    down_dates = {d for d, r in date_ret.items() if r < -0.005}  # -0.5% 이하

    print(f"    Up days: {len(up_dates)}, Down days: {len(down_dates)}")

    # ── 단어별 분석 ───────────────────────────────────────────────────────────
    # 총 빈도 기준 상위 단어
    totals = {t: sum(v) for t, v in t_series.items()}
    top_terms = sorted(totals, key=totals.get, reverse=True)[:300]

    bullish_words = []
    bearish_words = []
    neutral_words = []

    for term in top_terms:
        counts = t_series[term]
        zs     = zscore_series(counts)

        for lag in range(0, lag_range + 1):  # 0=당일, 1=전날, 2=이틀전
            xs, ys = [], []
            examples = []

            for d in common_dates:
                ti = t_date_idx.get(d)
                if ti is None:
                    continue
                # lag일 전 뉴스 → 오늘 수익률
                news_ti = ti - lag
                if news_ti < 0:
                    continue
                news_date = t_dates[news_ti]
                z = zs[news_ti]

                pi = p_date_idx.get(d)
                if pi is None or pi + 1 >= len(p_rets):
                    continue
                ret = p_rets[pi + 1]  # d 다음날 수익률
                if ret is None:
                    continue

                xs.append(z)
                ys.append(ret)

                if z >= z_thresh:
                    examples.append({
                        "news_date": news_date,
                        "price_date": d,
                        "word_z": round(z, 2),
                        "ret_1d": round(ret * 100, 2),
                    })

            if len(xs) < 10:
                continue

            corr = pearson(xs, ys)
            if corr is None:
                continue

            # Z >= z_thresh인 이벤트만
            events = [e for e in examples]
            if len(events) < min_events:
                continue

            hit_rate   = sum(1 for e in events if e["ret_1d"] > 0) / len(events)
            avg_ret_1d = sum(e["ret_1d"] for e in events) / len(events)

            # 3일 수익률도 계산
            ret3_list = []
            for e in events:
                pi = p_date_idx.get(e["price_date"])
                if pi is None:
                    continue
                r3 = [p_rets[pi + k] for k in range(1, 4)
                      if pi + k < len(p_rets) and p_rets[pi + k] is not None]
                if r3:
                    ret3_list.append(sum(r3) / len(r3) * 100)
            avg_ret_3d = round(sum(ret3_list) / len(ret3_list), 2) if ret3_list else None

            word_data = {
                "word":       term,
                "lead_days":  -lag,       # -1 = 전날, 0 = 당일
                "corr":       corr,
                "hit_rate":   round(hit_rate, 3),
                "avg_ret_1d": round(avg_ret_1d, 3),
                "avg_ret_3d": avg_ret_3d,
                "n_events":   len(events),
                "examples":   sorted(examples, key=lambda x: abs(x["ret_1d"]), reverse=True)[:5],
            }

            # 분류: 절대 상관 0.25+, 방향성 있는 것만
            if abs(corr) >= 0.25 and len(events) >= min_events:
                if corr > 0 and hit_rate >= 0.55:
                    bullish_words.append(word_data)
                elif corr < 0 and hit_rate <= 0.45:
                    bearish_words.append(word_data)
                else:
                    neutral_words.append(word_data)

    # 정렬: |corr| × hit_rate
    def score(w):
        hr = w["hit_rate"] if w["corr"] > 0 else (1 - w["hit_rate"])
        return abs(w["corr"]) * hr * math.sqrt(w["n_events"] / 5)

    bullish_words.sort(key=score, reverse=True)
    bearish_words.sort(key=score, reverse=True)

    print(f"    Bullish words: {len(bullish_words)}, Bearish: {len(bearish_words)}")

    # ── 오늘 신호 ─────────────────────────────────────────────────────────────
    active_bullish = []
    active_bearish = []

    for w in bullish_words[:30]:
        z = zscores_today.get(w["word"], 0)
        if z >= 1.5:
            active_bullish.append({"word": w["word"], "z": z, "expected_ret": w["avg_ret_1d"]})

    for w in bearish_words[:30]:
        z = zscores_today.get(w["word"], 0)
        if z >= 1.5:
            active_bearish.append({"word": w["word"], "z": z, "expected_ret": w["avg_ret_1d"]})

    # 종합 점수
    bull_score = sum(w["z"] * abs(w["expected_ret"]) for w in active_bullish)
    bear_score = sum(w["z"] * abs(w["expected_ret"]) for w in active_bearish)
    net_score  = bull_score - bear_score

    if net_score > 3:    action = "BUY"
    elif net_score > 1:  action = "WATCH"
    elif net_score < -3: action = "SELL"
    elif net_score < -1: action = "REDUCE"
    else:                action = "HOLD"

    today_signal = {
        "action":          action,
        "net_score":       round(net_score, 2),
        "bull_score":      round(bull_score, 2),
        "bear_score":      round(bear_score, 2),
        "active_bullish":  active_bullish[:5],
        "active_bearish":  active_bearish[:5],
        "summary":         (
            f"{len(active_bullish)} bullish, {len(active_bearish)} bearish signals active"
            if active_bullish or active_bearish
            else "No active signals today"
        ),
    }

    return {
        "ticker":        ticker,
        "updated":       datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "price_days":    len(p_dates),
        "news_days":     len(t_dates),
        "common_days":   len(common_dates),
        "up_days":       len(up_dates),
        "down_days":     len(down_dates),
        "analysis": {
            "bullish_words": bullish_words[:40],
            "bearish_words": bearish_words[:40],
            "neutral_words": neutral_words[:20],
        },
        "today_signal":  today_signal,
    }


# ── Entry ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trends",      default="site/data/trends.json")
    ap.add_argument("--prices",      default="site/data/prices.json")
    ap.add_argument("--out-dir",     default="site/data/ticker_analysis")
    ap.add_argument("--tickers",     default="MU",
                    help="쉼표 구분 티커 (기본: MU)")
    ap.add_argument("--min-events",  type=int,   default=5)
    ap.add_argument("--z-thresh",    type=float, default=1.5)
    ap.add_argument("--lag-range",   type=int,   default=2,
                    help="0=당일, 1=전날, 2=이틀전 분석")
    args = ap.parse_args()

    T      = json.loads(Path(args.trends).read_text())
    P_data = json.loads(Path(args.prices).read_text())

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    tickers = [t.strip().upper() for t in args.tickers.split(",")]

    for ticker in tickers:
        print(f"\nAnalyzing {ticker}...")
        result = analyze(ticker, T, P_data,
                         args.min_events, args.z_thresh, args.lag_range)

        out_path = out_dir / f"{ticker}.json"
        out_path.write_text(json.dumps(result, ensure_ascii=False, separators=(",",":")))

        if "error" in result:
            print(f"  ERROR: {result['error']}")
            continue

        sig = result["today_signal"]
        print(f"  → {out_path}")
        print(f"  Today signal: {sig['action']} (score={sig['net_score']})")
        print(f"  {sig['summary']}")

        b = result["analysis"]["bullish_words"]
        d = result["analysis"]["bearish_words"]
        if b:
            print(f"\n  Top BULLISH words (→ {ticker} up):")
            for w in b[:8]:
                lag_str = f"lag {w['lead_days']}d"
                print(f"    {w['word']:<18} {lag_str}  corr={w['corr']:+.3f}  "
                      f"hit={w['hit_rate']:.0%}  avg_ret={w['avg_ret_1d']:+.2f}%  n={w['n_events']}")
        if d:
            print(f"\n  Top BEARISH words (→ {ticker} down):")
            for w in d[:8]:
                lag_str = f"lag {w['lead_days']}d"
                print(f"    {w['word']:<18} {lag_str}  corr={w['corr']:+.3f}  "
                      f"hit={w['hit_rate']:.0%}  avg_ret={w['avg_ret_1d']:+.2f}%  n={w['n_events']}")


if __name__ == "__main__":
    main()