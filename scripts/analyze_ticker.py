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
    # bigram/noise 필터
    def _clean(t):
        if " " in t: return False          # bigram
        if len(t) < 4: return False        # 짧은 단어
        return True
    top_terms = [t for t in sorted(totals, key=totals.get, reverse=True)
                 if _clean(t)][:300]

    bullish_words = []
    bearish_words = []
    neutral_words = []

    # ── Train/Test split: 앞 70% train, 뒤 30% test ────────────────────────────
    split_idx = int(len(common_dates) * 0.7)
    train_dates = set(common_dates[:split_idx])
    test_dates  = set(common_dates[split_idx:])
    print(f"    Train: {len(train_dates)}d, Test: {len(test_dates)}d")

    def compute_stats(term, lag, date_set, z_thresh):
        """특정 기간에서 단어-수익률 통계 계산."""
        counts = t_series[term]
        zs     = zscore_series(counts)
        xs, ys, events = [], [], []

        for d in date_set:
            ti = t_date_idx.get(d)
            if ti is None: continue
            news_ti = ti - lag
            if news_ti < 0: continue
            z = zs[news_ti]
            pi = p_date_idx.get(d)
            if pi is None or pi + 1 >= len(p_rets): continue
            ret = p_rets[pi + 1]
            if ret is None: continue

            xs.append(z)
            ys.append(ret)
            if z >= z_thresh:
                events.append({
                    "news_date":  t_dates[news_ti],
                    "price_date": d,
                    "word_z":     round(z, 2),
                    "ret_1d":     round(ret * 100, 2),
                })

        if len(xs) < 5:
            return None

        corr = pearson(xs, ys)
        if corr is None:
            return None

        if not events:
            return {"corr": corr, "hit_rate": None, "avg_ret": None,
                    "n_events": 0, "events": []}

        hit_rate = sum(1 for e in events if e["ret_1d"] > 0) / len(events)
        avg_ret  = sum(e["ret_1d"] for e in events) / len(events)
        return {
            "corr":     corr,
            "hit_rate": round(hit_rate, 3),
            "avg_ret":  round(avg_ret, 3),
            "n_events": len(events),
            "events":   events,
        }

    for term in top_terms:
        for lag in range(0, lag_range + 1):
            # ── 1단계: Train 데이터에서 패턴 발견 ────────────────────────────
            train_stats = compute_stats(term, lag, train_dates, z_thresh)
            if train_stats is None or train_stats["n_events"] < min_events:
                continue

            train_corr = train_stats["corr"]
            train_hit  = train_stats["hit_rate"] or 0

            # Train에서 유의미한 패턴만 통과
            if abs(train_corr) < 0.15:
                continue
            if train_corr > 0 and train_hit < 0.55:
                continue
            if train_corr < 0 and train_hit > 0.45:
                continue

            # ── 2단계: Test 데이터로 검증 ──────────────────────────────────
            test_stats = compute_stats(term, lag, test_dates, z_thresh)
            if test_stats is None:
                continue

            test_hit = test_stats["hit_rate"]
            test_corr = test_stats["corr"]

            # Test에서도 같은 방향이어야 함 (과적합 제거)
            if test_corr is None:
                continue
            direction_match = (train_corr > 0 and test_corr > 0) or \
                              (train_corr < 0 and test_corr < 0)
            if not direction_match:
                continue

            # Test 데이터에 이벤트가 없으면 검증 불가 → 탈락
            if test_hit is None or test_stats["n_events"] < 2:
                continue

            # Train hit rate vs Test hit rate 편차 < 20% (안정성 검증)
            hit_drop = abs(train_hit - test_hit)
            if hit_drop > 0.25:
                continue  # 불안정한 패턴 제외

            # ── 3단계: 전체 기간 통계 (표시용) ──────────────────────────────
            full_stats = compute_stats(term, lag, set(common_dates), z_thresh)
            if full_stats is None or full_stats["n_events"] < min_events:
                continue

            # 3일 수익률
            ret3_list = []
            for e in full_stats["events"]:
                pi = p_date_idx.get(e["price_date"])
                if pi is None: continue
                r3 = [p_rets[pi + k] for k in range(1, 4)
                      if pi + k < len(p_rets) and p_rets[pi + k] is not None]
                if r3:
                    ret3_list.append(sum(r3) / len(r3) * 100)
            avg_ret_3d = round(sum(ret3_list) / len(ret3_list), 2) if ret3_list else None

            word_data = {
                "word":       term,
                "lead_days":  -lag,
                "corr":       full_stats["corr"],
                "hit_rate":   full_stats["hit_rate"],
                "avg_ret_1d": full_stats["avg_ret"],
                "avg_ret_3d": avg_ret_3d,
                "n_events":   full_stats["n_events"],
                # Train/Test 검증 결과 추가
                "train_corr": round(train_corr, 3),
                "train_hit":  round(train_hit, 3),
                "test_corr":  round(test_corr, 3),
                "test_hit":   round(test_hit, 3) if test_hit is not None else None,
                "stability":  round(1 - (abs(train_hit - (test_hit or train_hit))), 3),
                "examples":   sorted(full_stats["events"],
                                    key=lambda x: abs(x["ret_1d"]),
                                    reverse=True)[:5],
            }

            # ── 분류 ────────────────────────────────────────────────────────
            if full_stats["corr"] > 0 and full_stats["hit_rate"] >= 0.55:
                bullish_words.append(word_data)
            elif full_stats["corr"] < 0 and full_stats["hit_rate"] <= 0.45:
                bearish_words.append(word_data)

    # 정렬: |corr| × hit_rate
    def score(w):
        """Train/Test 안정성 + 상관 + hit rate + 표본 크기."""
        hr = w["hit_rate"] if w["corr"] > 0 else (1 - w["hit_rate"])
        stability = w.get("stability", 1.0)
        return abs(w["corr"]) * hr * math.sqrt(w["n_events"] / 5) * stability

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
    ap.add_argument("--min-events",  type=int,   default=3)
    ap.add_argument("--z-thresh",    type=float, default=0.8)
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
                test_hit_str = f"test_hit={w['test_hit']:.0%}" if w.get('test_hit') is not None else "test_hit=—"
            print(f"    {w['word']:<18} {lag_str}  "
                  f"corr={w['corr']:+.3f}  hit={w['hit_rate']:.0%}  "
                  f"{test_hit_str}  avg={w['avg_ret_1d']:+.2f}%  n={w['n_events']}  "
                  f"stab={w.get('stability',0):.2f}")
        if d:
            print(f"\n  Top BEARISH words (→ {ticker} down):")
            for w in d[:8]:
                lag_str = f"lag {w['lead_days']}d"
                test_hit_str = f"test_hit={w['test_hit']:.0%}" if w.get('test_hit') is not None else "test_hit=—"
            print(f"    {w['word']:<18} {lag_str}  "
                  f"corr={w['corr']:+.3f}  hit={w['hit_rate']:.0%}  "
                  f"{test_hit_str}  avg={w['avg_ret_1d']:+.2f}%  n={w['n_events']}  "
                  f"stab={w.get('stability',0):.2f}")


if __name__ == "__main__":
    main()