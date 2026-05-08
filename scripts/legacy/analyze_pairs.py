"""
analyze_pairs.py — 단어 조합 신호 분석
========================================
각 종목별로 2개 단어가 같은 날 동시에 Z>=threshold로 급등할 때
주가 반응을 통계 검정으로 분석.

예: tariff + china 동시 급등 → AAPL 다음날 +X%

출력: site/data/ticker_pairs/<TICKER>.json
{
  "ticker": "AAPL",
  "pairs": [
    {
      "word_a": "tariff",
      "word_b": "china",
      "lead_days": -1,
      "n_events": 12,
      "hit_rate": 0.83,
      "avg_ret_1d": +1.42,
      "p_value": 0.012,
      "test_hit": 0.80,
      "stability": 0.97,
      "dates": ["2026-01-15", "2026-02-03", ...]
    },
    ...
  ]
}
"""

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path


# ── 통계 ──────────────────────────────────────────────────────────────────────

def binomial_pvalue(hits: int, n: int, p_null: float = 0.5) -> float:
    """이항 양측검정 p-value (정규근사)."""
    if n == 0:
        return 1.0
    mean = n * p_null
    std  = math.sqrt(n * p_null * (1 - p_null))
    if std < 1e-9:
        return 1.0
    z = abs(hits - mean) / std

    def erf_approx(x):
        a1, a2, a3 = 0.254829592, -0.284496736, 1.421413741
        a4, a5 = -1.453152027, 1.061405429
        p = 0.3275911
        sign = 1 if x >= 0 else -1
        x = abs(x)
        t = 1.0 / (1.0 + p * x)
        y = 1.0 - (((((a5*t + a4)*t) + a3)*t + a2)*t + a1)*t * math.exp(-x*x)
        return sign * y

    p_one = 0.5 * (1 - erf_approx(z / math.sqrt(2)))
    return min(1.0, 2 * p_one)


def zscore_series(counts, window=28):
    out = []
    for i, c in enumerate(counts):
        if i < 3:
            out.append(0.0); continue
        hist = counts[max(0, i-window):i]
        mean = sum(hist) / len(hist)
        std  = math.sqrt(sum((x-mean)**2 for x in hist) / len(hist))
        out.append(round((c-mean)/std, 3) if std >= 0.5 else 0.0)
    return out




def compute_single_word_stats(word_zs, word, t_dates, t_idx, p_dates, p_idx,
                                p_rets, common_dates, lag, z_thresh):
    """단일 단어의 hit rate (조합 비교용)."""
    if word not in word_zs:
        return None
    zs = word_zs[word]
    events = []
    for d in common_dates:
        ti = t_idx.get(d)
        if ti is None: continue
        news_ti = ti - lag
        if news_ti < 0 or news_ti >= len(zs): continue
        if zs[news_ti] < z_thresh: continue
        pi = p_idx.get(d)
        if pi is None or pi + 1 >= len(p_rets): continue
        ret = p_rets[pi + 1]
        if ret is None: continue
        events.append({"ret": ret, "date": d})

    if len(events) < 3:
        return None
    hits = sum(1 for e in events if e["ret"] > 0)
    return {"n": len(events), "hits": hits, "hit_rate": hits / len(events)}


# ── 메인 분석 ─────────────────────────────────────────────────────────────────

def analyze_ticker_pairs(ticker, T, P_data, top_words, z_thresh, min_events, lag_range):
    """ticker 종목에 대해 모든 2-word combination 분석."""

    t_dates  = T["dates"]
    t_series = T["series"]

    pdata = P_data["tickers"].get(ticker)
    if not pdata:
        return {"error": f"{ticker} not in prices"}

    p_dates = pdata["dates"]
    p_rets  = pdata.get("returns", [None] * len(p_dates))

    p_idx = {d: i for i, d in enumerate(p_dates)}
    t_idx = {d: i for i, d in enumerate(t_dates)}

    common_dates = sorted(set(t_dates) & set(p_dates))
    if len(common_dates) < 30:
        return {"error": f"insufficient overlap ({len(common_dates)}d)"}

    # Train/test split
    split = int(len(common_dates) * 0.7)
    train_dates = set(common_dates[:split])
    test_dates  = set(common_dates[split:])

    # 미리 모든 단어 Z-score 계산
    word_zs = {w: zscore_series(t_series[w]) for w in top_words if w in t_series}

    print(f"\n{ticker}: {len(common_dates)}d overlap, "
          f"top {len(word_zs)} words, "
          f"{len(word_zs)*(len(word_zs)-1)//2} pairs to test")

    pairs_found = []
    pair_count = 0

    for word_a, word_b in combinations(sorted(word_zs.keys()), 2):
        pair_count += 1
        zs_a = word_zs[word_a]
        zs_b = word_zs[word_b]

        for lag in range(0, lag_range + 1):
            # ── 전체 기간 통계 ──
            full_events = []
            for d in common_dates:
                ti = t_idx.get(d)
                if ti is None: continue
                news_ti = ti - lag
                if news_ti < 0: continue

                z_a = zs_a[news_ti] if news_ti < len(zs_a) else 0
                z_b = zs_b[news_ti] if news_ti < len(zs_b) else 0
                if z_a < z_thresh or z_b < z_thresh:
                    continue

                pi = p_idx.get(d)
                if pi is None or pi + 1 >= len(p_rets): continue
                ret = p_rets[pi + 1]
                if ret is None: continue

                full_events.append({
                    "news_date":  t_dates[news_ti],
                    "price_date": d,
                    "z_a":        round(z_a, 2),
                    "z_b":        round(z_b, 2),
                    "ret_1d":     round(ret * 100, 2),
                })

            if len(full_events) < min_events:
                continue

            full_hits = sum(1 for e in full_events if e["ret_1d"] > 0)
            full_hit_rate = full_hits / len(full_events)
            avg_ret = sum(e["ret_1d"] for e in full_events) / len(full_events)

            # 의미있는 방향성만
            if abs(full_hit_rate - 0.5) < 0.10:
                continue

            # ── Train/Test 검증 ──
            train_events = [e for e in full_events if e["price_date"] in train_dates]
            test_events  = [e for e in full_events if e["price_date"] in test_dates]

            if len(train_events) < max(2, min_events - 1) or len(test_events) < 2:
                continue

            train_hits = sum(1 for e in train_events if e["ret_1d"] > 0)
            test_hits  = sum(1 for e in test_events if e["ret_1d"] > 0)
            train_hr = train_hits / len(train_events)
            test_hr  = test_hits / len(test_events)

            # 방향 일치
            train_bull = train_hr > 0.5
            test_bull  = test_hr > 0.5
            if train_bull != test_bull:
                continue

            # 안정성
            if abs(train_hr - test_hr) > 0.30:
                continue

            # ── 통계 검정 (양방향) ──
            if full_hit_rate > 0.5:
                p_value = binomial_pvalue(full_hits, len(full_events))
            else:
                p_value = binomial_pvalue(len(full_events) - full_hits, len(full_events))

            if p_value > 0.05:
                continue

            # ── 5단계: 단일 단어 대비 조합이 더 나은지 검증 ─────────────────
            # 조합 hit rate가 양쪽 단일 단어 hit rate보다 +5% 이상 높아야 의미있음
            single_a = compute_single_word_stats(word_zs, word_a, t_dates, t_idx,
                                                  p_dates, p_idx, p_rets,
                                                  common_dates, lag, z_thresh)
            single_b = compute_single_word_stats(word_zs, word_b, t_dates, t_idx,
                                                  p_dates, p_idx, p_rets,
                                                  common_dates, lag, z_thresh)

            # 단일 단어보다 의미있게 좋아야 함
            min_uplift = 0.05  # 최소 5%p 향상
            if single_a and single_b:
                # 같은 방향 (상승/하락)일 때만 비교 의미있음
                if full_hit_rate > 0.5:
                    uplift_a = full_hit_rate - single_a["hit_rate"]
                    uplift_b = full_hit_rate - single_b["hit_rate"]
                else:
                    uplift_a = single_a["hit_rate"] - full_hit_rate
                    uplift_b = single_b["hit_rate"] - full_hit_rate

                # 조합이 양쪽 단어보다 명확히 좋아야 함
                if uplift_a < min_uplift or uplift_b < min_uplift:
                    continue

            # 조합 발생일이 두 단어 단일 발생의 단순 교집합이 아니어야 함
            # (이벤트 수가 단일 이벤트 수의 80% 이상이면 사실상 같은 신호)
            if single_a and single_a["n"] > 0:
                overlap_ratio = len(full_events) / single_a["n"]
                if overlap_ratio > 0.85:
                    continue
            if single_b and single_b["n"] > 0:
                overlap_ratio = len(full_events) / single_b["n"]
                if overlap_ratio > 0.85:
                    continue

            # ── 통과 ──
            direction = "bullish" if full_hit_rate > 0.5 else "bearish"
            pair_data = {
                "word_a":      word_a,
                "word_b":      word_b,
                "lead_days":   -lag,
                "n_events":    len(full_events),
                "hit_rate":    round(full_hit_rate, 3),
                "avg_ret_1d":  round(avg_ret, 3),
                "p_value":     round(p_value, 4),
                "train_hit":   round(train_hr, 3),
                "test_hit":    round(test_hr, 3),
                "stability":   round(1 - abs(train_hr - test_hr), 3),
                "direction":   direction,
                # 단일 대비 조합의 우월함 정량화
                "single_a_hit": round(single_a["hit_rate"], 3) if single_a else None,
                "single_b_hit": round(single_b["hit_rate"], 3) if single_b else None,
                "uplift_pp":   round(min(uplift_a, uplift_b) * 100, 1)
                              if (single_a and single_b) else None,
                "examples":    sorted(full_events,
                                    key=lambda x: abs(x["ret_1d"]),
                                    reverse=True)[:3],
            }
            pairs_found.append(pair_data)

    # ── 정렬 (유의성 + 안정성 + 표본크기) ──
    def score(p):
        hr_strength = abs(p["hit_rate"] - 0.5) * 2
        p_conf      = 1.0 - p["p_value"] / 0.05
        return hr_strength * p_conf * p["stability"] * math.sqrt(p["n_events"] / 5)

    pairs_found.sort(key=score, reverse=True)

    print(f"  → Found {len(pairs_found)} significant pairs")
    if pairs_found:
        print(f"  Top 3:")
        for p in pairs_found[:3]:
            arrow = "▲" if p["direction"] == "bullish" else "▼"
            uplift_str = f"+{p.get('uplift_pp', 0):.0f}pp" if p.get('uplift_pp') else "—"
            print(f"    {arrow} {p['word_a']:>10}+{p['word_b']:<10} lag={p['lead_days']}d  "
                  f"hit={p['hit_rate']:.0%}  test={p['test_hit']:.0%}  "
                  f"uplift={uplift_str}  n={p['n_events']}  p={p['p_value']:.3f}")

    return {
        "ticker":       ticker,
        "updated":      datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "common_days":  len(common_dates),
        "pairs_tested": pair_count,
        "pairs":        pairs_found[:30],  # 상위 30개만 저장
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trends",  default="site/data/trends.json")
    ap.add_argument("--prices",  default="site/data/prices.json")
    ap.add_argument("--out-dir", default="site/data/ticker_pairs")
    ap.add_argument("--tickers", default="AAPL,MSFT,NVDA,GOOGL,MU,SPY")
    ap.add_argument("--top-words",   type=int,   default=30,
                    help="상위 N개 단어로만 조합 (전체는 너무 많음)")
    ap.add_argument("--z-thresh",    type=float, default=1.0)
    ap.add_argument("--min-events",  type=int,   default=5)
    ap.add_argument("--lag-range",   type=int,   default=2)
    args = ap.parse_args()

    T = json.loads(Path(args.trends).read_text())
    P = json.loads(Path(args.prices).read_text())

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 빈도 상위 N개 단어 (조합 폭발 방지)
    series = T.get("series", {})
    totals = {w: sum(c) for w, c in series.items()}
    top_words = [w for w, _ in sorted(totals.items(), key=lambda x: -x[1])
                 if " " not in w and len(w) >= 4][: args.top_words]

    print(f"Loaded: {len(T['dates'])} news days, top {len(top_words)} words")
    print(f"Words: {', '.join(top_words[:10])}...")

    tickers = [t.strip().upper() for t in args.tickers.split(",")]

    summary = []
    for ticker in tickers:
        result = analyze_ticker_pairs(ticker, T, P, top_words,
                                       args.z_thresh, args.min_events, args.lag_range)
        out_path = out_dir / f"{ticker}.json"
        out_path.write_text(json.dumps(result, ensure_ascii=False, separators=(",", ":")))

        if "error" in result:
            print(f"  ERROR: {result['error']}")
            continue
        summary.append({"ticker": ticker, "pairs": len(result["pairs"])})

    # 전체 요약
    print(f"\n=== Summary ===")
    for s in summary:
        print(f"  {s['ticker']:<6}  {s['pairs']:>3} pair signals")


if __name__ == "__main__":
    main()