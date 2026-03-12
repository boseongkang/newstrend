"""
build_signal_corr.py  v2
trends.json + prices.json → signal_corr.json

핵심 계산:
  - 각 (단어, 종목) 쌍에 대해 lag -5~+5일 Pearson 상관계수
  - lag < 0  → 단어가 주가보다 앞섬 (예측력 있음)
  - lag = 0  → 동시 반응
  - lag > 0  → 주가가 먼저 움직임 (후행)
  - 단어 Z-score ≥ 2인 날 다음날/5일 평균 수익률 계산

출력: site/data/signal_corr.json
{
  "updated": "...",
  "n_dates": 90,
  "pairs": [
    {
      "term": "tariff",
      "ticker": "AAPL",
      "best_lag": -1,         // 음수 = 단어가 주가를 선행
      "corr": 0.71,
      "hit_rate": 0.68,       // z>=2 이벤트 중 다음날 방향 맞춘 비율
      "n_events": 14,
      "avg_ret_1d": 1.2,      // z>=2 다음날 평균 수익률(%)
      "avg_ret_5d": 2.8,
      "signal_type": "leading_1d",
      "lag_corrs": {"-2": 0.3, "-1": 0.71, "0": 0.5, ...}
    }
  ],
  "term_stats": {
    "tariff": {"best_ticker": "AAPL", "best_corr": 0.71, "best_lag": -1}
  }
}
"""

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path


# ── 통계 헬퍼 ─────────────────────────────────────────────────────────

def pearson(xs, ys):
    """두 리스트의 Pearson 상관계수. 데이터 부족 시 None."""
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


def zscore_series(counts: list, window: int = 28) -> list:
    """일별 Z-score 시계열 반환."""
    result = []
    for i, c in enumerate(counts):
        if i < 3:
            result.append(0.0)
            continue
        hist = counts[max(0, i - window): i]
        mean = sum(hist) / len(hist)
        std  = math.sqrt(sum((x - mean) ** 2 for x in hist) / len(hist))
        result.append(round((c - mean) / std, 3) if std >= 0.5 else 0.0)
    return result


# ── 메인 로직 ─────────────────────────────────────────────────────────

def build_corr(trends_path: str, prices_path: str,
               top_terms: int, min_corr: float, min_events: int,
               lag_range: int) -> dict:

    T = json.loads(Path(trends_path).read_text())
    P = json.loads(Path(prices_path).read_text())

    t_dates  = T["dates"]       # ["2025-09-24", ...]
    t_series = T["series"]      # {term: [count, ...]}

    p_tickers = P["tickers"]    # {ticker: {dates, closes, returns}}

    # 상위 terms (총 언급량 기준)
    totals = {t: sum(v) for t, v in t_series.items()}
    top = sorted(totals, key=totals.get, reverse=True)[:top_terms]

    # 날짜 인덱스
    t_date_idx = {d: i for i, d in enumerate(t_dates)}

    pairs = []
    term_best: dict[str, dict] = {}

    for ticker, pdata in p_tickers.items():
        p_dates   = pdata["dates"]
        p_returns = pdata["returns"]   # [None, 0.005, ...]

        # 공통 날짜 (trends ∩ prices)
        common = sorted(set(t_dates) & set(p_dates))
        if len(common) < 10:
            continue

        p_ret_idx = {d: i for i, d in enumerate(p_dates)}

        for term in top:
            counts = t_series[term]
            zs     = zscore_series(counts)

            # lag loop: -lag_range ~ +lag_range
            # lag < 0 = 단어가 주가보다 앞섬
            best_lag, best_corr = 0, 0.0
            lag_corrs = {}

            for lag in range(-lag_range, lag_range + 1):
                xs, ys = [], []
                for d in common:
                    ti = t_date_idx[d]
                    # 주가 인덱스: lag일 후
                    target_date_idx = p_ret_idx.get(d)
                    if target_date_idx is None:
                        continue
                    pi_target = target_date_idx - lag  # lag<0 → 미래 주가
                    if pi_target < 1 or pi_target >= len(p_returns):
                        continue
                    ret = p_returns[pi_target]
                    if ret is None:
                        continue
                    xs.append(zs[ti])
                    ys.append(ret)

                c = pearson(xs, ys)
                if c is not None:
                    lag_corrs[str(lag)] = c
                    if abs(c) > abs(best_corr):
                        best_corr = c
                        best_lag  = lag

            if abs(best_corr) < min_corr:
                continue

            # ── z≥2 이벤트 분석 ───────────────────────────────────────
            events_1d, events_5d = [], []
            for i, d in enumerate(t_dates):
                if zs[i] < 2.0:
                    continue
                pi = p_ret_idx.get(d)
                if pi is None:
                    continue
                # 다음날 수익률
                if pi + 1 < len(p_returns) and p_returns[pi + 1] is not None:
                    events_1d.append(p_returns[pi + 1])
                # 5일 평균 수익률
                window_rets = [p_returns[pi + k]
                               for k in range(1, 6)
                               if pi + k < len(p_returns) and p_returns[pi + k] is not None]
                if window_rets:
                    events_5d.append(sum(window_rets) / len(window_rets))

            n_events = len(events_1d)
            if n_events < min_events:
                continue

            avg_ret_1d = round(sum(events_1d) / n_events * 100, 3)
            avg_ret_5d = round(sum(events_5d) / len(events_5d) * 100, 3) if events_5d else None
            hit_rate   = round(sum(1 for r in events_1d if r > 0) / n_events, 3)

            # signal_type 분류
            if best_lag <= -2:
                stype = "leading"
            elif best_lag == -1:
                stype = "leading_1d"
            elif best_lag == 0:
                stype = "coincident"
            elif best_lag >= 2:
                stype = "lagging"
            else:
                stype = "lagging_1d"

            pair = {
                "term":        term,
                "ticker":      ticker,
                "best_lag":    best_lag,
                "corr":        best_corr,
                "hit_rate":    hit_rate,
                "n_events":    n_events,
                "avg_ret_1d":  avg_ret_1d,
                "avg_ret_5d":  avg_ret_5d,
                "signal_type": stype,
                "lag_corrs":   lag_corrs,
            }
            pairs.append(pair)

            # term 최고 상관 종목
            if term not in term_best or abs(best_corr) > abs(term_best[term]["best_corr"]):
                term_best[term] = {
                    "best_ticker": ticker,
                    "best_corr":   best_corr,
                    "best_lag":    best_lag,
                    "signal_type": stype,
                }

    # best_corr 절댓값 내림차순
    pairs.sort(key=lambda x: abs(x["corr"]), reverse=True)

    return {
        "updated":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "n_dates":    len(t_dates),
        "n_pairs":    len(pairs),
        "pairs":      pairs,
        "term_stats": term_best,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trends",     default="site/data/trends.json")
    ap.add_argument("--prices",     default="site/data/prices.json")
    ap.add_argument("--out",        default="site/data/signal_corr.json")
    ap.add_argument("--top-terms",  type=int,   default=200)
    ap.add_argument("--min-corr",   type=float, default=0.25)
    ap.add_argument("--min-events", type=int,   default=3)
    ap.add_argument("--lag-range",  type=int,   default=5)
    args = ap.parse_args()

    print(f"Loading  trends: {args.trends}")
    print(f"Loading  prices: {args.prices}")

    result = build_corr(
        args.trends, args.prices,
        args.top_terms, args.min_corr, args.min_events, args.lag_range
    )

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")

    print(f"\n→ {args.out}")
    print(f"  pairs:     {result['n_pairs']}")
    print(f"  n_dates:   {result['n_dates']}")
    print(f"\n  Top 20 signal pairs (|corr| desc):")
    for p in result["pairs"][:20]:
        print(f"    {p['term']:<18} {p['ticker']:<6} "
              f"lag={p['best_lag']:+d}  corr={p['corr']:+.3f}  "
              f"hit={p['hit_rate']:.0%}  n={p['n_events']}  "
              f"1d={p['avg_ret_1d']:+.2f}%  [{p['signal_type']}]")

    print(f"\n  Leading signals (lag ≤ -1, predictive):")
    leading = [p for p in result["pairs"] if p["best_lag"] <= -1]
    for p in leading[:10]:
        print(f"    {p['term']:<18} {p['ticker']:<6} "
              f"lag={p['best_lag']:+d}  corr={p['corr']:+.3f}  "
              f"1d={p['avg_ret_1d']:+.2f}%")


if __name__ == "__main__":
    main()