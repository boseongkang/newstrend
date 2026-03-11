"""
build_signal_corr.py
────────────────────
단어 신호(Z-score 시계열) × 주가 수익률 리드-래그 상관분석

출력: site/data/signal_corr.json
{
  "updated": "2026-03-11",
  "pairs": [
    {
      "term": "tariff",
      "ticker": "AAPL",
      "best_lag": -1,          # 음수 = 단어가 주가보다 먼저 (예측력)
      "corr": 0.71,            # pearson 상관계수
      "hit_rate": 0.68,        # 단어 급등 다음날 주가 같은 방향 비율
      "n_events": 14,          # Z≥2 이벤트 수
      "avg_return_next1d": 1.2,  # 단어 Z≥2 이후 1일 평균 수익률(%)
      "avg_return_next5d": 2.8,  # 5일 평균
      "signal_type": "leading"   # leading / lagging / coincident / noise
    }, ...
  ],
  "term_stats": {
    "tariff": { "hot_days": 8, "avg_z": 2.3, "max_z": 5.1 }
  }
}
"""

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

# ── 리드-래그 범위: -5 ~ +5일 ──────────────────────────────────────────
# 음수 = 단어가 주가보다 N일 앞서 발생 (예측 신호)
# 양수 = 주가가 단어보다 먼저 움직임 (후행 신호)
LAG_RANGE = list(range(-5, 6))   # [-5, -4, ..., 0, ..., +5]
Z_THRESHOLD = 2.0                 # 이 Z-점수 이상을 "이벤트"로 간주
MIN_EVENTS = 3                    # 최소 이벤트 수 (없으면 노이즈로 처리)
MIN_CORR = 0.15                   # 최소 절대 상관계수
MIN_DATES = 30                    # 최소 공통 날짜 수


def pearson(x: list, y: list) -> float:
    """단순 pearson 상관계수."""
    n = len(x)
    if n < 5:
        return 0.0
    mx, my = sum(x)/n, sum(y)/n
    num = sum((a-mx)*(b-my) for a,b in zip(x,y))
    dx  = math.sqrt(sum((a-mx)**2 for a in x))
    dy  = math.sqrt(sum((b-my)**2 for b in y))
    if dx < 1e-9 or dy < 1e-9:
        return 0.0
    return round(num/(dx*dy), 4)


def rolling_zscore(series: list, window: int = 28) -> list:
    """각 날짜의 롤링 Z-점수 반환."""
    zs = []
    for i in range(len(series)):
        hist = series[max(0, i-window): i]
        if len(hist) < 3:
            zs.append(0.0)
            continue
        mean = sum(hist)/len(hist)
        std  = math.sqrt(sum((x-mean)**2 for x in hist)/len(hist))
        zs.append(round((series[i]-mean)/std, 3) if std > 0.5 else 0.0)
    return zs


def daily_returns(prices: list) -> list:
    """일별 로그 수익률(%)."""
    rets = [0.0]
    for i in range(1, len(prices)):
        p0, p1 = prices[i-1], prices[i]
        if p0 and p1 and p0 > 0:
            rets.append(round((p1/p0 - 1)*100, 4))
        else:
            rets.append(0.0)
    return rets


def classify_signal(best_lag: int, best_corr: float) -> str:
    if abs(best_corr) < MIN_CORR:
        return "noise"
    if best_lag <= -2:
        return "leading"     # 단어가 주가보다 2일+ 앞섬 → 예측력 있음
    if best_lag == -1:
        return "leading_1d"  # 하루 앞섬 → 실용적인 예측 신호
    if best_lag == 0:
        return "coincident"  # 같은 날 움직임
    return "lagging"         # 주가가 먼저 움직임 (뉴스 추종)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trends",  default="site/data/trends.json",
                    help="make_trends_json.py 출력")
    ap.add_argument("--prices",  default="site/data/prices.json",
                    help="prices.json (dates, tickers, close)")
    ap.add_argument("--out",     default="site/data/signal_corr.json")
    ap.add_argument("--top-terms",   type=int, default=150,
                    help="분석할 상위 단어 수 (너무 많으면 느림)")
    ap.add_argument("--top-tickers", type=int, default=30,
                    help="분석할 티커 수")
    ap.add_argument("--min-corr",    type=float, default=MIN_CORR)
    ap.add_argument("--z-threshold", type=float, default=Z_THRESHOLD)
    args = ap.parse_args()

    # ── 1. trends.json 로드 ────────────────────────────────────────────
    tp = Path(args.trends)
    if not tp.exists():
        raise SystemExit(f"trends.json not found: {tp}")
    T = json.loads(tp.read_text())

    term_dates  = T["dates"]           # ["2025-09-24", ...]
    term_series = T.get("series", {})  # {term: [cnt, ...]}
    top_terms   = T.get("top", list(term_series.keys()))[:args.top_terms]

    # ── 2. prices.json 로드 ───────────────────────────────────────────
    pp = Path(args.prices)
    if not pp.exists():
        print(f"[WARN] prices.json not found ({pp}), skipping price correlation")
        # prices 없어도 단어 통계만 저장
        term_stats = {}
        for term in top_terms:
            counts = term_series.get(term, [])
            zs     = rolling_zscore(counts)
            hot_days = sum(1 for z in zs if z >= args.z_threshold)
            term_stats[term] = {
                "hot_days": hot_days,
                "avg_z": round(sum(zs)/len(zs), 3) if zs else 0,
                "max_z": round(max(zs), 3) if zs else 0,
            }
        out = {"updated": datetime.utcnow().strftime("%Y-%m-%d"),
               "pairs": [], "term_stats": term_stats}
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(out, ensure_ascii=False, indent=2))
        print(f"wrote {args.out} (no price data)")
        return

    P = json.loads(pp.read_text())
    price_dates  = P["dates"]
    price_tickers = P["tickers"][:args.top_tickers]
    price_close  = P["close"]   # {ticker: [price, ...]}

    # ── 3. 공통 날짜 인덱스 구성 ──────────────────────────────────────
    # term_dates와 price_dates의 교집합으로 정렬된 날짜 배열 구성
    common_dates = sorted(set(term_dates) & set(price_dates))
    if len(common_dates) < MIN_DATES:
        print(f"[WARN] Only {len(common_dates)} common dates — need {MIN_DATES}+")
        print("       Increase history by accumulating more _tokens.csv files.")

    t_idx = {d: i for i, d in enumerate(term_dates)}
    p_idx = {d: i for i, d in enumerate(price_dates)}

    print(f"Loaded: {len(top_terms)} terms × {len(price_tickers)} tickers")
    print(f"Common dates: {len(common_dates)}")
    if common_dates:
        print(f"Date range: {common_dates[0]} → {common_dates[-1]}")

    # ── 4. 공통 날짜에서 단어 Z-점수 시계열 구성 ──────────────────────
    term_z: dict[str, list] = {}
    for term in top_terms:
        full_counts = term_series.get(term, [])
        full_z      = rolling_zscore(full_counts)
        term_z[term] = [full_z[t_idx[d]] if d in t_idx else 0.0
                        for d in common_dates]

    # ── 5. 공통 날짜에서 가격 수익률 시계열 구성 ─────────────────────
    ticker_rets: dict[str, list] = {}
    for ticker in price_tickers:
        full_prices = price_close.get(ticker, [])
        full_rets   = daily_returns(full_prices)
        ticker_rets[ticker] = [full_rets[p_idx[d]] if d in p_idx else 0.0
                               for d in common_dates]

    # ── 6. 리드-래그 상관분석 ─────────────────────────────────────────
    pairs = []
    n = len(common_dates)

    for term in top_terms:
        z_series = term_z[term]
        # 이벤트 여부만 먼저 체크 (연산량 절감)
        hot_days = sum(1 for z in z_series if z >= args.z_threshold)
        if hot_days < MIN_EVENTS:
            continue

        for ticker in price_tickers:
            ret_series = ticker_rets[ticker]

            # 리드-래그별 상관계수 계산
            lag_corrs = {}
            for lag in LAG_RANGE:
                if lag < 0:
                    # 단어가 앞선다: z[0..n+lag] vs ret[-lag..n]
                    zs = z_series[:n+lag]
                    rs = ret_series[-lag:]
                elif lag > 0:
                    # 가격이 앞선다: z[lag..n] vs ret[0..n-lag]
                    zs = z_series[lag:]
                    rs = ret_series[:n-lag]
                else:
                    zs, rs = z_series, ret_series

                if len(zs) < 5:
                    lag_corrs[lag] = 0.0
                    continue
                lag_corrs[lag] = pearson(zs, rs)

            # 가장 강한 상관계수 (절대값 기준)
            best_lag  = max(lag_corrs, key=lambda l: abs(lag_corrs[l]))
            best_corr = lag_corrs[best_lag]

            if abs(best_corr) < args.min_corr:
                continue

            # ── 이벤트 기반 히트레이트 계산 ──────────────────────────
            # Z ≥ threshold 인 날 이후 best_lag 일 수익률의 방향 일치 비율
            event_returns = []
            for i, z in enumerate(z_series):
                if z < args.z_threshold:
                    continue
                target_i = i - best_lag  # lag 음수면 미래 날짜
                if 0 <= target_i < n:
                    event_returns.append(ret_series[target_i])

            if not event_returns:
                continue

            # 평균 수익률 (Z≥threshold 이벤트 이후 best_lag일)
            avg_ret = round(sum(event_returns)/len(event_returns), 3)

            # 히트레이트: 이벤트 후 수익률 방향이 corr 방향과 일치하는 비율
            if best_corr > 0:
                hits = sum(1 for r in event_returns if r > 0)
            else:
                hits = sum(1 for r in event_returns if r < 0)
            hit_rate = round(hits / len(event_returns), 3) if event_returns else 0.0

            # 1일, 5일 평균 수익률도 계산
            def avg_fwd_return(days):
                rets_fwd = []
                for i, z in enumerate(z_series):
                    if z < args.z_threshold:
                        continue
                    end_i = i + days
                    if end_i < n:
                        # 누적 수익률: 이벤트 후 days일 합산
                        cum = sum(ret_series[i+1:i+days+1])
                        rets_fwd.append(cum)
                return round(sum(rets_fwd)/len(rets_fwd), 3) if rets_fwd else 0.0

            pairs.append({
                "term":              term,
                "ticker":            ticker,
                "best_lag":          best_lag,
                "corr":              best_corr,
                "hit_rate":          hit_rate,
                "n_events":          len(event_returns),
                "avg_return_next1d": avg_fwd_return(1),
                "avg_return_next5d": avg_fwd_return(5),
                "signal_type":       classify_signal(best_lag, best_corr),
                "lag_corrs":         {str(k): round(v,4) for k,v in lag_corrs.items()},
            })

    # ── 7. 랭킹: 예측력 높은 순 ──────────────────────────────────────
    # leading 신호 우선, 히트레이트 × |corr| 기준
    def score(p):
        lead_bonus = 1.5 if p["signal_type"] in ("leading","leading_1d") else 1.0
        return abs(p["corr"]) * p["hit_rate"] * lead_bonus

    pairs.sort(key=score, reverse=True)

    # ── 8. 단어 통계 ──────────────────────────────────────────────────
    term_stats = {}
    for term in top_terms:
        zs = term_z.get(term, [])
        term_stats[term] = {
            "hot_days": sum(1 for z in zs if z >= args.z_threshold),
            "avg_z":    round(sum(zs)/len(zs), 3) if zs else 0.0,
            "max_z":    round(max(zs), 3) if zs else 0.0,
        }

    # ── 9. 저장 ───────────────────────────────────────────────────────
    out = {
        "updated":    datetime.utcnow().strftime("%Y-%m-%d"),
        "n_dates":    len(common_dates),
        "date_range": [common_dates[0], common_dates[-1]] if common_dates else [],
        "pairs":      pairs[:500],   # 상위 500쌍만 저장 (파일 크기 제한)
        "term_stats": term_stats,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2))

    # 요약 출력
    leading = [p for p in pairs if p["signal_type"] in ("leading","leading_1d")]
    print(f"\nwrote {out_path}")
    print(f"  total pairs: {len(pairs)}")
    print(f"  leading signals (predictive): {len(leading)}")

    if leading:
        print("\n  Top 10 predictive word→ticker signals:")
        print(f"  {'TERM':<18} {'TICKER':<7} {'LAG':>4} {'CORR':>6} {'HIT%':>5} "
              f"{'N':>3} {'1D%':>6} {'5D%':>6} TYPE")
        print("  " + "-"*75)
        for p in leading[:10]:
            print(f"  {p['term']:<18} {p['ticker']:<7} {p['best_lag']:>4}d "
                  f"{p['corr']:>+.3f} {p['hit_rate']*100:>4.0f}% "
                  f"{p['n_events']:>3} {p['avg_return_next1d']:>+.2f}% "
                  f"{p['avg_return_next5d']:>+.2f}%  {p['signal_type']}")
    else:
        print("\n  [INFO] No strong leading signals found yet.")
        print("         More history needed — aim for 90+ days of _tokens.csv files.")
        print(f"         Current: {len(common_dates)} common dates")


if __name__ == "__main__":
    main()
