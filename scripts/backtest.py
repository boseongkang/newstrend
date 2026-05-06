"""
backtest.py — 시스템 신호 풀 백테스트
======================================
213일 동안 매일 BUY 신호 발생 시 5일 보유 시뮬레이션.

전략:
- 매일 새 BUY/WATCH 신호 발생 시 즉시 매수 (다음날 시가)
- 5거래일 보유 후 매도 (종가)
- HOLD/SELL/REDUCE는 거래 안함 (관찰만)
- 분석: 종목 + 단어 신호 + RSI 조합으로 BUY 결정

비교:
- SPY 매수후 보유 (Buy & Hold benchmark)
- 시스템 BUY only (active)
"""
import argparse
import json
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path


# ── 통계 ──────────────────────────────────────────────────────────────────────
def zscore_at(counts, i, window=28):
    if i < 3 or i >= len(counts):
        return 0
    hist = counts[max(0, i - window):i]
    if not hist: return 0
    mean = sum(hist) / len(hist)
    std = math.sqrt(sum((x - mean) ** 2 for x in hist) / len(hist))
    return (counts[i] - mean) / std if std >= 0.5 else 0


def binomial_pvalue(hits, n, p_null=0.5):
    if n == 0: return 1.0
    mean, std = n * p_null, math.sqrt(n * p_null * (1 - p_null))
    if std < 1e-9: return 1.0
    z = abs(hits - mean) / std
    def erf(x):
        a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
        p, sign, x = 0.3275911, (1 if x >= 0 else -1), abs(x)
        t = 1.0 / (1.0 + p * x)
        return sign * (1 - (((((a5*t+a4)*t+a3)*t+a2)*t+a1)*t * math.exp(-x*x)))
    return min(1.0, 2 * 0.5 * (1 - erf(z / math.sqrt(2))))


def rsi_at(closes, i, period=14):
    """간단한 RSI (Wilder)."""
    if i < period or i >= len(closes):
        return 50.0

    gains, losses = 0, 0
    for j in range(i - period + 1, i + 1):
        if j > 0 and closes[j] is not None and closes[j-1] is not None:
            change = closes[j] - closes[j-1]
            if change > 0: gains += change
            else: losses += -change

    if gains + losses == 0: return 50.0
    rs = gains / max(losses, 0.001)
    return 100 - 100 / (1 + rs)


# ── 단일 종목 단어 패턴 학습 (전체 기간) ──────────────────────────────────────
def learn_word_patterns(ticker, T, P_data, min_events=8, z_thresh=1.0, max_lag=2):
    """과거 전체 기간에서 검증된 단어 패턴 추출."""
    pdata = P_data["tickers"].get(ticker)
    if not pdata: return [], []

    t_dates, t_series = T["dates"], T.get("series", {})
    p_dates, p_closes = pdata["dates"], pdata["closes"]
    p_idx = {d: i for i, d in enumerate(p_dates)}
    t_idx = {d: i for i, d in enumerate(t_dates)}
    common = sorted(set(t_dates) & set(p_dates))
    if len(common) < 30: return [], []

    # 종목 수익률 (다음날 종가 vs 오늘 종가)
    rets = {}
    for i in range(len(p_dates) - 1):
        if p_closes[i] and p_closes[i+1]:
            rets[p_dates[i]] = (p_closes[i+1] / p_closes[i] - 1) * 100

    # Top 200 단어
    series = {w: c for w, c in t_series.items() if " " not in w and len(w) >= 4}
    totals = sorted(series.items(), key=lambda x: -sum(x[1]))[:200]

    bull, bear = [], []
    for word, counts in totals:
        zs = [zscore_at(counts, i) for i in range(len(counts))]
        for lag in range(0, max_lag + 1):
            events = []
            for d in common:
                ti = t_idx.get(d)
                if ti is None or ti - lag < 0: continue
                z = zs[ti - lag]
                if z < z_thresh: continue
                if d in rets:
                    events.append(rets[d])

            if len(events) < min_events: continue
            hits = sum(1 for r in events if r > 0)
            hr = hits / len(events)

            # 통계 검정
            if hr > 0.5:
                p = binomial_pvalue(hits, len(events))
            else:
                p = binomial_pvalue(len(events) - hits, len(events))

            if p > 0.05: continue

            avg = sum(events) / len(events)
            entry = {"word": word, "lag": lag, "hit": hr, "avg": avg, "n": len(events), "p": p}

            if hr >= 0.6 and avg > 0.2:
                bull.append(entry)
            elif hr <= 0.4 and avg < -0.2:
                bear.append(entry)

    return bull, bear


# ── 전략: 매일 BUY 신호 생성 ──────────────────────────────────────────────────
def get_signal(ticker, date_idx, T, P_data, bull_words, bear_words, t_idx, p_dates, p_closes):
    """특정 날짜의 매수 신호 (활성 단어 + RSI 조합)."""
    t_dates = T["dates"]
    t_series = T.get("series", {})

    if date_idx >= len(t_dates): return None
    d = t_dates[date_idx]
    pi = p_dates.index(d) if d in p_dates else None
    if pi is None or pi + 6 >= len(p_closes): return None

    cur_price = p_closes[pi]
    if cur_price is None: return None

    # 활성 강세 단어 점수
    bull_score = 0
    active_bull = []
    for w in bull_words[:15]:
        word, lag = w["word"], w["lag"]
        if word not in t_series: continue
        if date_idx - lag < 0: continue
        z = zscore_at(t_series[word], date_idx - lag)
        if z >= z_thresh:
            bull_score += w["hit"] * abs(w["avg"])
            active_bull.append(word)

    # RSI
    rsi = rsi_at(p_closes, pi)

    # 매수 결정 로직
    # 1) 강한 뉴스 신호 (>2.0) + RSI 50 이하
    # 2) 중간 뉴스 신호 (>1.0) + RSI < 35 (oversold)
    # 3) RSI < 30 (deep oversold)

    if bull_score >= 2.5 and rsi < 60:
        return {"action": "BUY_STRONG_NEWS", "rsi": rsi, "score": bull_score, "words": active_bull}
    if bull_score >= 1.0 and rsi < 40:
        return {"action": "BUY_NEWS_OVERSOLD", "rsi": rsi, "score": bull_score, "words": active_bull}
    if rsi < 30:
        return {"action": "BUY_DEEP_OVERSOLD", "rsi": rsi, "score": bull_score, "words": []}

    return None


# ── 백테스트 메인 ─────────────────────────────────────────────────────────────
z_thresh = 1.0  # 글로벌

def backtest_ticker(ticker, T, P_data, hold_days=5):
    """종목별 백테스트."""
    pdata = P_data["tickers"].get(ticker)
    if not pdata: return None

    p_dates, p_closes = pdata["dates"], pdata["closes"]
    t_dates = T["dates"]
    t_idx = {d: i for i, d in enumerate(t_dates)}

    # 단어 패턴 학습 (앞 70%만 사용 = 과적합 방지)
    split = int(len(t_dates) * 0.7)
    train_T = {"dates": t_dates[:split], "series": {w: c[:split] for w, c in T.get("series", {}).items()}}
    train_P = {"tickers": {ticker: {
        "dates": [d for d in p_dates if d in set(t_dates[:split])],
        "closes": [p_closes[p_dates.index(d)] for d in p_dates if d in set(t_dates[:split])],
    }}}
    bull, bear = learn_word_patterns(ticker, train_T, train_P)

    if not bull:
        return {"ticker": ticker, "trades": 0, "skip": "no_bull_signals"}

    # 백테스트 (뒤 30%에서 신호 적용)
    test_start = split
    trades = []

    for i in range(test_start, len(t_dates) - hold_days):
        sig = get_signal(ticker, i, T, P_data, bull, bear, t_idx, p_dates, p_closes)
        if not sig: continue

        d = t_dates[i]
        if d not in p_dates: continue
        pi = p_dates.index(d)

        if pi + hold_days >= len(p_closes): continue
        entry = p_closes[pi]
        exit_price = p_closes[pi + hold_days]
        if entry is None or exit_price is None: continue

        ret = (exit_price / entry - 1) * 100
        trades.append({
            "date": d,
            "entry": round(entry, 2),
            "exit": round(exit_price, 2),
            "ret": round(ret, 2),
            "action": sig["action"],
            "rsi": round(sig["rsi"], 1),
            "score": round(sig["score"], 2),
            "words": sig["words"][:3],
        })

    if not trades:
        return {"ticker": ticker, "trades": 0, "skip": "no_trades_in_test"}

    rets = [t["ret"] for t in trades]
    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r < 0]

    # 누적 수익률 (복리)
    cum_ret = 1.0
    equity_curve = [1.0]
    for r in rets:
        cum_ret *= (1 + r/100)
        equity_curve.append(cum_ret)

    # MDD
    peak = equity_curve[0]
    mdd = 0
    for v in equity_curve:
        peak = max(peak, v)
        dd = (peak - v) / peak * 100
        mdd = max(mdd, dd)

    # Sharpe (단순화)
    if len(rets) > 1:
        mean_ret = sum(rets) / len(rets)
        std = math.sqrt(sum((r - mean_ret)**2 for r in rets) / len(rets))
        sharpe = (mean_ret / std) * math.sqrt(252/5) if std > 0 else 0
    else:
        sharpe = 0

    return {
        "ticker": ticker,
        "trades": len(trades),
        "win_rate": round(len(wins) / len(trades) * 100, 1),
        "avg_ret": round(sum(rets) / len(rets), 2),
        "avg_win": round(sum(wins) / len(wins), 2) if wins else 0,
        "avg_loss": round(sum(losses) / len(losses), 2) if losses else 0,
        "total_ret": round((cum_ret - 1) * 100, 2),
        "mdd": round(mdd, 2),
        "sharpe": round(sharpe, 2),
        "trade_log": trades,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trends", default="site/data/trends.json")
    ap.add_argument("--prices", default="site/data/prices.json")
    ap.add_argument("--out",    default="site/data/backtest.json")
    ap.add_argument("--hold-days", type=int, default=5)
    ap.add_argument("--tickers", default=None)
    args = ap.parse_args()

    T = json.loads(Path(args.trends).read_text())
    P = json.loads(Path(args.prices).read_text())

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]
    else:
        tickers = list(P.get("tickers", {}).keys())

    print(f"Backtesting {len(tickers)} tickers...")
    print(f"News data: {T['dates'][0]} ~ {T['dates'][-1]} ({len(T['dates'])} days)")
    print(f"Train: 70% / Test: 30% (out-of-sample)\n")

    results = {}
    for ticker in sorted(tickers):
        res = backtest_ticker(ticker, T, P, args.hold_days)
        if not res or res.get("skip"):
            continue
        results[ticker] = res

    # 종합
    all_trades = []
    for r in results.values():
        all_trades.extend(r.get("trade_log", []))

    if not all_trades:
        print("No trades generated.")
        return

    rets = [t["ret"] for t in all_trades]
    wins = [r for r in rets if r > 0]

    cum_ret = 1.0
    for r in rets: cum_ret *= (1 + r/100)

    print(f"{'='*70}")
    print(f"📊 BACKTEST RESULTS")
    print(f"{'='*70}")
    print(f"Total trades:  {len(all_trades)}")
    print(f"Win rate:      {len(wins)/len(all_trades)*100:.1f}%")
    print(f"Avg return:    {sum(rets)/len(rets):+.2f}% per trade")
    print(f"Best trade:    {max(rets):+.2f}%")
    print(f"Worst trade:   {min(rets):+.2f}%")
    print(f"Cumulative:    {(cum_ret - 1)*100:+.1f}% (compounded)")
    print(f"\n{'='*70}")
    print(f"📈 BY TICKER (sorted by total return)")
    print(f"{'='*70}")
    print(f"{'Ticker':<7} {'Trades':>7} {'Win%':>7} {'AvgRet':>8} {'Total':>8} {'MDD':>7} {'Sharpe':>8}")
    print("-" * 70)

    sorted_tickers = sorted(results.values(), key=lambda x: -x.get("total_ret", 0))
    for r in sorted_tickers:
        if r["trades"] < 3: continue
        print(f"{r['ticker']:<7} {r['trades']:>7} {r['win_rate']:>6.1f}% "
              f"{r['avg_ret']:>+7.2f}% {r['total_ret']:>+7.2f}% "
              f"{r['mdd']:>6.2f}% {r['sharpe']:>7.2f}")

    # Save
    Path(args.out).write_text(json.dumps({
        "updated": datetime.now().strftime("%Y-%m-%d"),
        "summary": {
            "total_trades": len(all_trades),
            "win_rate": round(len(wins) / len(all_trades) * 100, 1),
            "avg_ret": round(sum(rets) / len(rets), 2),
            "cum_ret": round((cum_ret - 1) * 100, 2),
        },
        "by_ticker": {k: {x: v[x] for x in v if x != "trade_log"}
                      for k, v in results.items()},
        "all_trades": all_trades,
    }, indent=2))

    print(f"\n→ {args.out}")


if __name__ == "__main__":
    main()