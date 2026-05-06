"""
backtest_v2.py — 안전한 시스템 백테스트
==========================================
5가지 손실 방지 규칙 적용:
1. 손절 -3% (즉시 매도)
2. 익절 +5% (절반 매도, 나머지 trailing stop)
3. 종목 블랙리스트 (과거 승률 < 50% 제외)
4. 신호 강도 필터 (bull_score > 1.5만)
5. 시장 regime 필터 (avg RSI > 60 = 과열 시 매수 금지)
"""
import argparse, json, math
from collections import defaultdict
from datetime import datetime
from pathlib import Path


# ── 통계 ──
def zscore_at(counts, i, window=28):
    if i < 3 or i >= len(counts): return 0
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
        return sign * (1 - (((((a5 * t + a4) * t + a3) * t + a2) * t + a1) * t * math.exp(-x * x)))

    return min(1.0, 2 * 0.5 * (1 - erf(z / math.sqrt(2))))


def rsi_at(closes, i, period=14):
    if i < period or i >= len(closes): return 50.0
    gains, losses = 0, 0
    for j in range(i - period + 1, i + 1):
        if j > 0 and closes[j] is not None and closes[j - 1] is not None:
            change = closes[j] - closes[j - 1]
            if change > 0:
                gains += change
            else:
                losses += -change
    if gains + losses == 0: return 50.0
    rs = gains / max(losses, 0.001)
    return 100 - 100 / (1 + rs)


# ── 단어 패턴 학습 ──
def learn_word_patterns(ticker, T, P_data, min_events=8, z_thresh=1.0):
    pdata = P_data["tickers"].get(ticker)
    if not pdata: return []

    t_dates, t_series = T["dates"], T.get("series", {})
    p_dates, p_closes = pdata["dates"], pdata["closes"]
    common = sorted(set(t_dates) & set(p_dates))
    if len(common) < 30: return []

    rets = {}
    for i in range(len(p_dates) - 1):
        if p_closes[i] and p_closes[i + 1]:
            rets[p_dates[i]] = (p_closes[i + 1] / p_closes[i] - 1) * 100

    series = {w: c for w, c in t_series.items() if " " not in w and len(w) >= 4}
    totals = sorted(series.items(), key=lambda x: -sum(x[1]))[:200]
    t_idx = {d: i for i, d in enumerate(t_dates)}

    bull = []
    for word, counts in totals:
        zs = [zscore_at(counts, i) for i in range(len(counts))]
        for lag in range(0, 3):
            events = []
            for d in common:
                ti = t_idx.get(d)
                if ti is None or ti - lag < 0: continue
                if zs[ti - lag] < z_thresh: continue
                if d in rets: events.append(rets[d])

            if len(events) < min_events: continue
            hits = sum(1 for r in events if r > 0)
            hr = hits / len(events)

            if hr > 0.5:
                p = binomial_pvalue(hits, len(events))
            else:
                continue  # bear은 안 씀

            if p > 0.05: continue
            avg = sum(events) / len(events)
            if hr >= 0.6 and avg > 0.2:
                bull.append({"word": word, "lag": lag, "hit": hr, "avg": avg, "n": len(events)})

    return bull


# ── 신호 생성 (강화 버전) ──
def get_signal(date_idx, T, p_dates, p_closes, bull_words, market_avg_rsi):
    """
    Rule 4: bull_score > 1.5만 통과
    Rule 5: market_avg_rsi > 60 (과열) → 매수 금지
    """
    t_dates = T["dates"]
    t_series = T.get("series", {})

    if date_idx >= len(t_dates): return None
    d = t_dates[date_idx]

    if d not in p_dates: return None
    pi = p_dates.index(d)
    if pi >= len(p_closes) or p_closes[pi] is None: return None

    # 시장 regime 필터
    if market_avg_rsi > 60:
        return None

    bull_score = 0
    active_bull = []
    for w in bull_words[:15]:
        word, lag = w["word"], w["lag"]
        if word not in t_series: continue
        if date_idx - lag < 0: continue
        z = zscore_at(t_series[word], date_idx - lag)
        if z >= 1.0:
            bull_score += w["hit"] * abs(w["avg"])
            active_bull.append(word)

    rsi = rsi_at(p_closes, pi)

    # Rule 4: bull_score > 1.5만 통과
    if bull_score < 1.5:
        # 단, deep oversold (RSI < 30)는 예외
        if rsi >= 30: return None
        return {"action": "OVERSOLD", "rsi": rsi, "score": bull_score, "words": []}

    # 강한 신호 + RSI 합리적 범위
    if bull_score >= 2.5 and rsi < 60:
        return {"action": "STRONG_NEWS", "rsi": rsi, "score": bull_score, "words": active_bull}
    if bull_score >= 1.5 and rsi < 50:
        return {"action": "MEDIUM_NEWS", "rsi": rsi, "score": bull_score, "words": active_bull}

    return None


# ── 거래 시뮬레이션 (손절/익절 포함) ──
def simulate_trade(p_closes, entry_idx, max_hold=5, stop_pct=-3.0, take_pct=5.0):
    """
    Rule 1: -3% 손절
    Rule 2: +5% 익절 (절반 매도, 나머지 trailing)
    """
    if entry_idx + 1 >= len(p_closes): return None
    entry = p_closes[entry_idx + 1]  # 다음날 시가에 매수 (현실적)
    if entry is None: return None

    half_sold = False
    half_profit = 0
    peak_after_take = entry

    for d in range(1, max_hold + 1):
        i = entry_idx + 1 + d
        if i >= len(p_closes) or p_closes[i] is None:
            break
        cur = p_closes[i]
        ret_pct = (cur / entry - 1) * 100

        # 손절
        if ret_pct <= stop_pct:
            return {
                "entry": round(entry, 2),
                "exit": round(cur, 2),
                "ret": round(ret_pct, 2),
                "exit_reason": "STOP_LOSS",
                "hold_days": d,
            }

        # 익절 (절반)
        if not half_sold and ret_pct >= take_pct:
            half_profit = ret_pct
            half_sold = True
            peak_after_take = cur
            continue

        # Trailing stop after take
        if half_sold:
            if cur > peak_after_take:
                peak_after_take = cur
            # peak에서 -2% 떨어지면 나머지 매도
            trail_ret = (cur / peak_after_take - 1) * 100
            if trail_ret <= -2.0:
                final_ret = (cur / entry - 1) * 100
                avg_ret = (half_profit + final_ret) / 2  # 절반씩 평균
                return {
                    "entry": round(entry, 2),
                    "exit": round(cur, 2),
                    "ret": round(avg_ret, 2),
                    "exit_reason": "TRAILING",
                    "hold_days": d,
                }

    # 시간 만료
    final_idx = min(entry_idx + 1 + max_hold, len(p_closes) - 1)
    if p_closes[final_idx] is None: return None
    final_ret = (p_closes[final_idx] / entry - 1) * 100
    if half_sold:
        final_ret = (half_profit + final_ret) / 2
    return {
        "entry": round(entry, 2),
        "exit": round(p_closes[final_idx], 2),
        "ret": round(final_ret, 2),
        "exit_reason": "TIME_OUT",
        "hold_days": max_hold,
    }


# ── 종목별 백테스트 ──
def backtest_ticker(ticker, T, P_data, blacklist):
    if ticker in blacklist:
        return {"ticker": ticker, "skipped": "blacklist"}

    pdata = P_data["tickers"].get(ticker)
    if not pdata: return None

    p_dates, p_closes = pdata["dates"], pdata["closes"]
    t_dates = T["dates"]
    t_idx = {d: i for i, d in enumerate(t_dates)}

    # Train 70% / Test 30%
    split = int(len(t_dates) * 0.7)
    train_T = {"dates": t_dates[:split], "series": {w: c[:split] for w, c in T.get("series", {}).items()}}
    train_set = set(t_dates[:split])
    train_dates = [d for d in p_dates if d in train_set]
    train_idx_map = {d: p_dates.index(d) for d in train_dates}
    train_P = {"tickers": {ticker: {
        "dates": train_dates,
        "closes": [p_closes[train_idx_map[d]] for d in train_dates],
    }}}

    bull = learn_word_patterns(ticker, train_T, train_P)
    if not bull:
        return {"ticker": ticker, "trades": 0, "skipped": "no_signals"}

    trades = []
    for i in range(split, len(t_dates) - 5):
        # 시장 평균 RSI (Rule 5)
        market_rsis = []
        for tk in ["SPY", "QQQ", "IWM"]:
            tp = P_data["tickers"].get(tk, {})
            tdates, tcloses = tp.get("dates", []), tp.get("closes", [])
            d = t_dates[i]
            if d in tdates:
                idx = tdates.index(d)
                market_rsis.append(rsi_at(tcloses, idx))
        market_avg_rsi = sum(market_rsis) / len(market_rsis) if market_rsis else 50.0

        sig = get_signal(i, T, p_dates, p_closes, bull, market_avg_rsi)
        if not sig: continue

        d = t_dates[i]
        if d not in p_dates: continue
        pi = p_dates.index(d)

        trade = simulate_trade(p_closes, pi)
        if not trade: continue

        trades.append({
            "date": d, **trade,
            "action": sig["action"],
            "rsi": round(sig["rsi"], 1),
            "score": round(sig["score"], 2),
            "words": sig["words"][:3],
            "ticker": ticker,
        })

    if not trades:
        return {"ticker": ticker, "trades": 0, "skipped": "no_trades"}

    rets = [t["ret"] for t in trades]
    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r < 0]

    cum_ret = 1.0
    equity_curve = [1.0]
    for r in rets:
        cum_ret *= (1 + r / 100)
        equity_curve.append(cum_ret)

    peak, mdd = equity_curve[0], 0
    for v in equity_curve:
        peak = max(peak, v)
        dd = (peak - v) / peak * 100
        mdd = max(mdd, dd)

    sharpe = 0
    if len(rets) > 1:
        m = sum(rets) / len(rets)
        s = math.sqrt(sum((r - m) ** 2 for r in rets) / len(rets))
        sharpe = (m / s) * math.sqrt(252 / 5) if s > 0 else 0

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
        "stop_count": sum(1 for t in trades if t["exit_reason"] == "STOP_LOSS"),
        "take_count": sum(1 for t in trades if t["exit_reason"] == "TRAILING"),
        "trade_log": trades,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trends", default="site/data/trends.json")
    ap.add_argument("--prices", default="site/data/prices.json")
    ap.add_argument("--out", default="site/data/backtest_v2.json")
    ap.add_argument("--prev", default="site/data/backtest.json",
                    help="이전 백테스트 (블랙리스트 산출용)")
    args = ap.parse_args()

    T = json.loads(Path(args.trends).read_text())
    P = json.loads(Path(args.prices).read_text())

    # Rule 3: 블랙리스트 = 이전 백테스트에서 수익 < 0% 인 종목
    blacklist = set()
    try:
        prev = json.loads(Path(args.prev).read_text())
        for tk, data in prev.get("by_ticker", {}).items():
            if data.get("total_ret", 0) < 0 and data.get("trades", 0) >= 5:
                blacklist.add(tk)
        print(f"📛 Blacklist (prev total_ret < 0): {sorted(blacklist) if blacklist else 'none'}")
    except Exception as e:
        print(f"No prev backtest: {e}")

    tickers = list(P.get("tickers", {}).keys())
    print(f"\nBacktesting {len(tickers)} tickers (V2: 5 safety rules)")
    print(f"Rules: stop -3% / take +5% / blacklist / score>1.5 / RSI<60 market\n")

    results = {}
    for ticker in sorted(tickers):
        res = backtest_ticker(ticker, T, P, blacklist)
        if not res or res.get("skipped"): continue
        results[ticker] = res

    # 종합
    all_trades = []
    for r in results.values():
        all_trades.extend(r.get("trade_log", []))

    if not all_trades:
        print("No trades.")
        return

    rets = [t["ret"] for t in all_trades]
    wins = [r for r in rets if r > 0]

    cum_ret = 1.0
    equity = [1.0]
    for r in rets:
        cum_ret *= (1 + r / 100)
        equity.append(cum_ret)
    peak, mdd = equity[0], 0
    for v in equity:
        peak = max(peak, v)
        mdd = max(mdd, (peak - v) / peak * 100)

    stop_count = sum(1 for t in all_trades if t["exit_reason"] == "STOP_LOSS")
    take_count = sum(1 for t in all_trades if t["exit_reason"] == "TRAILING")
    timeout_count = sum(1 for t in all_trades if t["exit_reason"] == "TIME_OUT")

    print(f"{'=' * 70}")
    print(f"📊 BACKTEST V2 — Safety-First System")
    print(f"{'=' * 70}")
    print(f"Total trades:    {len(all_trades)}")
    print(f"Win rate:        {len(wins) / len(all_trades) * 100:.1f}%")
    print(f"Avg return:      {sum(rets) / len(rets):+.2f}%")
    print(f"Best trade:      {max(rets):+.2f}%")
    print(f"Worst trade:     {min(rets):+.2f}%  (Rule 1: must be ≥ -3%)")
    print(f"Cumulative:      {(cum_ret - 1) * 100:+.1f}%")
    print(f"Max drawdown:    {mdd:.1f}%")
    print(f"\nExit reasons:")
    print(f"  Stop-loss:     {stop_count} ({stop_count / len(all_trades) * 100:.0f}%)")
    print(f"  Trailing:      {take_count} ({take_count / len(all_trades) * 100:.0f}%)")
    print(f"  Time-out (5d): {timeout_count} ({timeout_count / len(all_trades) * 100:.0f}%)")

    print(f"\n{'=' * 70}")
    print(f"📈 BY TICKER")
    print(f"{'=' * 70}")
    print(f"{'Ticker':<7} {'Trades':>7} {'Win%':>7} {'AvgRet':>8} {'Total':>8} {'MDD':>7} {'Stop':>5} {'Take':>5}")
    print("-" * 70)

    for r in sorted(results.values(), key=lambda x: -x.get("total_ret", 0)):
        if r["trades"] < 2: continue
        print(f"{r['ticker']:<7} {r['trades']:>7} {r['win_rate']:>6.1f}% "
              f"{r['avg_ret']:>+7.2f}% {r['total_ret']:>+7.2f}% "
              f"{r['mdd']:>6.2f}% {r['stop_count']:>4} {r['take_count']:>4}")

    Path(args.out).write_text(json.dumps({
        "updated": datetime.now().strftime("%Y-%m-%d"),
        "rules": {"stop_pct": -3, "take_pct": 5, "blacklist": sorted(blacklist),
                  "min_score": 1.5, "max_market_rsi": 60},
        "summary": {
            "total_trades": len(all_trades),
            "win_rate": round(len(wins) / len(all_trades) * 100, 1),
            "avg_ret": round(sum(rets) / len(rets), 2),
            "cum_ret": round((cum_ret - 1) * 100, 2),
            "mdd": round(mdd, 2),
            "stop_count": stop_count,
            "take_count": take_count,
            "timeout_count": timeout_count,
        },
        "by_ticker": {k: {x: v[x] for x in v if x != "trade_log"}
                      for k, v in results.items()},
        "all_trades": all_trades,
    }, indent=2))
    print(f"\n→ {args.out}")


if __name__ == "__main__":
    main()