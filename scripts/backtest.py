"""
backtest.py  v1
=============================
TA 신호 + 뉴스 Z-score 결합 백테스트 엔진

전략 유형:
  1. TA-only       — RSI/MACD/BB 신호만으로 진입/청산
  2. News-only     — 단어 Z-score 임계값 돌파로 진입
  3. Combined      — TA 신호 AND/OR 뉴스 Z ≥ threshold 동시 조건
  4. News-lead     — 뉴스 Z ≥ threshold 발생 후 N일 내 TA 확인

각 전략에 대해:
  - 진입/청산 날짜, 수익률
  - Hit rate, 평균 수익, 평균 손실, Profit factor
  - Sharpe ratio (일 수익률 기준, 연환산)
  - Max drawdown
  - Calmar ratio

출력:
  run/backtest_results.json     — 전체 결과
  run/backtest_summary.csv      — 전략별 요약 테이블
  run/backtest_trades.csv       — 개별 거래 내역

사용법:
  python scripts/backtest.py
  python scripts/backtest.py --ticker NVDA --news-term supply --news-z 2.0
  python scripts/backtest.py --strategy all --hold-days 5
"""

import argparse
import csv
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path


# ══════════════════════════════════════════════════════════════════════════════
# 1. 데이터 로더
# ══════════════════════════════════════════════════════════════════════════════

def load_ta(path: str) -> dict:
    try:
        d = json.loads(Path(path).read_text())
        return d.get("tickers", {})
    except Exception as e:
        sys.exit(f"technical_analysis.json 로드 실패: {e}")


def load_trends(path: str) -> dict:
    try:
        return json.loads(Path(path).read_text())
    except Exception:
        return {}


def build_news_series(T: dict) -> dict:
    """
    trends.json → {term: {date: z_score}}
    각 날짜별 개별 단어의 Z-score 시계열 반환
    """
    series  = T.get("series",  {})
    dates   = T.get("dates",   [])
    result  = {}

    def z_at(counts, i, window=28):
        if i < 3: return 0.0
        hist = counts[max(0, i - window): i]
        if not hist: return 0.0
        mean = sum(hist) / len(hist)
        std  = math.sqrt(sum((x - mean) ** 2 for x in hist) / len(hist))
        return (counts[i] - mean) / std if std >= 0.5 else 0.0

    for term, counts in series.items():
        result[term] = {d: round(z_at(counts, i), 3)
                        for i, d in enumerate(dates)}
    return result


def build_composite_news(T: dict, top_n: int = 20) -> dict:
    """
    날짜별 복합 뉴스 강도 = 상위 top_n 단어 |Z| 합산
    """
    series  = T.get("series",  {})
    dates   = T.get("dates",   [])
    zscores = T.get("zscores", {})

    hot = sorted(zscores, key=lambda t: abs(zscores[t]), reverse=True)[:top_n]

    def z_at(counts, i, window=28):
        if i < 3: return 0.0
        hist = counts[max(0, i - window): i]
        if not hist: return 0.0
        mean = sum(hist) / len(hist)
        std  = math.sqrt(sum((x - mean) ** 2 for x in hist) / len(hist))
        return (counts[i] - mean) / std if std >= 0.5 else 0.0

    result = {}
    for i, d in enumerate(dates):
        result[d] = round(sum(abs(z_at(series.get(t, []), i)) for t in hot), 3)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# 2. 성과 지표 계산
# ══════════════════════════════════════════════════════════════════════════════

def sharpe(returns: list, rf: float = 0.0, periods: int = 252) -> float:
    """연환산 Sharpe ratio. returns = 일별 수익률(소수)."""
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    var  = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    std  = math.sqrt(var)
    if std == 0:
        return None
    return round((mean - rf / periods) / std * math.sqrt(periods), 4)


def max_drawdown(equity_curve: list) -> float:
    """최대 낙폭 (0~1)."""
    peak = equity_curve[0]
    mdd  = 0.0
    for v in equity_curve:
        if v > peak:
            peak = v
        dd = (peak - v) / peak
        if dd > mdd:
            mdd = dd
    return round(mdd, 4)


def profit_factor(trades: list) -> float:
    """Profit Factor = gross_profit / gross_loss."""
    gross_profit = sum(t["pnl_pct"] for t in trades if t["pnl_pct"] > 0)
    gross_loss   = abs(sum(t["pnl_pct"] for t in trades if t["pnl_pct"] < 0))
    return round(gross_profit / gross_loss, 4) if gross_loss > 0 else None


def summarize_trades(trades: list, strategy_name: str) -> dict:
    if not trades:
        return {"strategy": strategy_name, "n_trades": 0}

    rets   = [t["pnl_pct"] / 100 for t in trades]
    wins   = [t for t in trades if t["pnl_pct"] > 0]
    losses = [t for t in trades if t["pnl_pct"] <= 0]

    equity = [1.0]
    for r in rets:
        equity.append(equity[-1] * (1 + r))

    total_ret = round((equity[-1] - 1) * 100, 3)
    mdd = max_drawdown(equity)
    sr  = sharpe(rets)
    pf  = profit_factor(trades)
    calmar = round(total_ret / (mdd * 100), 4) if mdd > 0 else None

    return {
        "strategy":         strategy_name,
        "n_trades":         len(trades),
        "hit_rate":         round(len(wins) / len(trades), 4),
        "avg_win_pct":      round(sum(t["pnl_pct"] for t in wins)  / len(wins),   4) if wins   else 0,
        "avg_loss_pct":     round(sum(t["pnl_pct"] for t in losses)/ len(losses), 4) if losses else 0,
        "total_return_pct": total_ret,
        "max_drawdown":     mdd,
        "sharpe":           sr,
        "profit_factor":    pf,
        "calmar":           calmar,
        "best_trade_pct":   round(max(t["pnl_pct"] for t in trades), 4),
        "worst_trade_pct":  round(min(t["pnl_pct"] for t in trades), 4),
        "avg_hold_days":    round(sum(t["hold_days"] for t in trades) / len(trades), 1),
    }


# ══════════════════════════════════════════════════════════════════════════════
# 3. 전략 정의
# ══════════════════════════════════════════════════════════════════════════════

class Strategy:
    """단순 Long-only, market-on-close, no transaction costs."""

    def __init__(self, name: str, hold_days: int = 5,
                 stop_loss_pct: float = None, take_profit_pct: float = None):
        self.name          = name
        self.hold_days     = hold_days
        self.stop_loss     = stop_loss_pct    # e.g. 5.0 = 5%
        self.take_profit   = take_profit_pct

    def run(self, records: list, entry_mask: list) -> list:
        """
        records    : TA records 리스트 (date, close, signals, ...)
        entry_mask : 같은 길이의 bool 리스트 — True인 날 다음 날 진입

        반환: trade 딕셔너리 리스트
        """
        n      = len(records)
        trades = []
        in_pos = False
        entry_idx = None
        entry_price = None

        i = 0
        while i < n:
            rec = records[i]
            c   = rec.get("close")
            if c is None:
                i += 1
                continue

            # 진입 체크
            if not in_pos and i < n - 1 and entry_mask[i]:
                # 다음 날 시가(close 근사) 진입
                entry_idx   = i + 1
                entry_price = records[entry_idx].get("close")
                if entry_price:
                    in_pos = True
                    i = entry_idx + 1
                    continue

            # 청산 체크
            if in_pos:
                days_held = i - entry_idx
                c_now     = rec.get("close")
                if c_now is None:
                    i += 1
                    continue

                pnl = (c_now - entry_price) / entry_price * 100

                # Stop loss
                if self.stop_loss and pnl <= -self.stop_loss:
                    trades.append(self._make_trade(
                        records, entry_idx, i, entry_price, c_now, "stop_loss"))
                    in_pos = False
                    i += 1
                    continue

                # Take profit
                if self.take_profit and pnl >= self.take_profit:
                    trades.append(self._make_trade(
                        records, entry_idx, i, entry_price, c_now, "take_profit"))
                    in_pos = False
                    i += 1
                    continue

                # Time-based exit
                if days_held >= self.hold_days:
                    trades.append(self._make_trade(
                        records, entry_idx, i, entry_price, c_now, "time"))
                    in_pos = False

            i += 1

        return trades

    def _make_trade(self, records, entry_idx, exit_idx,
                    entry_price, exit_price, exit_reason):
        return {
            "entry_date":   records[entry_idx]["date"],
            "exit_date":    records[exit_idx]["date"],
            "entry_price":  round(entry_price, 4),
            "exit_price":   round(exit_price, 4),
            "pnl_pct":      round((exit_price - entry_price) / entry_price * 100, 4),
            "hold_days":    exit_idx - entry_idx,
            "exit_reason":  exit_reason,
        }


# ══════════════════════════════════════════════════════════════════════════════
# 4. 진입 조건 빌더
# ══════════════════════════════════════════════════════════════════════════════

def mask_rsi_oversold(records: list, threshold: float = 30.0) -> list:
    """RSI < threshold → 다음 날 롱 진입."""
    return [bool(r.get("rsi14") and r["rsi14"] < threshold) for r in records]


def mask_rsi_overbought_short(records: list, threshold: float = 70.0) -> list:
    """RSI > threshold → 숏 신호 (여기서는 롱 청산 신호로만 사용)."""
    return [bool(r.get("rsi14") and r["rsi14"] > threshold) for r in records]


def mask_macd_bullish(records: list) -> list:
    return [bool("macd_bullish_cross" in (r.get("signals") or [])) for r in records]


def mask_bb_lower(records: list) -> list:
    return [bool("bb_lower_touch" in (r.get("signals") or [])) for r in records]


def mask_bb_upper(records: list) -> list:
    return [bool("bb_upper_touch" in (r.get("signals") or [])) for r in records]


def mask_news_z(records: list, news_by_date: dict, threshold: float = 2.0,
                direction: str = "above") -> list:
    """
    news_by_date: {date: z_score}  (단일 단어 또는 복합)
    direction: "above" = z >= threshold, "below" = z <= -threshold
    """
    out = []
    for r in records:
        z = news_by_date.get(r["date"], 0)
        if direction == "above":
            out.append(z >= threshold)
        else:
            out.append(z <= -threshold)
    return out


def mask_combined_and(mask_a: list, mask_b: list) -> list:
    return [a and b for a, b in zip(mask_a, mask_b)]


def mask_combined_or(mask_a: list, mask_b: list) -> list:
    return [a or b for a, b in zip(mask_a, mask_b)]


def mask_news_lead_ta(records: list,
                      news_mask: list,
                      ta_mask: list,
                      lead_window: int = 3) -> list:
    """
    뉴스 신호가 발생한 후 lead_window일 내에 TA 신호가 확인되면 진입.
    """
    n   = len(records)
    out = [False] * n
    for i in range(n):
        if not news_mask[i]:
            continue
        # lead_window일 안에 TA 신호 있는지 확인
        for j in range(i, min(i + lead_window + 1, n)):
            if ta_mask[j]:
                out[j] = True
                break
    return out


def mask_ta_confluence(records: list,
                       min_signals: int = 2) -> list:
    """
    여러 TA 신호가 동시에 발생할 때 진입 (신호 수 ≥ min_signals).
    Bullish signals: rsi_oversold, bb_lower_touch, macd_bullish_cross
    """
    bullish = {"rsi_oversold", "bb_lower_touch", "macd_bullish_cross"}
    out = []
    for r in records:
        sigs = r.get("signals") or []
        count = sum(1 for s in sigs if any(b in s for b in bullish))
        out.append(count >= min_signals)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 5. 전체 백테스트 실행
# ══════════════════════════════════════════════════════════════════════════════

def run_all_strategies(ticker: str, records: list,
                       news_composite: dict,
                       news_term_series: dict,
                       news_terms: list,
                       hold_days: int,
                       stop_loss: float,
                       take_profit: float,
                       news_z_thresh: float) -> list:
    """
    모든 전략을 실행하고 결과 리스트 반환.
    """
    strat = Strategy(
        name="base",
        hold_days=hold_days,
        stop_loss_pct=stop_loss,
        take_profit_pct=take_profit,
    )

    all_results = []

    # ── 1. TA-only 전략들 ──────────────────────────────────────────────────────
    strategies_ta = {
        "RSI_oversold_30":    mask_rsi_oversold(records, 30.0),
        "RSI_oversold_35":    mask_rsi_oversold(records, 35.0),
        "BB_lower_touch":     mask_bb_lower(records),
        "MACD_bullish_cross": mask_macd_bullish(records),
        "TA_confluence_2":    mask_ta_confluence(records, 2),
        "TA_confluence_3":    mask_ta_confluence(records, 3),
    }

    for name, mask in strategies_ta.items():
        trades = strat.run(records, mask)
        summary = summarize_trades(trades, f"{ticker}::{name}")
        summary["ticker"]   = ticker
        summary["category"] = "TA-only"
        summary["trades"]   = trades
        all_results.append(summary)

    # ── 2. News-only 전략들 ────────────────────────────────────────────────────
    # Composite news intensity
    comp_mask = mask_news_z(records, news_composite, news_z_thresh)
    trades = strat.run(records, comp_mask)
    summary = summarize_trades(trades, f"{ticker}::News_composite_z{news_z_thresh}")
    summary["ticker"]   = ticker
    summary["category"] = "News-only"
    summary["trades"]   = trades
    all_results.append(summary)

    # Individual term strategies
    for term in news_terms:
        term_series = news_term_series.get(term, {})
        if not term_series:
            continue
        n_mask = mask_news_z(records, term_series, news_z_thresh)
        if sum(n_mask) < 2:
            continue   # 신호 너무 적음
        trades = strat.run(records, n_mask)
        summary = summarize_trades(trades, f"{ticker}::News_{term}_z{news_z_thresh}")
        summary["ticker"]   = ticker
        summary["category"] = "News-only"
        summary["term"]     = term
        summary["trades"]   = trades
        all_results.append(summary)

    # ── 3. Combined 전략들 ─────────────────────────────────────────────────────
    rsi_mask = mask_rsi_oversold(records, 35.0)
    bb_mask  = mask_bb_lower(records)

    # News AND RSI
    and_mask = mask_combined_and(comp_mask, rsi_mask)
    if sum(and_mask) >= 2:
        trades  = strat.run(records, and_mask)
        summary = summarize_trades(trades, f"{ticker}::News_AND_RSI35")
        summary["ticker"]   = ticker
        summary["category"] = "Combined-AND"
        summary["trades"]   = trades
        all_results.append(summary)

    # News AND BB lower
    and_mask2 = mask_combined_and(comp_mask, bb_mask)
    if sum(and_mask2) >= 2:
        trades  = strat.run(records, and_mask2)
        summary = summarize_trades(trades, f"{ticker}::News_AND_BB_lower")
        summary["ticker"]   = ticker
        summary["category"] = "Combined-AND"
        summary["trades"]   = trades
        all_results.append(summary)

    # News OR RSI
    or_mask = mask_combined_or(comp_mask, rsi_mask)
    trades  = strat.run(records, or_mask)
    summary = summarize_trades(trades, f"{ticker}::News_OR_RSI35")
    summary["ticker"]   = ticker
    summary["category"] = "Combined-OR"
    summary["trades"]   = trades
    all_results.append(summary)

    # ── 4. News-lead-TA 전략 ──────────────────────────────────────────────────
    lead_mask = mask_news_lead_ta(records, comp_mask, rsi_mask, lead_window=3)
    if sum(lead_mask) >= 2:
        trades  = strat.run(records, lead_mask)
        summary = summarize_trades(trades, f"{ticker}::News_lead3d_RSI")
        summary["ticker"]   = ticker
        summary["category"] = "News-lead-TA"
        summary["trades"]   = trades
        all_results.append(summary)

    lead_mask2 = mask_news_lead_ta(records, comp_mask, bb_mask, lead_window=3)
    if sum(lead_mask2) >= 2:
        trades  = strat.run(records, lead_mask2)
        summary = summarize_trades(trades, f"{ticker}::News_lead3d_BB")
        summary["ticker"]   = ticker
        summary["category"] = "News-lead-TA"
        summary["trades"]   = trades
        all_results.append(summary)

    # ── 5. Term-specific lead-TA ───────────────────────────────────────────────
    for term in news_terms:
        term_series = news_term_series.get(term, {})
        if not term_series:
            continue
        t_mask = mask_news_z(records, term_series, news_z_thresh)
        lead   = mask_news_lead_ta(records, t_mask, rsi_mask, lead_window=3)
        if sum(lead) < 2:
            continue
        trades  = strat.run(records, lead)
        summary = summarize_trades(trades, f"{ticker}::Lead_{term}_RSI")
        summary["ticker"]   = ticker
        summary["category"] = "News-lead-TA"
        summary["term"]     = term
        summary["trades"]   = trades
        all_results.append(summary)

    return all_results


# ══════════════════════════════════════════════════════════════════════════════
# 6. Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(description="TA + News backtest engine")
    ap.add_argument("--ta",         default="site/data/technical_analysis.json")
    ap.add_argument("--trends",     default="site/data/trends.json")
    ap.add_argument("--out-dir",    default="run")
    ap.add_argument("--tickers",    default=None,
                    help="쉼표 구분 티커 (기본: 전체)")
    ap.add_argument("--hold-days",  type=int,   default=5)
    ap.add_argument("--stop-loss",  type=float, default=7.0,
                    help="Stop loss %% (기본 7%%)")
    ap.add_argument("--take-profit",type=float, default=15.0,
                    help="Take profit %% (기본 15%%)")
    ap.add_argument("--news-z",     type=float, default=2.0,
                    help="뉴스 진입 Z-score 임계값 (기본 2.0)")
    ap.add_argument("--top-terms",  type=int,   default=10,
                    help="개별 단어 전략에 사용할 상위 단어 수")
    ap.add_argument("--min-trades", type=int,   default=3,
                    help="최소 거래 수 필터")
    args = ap.parse_args()

    # ── Load ──────────────────────────────────────────────────────────────────
    ta_data = load_ta(args.ta)
    T       = load_trends(args.trends)

    if not T:
        print("trends.json 없음 — 뉴스 신호 없이 TA-only 실행")
    
    news_term_series = build_news_series(T) if T else {}
    news_composite   = build_composite_news(T) if T else {}

    # 개별 단어 전략용: 상위 N개 (|z| 기준)
    zscores   = T.get("zscores", {}) if T else {}
    top_terms = sorted(zscores, key=lambda t: abs(zscores[t]), reverse=True)[: args.top_terms]
    print(f"Top terms for news strategy: {top_terms}")

    tickers = [t.strip().upper() for t in args.tickers.split(",")]\
              if args.tickers else list(ta_data.keys())

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    all_results  = []
    all_trades   = []

    for ticker in tickers:
        if ticker not in ta_data:
            print(f"  [SKIP] {ticker}")
            continue
        records = ta_data[ticker].get("records", [])
        if len(records) < 20:
            print(f"  [SKIP] {ticker} — too few records ({len(records)})")
            continue

        results = run_all_strategies(
            ticker, records,
            news_composite, news_term_series, top_terms,
            args.hold_days, args.stop_loss, args.take_profit, args.news_z,
        )

        for r in results:
            if r["n_trades"] >= args.min_trades:
                # Attach trades separately
                trades = r.pop("trades", [])
                for t in trades:
                    t["strategy"] = r["strategy"]
                    t["ticker"]   = ticker
                all_trades.extend(trades)
                all_results.append(r)

        # Quick print
        best = sorted(
            [r for r in results if r["n_trades"] >= args.min_trades],
            key=lambda x: (x.get("sharpe") or -999), reverse=True
        )[:3]
        if best:
            print(f"\n  {ticker} — top 3 by Sharpe:")
            for r in best:
                print(f"    {r['strategy']:<45} "
                      f"n={r['n_trades']:2d}  "
                      f"hit={r.get('hit_rate',0):.0%}  "
                      f"ret={r.get('total_return_pct',0):+.2f}%  "
                      f"sharpe={r.get('sharpe') or 'N/A'}  "
                      f"mdd={r.get('max_drawdown',0):.1%}")

    # ── Write outputs ──────────────────────────────────────────────────────────
    # Full JSON
    full_path = out_dir / "backtest_results.json"
    full_path.write_text(json.dumps({
        "updated":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "params": {
            "hold_days":   args.hold_days,
            "stop_loss":   args.stop_loss,
            "take_profit": args.take_profit,
            "news_z":      args.news_z,
        },
        "results": all_results,
    }, ensure_ascii=False), encoding="utf-8")

    # Summary CSV
    sum_path = out_dir / "backtest_summary.csv"
    SUMMARY_FIELDS = [
        "strategy","ticker","category","term",
        "n_trades","hit_rate","avg_win_pct","avg_loss_pct",
        "total_return_pct","max_drawdown","sharpe","profit_factor",
        "calmar","best_trade_pct","worst_trade_pct","avg_hold_days",
    ]
    with open(sum_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS, extrasaction="ignore")
        w.writeheader()
        # Sort by Sharpe desc
        for r in sorted(all_results,
                        key=lambda x: (x.get("sharpe") or -999), reverse=True):
            w.writerow({k: r.get(k, "") for k in SUMMARY_FIELDS})

    # Trade log CSV
    trade_path = out_dir / "backtest_trades.csv"
    TRADE_FIELDS = [
        "strategy","ticker","entry_date","exit_date",
        "entry_price","exit_price","pnl_pct","hold_days","exit_reason",
    ]
    with open(trade_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=TRADE_FIELDS, extrasaction="ignore")
        w.writeheader()
        for t in sorted(all_trades, key=lambda x: x.get("entry_date", "")):
            w.writerow({k: t.get(k, "") for k in TRADE_FIELDS})

    print(f"\n{'='*60}")
    print(f"→ {full_path}   ({len(all_results)} strategies)")
    print(f"→ {sum_path}")
    print(f"→ {trade_path}  ({len(all_trades)} trades)")

    # ── Global leaderboard ─────────────────────────────────────────────────────
    ranked = sorted(
        [r for r in all_results if r.get("sharpe") is not None],
        key=lambda x: x["sharpe"], reverse=True
    )[:15]

    if ranked:
        print(f"\n{'='*60}")
        print("GLOBAL LEADERBOARD — Top 15 by Sharpe ratio")
        print(f"{'Strategy':<48} {'n':>3}  {'hit':>5}  {'ret%':>7}  "
              f"{'sharpe':>7}  {'mdd':>6}  {'PF':>5}")
        print("-" * 90)
        for r in ranked:
            pf = r.get("profit_factor")
            print(f"  {r['strategy']:<46} "
                  f"{r['n_trades']:3d}  "
                  f"{r.get('hit_rate',0):5.0%}  "
                  f"{r.get('total_return_pct',0):+7.2f}%  "
                  f"{r.get('sharpe') or 0:7.4f}  "
                  f"{r.get('max_drawdown',0):6.1%}  "
                  f"{pf if pf else '—':>5}")

    # ── Category summary ───────────────────────────────────────────────────────
    from collections import defaultdict
    cat_stats = defaultdict(list)
    for r in all_results:
        if r["n_trades"] >= args.min_trades:
            cat_stats[r.get("category","?")].append(r)

    print(f"\n{'='*60}")
    print("CATEGORY SUMMARY (avg Sharpe, avg hit rate)")
    for cat, rs in sorted(cat_stats.items()):
        sharpes = [r["sharpe"] for r in rs if r.get("sharpe") is not None]
        hits    = [r["hit_rate"] for r in rs if r.get("hit_rate") is not None]
        avg_sh  = sum(sharpes)/len(sharpes) if sharpes else 0
        avg_hit = sum(hits)/len(hits) if hits else 0
        print(f"  {cat:<20} n_strats={len(rs):3d}  "
              f"avg_sharpe={avg_sh:+.4f}  avg_hit={avg_hit:.0%}")


if __name__ == "__main__":
    main()
