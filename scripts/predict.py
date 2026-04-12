"""
predict.py — Real-time buy/hold/sell signal generator
======================================================
258일 TA 데이터 기반으로 지금 당장 작동.
뉴스 데이터가 쌓일수록 combined 신호가 추가됨.

출력: site/data/predictions.json
{
  "updated": "...",
  "data_quality": { "price_days": 258, "news_days": 24, "regime": "early" },
  "market_regime": { "bull_pct": 0.82, "sentiment": "RISK-ON" },
  "predictions": [
    {
      "ticker": "NVDA",
      "action": "BUY",           # BUY / HOLD / SELL / WATCH
      "confidence": 0.74,        # 0~1
      "horizon": "3-5d",
      "price": 180.25,
      "signals": {
        "ta": ["rsi_oversold", "bb_lower_touch"],
        "trend": "BULL",
        "rsi14": 45.2,
        "macd_bias": "bullish",
        "bb_position": "lower_third",
        "volatility": "normal"
      },
      "news": {                  # 뉴스 데이터 있을 때 채워짐
        "active_terms": ["supply"],
        "best_signal": "supply→NVDA lag=-5d conf=0.74",
        "news_z": 2.3
      },
      "reasons": ["RSI oversold (45.2)", "BB lower touch", "BULL trend (SMA50>SMA200)"],
      "risks": ["MACD still bearish", "Low volume confirmation"]
    }
  ]
}
"""

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path


# ══════════════════════════════════════════════════════════════════════════════
# 1. 로더
# ══════════════════════════════════════════════════════════════════════════════

def load_ta(path: str) -> dict:
    try:
        d = json.loads(Path(path).read_text())
        return d.get("tickers", {})
    except Exception:
        return {}



def load_ticker_analysis(analysis_dir: str, tickers: list) -> dict:
    """ticker_analysis/<TICKER>.json 로드 → {ticker: analysis_data}"""
    result = {}
    base = Path(analysis_dir)
    for ticker in tickers:
        p = base / f"{ticker}.json"
        if p.exists():
            try:
                result[ticker] = json.loads(p.read_text())
            except Exception:
                pass
    return result

def load_signals(path: str) -> list:
    try:
        d = json.loads(Path(path).read_text())
        return d.get("pairs", [])
    except Exception:
        return []


def get_latest(cols: dict, key: str):
    """columnar 포맷에서 최신 값 추출."""
    arr = cols.get(key, [])
    for v in reversed(arr):
        if v is not None:
            return v
    return None


def get_series(cols: dict, key: str, n: int) -> list:
    """최근 n개 값 추출 (None 포함)."""
    arr = cols.get(key, [])
    return arr[-n:] if len(arr) >= n else arr


# ══════════════════════════════════════════════════════════════════════════════
# 2. TA 신호 해석
# ══════════════════════════════════════════════════════════════════════════════

def interpret_rsi(rsi: float) -> tuple[str, str]:
    """(상태, 설명)"""
    if rsi is None:
        return "neutral", "RSI unavailable"
    if rsi <= 25:  return "strong_oversold",  f"RSI deeply oversold ({rsi:.1f}) — high rebound prob"
    if rsi <= 35:  return "oversold",          f"RSI oversold ({rsi:.1f}) — potential reversal"
    if rsi <= 45:  return "mild_oversold",     f"RSI weak ({rsi:.1f}) — slight bearish pressure"
    if rsi <= 55:  return "neutral",           f"RSI neutral ({rsi:.1f})"
    if rsi <= 65:  return "mild_overbought",   f"RSI elevated ({rsi:.1f})"
    if rsi <= 75:  return "overbought",        f"RSI overbought ({rsi:.1f}) — caution"
    return "strong_overbought", f"RSI extremely overbought ({rsi:.1f}) — high pullback risk"


def interpret_macd(macd: float, signal: float, hist: float,
                   hist_series: list) -> tuple[str, str]:
    if macd is None or signal is None or hist is None:
        return "neutral", "MACD unavailable"

    # hist 방향성: 최근 3개 추세
    recent = [h for h in hist_series if h is not None][-3:]
    if len(recent) >= 2:
        improving = recent[-1] > recent[0]
    else:
        improving = hist > 0

    if macd > signal:
        bias = "bullish_cross" if improving else "bullish_weakening"
        desc = f"MACD above signal ({macd:.3f} > {signal:.3f})"
    else:
        bias = "bearish_cross" if not improving else "bearish_recovering"
        desc = f"MACD below signal ({macd:.3f} < {signal:.3f})"

    if improving and hist > 0:
        desc += " — momentum building"
    elif not improving and hist < 0:
        desc += " — momentum fading"

    return bias, desc


def interpret_bb(close: float, upper: float, lower: float,
                 mid: float, pct: float) -> tuple[str, str]:
    if pct is None:
        return "neutral", "BB unavailable"
    if pct < -0.05:   return "below_lower", f"Price below BB lower ({pct:.2f}) — oversold"
    if pct < 0.15:    return "lower_third",  f"Price in lower BB zone ({pct:.2f}) — potential support"
    if pct < 0.40:    return "lower_mid",    f"Price in lower-mid BB ({pct:.2f})"
    if pct < 0.60:    return "mid",          f"Price near BB midline ({pct:.2f})"
    if pct < 0.85:    return "upper_mid",    f"Price in upper-mid BB ({pct:.2f})"
    if pct < 1.05:    return "upper_third",  f"Price in upper BB zone ({pct:.2f}) — resistance ahead"
    return "above_upper", f"Price above BB upper ({pct:.2f}) — overbought"


def interpret_trend(sma50: float, sma200: float,
                    close: float) -> tuple[str, str]:
    if sma50 is None or sma200 is None:
        # SMA200 없을 때 SMA50 vs SMA20
        return "unknown", "Trend: insufficient history"
    if sma50 > sma200 * 1.05:
        return "strong_bull", "Strong uptrend (SMA50 > SMA200 by >5%)"
    if sma50 > sma200:
        return "bull", "Uptrend (SMA50 > SMA200)"
    if sma50 < sma200 * 0.95:
        return "strong_bear", "Strong downtrend (SMA50 < SMA200 by >5%)"
    return "bear", "Downtrend (SMA50 < SMA200)"


def interpret_volatility(hv20: float, atr14: float, close: float) -> tuple[str, str]:
    if hv20 is None:
        return "unknown", "Volatility: unavailable"
    if hv20 > 60:   return "extreme",  f"Extreme volatility (HV20={hv20:.0f}%) — size down"
    if hv20 > 40:   return "high",     f"High volatility (HV20={hv20:.0f}%)"
    if hv20 > 25:   return "elevated", f"Elevated volatility (HV20={hv20:.0f}%)"
    if hv20 > 15:   return "normal",   f"Normal volatility (HV20={hv20:.0f}%)"
    return "low", f"Low volatility (HV20={hv20:.0f}%) — breakout watch"


# ══════════════════════════════════════════════════════════════════════════════
# 3. 뉴스 신호 매핑
# ══════════════════════════════════════════════════════════════════════════════

def get_news_context(ticker: str, pairs: list,
                     ta_cols: dict) -> dict:
    """signal_corr.json에서 해당 ticker의 현재 활성 신호 추출."""
    if not pairs:
        return {}

    dates   = ta_cols.get("date", [])
    today   = dates[-1] if dates else None
    news_z  = ta_cols.get("news_z", [])
    latest_nz = get_latest(ta_cols, "news_z") or 0

    # ticker 관련 leading 신호 (lag ≤ -1)
    leading = [p for p in pairs
               if p["ticker"] == ticker
               and p["best_lag"] <= -1
               and p["confidence"] >= 0.3]
    leading.sort(key=lambda x: x["confidence"], reverse=True)

    # 오늘 활성 (z ≥ 1.5인 단어)
    active = [p for p in leading if (p.get("news_z_today") or 0) >= 1.5]

    if not leading:
        return {"available": False, "reason": "No leading signals yet"}

    best = leading[0]
    return {
        "available":    True,
        "active_terms": [p["term"] for p in active[:3]],
        "best_signal":  f"{best['term']}→{ticker} lag={best['best_lag']}d conf={best['confidence']:.2f}",
        "best_conf":    best["confidence"],
        "best_lag":     best["best_lag"],
        "best_ret_1d":  best.get("avg_ret_1d"),
        "news_z_today": round(latest_nz, 2),
        "n_leading":    len(leading),
    }


# ══════════════════════════════════════════════════════════════════════════════
# 4. 핵심 — 종합 액션 판단
# ══════════════════════════════════════════════════════════════════════════════

def decide_action(rsi_state: str, macd_bias: str, bb_state: str,
                  trend: str, vol: str,
                  ta_signals: list,
                  news: dict,
                  price_days: int,
                  market_regime: str = "NEUTRAL") -> tuple[str, float, list, list]:
    """
    반환: (action, confidence, reasons, risks)
    action: BUY / HOLD / SELL / WATCH

    설계 원칙:
    - RSI + BB 가 핵심 신호 (각 최대 ±3점)
    - MACD 는 방향 확인자 (최대 ±1.5점)
    - trend 는 배수가 아닌 가산점 (±0.5 ~ ±1.0)
    - 신호 없으면 score=0 → HOLD
    """
    score   = 0.0
    reasons = []
    risks   = []

    # ── RSI: 핵심 역추세 신호 ─────────────────────────────────────────────────
    rsi_scores = {
        "strong_oversold":   +3.0,
        "oversold":          +2.2,
        "mild_oversold":     +1.0,
        "neutral":            0.0,
        "mild_overbought":   -0.8,
        "overbought":        -2.0,
        "strong_overbought": -3.0,
    }
    rs = rsi_scores.get(rsi_state, 0)
    score += rs
    if rs >= 1.0:  reasons.append(f"RSI {rsi_state.replace('_',' ')} — mean reversion setup")
    if rs <= -1.0: risks.append(f"RSI {rsi_state.replace('_',' ')} — pullback risk")

    # ── BB: 가격 위치 신호 ─────────────────────────────────────────────────────
    bb_scores = {
        "below_lower":  +3.0,
        "lower_third":  +1.8,
        "lower_mid":    +0.6,
        "mid":           0.0,
        "upper_mid":    -0.4,
        "upper_third":  -1.2,
        "above_upper":  -3.0,
    }
    bs = bb_scores.get(bb_state, 0)
    score += bs
    if bs >= 1.0:  reasons.append(f"Price at BB {bb_state.replace('_',' ')} — support zone")
    if bs <= -1.0: risks.append(f"Price at BB {bb_state.replace('_',' ')} — stretched")

    # ── MACD: 모멘텀 방향 확인 ────────────────────────────────────────────────
    macd_scores = {
        "bullish_cross":      +1.5,
        "bearish_recovering": +0.8,
        "neutral":             0.0,
        "bullish_weakening":  -0.4,
        "bearish_cross":      -1.5,
    }
    ms = macd_scores.get(macd_bias, 0)
    score += ms
    if ms >= 0.8:  reasons.append(f"MACD {macd_bias.replace('_',' ')}")
    if ms <= -0.8: risks.append(f"MACD {macd_bias.replace('_',' ')}")

    # ── TA 신호 복합 보너스 ────────────────────────────────────────────────────
    bullish_keywords = ["oversold","lower_touch","bullish_cross","golden_cross"]
    bearish_keywords = ["overbought","upper_touch","bearish_cross","death_cross"]
    ta_bull = sum(1 for s in ta_signals if any(b in s for b in bullish_keywords))
    ta_bear = sum(1 for s in ta_signals if any(b in s for b in bearish_keywords))

    if ta_bull >= 2:
        score += 0.8
        reasons.append(f"TA confluence: {ta_bull} bullish signals firing")
    elif ta_bull == 1 and rs > 0 and bs > 0:
        score += 0.3  # 작은 보너스

    if ta_bear >= 2:
        score -= 0.8
        risks.append(f"TA confluence: {ta_bear} bearish signals firing")

    # ── 트렌드: 가산점 (배수 아님) ────────────────────────────────────────────
    if trend == "strong_bull":
        score += 0.8
        reasons.append("Strong uptrend (SMA50 >> SMA200)")
    elif trend == "bull":
        score += 0.4
        reasons.append("Uptrend (SMA50 > SMA200)")
    elif trend == "bear":
        score -= 0.4
        risks.append("Downtrend — counter-trend risk")
    elif trend == "strong_bear":
        score -= 0.8
        risks.append("Strong downtrend — high counter-trend risk")

    # ── 시장 레짐 조정 ────────────────────────────────────────────────────────
    if market_regime == "RISK-OFF":
        score -= 0.5
        risks.append("Market in RISK-OFF regime")
    elif market_regime == "OVERSOLD-BOUNCE":
        if score > 0:
            score += 0.5   # 시장 전체 반등 구간 — 롱 신호 강화
            reasons.append("Market oversold bounce expected")
    elif market_regime == "RISK-ON":
        if score > 0:
            score += 0.3

    # ── 변동성 패널티 ──────────────────────────────────────────────────────────
    if vol == "extreme":
        score *= 0.6
        risks.append("Extreme volatility — reduce position size")
    elif vol == "high":
        score *= 0.85
        risks.append("High volatility")

    # ── 뉴스 신호 보너스 ──────────────────────────────────────────────────────
    if news.get("available") and news.get("active_terms"):
        ret  = news.get("best_ret_1d", 0) or 0
        conf = news.get("best_conf", 0) or 0
        boost = min(conf * 1.5, 1.2)
        if ret > 0:
            score += boost
            reasons.append(f"News signal: {news['active_terms'][0]} → bullish (conf={conf:.2f})")
        else:
            score -= boost * 0.5
            risks.append(f"News signal: {news['active_terms'][0]} → bearish")

    # ── 데이터 신뢰도 ─────────────────────────────────────────────────────────
    news_days = news.get("news_days", 0)
    if news_days < 30:
        data_conf = 0.75   # 뉴스 없음 → 신뢰도 낮춤
    elif news_days < 60:
        data_conf = 0.88
    else:
        data_conf = 1.0

    # ── score → confidence ────────────────────────────────────────────────────
    raw_conf   = 1 / (1 + math.exp(-score * 0.45))
    confidence = round(raw_conf * data_conf, 3)

    # ── 액션 결정 ─────────────────────────────────────────────────────────────
    if score >= 3.5:
        action = "BUY"
    elif score >= 1.5:
        action = "WATCH"
    elif score <= -3.5:
        action = "SELL"
    elif score <= -1.5:
        action = "REDUCE"   # 비중 축소 (SELL보다 약한 신호)
    else:
        action = "HOLD"

    # 트렌드 역행 시 강도 조정
    if action == "BUY" and trend in ("strong_bear", "bear"):
        action = "WATCH"
        risks.append("Counter-trend entry — wait for trend confirmation")
    if action in ("SELL","REDUCE") and trend in ("strong_bull","bull"):
        action = "HOLD"
        risks.append("Bull trend intact — short signals unreliable")

    return action, confidence, reasons[:5], risks[:4]


# ══════════════════════════════════════════════════════════════════════════════
# 5. 시장 전체 레짐 분석
# ══════════════════════════════════════════════════════════════════════════════

def analyze_market_regime(ta_data: dict) -> dict:
    """SPY, QQQ, IWM, DIA 기준 전체 시장 상태 판단."""
    market_tickers = ["SPY", "QQQ", "IWM", "DIA"]
    bull_count = 0
    total = 0
    rsi_avg = []
    bb_avg  = []

    for t in market_tickers:
        if t not in ta_data:
            continue
        cols = ta_data[t]
        trend = "bull" if (get_latest(cols, "sma50") or 0) > (get_latest(cols, "sma200") or 0) else "bear"
        if trend == "bull":
            bull_count += 1
        total += 1

        rsi = get_latest(cols, "rsi14")
        bb  = get_latest(cols, "bb_pct")
        if rsi: rsi_avg.append(rsi)
        if bb is not None: bb_avg.append(bb)

    bull_pct = bull_count / total if total else 0.5
    avg_rsi  = sum(rsi_avg) / len(rsi_avg) if rsi_avg else 50
    avg_bb   = sum(bb_avg)  / len(bb_avg)  if bb_avg  else 0.5

    # 레짐 결정
    if bull_pct >= 0.75 and avg_rsi >= 50:
        regime = "RISK-ON"
        regime_note = "Broad market bullish — favor longs"
    elif bull_pct <= 0.25 or avg_rsi < 35:
        regime = "RISK-OFF"
        regime_note = "Broad market bearish — reduce exposure"
    elif avg_rsi < 40:
        regime = "OVERSOLD-BOUNCE"
        regime_note = "Market oversold — watch for reversal"
    else:
        regime = "NEUTRAL"
        regime_note = "Mixed signals — be selective"

    # 공포/탐욕 근사 (RSI 기반)
    fear_greed = round((avg_rsi - 30) / 40 * 100)  # 30~70 → 0~100
    fear_greed = max(0, min(100, fear_greed))

    return {
        "regime":      regime,
        "regime_note": regime_note,
        "bull_pct":    round(bull_pct, 2),
        "avg_rsi":     round(avg_rsi, 1),
        "avg_bb":      round(avg_bb, 3),
        "fear_greed":  fear_greed,
        "fear_greed_label": (
            "Extreme Fear" if fear_greed < 20 else
            "Fear"         if fear_greed < 40 else
            "Neutral"      if fear_greed < 60 else
            "Greed"        if fear_greed < 80 else "Extreme Greed"
        ),
    }


# ══════════════════════════════════════════════════════════════════════════════
# 6. 메인
# ══════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ta",      default="site/data/technical_analysis.json")
    ap.add_argument("--signals", default="site/data/signal_corr.json")
    ap.add_argument("--trends",  default="site/data/trends.json")
    ap.add_argument("--out",     default="site/data/predictions.json")
    ap.add_argument("--tickers", default=None)
    ap.add_argument("--analysis-dir", default="site/data/ticker_analysis",
                    help="ticker_analysis JSON 디렉토리")
    ap.add_argument("--min-conf", type=float, default=0.0)
    args = ap.parse_args()

    ta_data  = load_ta(args.ta)
    pairs    = load_signals(args.signals)
    tickers_list = [t.strip().upper() for t in args.tickers.split(",")]\
                   if args.tickers else list(ta_data.keys())
    ta_news  = load_ticker_analysis(args.analysis_dir, tickers_list)

    # 뉴스 날짜 수
    try:
        T = json.loads(Path(args.trends).read_text())
        news_days  = len(T.get("dates", []))
        zscores    = T.get("zscores", {})
    except Exception:
        news_days, zscores = 0, {}

    # 가격 날짜 수 (첫 ticker 기준)
    first_cols = next(iter(ta_data.values()), {}) if ta_data else {}
    price_days = len(first_cols.get("date", []))

    print(f"Price days: {price_days}  |  News days: {news_days}")
    print(f"TA reliability: {'HIGH (200d+)' if price_days >= 200 else 'MEDIUM (100d+)' if price_days >= 100 else 'LOW'}")
    print(f"News reliability: {'HIGH (90d+)' if news_days >= 90 else 'MEDIUM (30d+)' if news_days >= 30 else 'LOW — TA-only mode'}")

    data_quality = {
        "price_days":  price_days,
        "news_days":   news_days,
        "regime":      "mature" if price_days >= 200 and news_days >= 90 else
                       "growing" if price_days >= 100 else "early",
        "ta_reliable":    price_days >= 100,
        "news_reliable":  news_days >= 60,
        "sma200_available": price_days >= 200,
    }

    market_regime = analyze_market_regime(ta_data)
    print(f"Market regime: {market_regime['regime']} | Fear/Greed: {market_regime['fear_greed']} ({market_regime['fear_greed_label']})")

    tickers = tickers_list

    predictions = []

    for ticker in tickers:
        if ticker not in ta_data:
            continue
        cols = ta_data[ticker]

        # ── 최신 값 추출 ──────────────────────────────────────────────────────
        close  = get_latest(cols, "close")
        rsi14  = get_latest(cols, "rsi14")
        macd   = get_latest(cols, "macd")
        sig    = get_latest(cols, "macd_signal")
        hist   = get_latest(cols, "macd_hist")
        bb_up  = get_latest(cols, "bb_upper")
        bb_lo  = get_latest(cols, "bb_lower")
        bb_mid = get_latest(cols, "bb_mid")
        bb_pct = get_latest(cols, "bb_pct")
        sma50  = get_latest(cols, "sma50")
        sma200 = get_latest(cols, "sma200")
        hv20   = get_latest(cols, "hv20")
        atr14  = get_latest(cols, "atr14")
        ta_sigs= (cols.get("signals") or [""])[-1].split("|")
        ta_sigs= [s for s in ta_sigs if s]
        date   = (cols.get("date") or [""])[-1]

        # hist 시계열 (MACD 방향성 판단용)
        hist_series = get_series(cols, "macd_hist", 5)

        # ── 해석 ──────────────────────────────────────────────────────────────
        rsi_state, rsi_desc    = interpret_rsi(rsi14)
        macd_bias, macd_desc   = interpret_macd(macd, sig, hist, hist_series)
        bb_state,  bb_desc     = interpret_bb(close, bb_up, bb_lo, bb_mid, bb_pct)
        trend,     trend_desc  = interpret_trend(sma50, sma200, close)
        vol_state, vol_desc    = interpret_volatility(hv20, atr14, close)

        # ── 뉴스 컨텍스트 ─────────────────────────────────────────────────────
        news = get_news_context(ticker, pairs, cols)
        news["news_days"] = news_days

        # ── 액션 결정 ─────────────────────────────────────────────────────────
        # ticker_analysis에서 오늘 뉴스 신호 가져오기
        ticker_news_sig = ta_news.get(ticker, {}).get("today_signal", {})
        news_action    = ticker_news_sig.get("action", "HOLD")
        news_net_score = ticker_news_sig.get("net_score", 0)
        active_bull    = ticker_news_sig.get("active_bullish", [])
        active_bear    = ticker_news_sig.get("active_bearish", [])

        # news 컨텍스트에 ticker_analysis 결과 병합
        if active_bull or active_bear:
            news["available"]     = True
            news["active_terms"]  = [w["word"] for w in active_bull[:3]]
            news["best_conf"]     = max((w.get("expected_ret",0) for w in active_bull), default=0) / 5
            news["best_ret_1d"]   = active_bull[0]["expected_ret"] if active_bull else 0
            news["best_lag"]      = -1
            news["best_signal"]   = (
                f"{active_bull[0]['word']}→{ticker} "
                f"hit={active_bull[0].get('hit_rate',0):.0%} "
                f"avg={active_bull[0]['expected_ret']:+.2f}%"
                if active_bull else ""
            )

        action, conf, reasons, risks = decide_action(
            rsi_state, macd_bias, bb_state, trend, vol_state,
            ta_sigs, news, price_days,
            market_regime=market_regime.get("regime", "NEUTRAL")
        )

        # 뉴스 신호가 강하면 action 보강
        if news_net_score >= 3 and action in ("HOLD", "WATCH"):
            action = "WATCH" if action == "HOLD" else "BUY"
            reasons.append(f"News signal boost: {ticker_news_sig.get('summary','')}")
        elif news_net_score <= -3 and action in ("HOLD", "WATCH"):
            action = "REDUCE"
            risks.append(f"News signal warning: {ticker_news_sig.get('summary','')}")

        if conf < args.min_conf:
            continue

        # ── 목표가 / 손절가 (ATR 기반) ────────────────────────────────────────
        atr = atr14 or (close * 0.02 if close else 0)
        target = round(close + atr * 2, 2) if close and action in ("BUY","WATCH") else None
        stop   = round(close - atr * 1.5, 2) if close and action in ("BUY","WATCH") else None

        pred = {
            "ticker":     ticker,
            "date":       date,
            "action":     action,
            "confidence": conf,
            "horizon":    "3-5d",
            "price":      close,
            "target":     target,
            "stop":       stop,
            "rr_ratio":   round((target - close) / (close - stop), 2)
                          if target and stop and close and (close - stop) > 0 else None,
            "signals": {
                "ta":          ta_sigs,
                "trend":       trend,
                "rsi14":       rsi14,
                "rsi_state":   rsi_state,
                "macd_bias":   macd_bias,
                "bb_position": bb_state,
                "volatility":  vol_state,
                "hv20":        hv20,
                "atr14":       atr14,
            },
            "news":    news if news.get("available") else {"available": False},
            "reasons": reasons,
            "risks":   risks,
            "descriptions": {
                "rsi":   rsi_desc,
                "macd":  macd_desc,
                "bb":    bb_desc,
                "trend": trend_desc,
                "vol":   vol_desc,
            }
        }
        predictions.append(pred)

    # ── 정렬: BUY > WATCH > HOLD > REDUCE > SELL, then by confidence ─────────
    order = {"BUY": 0, "WATCH": 1, "HOLD": 2, "REDUCE": 3, "SELL": 4}
    predictions.sort(key=lambda x: (order.get(x["action"], 9), -x["confidence"]))

    output = {
        "updated":       datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data_quality":  data_quality,
        "market_regime": market_regime,
        "n_buy":         sum(1 for p in predictions if p["action"] == "BUY"),
        "n_watch":       sum(1 for p in predictions if p["action"] == "WATCH"),
        "n_hold":        sum(1 for p in predictions if p["action"] == "HOLD"),
        "n_sell":        sum(1 for p in predictions if p["action"] == "SELL"),
        "n_reduce":      sum(1 for p in predictions if p["action"] == "REDUCE"),
        "predictions":   predictions,
    }

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(
        json.dumps(output, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8"
    )

    print(f"\n→ {args.out}  ({len(predictions)} tickers)")
    print(f"  BUY={output['n_buy']}  WATCH={output['n_watch']}  "
          f"HOLD={output['n_hold']}  SELL={output['n_sell']}  REDUCE={output.get('n_reduce',0)}")
    print()

    # 상위 BUY 신호 출력
    buys = [p for p in predictions if p["action"] in ("BUY", "WATCH")][:8]
    if buys:
        print("Top signals:")
        print(f"  {'Ticker':<6} {'Action':<7} {'Conf':>5}  {'Price':>8}  "
              f"{'Target':>8}  {'Stop':>8}  {'R/R':>5}  {'RSI':<8}  Key reason")
        print("  " + "-"*92)
        for p in buys:
            rr  = f"{p['rr_ratio']:.1f}" if p["rr_ratio"] else "—"
            tgt = f"{p['target']:.2f}"   if p["target"]   else "—"
            stp = f"{p['stop']:.2f}"     if p["stop"]      else "—"
            reason = p["reasons"][0] if p["reasons"] else "—"
            rsi_val = p["signals"].get("rsi14")
            rsi_str = f"RSI={rsi_val:.0f}" if rsi_val else ""
            print(f"  {p['ticker']:<6} {p['action']:<7} {p['confidence']:>5.2f}  "
                  f"{p['price']:>8.2f}  {tgt:>8}  {stp:>8}  {rr:>5}  "
                  f"{rsi_str:<8}  {reason}")


if __name__ == "__main__":
    main()