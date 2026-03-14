"""
analyze_prices.py  v1
=============================
정밀 기술적 분석 엔진 — prices.json → technical_analysis.json + CSV

지표:
  ── Trend ──
  · SMA  : 10, 20, 50, 100, 200
  · EMA  : 9, 12, 21, 26, 50
  · VWMA : 20 (거래량 가중 이동평균, prices.json에 volume 있으면 사용)

  ── Momentum ──
  · RSI   : 14, 21  (Wilder's smoothed RMA — 표준)
  · MACD  : 12/26/9 EMA (line, signal, histogram)
  · Stochastic : %K 14, %D 3 (slow)
  · ROC   : 10, 20 (Rate of Change)
  · Williams %R : 14

  ── Volatility ──
  · Bollinger Bands : 20/2.0  (population std, 표준)
  · ATR   : 14  (Wilder's smoothed)
  · Keltner Channel : EMA20 ± 2×ATR14
  · Historical Volatility : 20-day rolling annualised std of log returns

  ── Volume ──
  · OBV   : On-Balance Volume
  · Volume SMA : 20
  · Volume ratio : today / 20d avg

  ── Support / Resistance ──
  · 로컬 피벗 고점/저점 (lookback 10)
  · Pivot Points (Classic: PP, R1-R3, S1-S3)

  ── Signals (rule-based) ──
  · Golden Cross / Death Cross (SMA50 vs SMA200)
  · RSI 과매수(>70) / 과매도(<30)
  · MACD 크로스오버
  · BB 상단/하단 터치
  · 거래량 급등

  ── 뉴스 신호 결합 준비 ──
  · 각 날짜별 'news_z' 필드 슬롯 (trends.json 로드 시 자동 병합)

출력:
  site/data/technical_analysis.json   — 전체 데이터 (티커별)
  site/data/ta_summary.json           — 최신 날짜 요약 (대시보드용)
  run/ta_<TICKER>.csv                 — 티커별 전체 시계열
"""

import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


# ══════════════════════════════════════════════════════════════════════════════
# 1. 수학 / 통계 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def safe(v, default=None):
    """NaN/None 안전 처리."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return default
    return v

def r4(v):
    """소수점 4자리 반올림."""
    return round(v, 4) if v is not None else None


# ── Simple Moving Average ─────────────────────────────────────────────────────
def sma(series: list, n: int) -> list:
    """SMA(n). 앞 n-1개는 None."""
    out = [None] * len(series)
    for i in range(n - 1, len(series)):
        window = [x for x in series[i - n + 1: i + 1] if x is not None]
        out[i] = sum(window) / len(window) if len(window) == n else None
    return out


# ── Exponential Moving Average ────────────────────────────────────────────────
def ema(series: list, n: int, seed_with_sma: bool = True) -> list:
    """
    EMA(n).  k = 2/(n+1)
    seed_with_sma=True: 첫 값을 SMA(n)으로 시드 (표준 방식)
    """
    out  = [None] * len(series)
    k    = 2.0 / (n + 1)
    prev = None

    for i, v in enumerate(series):
        if v is None:
            out[i] = prev   # carry forward
            continue

        if prev is None:
            # 시드: 첫 n개 값으로 SMA 계산
            if seed_with_sma:
                window = [x for x in series[: i + 1] if x is not None]
                if len(window) >= n:
                    prev = sum(window[-n:]) / n
                    out[i] = prev
            else:
                prev = v
                out[i] = v
        else:
            prev = v * k + prev * (1 - k)
            out[i] = prev

    return out


# ── Wilder's RMA (RSI / ATR 전용) ────────────────────────────────────────────
def rma(series: list, n: int) -> list:
    """
    Wilder's Smoothed Moving Average = RMA(n).
    k = 1/n  (EMA의 k=2/(n+1) 와 다름 — RSI 공식에서 필수)
    시드: 첫 n개 평균
    """
    out  = [None] * len(series)
    k    = 1.0 / n
    prev = None

    for i, v in enumerate(series):
        if v is None:
            out[i] = prev
            continue
        if prev is None:
            window = [x for x in series[: i + 1] if x is not None]
            if len(window) >= n:
                prev = sum(window[-n:]) / n
                out[i] = prev
        else:
            prev = v * k + prev * (1 - k)
            out[i] = prev

    return out


# ── RSI — Wilder's 정확한 구현 ────────────────────────────────────────────────
def calc_rsi(closes: list, n: int = 14) -> list:
    """
    RSI(n) — Wilder's Smoothed Method (TradingView / Bloomberg 표준)

    공식:
      delta_t = close_t - close_{t-1}
      gain    = max(delta, 0)
      loss    = max(-delta, 0)
      avg_gain = RMA(gain, n)
      avg_loss = RMA(loss, n)
      RS  = avg_gain / avg_loss
      RSI = 100 - 100/(1+RS)
    """
    gains, losses = [None], [None]
    for i in range(1, len(closes)):
        if closes[i] is None or closes[i - 1] is None:
            gains.append(None)
            losses.append(None)
            continue
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))

    avg_g = rma(gains, n)
    avg_l = rma(losses, n)

    out = [None] * len(closes)
    for i in range(len(closes)):
        ag = avg_g[i]
        al = avg_l[i]
        if ag is None or al is None:
            continue
        if al == 0:
            out[i] = 100.0
        else:
            rs = ag / al
            out[i] = r4(100.0 - 100.0 / (1.0 + rs))
    return out


# ── MACD ──────────────────────────────────────────────────────────────────────
def calc_macd(closes: list,
              fast: int = 12, slow: int = 26, signal_n: int = 9
              ) -> tuple[list, list, list]:
    """
    MACD line    = EMA(fast) - EMA(slow)
    Signal line  = EMA(signal_n) of MACD line
    Histogram    = MACD - Signal

    반환: (macd_line, signal_line, histogram)
    """
    ema_fast   = ema(closes, fast)
    ema_slow   = ema(closes, slow)
    macd_line  = [
        r4(f - s) if f is not None and s is not None else None
        for f, s in zip(ema_fast, ema_slow)
    ]
    signal_line = ema(macd_line, signal_n)
    histogram   = [
        r4(m - sg) if m is not None and sg is not None else None
        for m, sg in zip(macd_line, signal_line)
    ]
    return macd_line, signal_line, histogram


# ── Bollinger Bands ───────────────────────────────────────────────────────────
def calc_bb(closes: list, n: int = 20, mult: float = 2.0
            ) -> tuple[list, list, list, list]:
    """
    Middle = SMA(n)
    Std    = population stddev (σ, not sample — TradingView 표준)
    Upper  = Middle + mult × Std
    Lower  = Middle - mult × Std
    %B     = (price - lower) / (upper - lower)

    반환: (middle, upper, lower, pct_b)
    """
    mid = sma(closes, n)
    upper, lower, pct_b = [None]*len(closes), [None]*len(closes), [None]*len(closes)

    for i in range(n - 1, len(closes)):
        window = [x for x in closes[i - n + 1: i + 1] if x is not None]
        if len(window) < n:
            continue
        m   = mid[i]
        std = math.sqrt(sum((x - m) ** 2 for x in window) / n)   # population std
        u   = m + mult * std
        l   = m - mult * std
        upper[i] = r4(u)
        lower[i] = r4(l)
        if u != l:
            pct_b[i] = r4((closes[i] - l) / (u - l))

    return mid, upper, lower, pct_b


# ── ATR — Wilder's ────────────────────────────────────────────────────────────
def calc_atr(highs: list, lows: list, closes: list, n: int = 14) -> list:
    """
    True Range = max(H-L, |H-prev_C|, |L-prev_C|)
    ATR        = RMA(TR, n)   — Wilder's smoothing
    """
    tr = [None]
    for i in range(1, len(closes)):
        h, l, pc = highs[i], lows[i], closes[i - 1]
        if any(x is None for x in [h, l, pc]):
            tr.append(None)
            continue
        tr.append(max(h - l, abs(h - pc), abs(l - pc)))
    return [r4(v) if v is not None else None for v in rma(tr, n)]


# ── Stochastic Oscillator ─────────────────────────────────────────────────────
def calc_stoch(highs: list, lows: list, closes: list,
               k_period: int = 14, d_period: int = 3) -> tuple[list, list]:
    """
    %K = (close - lowest_low(k)) / (highest_high(k) - lowest_low(k)) × 100
    %D = SMA(%K, d_period)    — Slow Stochastic
    """
    raw_k = [None] * len(closes)
    for i in range(k_period - 1, len(closes)):
        h_window = [x for x in highs[i - k_period + 1: i + 1] if x is not None]
        l_window = [x for x in lows[i - k_period + 1: i + 1]  if x is not None]
        if not h_window or not l_window or closes[i] is None:
            continue
        hh = max(h_window)
        ll = min(l_window)
        denom = hh - ll
        if denom == 0:
            raw_k[i] = 50.0
        else:
            raw_k[i] = r4((closes[i] - ll) / denom * 100)

    pct_k = sma(raw_k, d_period)   # Slow %K = SMA of Fast %K
    pct_d = sma(pct_k, d_period)   # %D = SMA of Slow %K
    return pct_k, pct_d


# ── Williams %R ───────────────────────────────────────────────────────────────
def calc_williams_r(highs, lows, closes, n: int = 14) -> list:
    out = [None] * len(closes)
    for i in range(n - 1, len(closes)):
        hw = [x for x in highs[i - n + 1: i + 1] if x is not None]
        lw = [x for x in lows[i - n + 1: i + 1]  if x is not None]
        if not hw or not lw or closes[i] is None:
            continue
        hh, ll = max(hw), min(lw)
        denom = hh - ll
        out[i] = r4((hh - closes[i]) / denom * -100) if denom else -50.0
    return out


# ── Rate of Change ────────────────────────────────────────────────────────────
def calc_roc(closes: list, n: int) -> list:
    out = [None] * len(closes)
    for i in range(n, len(closes)):
        if closes[i] is not None and closes[i - n] is not None and closes[i - n] != 0:
            out[i] = r4((closes[i] - closes[i - n]) / closes[i - n] * 100)
    return out


# ── Historical Volatility (annualised) ───────────────────────────────────────
def calc_hv(closes: list, n: int = 20) -> list:
    """20-day rolling stddev of log returns, annualised (×√252)."""
    log_rets = [None]
    for i in range(1, len(closes)):
        c, p = closes[i], closes[i - 1]
        if c and p and c > 0 and p > 0:
            log_rets.append(math.log(c / p))
        else:
            log_rets.append(None)

    out = [None] * len(closes)
    for i in range(n, len(closes)):
        window = [x for x in log_rets[i - n + 1: i + 1] if x is not None]
        if len(window) < n:
            continue
        mean = sum(window) / len(window)
        var  = sum((x - mean) ** 2 for x in window) / (len(window) - 1)  # sample var
        out[i] = r4(math.sqrt(var) * math.sqrt(252) * 100)   # annualised %
    return out


# ── Keltner Channel ───────────────────────────────────────────────────────────
def calc_keltner(closes, highs, lows, ema_n: int = 20,
                 atr_n: int = 14, mult: float = 2.0
                 ) -> tuple[list, list, list]:
    mid   = ema(closes, ema_n)
    atr14 = calc_atr(highs, lows, closes, atr_n)
    upper, lower = [None]*len(closes), [None]*len(closes)
    for i in range(len(closes)):
        if mid[i] is not None and atr14[i] is not None:
            upper[i] = r4(mid[i] + mult * atr14[i])
            lower[i] = r4(mid[i] - mult * atr14[i])
    return mid, upper, lower


# ── OBV ───────────────────────────────────────────────────────────────────────
def calc_obv(closes: list, volumes: list) -> list:
    out  = [None] * len(closes)
    prev = 0
    for i in range(len(closes)):
        if closes[i] is None or volumes[i] is None:
            out[i] = prev
            continue
        if i == 0:
            prev = volumes[i]
        elif closes[i] > closes[i - 1]:
            prev += volumes[i]
        elif closes[i] < closes[i - 1]:
            prev -= volumes[i]
        # equal → no change
        out[i] = prev
    return out


# ── Pivot Points (Classic) ────────────────────────────────────────────────────
def calc_pivot(high: float, low: float, close: float) -> dict:
    pp = (high + low + close) / 3
    r1 = 2 * pp - low
    r2 = pp + (high - low)
    r3 = high + 2 * (pp - low)
    s1 = 2 * pp - high
    s2 = pp - (high - low)
    s3 = low - 2 * (high - pp)
    return {k: r4(v) for k, v in
            dict(pp=pp, r1=r1, r2=r2, r3=r3, s1=s1, s2=s2, s3=s3).items()}


# ── Local Pivot Highs / Lows ──────────────────────────────────────────────────
def find_pivots(series: list, lookback: int = 10) -> tuple[list, list]:
    """
    반환: (pivot_highs, pivot_lows)
    각 리스트: 해당 인덱스가 피벗이면 price, 아니면 None
    """
    n = len(series)
    highs = [None] * n
    lows  = [None] * n
    for i in range(lookback, n - lookback):
        v = series[i]
        if v is None:
            continue
        window = [x for x in series[i - lookback: i + lookback + 1] if x is not None]
        if v == max(window):
            highs[i] = v
        if v == min(window):
            lows[i] = v
    return highs, lows


# ── Rule-based Signal Detection ───────────────────────────────────────────────
def detect_signals(i: int, dates: list, closes: list,
                   sma50: list, sma200: list,
                   rsi14: list, macd_line: list, signal_line: list,
                   bb_upper: list, bb_lower: list,
                   vol: list, vol_sma20: list) -> list:
    """
    i번째 날짜에서 발생한 신호 목록 반환.
    """
    sigs = []
    c = closes[i]
    if c is None:
        return sigs

    # ── Golden / Death Cross ──────────────────────────────────────────────────
    if i >= 1:
        s50, s200    = sma50[i],    sma200[i]
        s50p, s200p  = sma50[i-1],  sma200[i-1]
        if all(x is not None for x in [s50, s200, s50p, s200p]):
            if s50p <= s200p and s50 > s200:
                sigs.append("golden_cross")
            elif s50p >= s200p and s50 < s200:
                sigs.append("death_cross")

    # ── RSI 과매수 / 과매도 ────────────────────────────────────────────────────
    r = rsi14[i]
    if r is not None:
        if r >= 70:
            sigs.append(f"rsi_overbought_{r:.1f}")
        elif r <= 30:
            sigs.append(f"rsi_oversold_{r:.1f}")

    # ── MACD 크로스오버 ────────────────────────────────────────────────────────
    if i >= 1:
        ml, sl   = macd_line[i],   signal_line[i]
        mlp, slp = macd_line[i-1], signal_line[i-1]
        if all(x is not None for x in [ml, sl, mlp, slp]):
            if mlp <= slp and ml > sl:
                sigs.append("macd_bullish_cross")
            elif mlp >= slp and ml < sl:
                sigs.append("macd_bearish_cross")

    # ── Bollinger Band 터치 ────────────────────────────────────────────────────
    bu, bl = bb_upper[i], bb_lower[i]
    if bu is not None and c >= bu:
        sigs.append("bb_upper_touch")
    if bl is not None and c <= bl:
        sigs.append("bb_lower_touch")

    # ── 거래량 급등 (>2x 20일 평균) ────────────────────────────────────────────
    v, vma = vol[i], vol_sma20[i]
    if v is not None and vma is not None and vma > 0 and v >= 2 * vma:
        sigs.append(f"volume_spike_{v/vma:.1f}x")

    return sigs


# ══════════════════════════════════════════════════════════════════════════════
# 2. 메인 분석 루프
# ══════════════════════════════════════════════════════════════════════════════

def analyze_ticker(ticker: str, dates: list, closes: list,
                   highs: list = None, lows: list = None,
                   volumes: list = None,
                   news_zscores: dict = None) -> dict:
    """
    ticker 하나에 대한 전체 TA 계산.
    highs / lows / volumes 없으면 closes로 근사.
    news_zscores: {date: z_score} 뉴스 시그널 병합용
    """
    n = len(dates)
    if highs  is None: highs   = closes[:]
    if lows   is None: lows    = closes[:]
    if volumes is None: volumes = [None] * n

    # ── Trend ─────────────────────────────────────────────────────────────────
    sma10  = sma(closes, 10)
    sma20  = sma(closes, 20)
    sma50  = sma(closes, 50)
    sma100 = sma(closes, 100)
    sma200 = sma(closes, 200)
    ema9   = ema(closes, 9)
    ema12  = ema(closes, 12)
    ema21  = ema(closes, 21)
    ema26  = ema(closes, 26)
    ema50  = ema(closes, 50)

    # ── Momentum ──────────────────────────────────────────────────────────────
    rsi14 = calc_rsi(closes, 14)
    rsi21 = calc_rsi(closes, 21)
    macd_line, signal_line, macd_hist = calc_macd(closes, 12, 26, 9)
    stoch_k, stoch_d = calc_stoch(highs, lows, closes, 14, 3)
    willr14 = calc_williams_r(highs, lows, closes, 14)
    roc10  = calc_roc(closes, 10)
    roc20  = calc_roc(closes, 20)

    # ── Volatility ────────────────────────────────────────────────────────────
    bb_mid, bb_upper, bb_lower, bb_pct = calc_bb(closes, 20, 2.0)
    atr14    = calc_atr(highs, lows, closes, 14)
    hv20     = calc_hv(closes, 20)
    kc_mid, kc_upper, kc_lower = calc_keltner(closes, highs, lows, 20, 14, 2.0)

    # ── Volume ────────────────────────────────────────────────────────────────
    vol_sma20 = sma(volumes, 20)
    vol_ratio = [
        r4(volumes[i] / vol_sma20[i])
        if (volumes[i] is not None and vol_sma20[i] and vol_sma20[i] > 0)
        else None
        for i in range(n)
    ]
    obv = calc_obv(closes, volumes) if any(v is not None for v in volumes) else [None]*n

    # ── Support / Resistance ──────────────────────────────────────────────────
    pivot_h, pivot_l = find_pivots(closes, lookback=10)

    # ── Log returns ───────────────────────────────────────────────────────────
    log_rets = [None] + [
        r4(math.log(closes[i] / closes[i-1]))
        if (closes[i] and closes[i-1] and closes[i] > 0 and closes[i-1] > 0)
        else None
        for i in range(1, n)
    ]
    ret1d = [None] + [
        r4((closes[i] - closes[i-1]) / closes[i-1] * 100)
        if (closes[i] and closes[i-1] and closes[i-1] != 0)
        else None
        for i in range(1, n)
    ]

    # ── Pivot Points (based on previous day H/L/C) ────────────────────────────
    pivot_pts = [None]
    for i in range(1, n):
        pivot_pts.append(calc_pivot(highs[i-1], lows[i-1], closes[i-1])
                         if all(x is not None for x in [highs[i-1], lows[i-1], closes[i-1]])
                         else None)

    # ── Signals ───────────────────────────────────────────────────────────────
    signals_list = [
        detect_signals(i, dates, closes, sma50, sma200,
                       rsi14, macd_line, signal_line,
                       bb_upper, bb_lower, volumes, vol_sma20)
        for i in range(n)
    ]

    # ── Assemble per-day records ───────────────────────────────────────────────
    records = []
    for i in range(n):
        d = dates[i]
        rec = {
            "date":         d,
            "close":        r4(closes[i]),
            "high":         r4(highs[i]),
            "low":          r4(lows[i]),
            "volume":       volumes[i],
            "ret_1d":       ret1d[i],
            "log_ret":      log_rets[i],
            # Trend
            "sma10":        r4(sma10[i]),
            "sma20":        r4(sma20[i]),
            "sma50":        r4(sma50[i]),
            "sma100":       r4(sma100[i]),
            "sma200":       r4(sma200[i]),
            "ema9":         r4(ema9[i]),
            "ema12":        r4(ema12[i]),
            "ema21":        r4(ema21[i]),
            "ema26":        r4(ema26[i]),
            "ema50":        r4(ema50[i]),
            # Momentum
            "rsi14":        rsi14[i],
            "rsi21":        rsi21[i],
            "macd":         macd_line[i],
            "macd_signal":  signal_line[i],
            "macd_hist":    macd_hist[i],
            "stoch_k":      stoch_k[i],
            "stoch_d":      stoch_d[i],
            "willr14":      willr14[i],
            "roc10":        roc10[i],
            "roc20":        roc20[i],
            # Volatility
            "bb_mid":       r4(bb_mid[i]),
            "bb_upper":     bb_upper[i],
            "bb_lower":     bb_lower[i],
            "bb_pct":       bb_pct[i],
            "atr14":        atr14[i],
            "hv20":         hv20[i],
            "kc_upper":     kc_upper[i],
            "kc_lower":     kc_lower[i],
            # Volume
            "vol_sma20":    r4(vol_sma20[i]),
            "vol_ratio":    vol_ratio[i],
            "obv":          obv[i],
            # S/R
            "pivot_high":   pivot_h[i],
            "pivot_low":    pivot_l[i],
            "pivot_pp":     pivot_pts[i]["pp"]   if pivot_pts[i] else None,
            "pivot_r1":     pivot_pts[i]["r1"]   if pivot_pts[i] else None,
            "pivot_s1":     pivot_pts[i]["s1"]   if pivot_pts[i] else None,
            # Signals
            "signals":      signals_list[i],
            # News integration slot
            "news_z":       (news_zscores or {}).get(d),
        }
        records.append(rec)

    # ── Latest snapshot ───────────────────────────────────────────────────────
    last = records[-1] if records else {}
    trend = "N/A"
    if last.get("sma50") and last.get("sma200"):
        trend = "BULL" if last["sma50"] > last["sma200"] else "BEAR"

    snapshot = {
        "ticker":    ticker,
        "date":      last.get("date"),
        "close":     last.get("close"),
        "trend":     trend,
        "rsi14":     last.get("rsi14"),
        "macd_hist": last.get("macd_hist"),
        "bb_pct":    last.get("bb_pct"),
        "atr14":     last.get("atr14"),
        "hv20":      last.get("hv20"),
        "stoch_k":   last.get("stoch_k"),
        "signals":   last.get("signals", []),
        # 5-day signal count for intensity measure
        "recent_signals": sum(
            len(records[-k]["signals"])
            for k in range(1, min(6, len(records)+1))
        ),
    }

    return {
        "ticker":   ticker,
        "records":  records,
        "snapshot": snapshot,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 3. 뉴스 Z-score 병합
# ══════════════════════════════════════════════════════════════════════════════

def load_news_zscores(trends_path: str) -> dict:
    """
    trends.json → {term: {date: zscore}} 대신
    날짜별로 '가장 강한 z-score 합계'를 반환.
    → 각 날짜의 전체 뉴스 신호 강도를 하나의 숫자로 요약.
    """
    try:
        T = json.loads(Path(trends_path).read_text())
    except Exception:
        return {}

    zscores = T.get("zscores", {})
    series  = T.get("series",  {})
    dates   = T.get("dates",   [])

    if not dates:
        return {}

    def z_at(counts, i, window=28):
        if i < 3: return 0.0
        hist = counts[max(0, i-window): i]
        if not hist: return 0.0
        mean = sum(hist) / len(hist)
        std  = math.sqrt(sum((x-mean)**2 for x in hist) / len(hist))
        return (counts[i]-mean)/std if std >= 0.5 else 0.0

    # Hot terms = top 20 by current |z|
    hot_terms = sorted(zscores, key=lambda t: abs(zscores[t]), reverse=True)[:20]

    date_signal = {}
    for d_i, d in enumerate(dates):
        # Sum of |z| for hot terms on this date
        total = sum(abs(z_at((series.get(t) or []), d_i)) for t in hot_terms)
        date_signal[d] = round(total, 3)

    return date_signal


# ══════════════════════════════════════════════════════════════════════════════
# 4. Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(description="Precise technical analysis engine")
    ap.add_argument("--prices",   default="site/data/prices.json",
                    help="prices.json 경로")
    ap.add_argument("--trends",   default="site/data/trends.json",
                    help="trends.json 경로 (뉴스 신호 병합용, 선택)")
    ap.add_argument("--out-json", default="site/data/technical_analysis.json",
                    help="전체 JSON 출력 경로")
    ap.add_argument("--out-summary", default="site/data/ta_summary.json",
                    help="최신 스냅샷 요약 JSON")
    ap.add_argument("--out-csv-dir", default="run",
                    help="티커별 CSV 출력 디렉터리")
    ap.add_argument("--tickers",  default=None,
                    help="쉼표 구분 티커 (기본: prices.json 전체)")
    ap.add_argument("--last-days", type=int, default=0,
                    help="최근 N일만 처리 (0=전체)")
    args = ap.parse_args()

    # ── Load prices ────────────────────────────────────────────────────────────
    try:
        P = json.loads(Path(args.prices).read_text())
    except Exception as e:
        sys.exit(f"prices.json 로드 실패: {e}")

    p_tickers = P.get("tickers", {})
    tickers   = [t.strip().upper() for t in args.tickers.split(",")]\
                if args.tickers else list(p_tickers.keys())

    # ── Load news z-scores (optional) ─────────────────────────────────────────
    news_zs = {}
    if Path(args.trends).exists():
        news_zs = load_news_zscores(args.trends)
        print(f"Loaded news signals for {len(news_zs)} dates from {args.trends}")
    else:
        print(f"trends.json not found at {args.trends} — skipping news integration")

    # ── CSV dir ────────────────────────────────────────────────────────────────
    csv_dir = Path(args.out_csv_dir)
    csv_dir.mkdir(parents=True, exist_ok=True)

    results  = {}
    summary  = []

    CSV_FIELDS = [
        "date","close","high","low","volume","ret_1d","log_ret",
        "sma10","sma20","sma50","sma100","sma200",
        "ema9","ema12","ema21","ema26","ema50",
        "rsi14","rsi21",
        "macd","macd_signal","macd_hist",
        "stoch_k","stoch_d","willr14","roc10","roc20",
        "bb_mid","bb_upper","bb_lower","bb_pct",
        "atr14","hv20","kc_upper","kc_lower",
        "vol_sma20","vol_ratio","obv",
        "pivot_high","pivot_low","pivot_pp","pivot_r1","pivot_s1",
        "signals","news_z",
    ]

    for ticker in tickers:
        if ticker not in p_tickers:
            print(f"  [SKIP] {ticker} — not in prices.json")
            continue

        pdata   = p_tickers[ticker]
        dates   = pdata["dates"]
        closes  = [float(v) if v is not None else None for v in pdata["closes"]]
        returns = pdata.get("returns", [None]*len(dates))

        # highs/lows: prices.json에 없으면 closes로 근사
        highs   = [float(v) if v is not None else None
                   for v in pdata.get("highs",  closes)]
        lows    = [float(v) if v is not None else None
                   for v in pdata.get("lows",   closes)]
        volumes = [float(v) if v is not None else None
                   for v in pdata.get("volumes", [None]*len(dates))]

        # Trim to last N days
        if args.last_days > 0 and len(dates) > args.last_days:
            k = args.last_days
            dates, closes, highs, lows, volumes = \
                dates[-k:], closes[-k:], highs[-k:], lows[-k:], volumes[-k:]

        result = analyze_ticker(
            ticker, dates, closes, highs, lows, volumes, news_zs
        )
        results[ticker]  = result
        summary.append(result["snapshot"])

        # ── Write CSV ─────────────────────────────────────────────────────────
        csv_path = csv_dir / f"ta_{ticker}.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as cf:
            writer = csv.DictWriter(cf, fieldnames=CSV_FIELDS, extrasaction="ignore")
            writer.writeheader()
            for rec in result["records"]:
                row = {k: rec.get(k) for k in CSV_FIELDS}
                row["signals"] = "|".join(rec.get("signals", []))
                writer.writerow(row)

        snap = result["snapshot"]
        print(f"  ✓ {ticker:<6} "
              f"close={snap['close']}  trend={snap['trend']}  "
              f"rsi14={snap['rsi14']}  "
              f"macd_hist={snap['macd_hist']}  "
              f"bb%={snap['bb_pct']}  "
              f"signals={snap['signals']}")

    # ── Write JSON (columnar format — ~8x smaller than row-based) ─────────────
    out_path = Path(args.out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    columnar = {}
    for ticker, res in results.items():
        recs = res["records"]
        if not recs:
            continue
        # 모든 필드를 column 배열로 변환
        fields = [k for k in recs[0].keys() if k not in ("signals",)]
        cols = {f: [r.get(f) for r in recs] for f in fields}
        cols["signals"] = ["|".join(r.get("signals") or []) for r in recs]
        columnar[ticker] = cols

    payload = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tickers": columnar,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",",":")),
                        encoding="utf-8")

    # gzip 버전도 생성 (대시보드가 fetch 시 사용 가능)
    import gzip
    gz_path = out_path.with_suffix(".json.gz")
    with gzip.open(gz_path, "wt", encoding="utf-8", compresslevel=6) as gz:
        json.dump(payload, gz, ensure_ascii=False, separators=(",",":"))

    sum_path = Path(args.out_summary)
    summary.sort(key=lambda x: x.get("recent_signals", 0), reverse=True)
    sum_path.write_text(json.dumps({
        "updated":  datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "snapshot": summary,
    }, ensure_ascii=False), encoding="utf-8")

    print(f"\n→ {out_path}  ({len(results)} tickers)")
    print(f"→ {sum_path}  (summary)")
    print(f"→ {csv_dir}/ta_<TICKER>.csv  ({len(results)} files)")

    # ── Signal summary ─────────────────────────────────────────────────────────
    active = [(s["ticker"], s["signals"]) for s in summary if s["signals"]]
    if active:
        print(f"\nActive signals today:")
        for t, sigs in active:
            print(f"  {t:<6} {', '.join(sigs)}")

    # ── Trend overview ─────────────────────────────────────────────────────────
    bull = sum(1 for s in summary if s.get("trend") == "BULL")
    bear = sum(1 for s in summary if s.get("trend") == "BEAR")
    print(f"\nTrend: BULL={bull}  BEAR={bear}  N/A={len(summary)-bull-bear}")


if __name__ == "__main__":
    main()