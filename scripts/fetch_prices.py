"""
fetch_prices.py
yfinance로 티커 일별 종가 수집 → site/data/prices.json

출력 형식:
{
  "updated": "2026-03-12",
  "tickers": {
    "AAPL": {
      "dates":  ["2025-09-24", "2025-09-25", ...],
      "closes": [178.2, 179.1, ...],
      "returns": [null, 0.005, ...]   // 전일 대비 수익률
    },
    ...
  }
}

사용법:
  python scripts/fetch_prices.py
  python scripts/fetch_prices.py --days 180 --out site/data/prices.json
"""

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import yfinance as yf
except ImportError:
    raise SystemExit("yfinance 없음: pip install yfinance")

# ── 수집 대상 티커 ──────────────────────────────────────────────────
TICKERS = [
    # 빅테크
    "AAPL", "MSFT", "NVDA", "GOOGL", "META", "AMZN", "TSLA",
    # 반도체
    "AMD", "INTC", "AVGO", "QCOM", "ASML", "MU", "NXPI",
    # 금융
    "JPM", "BAC", "GS", "MS", "BLK",
    # 에너지
    "XOM", "CVX",
    # 지수 ETF (시장 전체 방향)
    "SPY", "QQQ", "IWM", "DIA",
    # 채권/VIX
    "TLT", "HYG",
    # 기타 매크로
    "GLD", "USO",
]


def fetch_prices(tickers: list[str], days: int) -> dict:
    end   = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days + 5)  # 주말/공휴일 여유

    result = {}
    print(f"Fetching {len(tickers)} tickers  {start} ~ {end}")

    for ticker in tickers:
        try:
            df = yf.download(
                ticker,
                start=start.isoformat(),
                end=(end + timedelta(days=1)).isoformat(),
                auto_adjust=True,
                progress=False,
                threads=False,
            )
        except Exception as e:
            print(f"  [ERR] {ticker}: {e}")
            continue

        if df is None or df.empty:
            print(f"  [SKIP] {ticker}: no data")
            continue

        closes = df["Close"].dropna()
        if len(closes) < 2:
            continue

        dates  = [d.strftime("%Y-%m-%d") for d in closes.index]
        prices = [round(float(v), 4) for v in closes.values]
        rets   = [None] + [
            round((prices[i] - prices[i-1]) / prices[i-1], 6)
            for i in range(1, len(prices))
        ]

        result[ticker] = {
            "dates":   dates,
            "closes":  prices,
            "returns": rets,
        }
        print(f"  ✓ {ticker:<6}  {dates[0]} ~ {dates[-1]}  ({len(dates)} days)")

    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=180,
                    help="수집할 과거 일수 (기본 180일)")
    ap.add_argument("--out",  default="site/data/prices.json",
                    help="출력 경로")
    ap.add_argument("--tickers", default=None,
                    help="쉼표 구분 티커 목록 (기본: 내장 목록)")
    args = ap.parse_args()

    tickers = [t.strip().upper() for t in args.tickers.split(",")] \
              if args.tickers else TICKERS

    data = fetch_prices(tickers, args.days)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "tickers": data,
    }
    out.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    n_days = max((len(v["dates"]) for v in data.values()), default=0)
    print(f"\n→ {out}  ({len(data)} tickers, up to {n_days} days)")


if __name__ == "__main__":
    main()