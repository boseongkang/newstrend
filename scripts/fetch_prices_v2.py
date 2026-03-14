"""
fetch_prices_v2.py — Incremental price storage engine
=====================================================
기존 fetch_prices.py를 대체.

아키텍처:
  data/prices/
    AAPL.csv          ← append-only OHLCV 원장 (영구 보존)
    MSFT.csv
    ...
    _manifest.json    ← 각 ticker 마지막 업데이트 날짜, 행 수, 체크섬

  site/data/
    prices.json       ← 대시보드용 최근 180일 뷰 (매일 재생성)
    prices_meta.json  ← 데이터 품질 리포트

CSV 스키마 (per ticker):
  date, open, high, low, close, volume, adj_close, split_factor, dividend

매일 실행 시:
  1. _manifest.json에서 마지막 날짜 확인
  2. 마지막 날짜 다음날부터 오늘까지만 fetch (보통 1행)
  3. 수정(adjusted) 감지: adj_close ≠ close 비율이 변하면 경고
  4. CSV에 append
  5. prices.json 재빌드 (최근 180일)
  6. _manifest.json 업데이트
"""

import argparse
import csv
import json
import math
import sys
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

try:
    import yfinance as yf
except ImportError:
    sys.exit("pip install yfinance")


# ── 티커 목록 ──────────────────────────────────────────────────────────────────
TICKERS = [
    "AAPL","MSFT","NVDA","GOOGL","META","AMZN","TSLA",
    "AMD","INTC","AVGO","QCOM","ASML","MU","NXPI",
    "JPM","BAC","GS","MS","BLK",
    "XOM","CVX",
    "SPY","QQQ","IWM","DIA",
    "TLT","HYG","GLD","USO",
]

CSV_FIELDS = ["date","open","high","low","close","volume","adj_close"]


# ══════════════════════════════════════════════════════════════════════════════
# CSV 원장 관리
# ══════════════════════════════════════════════════════════════════════════════

def load_csv(path: Path) -> list[dict]:
    """CSV 원장 로드. 없으면 빈 리스트."""
    if not path.exists():
        return []
    rows = []
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows


def save_csv(path: Path, rows: list[dict]):
    """CSV 원장 저장 (전체 덮어쓰기 — append 중 손상 방지)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        w.writerows(rows)


def append_rows(path: Path, new_rows: list[dict]) -> int:
    """기존 CSV에 새 행 추가. 중복 날짜 자동 제거. 반환: 실제 추가된 행 수."""
    existing = load_csv(path)
    existing_dates = {r["date"] for r in existing}
    to_add = [r for r in new_rows if r["date"] not in existing_dates]
    if to_add:
        all_rows = existing + to_add
        all_rows.sort(key=lambda r: r["date"])
        save_csv(path, all_rows)
    return len(to_add)


# ══════════════════════════════════════════════════════════════════════════════
# yfinance fetch (incremental)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_incremental(ticker: str,
                      since: date,
                      end: date) -> list[dict]:
    """
    since 다음날부터 end까지 OHLCV 다운로드.
    yfinance multi-level columns 완전 처리.
    반환: CSV 행 딕셔너리 리스트
    """
    start = since + timedelta(days=1)
    if start > end:
        return []

    try:
        df = yf.download(
            ticker,
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    except Exception as e:
        print(f"    [ERR] {ticker} fetch: {e}")
        return []

    if df is None or df.empty:
        return []

    # ── Multi-level columns 평탄화 ─────────────────────────────────────────
    # yfinance >= 0.2.x 는 (Price, Ticker) 형태의 MultiIndex 반환
    if isinstance(df.columns, __import__('pandas').MultiIndex):
        df.columns = [c[0] for c in df.columns]

    def safe_float(v):
        """Series / scalar 모두 안전하게 float 변환."""
        try:
            if hasattr(v, "iloc"):       # Series
                v = v.iloc[0]
            if v is None:
                return None
            f = float(v)
            return None if math.isnan(f) else round(f, 6)
        except Exception:
            return None

    def safe_int(v):
        try:
            if hasattr(v, "iloc"):
                v = v.iloc[0]
            if v is None:
                return 0
            return int(float(v))
        except Exception:
            return 0

    # 컬럼 이름 소문자 매핑
    col_map = {c.lower().replace(" ", "_"): c for c in df.columns}

    def gcol(name):
        """name 또는 변형으로 컬럼 찾기."""
        variants = [name, name.title(), name.upper(),
                    name.replace("_", " "), name.replace("_", " ").title()]
        for v in variants:
            if v in df.columns:
                return v
            if v.lower() in col_map:
                return col_map[v.lower()]
        return None

    rows = []
    for dt, row in df.iterrows():
        def g(col_name):
            c = gcol(col_name)
            return safe_float(row[c]) if c else None

        close = g("Close")
        adj   = g("Adj_Close") or g("Adj Close") or close
        vol_c = gcol("Volume")
        vol   = safe_int(row[vol_c]) if vol_c else 0

        rows.append({
            "date":      dt.strftime("%Y-%m-%d"),
            "open":      g("Open"),
            "high":      g("High"),
            "low":       g("Low"),
            "close":     close,
            "volume":    vol,
            "adj_close": adj,
        })
    return rows


# ══════════════════════════════════════════════════════════════════════════════
# Adjusted price 수정 감지
# ══════════════════════════════════════════════════════════════════════════════

def detect_adjustment(existing: list[dict], new_rows: list[dict],
                      ticker: str) -> list[str]:
    """
    기존 마지막 행과 새 데이터의 adj/close 비율 비교.
    비율이 크게 다르면 분할(split) 또는 배당 조정 가능성.
    반환: 경고 메시지 리스트
    """
    warnings = []
    if not existing or not new_rows:
        return warnings

    last = existing[-1]
    try:
        old_close = float(last["close"])
        old_adj   = float(last["adj_close"] or last["close"])
        old_ratio = old_adj / old_close if old_close else 1.0
    except:
        return warnings

    # 새 row의 adj/close 비율
    for row in new_rows:
        try:
            c = float(row["close"] or 0)
            a = float(row["adj_close"] or row["close"] or 0)
            if c > 0:
                new_ratio = a / c
                # 비율 변화 > 0.5% → 조정 의심
                if abs(new_ratio - old_ratio) > 0.005:
                    warnings.append(
                        f"{ticker} {row['date']}: "
                        f"adj ratio changed {old_ratio:.4f}→{new_ratio:.4f} "
                        f"(possible split/dividend)"
                    )
        except:
            pass
    return warnings


# ══════════════════════════════════════════════════════════════════════════════
# prices.json 빌드 (대시보드용)
# ══════════════════════════════════════════════════════════════════════════════

def build_prices_json(price_dir: Path, tickers: list[str],
                      last_days: int = 180) -> dict:
    """
    각 ticker의 CSV에서 최근 last_days일 데이터를 읽어
    prices.json 형식으로 조립.
    """
    result = {}
    for ticker in tickers:
        csv_path = price_dir / f"{ticker}.csv"
        rows = load_csv(csv_path)
        if not rows:
            continue
        rows = rows[-last_days:]

        dates  = [r["date"]  for r in rows]
        closes = [float(r["close"]) if r["close"] else None for r in rows]
        highs  = [float(r["high"])  if r["high"]  else None for r in rows]
        lows   = [float(r["low"])   if r["low"]   else None for r in rows]
        vols   = [int(r["volume"])  if r["volume"] else None for r in rows]
        adjs   = [float(r["adj_close"]) if r["adj_close"] else None for r in rows]

        rets = [None] + [
            round((closes[i] - closes[i-1]) / closes[i-1], 6)
            if closes[i] and closes[i-1] and closes[i-1] != 0 else None
            for i in range(1, len(closes))
        ]

        result[ticker] = {
            "dates":     dates,
            "closes":    closes,
            "highs":     highs,
            "lows":      lows,
            "volumes":   vols,
            "adj_closes":adjs,
            "returns":   rets,
        }
    return result


# ══════════════════════════════════════════════════════════════════════════════
# 데이터 품질 리포트
# ══════════════════════════════════════════════════════════════════════════════

def build_quality_report(price_dir: Path, tickers: list[str]) -> dict:
    report = {}
    for ticker in tickers:
        csv_path = price_dir / f"{ticker}.csv"
        rows = load_csv(csv_path)
        if not rows:
            report[ticker] = {"status": "missing", "n_rows": 0}
            continue

        closes = [float(r["close"]) if r["close"] else None for r in rows]
        nulls  = sum(1 for c in closes if c is None)
        first  = rows[0]["date"]
        last   = rows[-1]["date"]

        # 거래일 gap 감지 (5일 이상 공백)
        dates = [r["date"] for r in rows]
        gaps  = []
        for i in range(1, len(dates)):
            d1 = datetime.strptime(dates[i-1], "%Y-%m-%d").date()
            d2 = datetime.strptime(dates[i],   "%Y-%m-%d").date()
            delta = (d2 - d1).days
            if delta > 7:
                gaps.append(f"{dates[i-1]}→{dates[i]} ({delta}d)")

        # 이상치 감지: 전일 대비 ±30% 초과
        outliers = []
        for i in range(1, len(closes)):
            c, p = closes[i], closes[i-1]
            if c and p and p > 0:
                chg = abs(c - p) / p
                if chg > 0.30:
                    outliers.append(f"{rows[i]['date']} {chg:.1%}")

        report[ticker] = {
            "status":    "ok" if not nulls and not gaps else "warning",
            "n_rows":    len(rows),
            "first_date":first,
            "last_date": last,
            "null_count":nulls,
            "gaps":      gaps[:5],
            "outliers":  outliers[:5],
        }
    return report


# ══════════════════════════════════════════════════════════════════════════════
# Manifest
# ══════════════════════════════════════════════════════════════════════════════

def load_manifest(price_dir: Path) -> dict:
    p = price_dir / "_manifest.json"
    if p.exists():
        try:
            return json.loads(p.read_text())
        except:
            pass
    return {}


def save_manifest(price_dir: Path, manifest: dict):
    p = price_dir / "_manifest.json"
    p.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--price-dir",  default="data/prices",
                    help="OHLCV CSV 원장 디렉터리")
    ap.add_argument("--out-json",   default="site/data/prices.json")
    ap.add_argument("--out-meta",   default="site/data/prices_meta.json")
    ap.add_argument("--tickers",    default=None)
    ap.add_argument("--last-days",  type=int, default=180,
                    help="prices.json에 포함할 최근 일수")
    ap.add_argument("--full-reload", action="store_true",
                    help="전체 재다운로드 (분할/조정 발생 시 사용)")
    ap.add_argument("--since",      default=None,
                    help="YYYY-MM-DD: 이 날짜부터 재다운로드 (--full-reload 대신)")
    args = ap.parse_args()

    price_dir = Path(args.price_dir)
    price_dir.mkdir(parents=True, exist_ok=True)

    tickers = [t.strip().upper() for t in args.tickers.split(",")]\
              if args.tickers else TICKERS

    today    = date.today()
    manifest = load_manifest(price_dir)
    all_warnings = []

    print(f"Incremental price update — {today}")
    print(f"Mode: {'FULL RELOAD' if args.full_reload else 'INCREMENTAL'}")
    print(f"Tickers: {len(tickers)}")
    print()

    added_total = 0

    for ticker in tickers:
        csv_path = price_dir / f"{ticker}.csv"
        existing = load_csv(csv_path)

        # ── 시작 날짜 결정 ──────────────────────────────────────────────────────
        if args.full_reload or args.since:
            if args.since:
                since = datetime.strptime(args.since, "%Y-%m-%d").date()
            else:
                since = today - timedelta(days=args.last_days + 10)
            # 전체 재다운로드: 기존 데이터 보존 후 병합
            existing_before = since
        elif existing:
            last_date = existing[-1]["date"]
            since = datetime.strptime(last_date, "%Y-%m-%d").date()
        else:
            # 첫 실행: 전체 last_days 다운로드
            since = today - timedelta(days=args.last_days + 10)

        # ── Fetch ───────────────────────────────────────────────────────────────
        new_rows = fetch_incremental(ticker, since, today)

        if not new_rows:
            print(f"  — {ticker:<6} no new data (last: {existing[-1]['date'] if existing else 'none'})")
            continue

        # ── 조정 감지 ────────────────────────────────────────────────────────────
        warnings = detect_adjustment(existing, new_rows, ticker)
        all_warnings.extend(warnings)
        for w in warnings:
            print(f"  ⚠ {w}")

        # ── Append ──────────────────────────────────────────────────────────────
        n_added = append_rows(csv_path, new_rows)
        added_total += n_added

        total_rows = len(load_csv(csv_path))
        manifest[ticker] = {
            "last_date": new_rows[-1]["date"],
            "n_rows":    total_rows,
            "updated":   today.isoformat(),
        }

        status = "✓" if not warnings else "⚠"
        print(f"  {status} {ticker:<6} +{n_added:2d} rows → {total_rows} total "
              f"(through {new_rows[-1]['date']})")

    # ── Build prices.json ───────────────────────────────────────────────────────
    print(f"\nBuilding prices.json (last {args.last_days} days)…")
    prices = build_prices_json(price_dir, tickers, args.last_days)

    out = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tickers": prices,
    }
    import gzip as _gz
    Path(args.out_json).parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(out, ensure_ascii=False, separators=(",",":"))
    Path(args.out_json).write_text(raw, encoding="utf-8")
    # gzip 버전 (GitHub Pages는 정적이므로 fetch 시 Accept-Encoding 처리)
    with _gz.open(Path(args.out_json).with_suffix(".json.gz"), "wt",
                  encoding="utf-8", compresslevel=6) as gz:
        gz.write(raw)

    # ── Quality report ──────────────────────────────────────────────────────────
    quality = build_quality_report(price_dir, tickers)
    meta = {
        "updated":  datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "warnings": all_warnings,
        "quality":  quality,
        "summary": {
            "total_tickers": len([t for t in tickers if (price_dir/f"{t}.csv").exists()]),
            "total_rows_added_today": added_total,
            "tickers_with_warnings":  len([t for t in quality if quality[t].get("status") == "warning"]),
        }
    }
    Path(args.out_meta).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_meta).write_text(json.dumps(meta, indent=2, ensure_ascii=False))

    # ── Save manifest ───────────────────────────────────────────────────────────
    save_manifest(price_dir, manifest)

    print(f"\n→ {args.out_json}   ({len(prices)} tickers)")
    print(f"→ {args.out_meta}")
    print(f"→ {price_dir}/_manifest.json")
    print(f"\nRows added today: {added_total}")
    if all_warnings:
        print(f"\n⚠ {len(all_warnings)} adjustment warnings — check prices_meta.json")


if __name__ == "__main__":
    main()