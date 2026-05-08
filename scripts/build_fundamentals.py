"""
build_fundamentals.py — Phase 2: per-ticker fundamentals + aggregate index
==========================================================================
4-Pillar 분석 시스템의 Pillar 4 (재무) 데이터셋 빌더.
sec_edgar_fetcher.SECFetcher 를 wrapping해 (1) 디스크 캐시, (2) 종목별 분석
파일, (3) 대시보드용 집계 인덱스를 만듦.

입력:
  config/prices_tickers.txt        ← universe (현재 77 종목)
  config/ticker_to_cik.json        ← ticker → CIK 매핑 (Phase 1.5 산출)

출력 (gitignored, large):
  data/sec_cache/CIK{N}.json       ← raw companyfacts 캐시 (~3–5MB/종목)
  data/sec_cache/_manifest.json    ← {ticker: {cik, last_fetched, ok, error}}

출력 (committed, compact):
  site/data/fundamentals/{T}.json  ← 종목별 summary + 분기 12 + 연간 5
  site/data/fundamentals.json      ← 전체 universe 집계 인덱스

캐시 정책:
  - 같은 날 (≤24h) 재실행 → 디스크 캐시 재사용, API 호출 0
  - --refresh 플래그로 강제 재취득
  - 외국 ADR / ETF / 신규 상장 등 us-gaap 빈 데이터는 스킵하고 사유 기록

CLI:
  python scripts/build_fundamentals.py                # 전체 universe
  python scripts/build_fundamentals.py AAPL MSFT      # 일부만
  python scripts/build_fundamentals.py --refresh      # 캐시 무시 후 fresh fetch
  python scripts/build_fundamentals.py --no-aggregate # per-ticker만
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# scripts/ 디렉토리를 import path에 추가 (sec_edgar_fetcher 동거주)
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
from sec_edgar_fetcher import SECFetcher  # noqa: E402


# ── 경로 ─────────────────────────────────────────────────────────────────────
TICKERS_FILE     = ROOT / "config" / "prices_tickers.txt"
RAW_CACHE_DIR    = ROOT / "data" / "sec_cache"
CACHE_MANIFEST   = RAW_CACHE_DIR / "_manifest.json"
PER_TICKER_DIR   = ROOT / "site" / "data" / "fundamentals"
AGGREGATE_FILE   = ROOT / "site" / "data" / "fundamentals.json"

CACHE_TTL_HOURS  = 24

# 시계열 깊이
QUARTERLY_DEPTH  = 12     # ≈ 3년치 분기
ANNUAL_DEPTH     = 5

# 종목별 시계열 대상 metric
QUARTERLY_METRICS = ["revenue", "net_income", "operating_income"]
ANNUAL_METRICS    = ["revenue", "net_income", "assets", "equity"]


# ══════════════════════════════════════════════════════════════════════════════
# 디스크 캐시
# ══════════════════════════════════════════════════════════════════════════════

def cache_path_for(cik: str) -> Path:
    return RAW_CACHE_DIR / f"CIK{cik}.json"


def is_cache_fresh(path: Path, ttl_hours: float = CACHE_TTL_HOURS) -> bool:
    if not path.exists():
        return False
    age_hours = (time.time() - path.stat().st_mtime) / 3600
    return age_hours < ttl_hours


def load_manifest() -> dict:
    if CACHE_MANIFEST.exists():
        try:
            return json.loads(CACHE_MANIFEST.read_text())
        except json.JSONDecodeError:
            return {}
    return {}


def save_manifest(m: dict) -> None:
    RAW_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_MANIFEST.write_text(json.dumps(m, indent=2))


def fetch_with_cache(
    fetcher: SECFetcher, ticker: str, refresh: bool = False
) -> tuple[dict, str]:
    """Returns (facts, source) where source ∈ {'cache', 'fetched'}.

    캐시 hit 시에도 fetcher의 인메모리 캐시를 warm 시켜서 후속 get_metric/
    get_summary_metrics 호출이 네트워크에 다시 안 닿게 함.
    """
    cik = fetcher.resolve_cik(ticker)
    path = cache_path_for(cik)
    if not refresh and is_cache_fresh(path):
        facts = json.loads(path.read_text())
        fetcher._facts_cache[ticker.upper()] = facts   # warm in-memory cache
        return facts, "cache"

    facts = fetcher.get_company_facts(ticker)
    RAW_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(facts))
    return facts, "fetched"


# ══════════════════════════════════════════════════════════════════════════════
# 종목별 빌드
# ══════════════════════════════════════════════════════════════════════════════

def _slim_record(r: dict) -> dict:
    """시계열에 저장할 record 압축본."""
    out = {"end": r.get("end"), "val": r.get("val")}
    if r.get("days") is not None:
        out["days"] = r["days"]
    if r.get("fp"):
        out["fp"] = r["fp"]
    if r.get("fy") is not None:
        out["fy"] = r["fy"]
    return out


def build_one(fetcher: SECFetcher, ticker: str, refresh: bool = False) -> dict:
    """단일 종목의 site/data/fundamentals/{T}.json payload를 만듦.

    데이터 누락(빈 us-gaap 등)일 경우 status='empty' 로 표시하고 빈 시계열로 반환.
    """
    facts, source = fetch_with_cache(fetcher, ticker, refresh=refresh)

    summary = fetcher.get_summary_metrics(ticker)

    # 시계열 추출 — 여기서 한 번 더 in-memory cache hit
    quarterly: dict[str, list[dict]] = {}
    for m in QUARTERLY_METRICS:
        recs = fetcher.get_quarterly_data(ticker, m, n=QUARTERLY_DEPTH)
        quarterly[m] = [_slim_record(r) for r in recs]

    annual: dict[str, list[dict]] = {}
    for m in ANNUAL_METRICS:
        recs = fetcher.get_annual_data(ticker, m, n=ANNUAL_DEPTH)
        annual[m] = [_slim_record(r) for r in recs]

    has_data = bool(summary["raw"]["revenue"] or summary["raw"]["assets"])
    status = "ok" if has_data else "empty"

    return {
        "ticker":     ticker.upper(),
        "entity":     summary["entity"],
        "cik":        summary["cik"],
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source":     source,
        "status":     status,
        "fy_end":     summary["fy_end"],
        "as_of":      summary["as_of"],
        "summary":    summary,
        "quarterly":  quarterly,
        "annual":     annual,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 집계
# ══════════════════════════════════════════════════════════════════════════════

def aggregate_index(per_ticker: dict[str, dict], skipped: dict[str, str]) -> dict:
    """site/data/fundamentals.json — 한 줄 요약 집계."""
    rows = {}
    for tk, payload in per_ticker.items():
        s = payload["summary"]
        rows[tk] = {
            "entity":     payload["entity"],
            "cik":        payload["cik"],
            "fy_end":     payload["fy_end"],
            "as_of":      payload["as_of"],
            "raw":        s["raw"],
            "ratios":     s["ratios"],
            "tags_used":  s["tags_used"],
        }
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "universe_size": len(per_ticker) + len(skipped),
        "fetched_ok":    len(per_ticker),
        "skipped":       skipped,                 # {ticker: reason}
        "tickers":       rows,
    }


# ══════════════════════════════════════════════════════════════════════════════
# universe + CLI
# ══════════════════════════════════════════════════════════════════════════════

def load_universe() -> list[str]:
    return [
        t.strip().upper()
        for t in TICKERS_FILE.read_text().splitlines()
        if t.strip() and not t.startswith("#")
    ]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Build per-ticker fundamentals + aggregate index")
    ap.add_argument("tickers", nargs="*", help="대상 종목 (생략 시 전체 universe)")
    ap.add_argument("--refresh", action="store_true", help="캐시 무시하고 재취득")
    ap.add_argument("--no-aggregate", action="store_true", help="집계 인덱스 생성 생략")
    ap.add_argument("--no-write", action="store_true", help="파일 쓰지 않고 fetch+빌드만")
    args = ap.parse_args(argv)

    fetcher = SECFetcher()
    universe = [t.upper() for t in args.tickers] if args.tickers else load_universe()
    print(f"[build] universe={len(universe)} refresh={args.refresh}", file=sys.stderr)

    PER_TICKER_DIR.mkdir(parents=True, exist_ok=True)

    manifest = load_manifest()
    per_ticker: dict[str, dict] = {}
    skipped: dict[str, str] = {}

    t0 = time.time()
    for i, tk in enumerate(universe, 1):
        try:
            payload = build_one(fetcher, tk, refresh=args.refresh)
        except KeyError as e:
            skipped[tk] = f"unresolved: {e}"
            print(f"[{i:3d}/{len(universe)}] {tk:6s} SKIP unresolved", file=sys.stderr)
            manifest[tk] = {"ok": False, "error": str(e),
                            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds")}
            continue
        except FileNotFoundError as e:
            skipped[tk] = f"sec 404: {e}"
            print(f"[{i:3d}/{len(universe)}] {tk:6s} SKIP sec 404", file=sys.stderr)
            manifest[tk] = {"ok": False, "error": "sec 404",
                            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds")}
            continue
        except Exception as e:
            skipped[tk] = f"{type(e).__name__}: {e}"
            print(f"[{i:3d}/{len(universe)}] {tk:6s} ERR {type(e).__name__}: {e}", file=sys.stderr)
            manifest[tk] = {"ok": False, "error": f"{type(e).__name__}: {e}",
                            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds")}
            continue

        if payload["status"] == "empty":
            skipped[tk] = "empty us-gaap"
            print(f"[{i:3d}/{len(universe)}] {tk:6s} SKIP empty (ETF/ADR?)", file=sys.stderr)
            manifest[tk] = {"cik": payload["cik"], "ok": False,
                            "error": "empty us-gaap",
                            "ts": payload["fetched_at"]}
            continue

        per_ticker[tk] = payload
        manifest[tk] = {"cik": payload["cik"], "ok": True,
                        "source": payload["source"],
                        "ts": payload["fetched_at"]}

        # 종목별 파일 즉시 저장 — 큰 universe에서 중간 실패해도 진행분 보존
        if not args.no_write:
            (PER_TICKER_DIR / f"{tk}.json").write_text(
                json.dumps(payload, indent=2, default=str)
            )

        rev = (payload["summary"]["raw"].get("revenue") or 0) / 1e9
        ni = (payload["summary"]["raw"].get("net_income") or 0) / 1e9
        print(f"[{i:3d}/{len(universe)}] {tk:6s} OK  "
              f"rev=${rev:>7.1f}B  ni=${ni:>6.1f}B  src={payload['source']}",
              file=sys.stderr)

    elapsed = time.time() - t0

    if not args.no_write:
        save_manifest(manifest)
        if not args.no_aggregate:
            agg = aggregate_index(per_ticker, skipped)
            AGGREGATE_FILE.write_text(json.dumps(agg, indent=2, default=str))
            print(f"\n[build] wrote {AGGREGATE_FILE.relative_to(ROOT)}", file=sys.stderr)

    print(
        f"\n[build] done  ok={len(per_ticker)}  skipped={len(skipped)}  "
        f"elapsed={elapsed:.1f}s  http_stats={fetcher.stats}",
        file=sys.stderr,
    )
    if skipped:
        print(f"[build] skip detail: {skipped}", file=sys.stderr)

    return 0 if per_ticker else 1


if __name__ == "__main__":
    raise SystemExit(main())
