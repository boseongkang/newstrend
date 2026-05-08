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


def subs_cache_path_for(cik: str) -> Path:
    return RAW_CACHE_DIR / f"subs_CIK{cik}.json"


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


def fetch_subs_with_cache(
    fetcher: SECFetcher, ticker: str, refresh: bool = False
) -> tuple[dict, str]:
    """submissions API 응답 디스크 캐시 (별도 파일, 같은 24h TTL).

    Returns (subs, source). 캐시 hit 시 fetcher의 _subs_cache 를 warm.
    """
    cik = fetcher.resolve_cik(ticker)
    path = subs_cache_path_for(cik)
    if not refresh and is_cache_fresh(path):
        subs = json.loads(path.read_text())
        fetcher._subs_cache[ticker.upper()] = subs
        return subs, "cache"

    subs = fetcher.get_submissions(ticker)
    RAW_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(subs))
    return subs, "fetched"


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

    Status:
      'ok'             — companyfacts + submissions 둘 다 있음
      'metadata_only'  — submissions만, companyfacts가 404 또는 us-gaap 빈
                         (예: ETF QQQ, IFRS 발행자 TSM)
      'empty'          — 양쪽 다 실패 (submissions도 404 — 매우 드뭄)

    KeyError (CIK 미매핑)는 그대로 raise — 호출자에서 unresolved로 처리.
    """
    # ── METADATA (먼저) — submissions은 거의 항상 있고 작음
    metadata: dict | None = None
    meta_source: str | None = None
    try:
        _, meta_source = fetch_subs_with_cache(fetcher, ticker, refresh=refresh)
        metadata = fetcher.get_company_metadata(ticker)
    except FileNotFoundError:
        pass  # submissions 404 — 가능성 거의 없음, 메타 없이 계속
    except Exception:
        pass  # 네트워크/JSON 에러 — 메타 없이 계속

    # ── FINANCIALS (companyfacts 시도, 실패해도 메타로 fallback 가능)
    summary: dict | None = None
    quarterly: dict[str, list[dict]] = {m: [] for m in QUARTERLY_METRICS}
    annual:    dict[str, list[dict]] = {m: [] for m in ANNUAL_METRICS}
    facts_source: str | None = None
    facts_status = "missing"

    try:
        _, facts_source = fetch_with_cache(fetcher, ticker, refresh=refresh)
        summary = fetcher.get_summary_metrics(ticker)
        for m in QUARTERLY_METRICS:
            recs = fetcher.get_quarterly_data(ticker, m, n=QUARTERLY_DEPTH)
            quarterly[m] = [_slim_record(r) for r in recs]
        for m in ANNUAL_METRICS:
            recs = fetcher.get_annual_data(ticker, m, n=ANNUAL_DEPTH)
            annual[m] = [_slim_record(r) for r in recs]
        has_data = bool(summary["raw"]["revenue"] or summary["raw"]["assets"])
        facts_status = "ok" if has_data else "empty"
    except FileNotFoundError:
        facts_status = "not_found"        # 예: QQQ companyfacts 404

    # ── 종합 status
    if facts_status == "ok":
        status = "ok"
    elif metadata:
        status = "metadata_only"
    else:
        status = "empty"                  # 양쪽 다 실패

    name = (metadata or {}).get("name") or (summary or {}).get("entity")
    cik  = (metadata or {}).get("cik")  or (summary or {}).get("cik")

    return {
        "ticker":       ticker.upper(),
        "entity":       name,
        "cik":          cik,
        "fetched_at":   datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source":       {"facts": facts_source, "submissions": meta_source},
        "status":       status,
        "facts_status": facts_status,
        "fy_end":       (summary or {}).get("fy_end"),
        "as_of":        (summary or {}).get("as_of"),
        "metadata":     metadata,
        "summary":      summary,
        "quarterly":    quarterly,
        "annual":       annual,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 집계
# ══════════════════════════════════════════════════════════════════════════════

def aggregate_index(per_ticker: dict[str, dict], skipped: dict[str, str]) -> dict:
    """site/data/fundamentals.json — 한 줄 요약 집계.

    metadata-only 종목도 포함 (sector 분석에 필요). financial slice는 None.
    """
    rows = {}
    ok_count = 0
    meta_only_count = 0
    for tk, payload in per_ticker.items():
        s = payload.get("summary")
        rows[tk] = {
            "entity":     payload["entity"],
            "cik":        payload["cik"],
            "status":     payload["status"],
            "fy_end":     payload["fy_end"],
            "as_of":      payload["as_of"],
            "metadata":   payload.get("metadata"),
            "raw":        s["raw"]    if s else None,
            "ratios":     s["ratios"] if s else None,
            "tags_used":  s.get("tags_used") if s else None,
        }
        if payload["status"] == "ok":
            ok_count += 1
        elif payload["status"] == "metadata_only":
            meta_only_count += 1

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "universe_size":          len(per_ticker) + len(skipped),
        "fetched_ok":             ok_count,
        "fetched_metadata_only":  meta_only_count,
        "skipped":                skipped,         # {ticker: reason}
        "tickers":                rows,
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
            skipped[tk] = f"empty (facts={payload['facts_status']}, no meta)"
            print(f"[{i:3d}/{len(universe)}] {tk:6s} SKIP empty", file=sys.stderr)
            manifest[tk] = {"cik": payload["cik"], "ok": False,
                            "error": "empty (no facts, no meta)",
                            "ts": payload["fetched_at"]}
            continue

        per_ticker[tk] = payload
        manifest[tk] = {
            "cik":    payload["cik"],
            "ok":     True,
            "status": payload["status"],
            "source": payload["source"],
            "ts":     payload["fetched_at"],
        }

        # 종목별 파일 즉시 저장 — 큰 universe에서 중간 실패해도 진행분 보존
        if not args.no_write:
            (PER_TICKER_DIR / f"{tk}.json").write_text(
                json.dumps(payload, indent=2, default=str)
            )

        if payload["status"] == "ok":
            rev = (payload["summary"]["raw"].get("revenue") or 0) / 1e9
            ni  = (payload["summary"]["raw"].get("net_income") or 0) / 1e9
            sec = (payload.get("metadata") or {}).get("owner_org") or "—"
            print(f"[{i:3d}/{len(universe)}] {tk:6s} OK   "
                  f"rev=${rev:>7.1f}B  ni=${ni:>6.1f}B  sec={sec[:18]:<18}  "
                  f"src={payload['source']['facts']}", file=sys.stderr)
        else:  # metadata_only
            meta = payload.get("metadata") or {}
            etype = meta.get("entity_type") or "?"
            sec = meta.get("owner_org") or "—"
            print(f"[{i:3d}/{len(universe)}] {tk:6s} META {etype:<10s}  "
                  f"sec={sec[:18]:<18}  facts={payload['facts_status']}",
                  file=sys.stderr)

    elapsed = time.time() - t0

    if not args.no_write:
        save_manifest(manifest)
        if not args.no_aggregate:
            agg = aggregate_index(per_ticker, skipped)
            AGGREGATE_FILE.write_text(json.dumps(agg, indent=2, default=str))
            print(f"\n[build] wrote {AGGREGATE_FILE.relative_to(ROOT)}", file=sys.stderr)

    ok_n   = sum(1 for p in per_ticker.values() if p["status"] == "ok")
    meta_n = sum(1 for p in per_ticker.values() if p["status"] == "metadata_only")
    print(
        f"\n[build] done  ok={ok_n}  metadata_only={meta_n}  skipped={len(skipped)}  "
        f"elapsed={elapsed:.1f}s  http_stats={fetcher.stats}",
        file=sys.stderr,
    )
    if skipped:
        print(f"[build] skip detail: {skipped}", file=sys.stderr)

    return 0 if per_ticker else 1


if __name__ == "__main__":
    raise SystemExit(main())
