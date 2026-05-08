"""
sec_edgar_fetcher.py — SEC EDGAR companyfacts client
=====================================================
4-Pillar 분석 시스템의 Pillar 4 (재무) 데이터 소스.
yfinance 대체 — 정부 공식 API, 무료, 무인증, 차단 위험 0.

아키텍처:
  config/ticker_to_cik.json
    └ ticker → 10-digit CIK 로컬 매핑 (10,000+ 종목 지원)
  data.sec.gov/api/xbrl/companyfacts/CIK{N}.json
    └ per-company XBRL facts (us-gaap + dei)

핵심 책임:
  1) User-Agent (.env의 SEC_USER_AGENT) 헤더 부착
  2) rate limit 9 req/s (공식 한도 10, 안전 마진)
  3) retry (429 / 5xx → 지수 backoff)
  4) tag fallback: Revenues vs RevenueFromContractWithCustomer... 등
     동일 logical metric에 대해 여러 us-gaap tag를 순차 시도 후 병합
  5) record dedup: (end, fp, form) 기준, form 우선순위 + 최신 filed
  6) fp=None / 비정기 8-K 필터, calendar-end 정렬

공개 메서드:
  - get_company_facts(ticker) → raw companyfacts JSON
  - get_metric(ticker, name)   → 정렬·중복제거된 records list
  - get_quarterly_data(ticker, name, n=12) → 최근 분기 records
  - get_summary_metrics(ticker) → 최신 연간 + 계산된 ratios snapshot

CLI:
  python scripts/sec_edgar_fetcher.py AAPL
  python scripts/sec_edgar_fetcher.py AAPL MSFT NVDA
  python scripts/sec_edgar_fetcher.py AAPL --metric net_income --quarterly
  python scripts/sec_edgar_fetcher.py --raw AAPL > /tmp/aapl_facts.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import requests


# ── 상수 ─────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CIK_MAP = ROOT / "config" / "ticker_to_cik.json"
USER_AGENT_FALLBACK = "Newstrend boseong@example.com"
BASE_URL = "https://data.sec.gov"
RATE_LIMIT_HZ = 9            # 10/s 공식 한도, 1 안전 마진
DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 4

# 같은 (end, fp) 기간을 여러 폼이 보고할 때 우선순위 (낮을수록 우선)
FORM_PRIORITY = {
    "10-K": 0, "10-K/A": 1,
    "20-F": 2, "20-F/A": 3,
    "40-F": 4, "40-F/A": 5,
    "10-Q": 10, "10-Q/A": 11,
    "6-K": 20,
    "8-K": 30,
}

ANNUAL_FORMS = ("10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A")
QUARTERLY_FORMS = ("10-Q", "10-Q/A", "6-K")

# logical metric name → 시도 순서대로의 us-gaap tag 리스트
# 회사가 시기별·산업별로 다른 태그를 사용 (ASC 606 도입 등) → fallback 필수
METRIC_TAGS: dict[str, list[str]] = {
    "revenue": [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "SalesRevenueNet",
        "SalesRevenueGoodsNet",
    ],
    "net_income": [
        "NetIncomeLoss",
        "ProfitLoss",
    ],
    "operating_income": [
        "OperatingIncomeLoss",
    ],
    "assets": ["Assets"],
    "assets_current": ["AssetsCurrent"],
    "liabilities": ["Liabilities"],
    "liabilities_current": ["LiabilitiesCurrent"],
    "equity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ],
    "eps_basic": ["EarningsPerShareBasic"],
    "cash": [
        "CashAndCashEquivalentsAtCarryingValue",
        "Cash",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    ],
    "long_term_debt": [
        "LongTermDebt",
        "LongTermDebtNoncurrent",
    ],
}

# 연간 / 분기에 어울리는 metric (flow vs stock)
FLOW_METRICS = {"revenue", "net_income", "operating_income"}     # 기간 합계
STOCK_METRICS = {                                                # 시점 잔액
    "assets", "assets_current", "liabilities", "liabilities_current",
    "equity", "cash", "long_term_debt",
}


# ══════════════════════════════════════════════════════════════════════════════
# Fetcher
# ══════════════════════════════════════════════════════════════════════════════

class SECFetcher:
    """SEC EDGAR companyfacts API 클라이언트.

    인스턴스 1개로 한 프로세스 안에서 세션·캐시·rate-limit 공유.
    """

    def __init__(
        self,
        cik_map_path: str | Path | None = None,
        user_agent: str | None = None,
        rate_limit_hz: float = RATE_LIMIT_HZ,
    ):
        self.user_agent = user_agent or os.getenv("SEC_USER_AGENT", USER_AGENT_FALLBACK)
        if "example.com" in self.user_agent:
            print(
                f"[sec] WARN: SEC_USER_AGENT 미설정, placeholder 사용 → "
                f"{self.user_agent!r}. .env에 실제 이메일 추가 권장.",
                file=sys.stderr,
            )

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "Host": "data.sec.gov",
        })

        self.cik_map = self._load_cik_map(cik_map_path)
        self.rate_limit_hz = rate_limit_hz
        self._min_gap = 1.0 / rate_limit_hz
        self._last_call = 0.0
        self._facts_cache: dict[str, dict] = {}
        self.stats = {"requests": 0, "retries": 0, "cache_hits": 0, "errors": 0}

    # ── CIK 매핑 ─────────────────────────────────────────────────────────────

    @staticmethod
    def _load_cik_map(path: str | Path | None) -> dict[str, dict]:
        path = Path(path) if path else DEFAULT_CIK_MAP
        if not path.exists():
            raise FileNotFoundError(
                f"CIK map not found: {path}. "
                f"먼저 https://www.sec.gov/files/company_tickers.json 으로 생성."
            )
        d = json.loads(path.read_text())
        # 두 가지 형식 모두 지원: {"tickers": {...}} 또는 {"AAPL": {...}}
        return d.get("tickers", d)

    def resolve_cik(self, ticker: str) -> str:
        ticker = ticker.upper()
        entry = self.cik_map.get(ticker)
        if entry is None:
            raise KeyError(f"ticker {ticker!r} not in CIK map ({len(self.cik_map)} entries)")
        cik = entry["cik"] if isinstance(entry, dict) else entry
        return f"{int(cik):010d}"

    # ── HTTP ────────────────────────────────────────────────────────────────

    def _throttle(self) -> None:
        wait = self._last_call + self._min_gap - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _get(self, url: str, max_retries: int = DEFAULT_RETRIES) -> dict:
        last_status: int | None = None
        for attempt in range(max_retries):
            self._throttle()
            self.stats["requests"] += 1
            try:
                r = self.session.get(url, timeout=DEFAULT_TIMEOUT)
            except requests.RequestException as e:
                self.stats["retries"] += 1
                backoff = 2 ** attempt
                print(f"[sec] network error ({e}); retry in {backoff}s", file=sys.stderr)
                time.sleep(backoff)
                continue

            last_status = r.status_code
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404:
                raise FileNotFoundError(f"SEC 404: {url}")
            if r.status_code in (429, 500, 502, 503, 504):
                self.stats["retries"] += 1
                backoff = 2 ** attempt
                print(
                    f"[sec] HTTP {r.status_code} on {url}; retry in {backoff}s "
                    f"(attempt {attempt+1}/{max_retries})",
                    file=sys.stderr,
                )
                time.sleep(backoff)
                continue
            r.raise_for_status()

        self.stats["errors"] += 1
        raise RuntimeError(f"SEC GET failed after {max_retries} attempts (last={last_status}): {url}")

    # ── 공개 API ────────────────────────────────────────────────────────────

    def get_company_facts(self, ticker: str) -> dict:
        """Return full companyfacts JSON for a ticker, with in-process cache."""
        ticker = ticker.upper()
        if ticker in self._facts_cache:
            self.stats["cache_hits"] += 1
            return self._facts_cache[ticker]
        cik = self.resolve_cik(ticker)
        url = f"{BASE_URL}/api/xbrl/companyfacts/CIK{cik}.json"
        data = self._get(url)
        self._facts_cache[ticker] = data
        return data

    def get_metric(self, ticker: str, metric_name: str) -> list[dict]:
        """Logical metric → deduped, calendar-sorted records.

        - tag fallback: METRIC_TAGS의 모든 태그를 순회해 합침
        - dedup: (end, fp, form) 기준 → form 우선순위, 그 다음 최신 filed
        - fp=None 등 malformed 레코드 제거
        """
        tags = METRIC_TAGS.get(metric_name, [metric_name])
        facts = self.get_company_facts(ticker)
        merged: list[dict] = []
        for tag in tags:
            merged.extend(self._records_for_tag(facts, tag))
        if not merged:
            return []

        # 그룹 키에 unit + start 포함:
        #  - start 누락 시 YTD(~272d)/standalone(~90d)이 충돌해 한쪽이 묻힘
        #  - unit 누락 시 PDD/BIDU 등 외국 발행자가 USD/CNY 동시 보고할 때
        #    한 통화가 묻혀서 다른 통화 값이 USD인 척 노출됨
        # 그룹 안에서는 최신 filed 1건만 유지 (재출간/Amendment 처리).
        groups: dict[tuple, list[dict]] = defaultdict(list)
        for r in merged:
            groups[(r.get("unit"), r.get("start"), r["end"], r["fp"], r["form"])].append(r)
        deduped: list[dict] = []
        for recs in groups.values():
            recs.sort(key=lambda r: r.get("filed") or "", reverse=True)
            deduped.append(recs[0])

        # 단위가 여러 개면 USD를 최우선. EPS류는 USD/shares가 자연 단위.
        # USD가 아예 없는 외국-only 발행자는 단일 통화로 폴백.
        units_present = {r["unit"] for r in deduped}
        if len(units_present) > 1:
            for pref in ("USD", "USD/shares", "shares"):
                if pref in units_present:
                    deduped = [r for r in deduped if r["unit"] == pref]
                    break
            else:
                chosen = sorted(units_present)[0]
                deduped = [r for r in deduped if r["unit"] == chosen]

        deduped.sort(key=lambda r: r["end"])
        return deduped

    def get_quarterly_data(self, ticker: str, metric_name: str, n: int = 12) -> list[dict]:
        """최근 n 분기 records.

        flow metric (revenue/net_income/operating_income)인 경우
        standalone Q-only 기간 (~90일) 만 추출 — 같은 fp의 YTD 레코드 제외.
        stock metric은 days=None이지만 분기 보고가 잘 의미 없으므로 그대로 통과.
        """
        recs = self.get_metric(ticker, metric_name)
        is_flow = metric_name in FLOW_METRICS
        q = []
        for r in recs:
            if r.get("form") not in QUARTERLY_FORMS:
                continue
            fp = r.get("fp")
            if not (fp and fp.startswith("Q")):
                continue
            if is_flow:
                d = r.get("days")
                if d is None or not (80 <= d <= 100):
                    continue                    # YTD / 6M / 9M 누적 제거
            q.append(r)
        return q[-n:]

    def get_annual_data(self, ticker: str, metric_name: str, n: int = 5) -> list[dict]:
        """최근 n 연간 records (10-K/20-F, fp=FY).

        flow metric인 경우 365일 ± 마진 길이만 채택 (52/53주 회계연도 모두 포함).
        """
        recs = self.get_metric(ticker, metric_name)
        is_flow = metric_name in FLOW_METRICS
        a = []
        for r in recs:
            if r.get("form") not in ANNUAL_FORMS:
                continue
            if r.get("fp") != "FY":
                continue
            if is_flow:
                d = r.get("days")
                if d is not None and not (350 <= d <= 380):
                    continue
            a.append(r)
        if not a:
            # 외국 발행자 (20-F)는 fp 라벨이 다를 수 있음 → form만 보고 fallback
            a = [r for r in recs if r.get("form") in ANNUAL_FORMS]
        return a[-n:]

    def get_summary_metrics(self, ticker: str) -> dict:
        """최신 연간 스냅샷 + 계산된 ratios. None-safe."""
        facts = self.get_company_facts(ticker)

        latest_flow: dict[str, dict | None] = {}
        for m in FLOW_METRICS:
            recs = self.get_annual_data(ticker, m, n=1)
            latest_flow[m] = recs[-1] if recs else None

        latest_stock: dict[str, dict | None] = {}
        for m in STOCK_METRICS:
            recs = self.get_metric(ticker, m)
            # 자산/자본 등은 분기별로도 보고됨 → 가장 최근 end 1건
            latest_stock[m] = recs[-1] if recs else None

        eps_recs = self.get_annual_data(ticker, "eps_basic", n=1)
        latest_eps = eps_recs[-1] if eps_recs else None

        def _v(rec: dict | None) -> float | None:
            return float(rec["val"]) if rec and rec.get("val") is not None else None

        rev = _v(latest_flow.get("revenue"))
        ni = _v(latest_flow.get("net_income"))
        op = _v(latest_flow.get("operating_income"))
        assets = _v(latest_stock.get("assets"))
        liab = _v(latest_stock.get("liabilities"))
        eq = _v(latest_stock.get("equity"))
        cur_a = _v(latest_stock.get("assets_current"))
        cur_l = _v(latest_stock.get("liabilities_current"))
        ltd = _v(latest_stock.get("long_term_debt"))

        def _safe_div(num: float | None, den: float | None) -> float | None:
            if num is None or den is None or den == 0:
                return None
            return num / den

        return {
            "ticker": ticker.upper(),
            "entity": facts.get("entityName"),
            "cik": facts.get("cik"),
            "as_of": (latest_stock.get("assets") or {}).get("end"),
            "fy_end": (latest_flow.get("revenue") or {}).get("end"),
            "currency": (latest_flow.get("revenue") or {}).get("unit"),
            "raw": {
                "revenue":           _v(latest_flow.get("revenue")),
                "net_income":        ni,
                "operating_income":  op,
                "assets":            assets,
                "liabilities":       liab,
                "equity":            eq,
                "cash":              _v(latest_stock.get("cash")),
                "long_term_debt":    ltd,
                "assets_current":    cur_a,
                "liabilities_current": cur_l,
                "eps_basic":         _v(latest_eps),
            },
            "ratios": {
                "roe":               _safe_div(ni, eq),
                "roa":               _safe_div(ni, assets),
                "debt_to_equity":    _safe_div(liab, eq),
                "lt_debt_to_equity": _safe_div(ltd, eq),
                "profit_margin":     _safe_div(ni, rev),
                "operating_margin":  _safe_div(op, rev),
                "current_ratio":     _safe_div(cur_a, cur_l),
            },
            "tags_used": {
                m: (rec or {}).get("tag") for m, rec in {
                    **latest_flow, **latest_stock, "eps_basic": latest_eps,
                }.items()
            },
        }

    # ── 내부 ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _records_for_tag(facts: dict, tag: str, taxonomy: str = "us-gaap") -> list[dict]:
        from datetime import date as _date
        node = facts.get("facts", {}).get(taxonomy, {}).get(tag)
        if node is None:
            return []
        out: list[dict] = []
        for unit, recs in (node.get("units") or {}).items():
            for r in recs or []:
                end = r.get("end")
                if not end:                       # malformed record 차단
                    continue
                start = r.get("start")
                days = None
                if start:
                    try:
                        days = (_date.fromisoformat(end) - _date.fromisoformat(start)).days
                    except ValueError:
                        days = None
                out.append({
                    "tag":   tag,
                    "unit":  unit,
                    "start": start,
                    "end":   end,
                    "days":  days,                # flow concept의 기간 길이 (일)
                    "val":   r.get("val"),
                    "fy":    r.get("fy"),
                    "fp":    r.get("fp"),
                    "form":  r.get("form"),
                    "filed": r.get("filed"),
                    "accn":  r.get("accn"),
                })
        return out


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def _format_summary(s: dict) -> str:
    raw = s["raw"]; ratios = s["ratios"]
    lines = [
        f"{s['ticker']}  ({s['entity']})  CIK={s['cik']}  FY end={s['fy_end']}  BS={s['as_of']}",
    ]
    def _fmt_money(v):
        if v is None: return "—"
        if abs(v) >= 1e9:  return f"${v/1e9:>8.2f}B"
        if abs(v) >= 1e6:  return f"${v/1e6:>8.2f}M"
        return f"${v:>10,.0f}"
    def _fmt_pct(v):
        return "—" if v is None else f"{v*100:>7.2f}%"
    def _fmt_x(v):
        return "—" if v is None else f"{v:>7.2f}x"

    lines.append(f"  Revenue          {_fmt_money(raw['revenue'])}      Net income       {_fmt_money(raw['net_income'])}")
    lines.append(f"  Operating inc    {_fmt_money(raw['operating_income'])}      Assets           {_fmt_money(raw['assets'])}")
    lines.append(f"  Liabilities      {_fmt_money(raw['liabilities'])}      Equity           {_fmt_money(raw['equity'])}")
    lines.append(f"  Cash             {_fmt_money(raw['cash'])}      LT debt          {_fmt_money(raw['long_term_debt'])}")
    lines.append(f"  EPS basic        {raw['eps_basic']!s:>11}")
    lines.append("")
    lines.append(f"  ROE  {_fmt_pct(ratios['roe'])}   ROA  {_fmt_pct(ratios['roa'])}   D/E  {_fmt_x(ratios['debt_to_equity'])}   LT D/E  {_fmt_x(ratios['lt_debt_to_equity'])}")
    lines.append(f"  Profit margin   {_fmt_pct(ratios['profit_margin'])}   Op margin  {_fmt_pct(ratios['operating_margin'])}   Current  {_fmt_x(ratios['current_ratio'])}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="SEC EDGAR companyfacts fetcher")
    ap.add_argument("tickers", nargs="+", help="Tickers (e.g. AAPL MSFT NVDA)")
    ap.add_argument("--metric", help="단일 metric만 출력 (예: net_income)")
    ap.add_argument("--quarterly", action="store_true", help="--metric과 함께 분기 데이터")
    ap.add_argument("--annual", action="store_true", help="--metric과 함께 연간 데이터")
    ap.add_argument("--n", type=int, default=8, help="--metric 출력 행 수")
    ap.add_argument("--raw", action="store_true", help="companyfacts JSON 그대로 stdout")
    ap.add_argument("--json", action="store_true", help="summary JSON 출력")
    args = ap.parse_args(argv)

    fetcher = SECFetcher()
    rc = 0
    t0 = time.time()

    for tk in args.tickers:
        try:
            if args.raw:
                json.dump(fetcher.get_company_facts(tk), sys.stdout, indent=2)
                continue
            if args.metric:
                if args.quarterly:
                    recs = fetcher.get_quarterly_data(tk, args.metric, n=args.n)
                elif args.annual:
                    recs = fetcher.get_annual_data(tk, args.metric, n=args.n)
                else:
                    recs = fetcher.get_metric(tk, args.metric)[-args.n:]
                print(f"\n{tk}  metric={args.metric}  rows={len(recs)}")
                for r in recs:
                    print(f"  {r['end']}  {r.get('fp','-'):>3}  {r.get('form','-'):>6}  "
                          f"val={r['val']:>15}  tag={r['tag']}  filed={r.get('filed')}")
                continue

            s = fetcher.get_summary_metrics(tk)
            if args.json:
                json.dump(s, sys.stdout, indent=2, default=str); print()
            else:
                print(_format_summary(s)); print()

        except (KeyError, FileNotFoundError) as e:
            print(f"[skip] {tk}: {e}", file=sys.stderr)
            rc = 2
        except Exception as e:
            print(f"[error] {tk}: {type(e).__name__}: {e}", file=sys.stderr)
            rc = 3

    elapsed = time.time() - t0
    print(f"\n[stats] {fetcher.stats}  elapsed={elapsed:.2f}s", file=sys.stderr)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
