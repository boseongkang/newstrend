"""
sec_form4_fetcher.py — Pillar 5 (Insider Trading) data source
==============================================================
SEC EDGAR Form 4 (Section 16 insider transactions) raw fetcher + parser.
4-Pillar에서 5-Pillar로 진화하는 첫 단계 — 임원 매매 행동 데이터.

아키텍처:
  /submissions/CIK{N}.json (이미 SECFetcher가 캐시 중)
    └ filings.recent → form 4 인덱스 (accession, filingDate, primaryDocument)
  www.sec.gov/Archives/edgar/data/{cik}/{accn_clean}/{primaryDocument}
    └ Form 4 XML (개별 거래 본문)

핵심 책임:
  1) submissions에서 form ∈ {"4","4/A"} & last N days 필터
  2) Archives에서 XML 다운로드 (data.sec.gov ≠ www.sec.gov, 별도 세션)
  3) 9 req/s 통합 throttle (SECFetcher와 같은 인스턴스의 _throttle 공유)
  4) 영구 캐시: by_accession/{N}.xml — Form 4는 immutable (수정시 4/A 새 accession)
  5) 파싱: stdlib xml.etree.ElementTree (외부 의존 0)
  6) 정책 결정 분리 — 이 모듈은 raw 100% 보존, 10b5-1만 *플래그* (정책은 insider_analyzer.py)

Form 4 transaction codes (참고):
  P  open-market or private purchase of non-derivative   ← 진짜 매수 시그널
  S  open-market or private sale of non-derivative       ← 진짜 매도 시그널
  A  grant/award                                          (실거래 아님)
  F  payment of exercise price/tax with shares
  M  exercise/conversion of derivative
  G  bona-fide gift
  J/K other
  D/V acquired/disposed code (orthogonal axis)

CLI:
  python scripts/sec_form4_fetcher.py AAPL
  python scripts/sec_form4_fetcher.py AAPL --days 30
  python scripts/sec_form4_fetcher.py AAPL --raw       # filings.recent 인덱스만
  python scripts/sec_form4_fetcher.py --all            # 84 universe
  python scripts/sec_form4_fetcher.py AAPL --json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from xml.etree import ElementTree as ET

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
from sec_edgar_fetcher import SECFetcher  # noqa: E402

# ── 경로 / 상수 ───────────────────────────────────────────────────────────
CACHE_DIR     = ROOT / "data" / "sec_form4_cache"
BY_ACCN_DIR   = CACHE_DIR / "by_accession"
PARSED_DIR    = CACHE_DIR / "parsed"
MANIFEST_PATH = CACHE_DIR / "_manifest.json"

ARCHIVE_BASE  = "https://www.sec.gov/Archives/edgar/data"
PARSED_TTL_HOURS = 24

FORM4_FORMS       = {"4", "4/A"}
OPEN_MARKET_CODES = {"P", "S"}   # 진짜 매수/매도 시그널


@dataclass
class FilingMeta:
    accession: str          # "0000320193-26-000077"
    form: str               # "4" or "4/A"
    filing_date: str        # "2026-04-17"
    primary_document: str   # "wf-form4_NNN.xml"


# ══════════════════════════════════════════════════════════════════════════════
# Fetcher
# ══════════════════════════════════════════════════════════════════════════════

class SECForm4Fetcher:
    """Form 4 raw fetcher. SECFetcher 인스턴스를 컴포지션해 throttle 공유."""

    def __init__(
        self,
        fetcher: SECFetcher | None = None,
        cache_dir: Path | None = None,
    ):
        self.f = fetcher or SECFetcher()
        self.cache_dir = Path(cache_dir or CACHE_DIR)

        # archives는 host=www.sec.gov, fetcher의 Host header(data.sec.gov)와
        # 다르므로 별도 세션 — fetcher session은 손대지 않음.
        self.archive_session = requests.Session()
        self.archive_session.headers.update({
            "User-Agent":     self.f.user_agent,
            "Accept":         "application/xml,text/xml,*/*",
            "Accept-Encoding": "gzip, deflate",
        })
        self.stats = {
            "xml_fetched": 0, "xml_cached": 0,
            "parse_errors": 0, "no_filings": 0,
        }

    # ── 1) filings.recent → Form 4 index ──────────────────────────────────

    def list_form4_filings(self, ticker: str, days: int = 90) -> list[FilingMeta]:
        """submissions API에서 last `days`일 form 4 filing 메타데이터 추출.

        filings.recent은 ~1000건 캡 — 90일 form 4는 거의 항상 그 안에 들어감.
        매우 prolific 발행자(>1000건 in 90일)는 filings.files 오버플로 처리 필요하나
        v1 universe에서는 발생 가능성 0 — 후일 필요 시 확장.
        """
        subs = self.f.get_submissions(ticker)
        recent = (subs.get("filings") or {}).get("recent") or {}
        forms     = recent.get("form") or []
        accns     = recent.get("accessionNumber") or []
        dates     = recent.get("filingDate") or []
        primaries = recent.get("primaryDocument") or []
        if not (len(forms) == len(accns) == len(dates) == len(primaries)):
            raise ValueError(
                f"submissions.recent arrays length mismatch for {ticker}: "
                f"forms={len(forms)} accns={len(accns)} dates={len(dates)} "
                f"primaries={len(primaries)}"
            )

        cutoff = (date.today() - timedelta(days=days)).isoformat()
        out: list[FilingMeta] = []
        for form, accn, fd, pd in zip(forms, accns, dates, primaries):
            if form not in FORM4_FORMS:
                continue
            if fd < cutoff:
                continue
            out.append(FilingMeta(
                accession=accn, form=form,
                filing_date=fd, primary_document=pd,
            ))
        return out

    # ── 2) XML 다운로드 (immutable disk cache) ────────────────────────────

    @staticmethod
    def _raw_xml_path(primary_doc: str) -> str:
        """submissions의 primaryDocument는 XSLT로 렌더된 HTML 경로:
            'xslF345X06/form4.xml'           → 'form4.xml'
            'xslF345X05/wk-form4_NNN.xml'    → 'wk-form4_NNN.xml'
        같은 디렉토리에 prefix를 뺀 raw XML이 있음.
        """
        m = re.match(r"^xsl[^/]*/(.+)$", primary_doc)
        return m.group(1) if m else primary_doc

    def _archive_url(self, cik: str, accession: str, primary_doc: str) -> str:
        accn_clean = accession.replace("-", "")
        cik_int = int(cik)                    # leading zero 제거
        return f"{ARCHIVE_BASE}/{cik_int}/{accn_clean}/{self._raw_xml_path(primary_doc)}"

    def _xml_cache_path(self, accession: str) -> Path:
        return self.cache_dir / "by_accession" / f"{accession.replace('-', '')}.xml"

    def fetch_form4_xml(self, cik: str, filing: FilingMeta) -> str:
        """Form 4 XML 본문 — 영구 캐시 (Form 4 once filed, never mutates).

        수정은 4/A 라는 *새 accession*이 발급되므로 accession 단위 캐시는 TTL 불필요.
        """
        cache_path = self._xml_cache_path(filing.accession)
        if cache_path.exists():
            cached = cache_path.read_text()
            # 과거에 잘못된 XSLT-rendered HTML이 캐시됐을 수 있음 — XML 헤더로 sanity 체크
            if cached.lstrip().startswith("<?xml") or "<ownershipDocument" in cached[:512]:
                self.stats["xml_cached"] += 1
                return cached
            cache_path.unlink()  # 손상 캐시 폐기, 새로 받음

        url = self._archive_url(cik, filing.accession, filing.primary_document)
        # SEC 공식 9 req/s — fetcher의 통합 throttle 공유.
        self.f._throttle()
        r = self.archive_session.get(url, timeout=30)
        r.raise_for_status()
        text = r.text
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(text)
        self.stats["xml_fetched"] += 1
        return text

    # ── 3) 파싱 (stdlib only) ─────────────────────────────────────────────

    @staticmethod
    def _txt(elem: ET.Element | None, path: str) -> str | None:
        """직접 자식의 텍스트."""
        if elem is None:
            return None
        node = elem.find(path)
        if node is None or node.text is None:
            return None
        s = node.text.strip()
        return s or None

    @staticmethod
    def _val(elem: ET.Element | None, path: str) -> str | None:
        """Form 4의 leaf는 대부분 <value>...</value>로 한 번 더 감싼 구조."""
        if elem is None:
            return None
        node = elem.find(f"{path}/value")
        if node is None or node.text is None:
            return None
        s = node.text.strip()
        return s or None

    @staticmethod
    def _bool_flag(elem: ET.Element | None, path: str) -> bool:
        if elem is None:
            return False
        node = elem.find(path)
        if node is None or node.text is None:
            return False
        return node.text.strip() in {"1", "true", "TRUE"}

    @staticmethod
    def _to_float(s: str | None) -> float | None:
        if not s:
            return None
        try:
            return float(s.replace(",", ""))
        except ValueError:
            return None

    def parse_form4(
        self, xml_str: str,
        accession: str = "", filing_date: str = "", form: str = "4",
    ) -> dict:
        """Form 4 XML → 정규화 dict.

        - 첫 번째 reportingOwner만 owner로 (joint filing은 드물지만 카운트만 보고)
        - non-derivative + derivative 모두 transactions로 합치되 is_derivative 플래그
        - holdings-only(*Holding* 요소)는 Transaction 엘리먼트에 없으니 자연 제외
        - 10b5-1 감지: 문서 레벨 aff10b5One 또는 footnote 텍스트의 "10b5-1" 매치
        """
        root = ET.fromstring(xml_str)

        # Issuer
        issuer = root.find("issuer")
        issuer_out = {
            "cik":    self._txt(issuer, "issuerCik"),
            "name":   self._txt(issuer, "issuerName"),
            "ticker": self._txt(issuer, "issuerTradingSymbol"),
        }

        # Reporting owner
        ros = root.findall("reportingOwner")
        ro = ros[0] if ros else None
        rid = ro.find("reportingOwnerId") if ro is not None else None
        rel = ro.find("reportingOwnerRelationship") if ro is not None else None
        owner = {
            "cik":            self._txt(rid, "rptOwnerCik"),
            "name":           self._txt(rid, "rptOwnerName"),
            "is_director":    self._bool_flag(rel, "isDirector"),
            "is_officer":     self._bool_flag(rel, "isOfficer"),
            "is_10pct_owner": self._bool_flag(rel, "isTenPercentOwner"),
            "is_other":       self._bool_flag(rel, "isOther"),
            "title":          self._txt(rel, "officerTitle"),
        }

        # Footnotes (id → text)
        footnotes: dict[str, str] = {}
        for fn in root.findall("footnotes/footnote"):
            fid = fn.get("id")
            if fid:
                footnotes[fid] = (fn.text or "").strip()
        # 신규 schema의 문서 레벨 10b5-1 플래그
        doc_aff_10b5_1 = self._bool_flag(root, "aff10b5One")

        transactions: list[dict] = []
        for tx_elem in root.findall("nonDerivativeTable/nonDerivativeTransaction"):
            transactions.append(self._parse_tx(
                tx_elem, footnotes, doc_aff_10b5_1, is_derivative=False
            ))
        for tx_elem in root.findall("derivativeTable/derivativeTransaction"):
            transactions.append(self._parse_tx(
                tx_elem, footnotes, doc_aff_10b5_1, is_derivative=True
            ))

        return {
            "accession":          accession,
            "form":               form,
            "filed":              filing_date,
            "period_of_report":   self._val(root, "periodOfReport") or self._txt(root, "periodOfReport"),
            "issuer":             issuer_out,
            "owner":              owner,
            "n_reporting_owners": len(ros),
            "doc_aff_10b5_1":     doc_aff_10b5_1,
            "transactions":       transactions,
        }

    @classmethod
    def _parse_tx(
        cls, tx: ET.Element, footnotes: dict[str, str],
        doc_aff_10b5_1: bool, is_derivative: bool,
    ) -> dict:
        coding = tx.find("transactionCoding")
        code = cls._txt(coding, "transactionCode")

        # transactionCoding 안 + 자식 어디든 footnoteId 참조 수집
        fn_ids: set[str] = set()
        for ref in tx.iter("footnoteId"):
            fid = ref.get("id")
            if fid:
                fn_ids.add(fid)

        # 10b5-1 감지: 문서 플래그 OR 참조 footnote 텍스트에 "10b5-1"
        ref_texts = [footnotes.get(fid, "") for fid in fn_ids]
        is_10b5_1 = doc_aff_10b5_1 or any(
            re.search(r"10b5[\s‐‑-]?1", t, re.IGNORECASE)
            for t in ref_texts
        )

        amts = tx.find("transactionAmounts")
        post = tx.find("postTransactionAmounts")
        shares = cls._to_float(cls._val(amts, "transactionShares"))
        price  = cls._to_float(cls._val(amts, "transactionPricePerShare"))
        ad     = cls._val(amts, "transactionAcquiredDisposedCode")

        return {
            "security":           cls._val(tx, "securityTitle"),
            "date":               cls._val(tx, "transactionDate"),
            "code":               code,
            "is_open_market":     code in OPEN_MARKET_CODES,
            "is_10b5_1":          is_10b5_1,
            "is_derivative":      is_derivative,
            "shares":             shares,
            "price":              price,
            "value":              (shares * price) if (shares is not None and price is not None) else None,
            "acquired_disposed":  ad,
            "shares_owned_after": cls._to_float(
                cls._val(post, "sharesOwnedFollowingTransaction")
            ),
            "footnote_refs":      sorted(fn_ids) or None,
        }

    # ── 4) 종목별 end-to-end ──────────────────────────────────────────────

    def get_ticker_form4(
        self, ticker: str, days: int = 90, refresh: bool = False,
    ) -> dict:
        """ticker → last `days` aggregated form 4 결과.

        parsed/{T}.json 24h TTL — refresh=True로 우회.
        XML cache (by_accession/) 는 immutable이라 항상 hit.
        """
        ticker = ticker.upper()
        cik = self.f.resolve_cik(ticker)
        parsed_path = PARSED_DIR / f"{ticker}.json"
        if not refresh and self._is_parsed_fresh(parsed_path):
            try:
                return json.loads(parsed_path.read_text())
            except json.JSONDecodeError:
                pass  # 캐시 손상 시 재생성

        filings = self.list_form4_filings(ticker, days=days)
        out_filings: list[dict] = []
        for fm in filings:
            try:
                xml = self.fetch_form4_xml(cik, fm)
                parsed = self.parse_form4(
                    xml, accession=fm.accession,
                    filing_date=fm.filing_date, form=fm.form,
                )
                out_filings.append(parsed)
            except Exception as e:
                self.stats["parse_errors"] += 1
                print(
                    f"[form4] {ticker} {fm.accession} parse/fetch error: "
                    f"{type(e).__name__}: {e}",
                    file=sys.stderr,
                )

        # 집계 통계 (raw — 정책 적용은 insider_analyzer.py)
        all_tx = [t for f in out_filings for t in f["transactions"]]
        n_tx           = len(all_tx)
        n_open_market  = sum(1 for t in all_tx if t["is_open_market"])
        n_purchases    = sum(1 for t in all_tx if t["code"] == "P")
        n_sales        = sum(1 for t in all_tx if t["code"] == "S")
        n_10b5_1       = sum(1 for t in all_tx if t["is_10b5_1"])
        n_derivative   = sum(1 for t in all_tx if t["is_derivative"])
        distinct_owners = len({f["owner"].get("cik") for f in out_filings if f["owner"].get("cik")})

        result = {
            "ticker":          ticker,
            "cik":             cik,
            "days":            days,
            "fetched_at":      datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "n_filings":       len(out_filings),
            "n_transactions":  n_tx,
            "n_open_market":   n_open_market,
            "n_purchases":     n_purchases,
            "n_sales":         n_sales,
            "n_10b5_1":        n_10b5_1,
            "n_derivative":    n_derivative,
            "distinct_owners": distinct_owners,
            "filings":         out_filings,
        }
        if not out_filings:
            self.stats["no_filings"] += 1
        parsed_path.parent.mkdir(parents=True, exist_ok=True)
        parsed_path.write_text(json.dumps(result, indent=2, default=str))
        return result

    @staticmethod
    def _is_parsed_fresh(path: Path) -> bool:
        if not path.exists():
            return False
        age_h = (time.time() - path.stat().st_mtime) / 3600
        return age_h < PARSED_TTL_HOURS

    def update_manifest(self, results: dict[str, dict]) -> None:
        MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
        manifest: dict = {}
        if MANIFEST_PATH.exists():
            try:
                manifest = json.loads(MANIFEST_PATH.read_text())
            except json.JSONDecodeError:
                manifest = {}
        for tk, r in results.items():
            if r is None:
                continue
            n_filings = r.get("n_filings", 0)
            manifest[tk] = {
                "cik":              r.get("cik"),
                "ts":               r.get("fetched_at"),
                "n_filings":        n_filings,
                "n_transactions":   r.get("n_transactions"),
                "n_purchases":      r.get("n_purchases"),
                "n_sales":          r.get("n_sales"),
                "n_10b5_1":         r.get("n_10b5_1"),
                "distinct_owners":  r.get("distinct_owners"),
                "status":           "ok" if n_filings > 0 else "no_filings",
            }
        MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=True))


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def _format_result(r: dict) -> str:
    lines = [
        f"\n{r['ticker']}  CIK={r['cik']}  fetched={r['fetched_at']}  days={r['days']}",
        (
            f"  filings={r['n_filings']}  txs={r['n_transactions']}  "
            f"open_market={r['n_open_market']}  P={r['n_purchases']}  "
            f"S={r['n_sales']}  10b5-1={r['n_10b5_1']}  "
            f"deriv={r['n_derivative']}  insiders={r['distinct_owners']}"
        ),
    ]
    for f in r["filings"]:
        owner = f["owner"]
        title = owner.get("title") or (
            "Director" if owner.get("is_director") else "10%" if owner.get("is_10pct_owner") else "—"
        )
        lines.append(f"\n  [{f['form']}] {f['filed']}  {owner.get('name','?')}  ({title})")
        for t in f["transactions"]:
            flags = []
            if t.get("is_10b5_1"):    flags.append("10b5-1")
            if t.get("is_derivative"): flags.append("deriv")
            flag = (" [" + ",".join(flags) + "]") if flags else ""
            shares = t.get("shares") or 0.0
            price  = t.get("price")  or 0.0
            value  = t.get("value")  or 0.0
            ad = t.get("acquired_disposed") or "?"
            lines.append(
                f"    {t.get('date','?')}  code={(t.get('code') or '-'):>2}  "
                f"{ad}  {shares:>12,.0f} sh  @ ${price:>8.2f}  "
                f"= ${value:>15,.0f}{flag}"
            )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="SEC Form 4 (insider trading) fetcher")
    ap.add_argument("tickers", nargs="*", help="Tickers (default: --all)")
    ap.add_argument("--all", action="store_true",
                    help="84-종목 universe 전체 (config/prices_tickers.txt)")
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--refresh", action="store_true",
                    help="parsed/ TTL 우회 (XML 영구캐시는 항상 사용)")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--raw", action="store_true",
                    help="filings.recent에서 form 4 인덱스만 출력 (XML 다운로드 X)")
    args = ap.parse_args(argv)

    if args.all:
        u_path = ROOT / "config" / "prices_tickers.txt"
        tickers = [t.strip() for t in u_path.read_text().splitlines() if t.strip()]
    else:
        tickers = [t.upper() for t in args.tickers]
    if not tickers:
        ap.error("Provide tickers or --all")

    fetcher = SECForm4Fetcher()
    rc = 0
    t0 = time.time()
    results: dict[str, dict] = {}

    for tk in tickers:
        try:
            if args.raw:
                fls = fetcher.list_form4_filings(tk, days=args.days)
                print(f"\n{tk}  form4 filings (last {args.days}d): {len(fls)}")
                for fm in fls:
                    print(f"  {fm.filing_date}  {fm.form:>4}  {fm.accession}  {fm.primary_document}")
                continue
            r = fetcher.get_ticker_form4(tk, days=args.days, refresh=args.refresh)
            results[tk] = r
            if args.json:
                json.dump(r, sys.stdout, indent=2, default=str); print()
            else:
                print(_format_result(r))
        except (KeyError, FileNotFoundError) as e:
            print(f"[skip] {tk}: {e}", file=sys.stderr)
            rc = 2
        except Exception as e:
            print(f"[error] {tk}: {type(e).__name__}: {e}", file=sys.stderr)
            rc = 3

    if results:
        fetcher.update_manifest(results)
    elapsed = time.time() - t0
    print(
        f"\n[stats] form4={fetcher.stats}  sec={fetcher.f.stats}  elapsed={elapsed:.2f}s",
        file=sys.stderr,
    )
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
