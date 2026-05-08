"""
insider_analyzer.py — Pillar 5 (Insider Trading) 0..1 score
============================================================
SEC Form 4 raw 거래 → 종목별 insider sentiment.

설계 철학 (학술 lit):
  - P (open-market 매수)  = 강한 양의 alpha (~+12% / 12mo, Cohen-Malloy-Pomorski)
  - S (open-market 매도)  = 약한 음의 alpha, 대부분 RSU vest noise
  - 10b5-1 매도          = 사전 plan, 정보 약함 → 0.3× 다운-웨이트
  - Cluster buying       = 가장 강력한 시그널 (≥2 distinct insiders / 30d)
  - 직책 가중             = CEO/CFO > 다른 임원 > director > 10pct

P=0 종목 (78/84) = 0.5 neutral  (RSU churn으로 페널티 X)
P>0 종목 (17/84) = composite computed

Score 식 (P>0일 때만):
  pillar5 = 0.50 × buy_signal
          + 0.30 × cluster_score
          + 0.20 × sales_pressure_inverse

  buy_signal     = 0.5 + 0.5 × log1p(weighted_buy_value) / log1p(50M)   ∈ [0.5, 1.0]
  cluster_score  = min(1, max_distinct_buyers_in_30d / 4)               ∈ [0, 1]
  sales_pres_inv = 0.7 - 0.4 × min(1, n_discretionary_sells / 30)        ∈ [0.3, 0.7]

입력:
  data/sec_form4_cache/parsed/{T}.json   (sec_form4_fetcher.py 산출)

출력:
  site/data/insider/{T}.json              (per-ticker 풀 페이로드)
  site/data/insider.json                  (universe 인덱스, 대시보드/predict.py용)

CLI:
  python scripts/insider_analyzer.py                  # rank all 84 (parsed/ 기반)
  python scripts/insider_analyzer.py GS NVDA          # 특정 종목
  python scripts/insider_analyzer.py --buyers         # P>0 종목만
  python scripts/insider_analyzer.py --top 20
  python scripts/insider_analyzer.py --json
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# ── 경로 ─────────────────────────────────────────────────────────────────────
PARSED_DIR    = ROOT / "data" / "sec_form4_cache" / "parsed"
TICKERS_FILE  = ROOT / "config" / "prices_tickers.txt"
OUTPUT_DIR    = ROOT / "site" / "data" / "insider"
OUTPUT_INDEX  = ROOT / "site" / "data" / "insider.json"


# ── 가중치 / 캡 ──────────────────────────────────────────────────────────────
W_BUY     = 0.50
W_CLUSTER = 0.30
W_SP_INV  = 0.20

WEIGHT_10B5_1 = 0.3                    # confirmed: 학술 표준 다운-웨이트
ROLE_WEIGHTS = {
    "ceo_cfo":  2.0,
    "officer":  1.5,
    "director": 1.0,
    "ten_pct":  0.8,
    "other":    0.5,
}

BUY_VALUE_CAP        = 50_000_000      # weighted buy value (USD) → 1.0
CLUSTER_WINDOW_DAYS  = 30
CLUSTER_CAP          = 4               # 4명 distinct buyers → 1.0
DISC_SELL_NORM       = 30              # n discretionary sells → full penalty
CLUSTER_MIN_VALUE    = 10_000          # 한 buyer의 윈도우 합계 < $10K → cluster 자격 X
                                       # (예: TSMC 의무 보유 프로그램 같은 noise 제거)

NEUTRAL = 0.5


# ── 직책 분류 ────────────────────────────────────────────────────────────────
RE_CEO  = re.compile(r"(?:^|[^a-z])(ceo|chief\s+executive|president(?:\s+&|,| and)?\s*ceo)", re.I)
RE_CFO  = re.compile(r"(?:^|[^a-z])(cfo|chief\s+financial)", re.I)


def classify_role(owner: dict) -> str:
    title = (owner.get("title") or "").lower()
    if owner.get("is_officer"):
        if RE_CEO.search(title) or RE_CFO.search(title):
            return "ceo_cfo"
        return "officer"
    if owner.get("is_director"):
        return "director"
    if owner.get("is_10pct_owner"):
        return "ten_pct"
    return "other"


# ── Amendment dedup ──────────────────────────────────────────────────────────

def dedup_filings(filings: list[dict]) -> list[dict]:
    """4/A는 prior 거래 재고지일 수 있음. (date, owner_cik, code, shares, price)
    키로 더 최근 filed 1건만 유지. v1 universe엔 4/A 거의 없을 것이지만 안전망.
    """
    sorted_filings = sorted(filings, key=lambda f: f.get("filed", ""), reverse=True)
    seen: set[tuple] = set()
    out: list[dict] = []
    for f in sorted_filings:
        owner_cik = (f.get("owner") or {}).get("cik")
        kept = []
        for t in f.get("transactions") or []:
            key = (
                t.get("date"), owner_cik, t.get("code"),
                round(t.get("shares") or 0.0, 4),
                round(t.get("price")  or 0.0, 4),
            )
            if key in seen:
                continue
            seen.add(key)
            kept.append(t)
        if kept:
            f2 = dict(f); f2["transactions"] = kept
            out.append(f2)
    # 시간 순 정렬 복원 (오래된 → 최신)
    out.sort(key=lambda f: f.get("filed", ""))
    return out


# ── 점수 ────────────────────────────────────────────────────────────────────

def _max_cluster_30d_qualified(
    p_events: list[tuple[str, str, float]], min_value: float,
) -> int:
    """rolling 30일 윈도우의 최대 distinct *qualified* buyer 수.

    p_events: (date_str, owner_cik, value) 리스트
    qualification: 해당 윈도우 안에서 그 buyer의 P 거래 합계 ≥ min_value
    (micro-purchase / 의무 보유 프로그램 noise 필터링)
    """
    if not p_events:
        return 0
    parsed: list[tuple[date, str, float]] = []
    for d_str, cik, v in p_events:
        try:
            parsed.append((date.fromisoformat(d_str), cik, float(v or 0)))
        except (TypeError, ValueError):
            continue
    if not parsed:
        return 0
    parsed.sort()
    best = 0
    for anchor, _, _ in parsed:
        win_start = anchor - timedelta(days=CLUSTER_WINDOW_DAYS)
        # 윈도우 안 buyer별 합계
        sums: dict[str, float] = {}
        for d, cik, v in parsed:
            if win_start <= d <= anchor:
                sums[cik] = sums.get(cik, 0.0) + v
        qualified = sum(1 for v in sums.values() if v >= min_value)
        if qualified > best:
            best = qualified
    return best


def score_ticker(parsed_payload: dict) -> dict:
    """parsed_form4_payload (sec_form4_fetcher 산출) → Pillar 5 score dict.

    적용되는 필터:
      A) transaction_date >= today - window_days
         (Form 4는 *filing date* 기준 90d 인덱스이지만 거래 본문은 수년 전일 수
          있음 — 실제 alpha는 최근 거래에서만 나옴)
      B) owner_cik == issuer_cik 필터
         (예: GOLDMAN SACHS GROUP INC가 GS 자체 시장조성 활동을 P로 보고하는 케이스
          → 진짜 insider sentiment 아님)
    """
    ticker = parsed_payload["ticker"]
    issuer_cik_raw = (parsed_payload.get("filings") or [{}])[0].get("issuer", {}).get("cik")
    # issuer.cik는 "0000320193" 또는 "320193" 변형 가능 — int로 정규화
    try:
        issuer_cik_int = int(issuer_cik_raw) if issuer_cik_raw else None
    except (TypeError, ValueError):
        issuer_cik_int = None

    window_days = parsed_payload.get("days") or 90
    cutoff = (date.today() - timedelta(days=window_days)).isoformat()

    filings = dedup_filings(parsed_payload.get("filings") or [])

    # transaction stream with owner context (필터 A, B 적용)
    txs: list[dict] = []
    n_filtered_old = 0
    n_filtered_self = 0
    for f in filings:
        owner = f.get("owner") or {}
        # 필터 B: 자기-주식 (firm-on-self)
        try:
            owner_cik_int = int(owner.get("cik")) if owner.get("cik") else None
        except (TypeError, ValueError):
            owner_cik_int = None
        if owner_cik_int is not None and issuer_cik_int is not None \
                and owner_cik_int == issuer_cik_int:
            n_filtered_self += len(f.get("transactions") or [])
            continue

        role = classify_role(owner)
        role_w = ROLE_WEIGHTS.get(role, 0.5)
        for t in f.get("transactions") or []:
            # 필터 A: 거래 날짜가 분석 윈도우 안에 있어야 함
            tdate = t.get("date")
            if tdate and tdate < cutoff:
                n_filtered_old += 1
                continue
            txs.append({
                **t,
                "owner_cik":   owner.get("cik"),
                "owner_name":  owner.get("name"),
                "owner_title": owner.get("title"),
                "role":        role,
                "role_w":      role_w,
            })

    # ── BUY SIGNAL ───────────────────────────────────────────────────
    purchases = [
        t for t in txs
        if t["code"] == "P" and t.get("value")
    ]
    weighted_buy_value = sum(
        (t["value"] or 0.0) * t["role_w"] *
        (WEIGHT_10B5_1 if t.get("is_10b5_1") else 1.0)
        for t in purchases
    )
    if not purchases:
        buy_signal = NEUTRAL
    else:
        scaled = math.log1p(weighted_buy_value) / math.log1p(BUY_VALUE_CAP)
        buy_signal = NEUTRAL + 0.5 * min(1.0, max(0.0, scaled))

    # ── CLUSTER ──────────────────────────────────────────────────────
    # 한 buyer의 윈도우 합계 ≥ CLUSTER_MIN_VALUE 만 cluster 멤버로 카운트
    # (TSMC 의무 보유 프로그램 같은 micro-purchase noise 제거)
    p_events = [
        (t["date"], t["owner_cik"], t.get("value") or 0.0)
        for t in purchases
        if t.get("date") and t.get("owner_cik")
    ]
    max_distinct = _max_cluster_30d_qualified(p_events, CLUSTER_MIN_VALUE)
    cluster_score = min(1.0, max_distinct / CLUSTER_CAP)

    # ── SALES PRESSURE INVERSE ──────────────────────────────────────
    discretionary_sells = [
        t for t in txs
        if t["code"] == "S" and not t.get("is_10b5_1")
    ]
    n_disc_sells = len(discretionary_sells)
    sp_inv = 0.7 - 0.4 * min(1.0, n_disc_sells / DISC_SELL_NORM)
    sp_inv = max(0.3, min(0.7, sp_inv))

    # ── COMPOSITE ───────────────────────────────────────────────────
    n_filings = len(filings)
    if n_filings == 0:
        score = NEUTRAL
        status = "no_filings"
    elif not purchases:
        # P=0 정책: RSU churn은 페널티 X — 평탄 0.5
        score = NEUTRAL
        status = "neutral_rsu_churn"
    else:
        score = (W_BUY * buy_signal
                 + W_CLUSTER * cluster_score
                 + W_SP_INV  * sp_inv)
        # P>0 = 항상 ≥ neutral 0.5 (asymmetric philosophy: 매수는 약해도 양의 시그널,
        # 매도는 P=0 정책에 의해 이미 neutral에서 멈춤)
        score = max(NEUTRAL, score)
        status = "ok"

    # ── Top buyers (UI/explanation) ─────────────────────────────────
    top_buyers: list[dict] = []
    if purchases:
        agg: dict = defaultdict(lambda: {
            "name": "", "title": "", "role": "",
            "value": 0.0, "n_tx": 0, "is_10b5_1_any": False,
        })
        for t in purchases:
            k = t["owner_cik"]
            b = agg[k]
            b["name"]  = t["owner_name"]
            b["title"] = t["owner_title"]
            b["role"]  = t["role"]
            b["value"] += (t["value"] or 0.0)
            b["n_tx"]  += 1
            if t.get("is_10b5_1"):
                b["is_10b5_1_any"] = True
        top_buyers = sorted(agg.values(), key=lambda x: -x["value"])[:5]
        for b in top_buyers:
            b["value"] = round(b["value"], 2)

    # ── Summary ─────────────────────────────────────────────────────
    bits = []
    if max_distinct >= 2:
        bits.append(f"Cluster: {max_distinct} distinct insiders / 30d")
    if purchases:
        bits.append(f"${weighted_buy_value/1e6:.2f}M weighted buys ({len(purchases)} P txs)")
    if not bits:
        if n_filings == 0:
            bits.append("No Form 4 filings (foreign issuer / no insiders)")
        else:
            bits.append(f"RSU churn / no insider buys ({n_disc_sells} discretionary S)")

    return {
        "ticker":         ticker,
        "pillar5_score":  round(score, 4),
        "status":         status,
        "components": {
            "buy_signal":                round(buy_signal, 4),
            "cluster_score":             round(cluster_score, 4),
            "sales_pressure_inverse":    round(sp_inv, 4),
            "weighted_buy_value":        round(weighted_buy_value, 2),
            "n_purchases":               len(purchases),
            "n_distinct_buyers_30d_max": max_distinct,
            "n_sales_discretionary":     n_disc_sells,
            "n_filings":                 n_filings,
            "n_transactions":            len(txs),
            "n_filtered_old_txs":        n_filtered_old,
            "n_filtered_self_txs":       n_filtered_self,
        },
        "top_buyers":     top_buyers,
        "summary":        "; ".join(bits),
        "as_of":          parsed_payload.get("fetched_at"),
        "window_days":    parsed_payload.get("days"),
    }


def score_unmapped(ticker: str, reason: str) -> dict:
    """CIK map에 없는 ticker (예: ANSS) → neutral 0.5."""
    return {
        "ticker":        ticker,
        "pillar5_score": NEUTRAL,
        "status":        reason,
        "components": {
            "buy_signal": NEUTRAL, "cluster_score": 0.0, "sales_pressure_inverse": 0.5,
            "weighted_buy_value": 0.0,
            "n_purchases": 0, "n_distinct_buyers_30d_max": 0,
            "n_sales_discretionary": 0,
            "n_filings": 0, "n_transactions": 0,
        },
        "top_buyers": [],
        "summary":    f"No data ({reason})",
        "as_of":      None,
        "window_days": None,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Universe scoring + IO
# ══════════════════════════════════════════════════════════════════════════════

def load_universe() -> list[str]:
    return [t.strip() for t in TICKERS_FILE.read_text().splitlines() if t.strip()]


def load_parsed(ticker: str) -> dict | None:
    path = PARSED_DIR / f"{ticker}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return None


def score_universe(tickers: list[str]) -> list[dict]:
    out: list[dict] = []
    for tk in tickers:
        parsed = load_parsed(tk.upper())
        if parsed is None:
            out.append(score_unmapped(tk.upper(), "unmapped_or_uncached"))
            continue
        out.append(score_ticker(parsed))
    return out


def write_outputs(scores: list[dict]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for s in scores:
        (OUTPUT_DIR / f"{s['ticker']}.json").write_text(
            json.dumps(s, indent=2, default=str)
        )

    index = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "n_tickers":    len(scores),
        "weights": {
            "buy_signal":             W_BUY,
            "cluster_score":          W_CLUSTER,
            "sales_pressure_inverse": W_SP_INV,
            "weight_10b5_1":          WEIGHT_10B5_1,
            "buy_value_cap":          BUY_VALUE_CAP,
            "cluster_window_days":    CLUSTER_WINDOW_DAYS,
            "cluster_cap":            CLUSTER_CAP,
        },
        "tickers": {
            s["ticker"]: {
                "score":       s["pillar5_score"],
                "status":      s["status"],
                "n_purchases": s["components"]["n_purchases"],
                "n_distinct_buyers_30d_max": s["components"]["n_distinct_buyers_30d_max"],
                "weighted_buy_value":        s["components"]["weighted_buy_value"],
                "summary":     s["summary"],
            }
            for s in scores
        },
    }
    OUTPUT_INDEX.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_INDEX.write_text(json.dumps(index, indent=2, default=str))


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def _format_row(s: dict) -> str:
    c = s["components"]
    return (
        f"{s['ticker']:<6}  P5={s['pillar5_score']:.3f}  "
        f"[{s['status']:<18}]  "
        f"P={c['n_purchases']:>3}  "
        f"clust={c['n_distinct_buyers_30d_max']:>2}  "
        f"$buy={c['weighted_buy_value']/1e6:>7.2f}M  "
        f"discS={c['n_sales_discretionary']:>3}  "
        f"|  {s['summary']}"
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Pillar 5 — Insider trading analyzer")
    ap.add_argument("tickers", nargs="*", help="(default: all in prices_tickers.txt)")
    ap.add_argument("--buyers", action="store_true",
                    help="P>0 종목만 출력")
    ap.add_argument("--top", type=int, default=None,
                    help="P5 점수 상위 N (with --buyers는 P>0 안에서)")
    ap.add_argument("--bottom", type=int, default=None,
                    help="P5 점수 하위 N")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--no-write", action="store_true",
                    help="site/data/insider/ 출력 없이 stdout만")
    args = ap.parse_args(argv)

    tickers = [t.upper() for t in args.tickers] if args.tickers else load_universe()
    scores = score_universe(tickers)

    if args.buyers:
        scores = [s for s in scores if s["components"]["n_purchases"] > 0]

    scores.sort(key=lambda s: -s["pillar5_score"])

    if args.top:
        view = scores[:args.top]
    elif args.bottom:
        view = scores[-args.bottom:]
    else:
        view = scores

    if args.json:
        json.dump(view, sys.stdout, indent=2, default=str); print()
    else:
        print(f"\n{'TICKER':<6}  PILLAR5  STATUS               P  CLUST  WGT BUY    DISC S  | SUMMARY")
        print("-" * 100)
        for s in view:
            print(_format_row(s))
        # Aggregate stats
        with_buys = [s for s in scores if s["components"]["n_purchases"] > 0]
        n_neutral = sum(1 for s in scores if s["status"] == "neutral_rsu_churn")
        n_no_data = sum(1 for s in scores if s["status"] in {"no_filings", "unmapped_or_uncached"})
        print(f"\n[stats] total={len(scores)}  with_buys={len(with_buys)}  "
              f"neutral_rsu_churn={n_neutral}  no_data={n_no_data}")

    if not args.no_write and not args.tickers:
        # 전체 universe 모드일 때만 site/data/ 갱신
        write_outputs(score_universe(load_universe()))
        print(f"[wrote] {OUTPUT_INDEX} + {OUTPUT_DIR}/", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
