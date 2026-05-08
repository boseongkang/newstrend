"""
fundamentals_analyzer.py — Pillar 4 quality / growth / health scoring
======================================================================
SEC EDGAR fundamentals → 0..1 점수.

설계 원칙:
  - 가격 비의존 (PER/PBR 같은 valuation은 predict.py 통합 단계에서)
  - 4 Pillar 직교성: TA(가격), Word(뉴스), Sector, Fundamentals(품질)
  - 모든 점수 0..1 정규화 → predict.py 가중합과 호환

3개 sub-score:
  quality_score  ROE / op_margin / profit_margin 정규화 평균
  growth_score   3년 revenue CAGR / NI CAGR 평균
  health_score   D/E (낮을수록), current_ratio, NI 부호 (적자 페널티 0.5×)

fundamental_score = 0.35·quality + 0.35·growth + 0.30·health
결측 sub-score는 0.5 (중립) 으로 채움.

CLI:
  python scripts/fundamentals_analyzer.py                # 전체 universe rank
  python scripts/fundamentals_analyzer.py AAPL MSFT NVDA # 특정 종목
  python scripts/fundamentals_analyzer.py --json         # JSON 출력
  python scripts/fundamentals_analyzer.py --top 10       # 상/하위 N만
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parent.parent
PER_TICKER_DIR = ROOT / "site" / "data" / "fundamentals"


# ── 점수 캡 (정규화 한도) ──────────────────────────────────────────────────
ROE_CAP             = 0.40   # ROE 40% 이상이면 만점
OP_MARGIN_CAP       = 0.40
PROFIT_MARGIN_CAP   = 0.30
REV_CAGR_CAP        = 0.30   # 연 30% 이상 성장이면 만점
NI_CAGR_CAP         = 0.30
DE_CAP              = 3.0    # D/E 3.0 이상이면 health=0
CURRENT_LO          = 0.5    # current_ratio (cr - 0.5) / 1.5 → [0,1]
CURRENT_RANGE       = 1.5

# 통합 가중치
W_QUALITY = 0.35
W_GROWTH  = 0.35
W_HEALTH  = 0.30

CAGR_YEARS = 3
LOSS_PENALTY = 0.5

NEUTRAL_FILL = 0.5    # sub-score 결측 시 채움값


# ══════════════════════════════════════════════════════════════════════════════
# 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def _norm(x: float | None, cap: float) -> float:
    """[0, cap] 범위로 클램프 후 [0,1] 정규화. None / 음수 → 0."""
    if x is None or cap <= 0:
        return 0.0
    return max(0.0, min(cap, x)) / cap


def _safe_cagr(start: float | None, end: float | None, years: int) -> float | None:
    """양수 endpoint만 유효한 CAGR. 둘 중 하나가 음수/0이면 None."""
    if start is None or end is None or years <= 0:
        return None
    if start <= 0 or end <= 0:
        return None
    return (end / start) ** (1.0 / years) - 1.0


def _series_endpoints(records: list[dict], n_years: int) -> tuple[float | None, float | None]:
    """annual records list에서 (n년전 값, 최신 값) 튜플 반환.

    records에 n+1개 미만이면 가능한 가장 오래된 ↔ 최신 사용.
    """
    vals = [r.get("val") for r in records if r and r.get("val") is not None]
    if len(vals) < 2:
        return None, None
    if len(vals) >= n_years + 1:
        return vals[-(n_years + 1)], vals[-1]
    return vals[0], vals[-1]


# ══════════════════════════════════════════════════════════════════════════════
# 점수 함수 — 각각 returns (score | None, rationale_parts)
# ══════════════════════════════════════════════════════════════════════════════

def calculate_quality_score(payload: dict) -> tuple[float | None, list[dict]]:
    summary = payload.get("summary") or {}
    ratios = summary.get("ratios") or {}
    roe   = ratios.get("roe")
    op_m  = ratios.get("operating_margin")
    pm    = ratios.get("profit_margin")
    if all(v is None for v in (roe, op_m, pm)):
        return None, []

    parts = [
        {"name": "roe",            "score": _norm(roe, ROE_CAP),            "value": roe},
        {"name": "operating_margin", "score": _norm(op_m, OP_MARGIN_CAP),   "value": op_m},
        {"name": "profit_margin",  "score": _norm(pm, PROFIT_MARGIN_CAP),   "value": pm},
    ]
    return mean(p["score"] for p in parts), parts


def calculate_growth_score(payload: dict) -> tuple[float | None, list[dict]]:
    annual = payload.get("annual") or {}
    rev_records = annual.get("revenue") or []
    ni_records  = annual.get("net_income") or []
    if len(rev_records) < 2 and len(ni_records) < 2:
        return None, []

    rev_start, rev_end = _series_endpoints(rev_records, CAGR_YEARS)
    ni_start,  ni_end  = _series_endpoints(ni_records,  CAGR_YEARS)
    rev_cagr = _safe_cagr(rev_start, rev_end, CAGR_YEARS)
    ni_cagr  = _safe_cagr(ni_start,  ni_end,  CAGR_YEARS)

    rev_score = _norm(rev_cagr, REV_CAGR_CAP)
    ni_score  = _norm(ni_cagr,  NI_CAGR_CAP)
    score = 0.5 * rev_score + 0.5 * ni_score

    parts = [
        {"name": f"rev_cagr_{CAGR_YEARS}y", "score": rev_score, "value": rev_cagr},
        {"name": f"ni_cagr_{CAGR_YEARS}y",  "score": ni_score,  "value": ni_cagr},
    ]
    return score, parts


def calculate_health_score(payload: dict) -> tuple[float | None, list[dict]]:
    summary = payload.get("summary") or {}
    ratios = summary.get("ratios") or {}
    raw    = summary.get("raw") or {}
    d_to_e = ratios.get("debt_to_equity")
    cr     = ratios.get("current_ratio")
    ni     = raw.get("net_income")

    if d_to_e is None and cr is None:
        return None, []

    de_score = (1.0 - _norm(d_to_e, DE_CAP)) if d_to_e is not None else NEUTRAL_FILL
    cr_score = max(0.0, min(CURRENT_RANGE, (cr or 0) - CURRENT_LO)) / CURRENT_RANGE
    base = mean([de_score, cr_score])

    if ni is not None and ni < 0:
        base *= LOSS_PENALTY
        ni_note = f"loss penalty ×{LOSS_PENALTY}"
    elif ni is not None:
        ni_note = "profitable"
    else:
        ni_note = "ni unknown"

    parts = [
        {"name": "debt_to_equity", "score": de_score, "value": d_to_e},
        {"name": "current_ratio",  "score": cr_score, "value": cr},
        {"name": "ni_sign",        "score": None,    "value": ni_note},
    ]
    return base, parts


# ══════════════════════════════════════════════════════════════════════════════
# 통합
# ══════════════════════════════════════════════════════════════════════════════

def score_ticker(payload: dict) -> dict:
    """payload (site/data/fundamentals/{T}.json 구조) → 점수 + rationale."""
    q, q_parts = calculate_quality_score(payload)
    g, g_parts = calculate_growth_score(payload)
    h, h_parts = calculate_health_score(payload)

    if q is None and g is None and h is None:
        return {
            "ticker": payload.get("ticker"),
            "quality_score": None,
            "growth_score":  None,
            "health_score":  None,
            "fundamental_score": None,
            "summary": "no fundamentals available (metadata-only)",
            "rationale": {"quality": [], "growth": [], "health": []},
        }

    qf = q if q is not None else NEUTRAL_FILL
    gf = g if g is not None else NEUTRAL_FILL
    hf = h if h is not None else NEUTRAL_FILL
    fund = W_QUALITY * qf + W_GROWTH * gf + W_HEALTH * hf

    return {
        "ticker": payload.get("ticker"),
        "quality_score":     None if q is None else round(q, 3),
        "growth_score":      None if g is None else round(g, 3),
        "health_score":      None if h is None else round(h, 3),
        "fundamental_score": round(fund, 3),
        "summary": generate_summary(payload, q, g, h),
        "rationale": {
            "quality": q_parts,
            "growth":  g_parts,
            "health":  h_parts,
        },
    }


def generate_summary(payload: dict, q, g, h) -> str:
    """사람 읽는 한 줄 요약. 본 함수가 score_ticker 외에서도 호출 가능."""
    summary = payload.get("summary") or {}
    ratios  = summary.get("ratios") or {}
    raw     = summary.get("raw") or {}
    annual  = payload.get("annual") or {}
    parts: list[str] = []

    roe = ratios.get("roe")
    op_m = ratios.get("operating_margin")
    if q is not None and roe is not None and op_m is not None:
        if q >= 0.7:
            label = "strong"
        elif q >= 0.4:
            label = "moderate"
        else:
            label = "weak"
        parts.append(f"{label} quality (ROE {roe*100:.0f}%, op {op_m*100:.0f}%)")

    rev_records = annual.get("revenue") or []
    rs, re = _series_endpoints(rev_records, CAGR_YEARS)
    cagr = _safe_cagr(rs, re, CAGR_YEARS)
    if cagr is not None:
        parts.append(f"rev CAGR {cagr*100:+.0f}%/y")
    elif rs is not None and re is not None:
        parts.append("rev declining")

    ni = raw.get("net_income")
    de = ratios.get("debt_to_equity")
    if ni is not None and ni < 0:
        parts.append(f"loss ${ni/1e9:.1f}B")
    elif de is not None:
        if de < 1:
            parts.append(f"low leverage (D/E {de:.2f})")
        elif de < 2:
            parts.append(f"moderate leverage (D/E {de:.2f})")
        else:
            parts.append(f"high leverage (D/E {de:.2f})")

    return "; ".join(parts) if parts else "fundamentals incomplete"


# ══════════════════════════════════════════════════════════════════════════════
# universe + CLI
# ══════════════════════════════════════════════════════════════════════════════

def load_payload(ticker: str) -> dict | None:
    p = PER_TICKER_DIR / f"{ticker.upper()}.json"
    if not p.exists():
        return None
    return json.loads(p.read_text())


def score_universe() -> list[dict]:
    out = []
    for p in sorted(PER_TICKER_DIR.glob("*.json")):
        try:
            out.append(score_ticker(json.loads(p.read_text())))
        except Exception as e:
            print(f"[warn] {p.name}: {type(e).__name__}: {e}", file=sys.stderr)
    return out


def _print_table(rows: list[dict]) -> None:
    print(f"{'ticker':<8s} {'fund':>5s} {'qual':>5s} {'grow':>5s} {'heal':>5s}  summary")
    print("-" * 110)
    for r in rows:
        def _f(v):
            return "  —  " if v is None else f"{v:.3f}"
        print(f"{r['ticker']:<8s} {_f(r['fundamental_score'])} "
              f"{_f(r['quality_score'])} {_f(r['growth_score'])} {_f(r['health_score'])}  "
              f"{r['summary']}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Pillar 4 fundamentals scoring")
    ap.add_argument("tickers", nargs="*", help="대상 종목 (생략 시 universe)")
    ap.add_argument("--json", action="store_true", help="JSON 출력")
    ap.add_argument("--top", type=int, help="rank 리포트 시 상/하위 N만")
    args = ap.parse_args(argv)

    if args.tickers:
        results = []
        for tk in args.tickers:
            payload = load_payload(tk)
            if payload is None:
                print(f"[skip] {tk}: payload not found", file=sys.stderr)
                continue
            results.append(score_ticker(payload))
    else:
        results = score_universe()

    if args.json:
        json.dump(results, sys.stdout, indent=2, default=str)
        print()
        return 0

    scored = [r for r in results if r["fundamental_score"] is not None]
    unscored = [r for r in results if r["fundamental_score"] is None]
    scored.sort(key=lambda r: -r["fundamental_score"])

    if args.top and not args.tickers:
        n = args.top
        print(f"=== TOP {n} ===")
        _print_table(scored[:n])
        print(f"\n=== BOTTOM {n} ===")
        _print_table(scored[-n:])
    else:
        _print_table(scored if not args.tickers else results)

    if not args.tickers:
        bins = Counter()
        for r in scored:
            s = r["fundamental_score"]
            if s < 0.3:   bins["weak (<.3)"] += 1
            elif s < 0.5: bins["avg (.3-.5)"] += 1
            elif s < 0.7: bins["solid (.5-.7)"] += 1
            else:         bins["excellent (.7+)"] += 1

        print(f"\n[stats] scored={len(scored)}  metadata_only={len(unscored)}")
        for k in ("excellent (.7+)", "solid (.5-.7)", "avg (.3-.5)", "weak (<.3)"):
            print(f"  {k:18s} {bins.get(k, 0)}")
        if scored:
            print(f"  mean fundamental_score = {mean(r['fundamental_score'] for r in scored):.3f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
