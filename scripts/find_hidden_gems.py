"""
find_hidden_gems.py — 4-Pillar 합성 + 무명도 ranking
======================================================
"숨겨진 유망주 발견" — 큰 회사가 아닌, 작지만 우수한 종목 자동 surfacing.

4 Pillar 합성 (각 0..1, missing = 0.5 neutral):
  P1 sentiment       FinBERT filtered_score → (s+1)/2
  P2 sector_rel      자기 sector 내 fundamental_score percentile
  P3 ta              predict.py predictions.json action+confidence 매핑
  P4 fundamentals    fundamentals_analyzer.score_ticker → 0..1

quality          = 0.40·P4 + 0.25·P1 + 0.25·P3 + 0.10·P2

무명도 (obscurity, 0..1):
  v1 = 1 - revenue_percentile(universe).
       시총은 가격 데이터 부족 (29/81 ticker)으로 v2에 미룸.
       매출 percentile은 81 ticker 모두 가용.

hidden_gems_score = quality · (0.5 + 0.5·obscurity)
  → obscurity가 quality를 압도하지 않음. 작아서 점수만 받는 종목 차단.

Hard gates (ranking 전 제외):
  - P4 fundamental_score < 0.40 (weak fundamentals = value trap 위험)
  - net_income < 0 (적자 회사)
  - status != "ok" (TSM/QQQ 같은 metadata-only)
  - entity_type == "investment" (ETF)

출력: site/data/hidden_gems.json (Top N + 사유 + 위험)

CLI:
  python scripts/find_hidden_gems.py
  python scripts/find_hidden_gems.py --top 50
  python scripts/find_hidden_gems.py --json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
from fundamentals_analyzer import score_ticker as score_fundamentals  # noqa: E402

# ── 경로 ─────────────────────────────────────────────────────────────────────
FUNDAMENTALS_DIR  = ROOT / "site" / "data" / "fundamentals"
SENTIMENT_PATH    = ROOT / "site" / "data" / "ticker_sentiment.json"
PREDICTIONS_PATH  = ROOT / "site" / "data" / "predictions.json"
OUT_PATH          = ROOT / "site" / "data" / "hidden_gems.json"

# ── 가중치 + 게이트 ─────────────────────────────────────────────────────────
W_P1, W_P2, W_P3, W_P4 = 0.25, 0.10, 0.25, 0.40
GATE_FUND_MIN          = 0.40
NEUTRAL                = 0.5
DEFAULT_TOP_N          = 30

# action → P3 base score (predict.py 결과를 0..1로 흡수)
P3_ACTION_BASE = {
    "BUY":    0.85,
    "WATCH":  0.65,
    "HOLD":   0.50,
    "REDUCE": 0.35,
    "SELL":   0.15,
}


# ══════════════════════════════════════════════════════════════════════════════
# Pillar별 점수 로더
# ══════════════════════════════════════════════════════════════════════════════

def load_sentiment_scores() -> dict[str, float]:
    """ticker_sentiment.json 최신 일자 → ticker별 0..1 점수.

    filtered_score (-1..+1) → (s+1)/2.
    """
    if not SENTIMENT_PATH.exists():
        return {}
    d = json.loads(SENTIMENT_PATH.read_text())
    if not d.get("dates"):
        return {}
    out: dict[str, float] = {}
    for tk, td in d.get("tickers", {}).items():
        fs_series = td.get("filtered_score") or []
        if not fs_series:
            continue
        fs = fs_series[-1]
        if fs is None:
            continue
        out[tk.upper()] = max(0.0, min(1.0, (fs + 1.0) / 2.0))
    return out


def load_ta_scores() -> dict[str, float]:
    """predict.py 의 최신 predictions.json → action/confidence 기반 0..1 점수.

    smooth: high confidence → action_base 강조, low confidence → 0.5 neutral 쪽.
    """
    if not PREDICTIONS_PATH.exists():
        return {}
    try:
        d = json.loads(PREDICTIONS_PATH.read_text())
    except Exception:
        return {}
    out: dict[str, float] = {}
    for p in d.get("predictions", []):
        tk = p.get("ticker")
        action = p.get("action", "HOLD")
        conf = p.get("confidence")
        base = P3_ACTION_BASE.get(action, NEUTRAL)
        if not isinstance(conf, (int, float)):
            conf = 0.5
        # confidence-blended: high conf pulls toward action base, low conf toward 0.5
        out[tk] = base * conf + NEUTRAL * (1 - conf)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 보조: percentile 계산
# ══════════════════════════════════════════════════════════════════════════════

def percentile_rank(items: list[tuple[str, float]]) -> dict[str, float]:
    """Sorted ascending. Returns {ticker: percentile in [0,1]}."""
    items_sorted = sorted(items, key=lambda x: x[1])
    n = len(items_sorted)
    if n <= 1:
        return {tk: NEUTRAL for tk, _ in items_sorted}
    return {tk: i / (n - 1) for i, (tk, _) in enumerate(items_sorted)}


# ══════════════════════════════════════════════════════════════════════════════
# 메인 빌더
# ══════════════════════════════════════════════════════════════════════════════

def _gate_pass(payload: dict, fund_score: dict) -> tuple[bool, str | None]:
    """Hard gate. Return (passes, reason_if_excluded)."""
    if payload.get("status") != "ok":
        return False, f"status={payload.get('status')}"
    metadata = payload.get("metadata") or {}
    if metadata.get("entity_type") == "investment":
        return False, "ETF"
    raw = (payload.get("summary") or {}).get("raw") or {}
    ni = raw.get("net_income")
    if ni is None or ni < 0:
        return False, "loss"
    fs = fund_score.get("fundamental_score")
    if fs is None or fs < GATE_FUND_MIN:
        return False, f"fund<{GATE_FUND_MIN}"
    return True, None


def _extract_growth_value(fund_score: dict, name: str) -> float | None:
    """fund_score['rationale']['growth'] list에서 특정 metric의 raw value."""
    for p in (fund_score.get("rationale", {}).get("growth") or []):
        if p.get("name") == name:
            v = p.get("value")
            return float(v) if isinstance(v, (int, float)) else None
    return None


def _build_reasons_risks(payload: dict, fund_score: dict, sub: dict) -> tuple[list[str], list[str]]:
    """투명한 자연어 사유/위험 — UI에 그대로 노출."""
    summary = payload.get("summary") or {}
    ratios  = summary.get("ratios") or {}
    raw     = summary.get("raw") or {}
    metadata = payload.get("metadata") or {}
    reasons: list[str] = []
    risks:   list[str] = []

    # 1) Fundamentals quality
    q = fund_score.get("quality_score")
    if q is not None:
        roe = ratios.get("roe")
        op_m = ratios.get("operating_margin")
        if q >= 0.70 and roe is not None and op_m is not None:
            reasons.append(f"Strong quality (ROE {roe*100:.0f}%, op margin {op_m*100:.0f}%)")
        elif q >= 0.50 and roe is not None:
            reasons.append(f"Solid quality (ROE {roe*100:.0f}%)")

    # 2) Growth
    g = fund_score.get("growth_score")
    rev_cagr = _extract_growth_value(fund_score, "rev_cagr_3y")
    if g is not None and g >= 0.50 and rev_cagr is not None:
        reasons.append(f"Growing revenue (CAGR {rev_cagr*100:+.0f}%/y, 3-yr)")

    # 3) Obscurity — phrasing: obs는 "more obscure than X% of universe"
    obs = sub["obscurity"]
    rev_b = (raw.get("revenue") or 0) / 1e9
    if obs >= 0.95:
        reasons.append(f"Among smallest in universe (revenue ${rev_b:.1f}B)")
    elif obs >= 0.70:
        reasons.append(f"Mid-cap discovery — revenue ${rev_b:.1f}B (smaller than {obs*100:.0f}% of universe)")
    elif obs >= 0.50:
        reasons.append(f"Below-mega-cap — revenue ${rev_b:.1f}B")

    # 4) Sector-relative
    if sub["P2"] >= 0.70:
        reasons.append(f"Top of sector ({metadata.get('owner_org','?')}) by fundamentals")

    # 5) Sentiment
    if sub["P1"] >= 0.65:
        reasons.append("Bullish FinBERT sentiment")
    elif sub["P1"] <= 0.35:
        risks.append("Bearish FinBERT sentiment")

    # 6) TA timing
    if sub["P3"] >= 0.65:
        reasons.append("TA setup positive (BUY/WATCH)")
    elif sub["P3"] <= 0.35:
        risks.append("TA setup negative (REDUCE/SELL)")

    # 7) Leverage risk (sector-aware: Finance는 다른 cap)
    de = ratios.get("debt_to_equity")
    if de is not None and metadata.get("owner_org") != "02 Finance" and de > 2.0:
        risks.append(f"High leverage (D/E {de:.2f})")

    # 8) Slow growth
    if rev_cagr is not None and rev_cagr < 0.05:
        risks.append(f"Slow growth (rev CAGR {rev_cagr*100:+.0f}%/y)")

    # 9) Coverage gap — TA 미적용 ticker
    if not sub.get("has_ta"):
        risks.append("No price data — research only (universe gap)")

    # 10) Negative-equity hidden trap (memory: SBUX/MAR pattern)
    if de is not None and de < 0:
        risks.append(f"Negative equity (D/E {de:.2f}) — buyback artifact, ROE unreliable")

    return reasons, risks


def find_gems(top_n: int = DEFAULT_TOP_N) -> dict:
    # ── 1) 모든 per-ticker payload 로드 + fundamentals 점수 ──
    payloads: dict[str, dict] = {}
    fund_scores: dict[str, dict] = {}
    for p in sorted(FUNDAMENTALS_DIR.glob("*.json")):
        try:
            payload = json.loads(p.read_text())
        except Exception as e:
            print(f"[warn] {p.name}: {e}", file=sys.stderr)
            continue
        tk = (payload.get("ticker") or p.stem).upper()
        payloads[tk] = payload
        fund_scores[tk] = score_fundamentals(payload)

    # ── 2) 게이트 ──
    gate_out: dict[str, str] = {}
    qualifying: list[str] = []
    for tk, payload in payloads.items():
        ok, reason = _gate_pass(payload, fund_scores[tk])
        if ok:
            qualifying.append(tk)
        else:
            gate_out[tk] = reason or ""

    # ── 3) 외부 신호 로드 ──
    p1_map = load_sentiment_scores()
    p3_map = load_ta_scores()

    # ── 4) Sector-relative percentile (Pillar 2) ──
    by_sector: dict[str, list[tuple[str, float]]] = {}
    for tk in qualifying:
        sec = (payloads[tk].get("metadata") or {}).get("owner_org") or "(none)"
        fs = fund_scores[tk]["fundamental_score"]
        by_sector.setdefault(sec, []).append((tk, fs))
    p2_map: dict[str, float] = {}
    for sec, items in by_sector.items():
        for tk, pct in percentile_rank(items).items():
            p2_map[tk] = pct

    # ── 5) Obscurity (1 - revenue percentile) ──
    rev_items: list[tuple[str, float]] = []
    for tk in qualifying:
        rev = ((payloads[tk].get("summary") or {}).get("raw") or {}).get("revenue")
        if rev and rev > 0:
            rev_items.append((tk, rev))
    rev_pct = percentile_rank(rev_items)
    obscurity_map: dict[str, float] = {tk: 1.0 - pct for tk, pct in rev_pct.items()}

    # ── 6) 합성 ──
    rows: list[dict] = []
    for tk in qualifying:
        p1 = p1_map.get(tk, NEUTRAL)
        p2 = p2_map.get(tk, NEUTRAL)
        p3 = p3_map.get(tk, NEUTRAL)
        p4 = fund_scores[tk]["fundamental_score"]
        obs = obscurity_map.get(tk, NEUTRAL)

        quality = W_P1 * p1 + W_P2 * p2 + W_P3 * p3 + W_P4 * p4
        hidden = quality * (0.5 + 0.5 * obs)

        sub = {
            "P1": round(p1, 3),
            "P2": round(p2, 3),
            "P3": round(p3, 3),
            "P4": round(p4, 3),
            "obscurity": round(obs, 3),
            "quality":   round(quality, 3),
            "has_ta":    tk in p3_map,
        }
        reasons, risks = _build_reasons_risks(payloads[tk], fund_scores[tk], sub)

        raw = (payloads[tk].get("summary") or {}).get("raw") or {}
        metadata = payloads[tk].get("metadata") or {}
        rows.append({
            "ticker":   tk,
            "entity":   payloads[tk].get("entity"),
            "hidden_gems_score": round(hidden, 3),
            "scores":   sub,
            "metadata": {
                "sector":       metadata.get("owner_org"),
                "exchanges":    metadata.get("exchanges") or [],
                "revenue_b":    round((raw.get("revenue") or 0) / 1e9, 2),
                "fund_summary": fund_scores[tk].get("summary"),
            },
            "reasons": reasons,
            "risks":   risks,
        })

    # ── 7) 정렬 (composite → fundamental → growth) ──
    rows.sort(key=lambda r: (
        -r["hidden_gems_score"],
        -r["scores"]["P4"],
        -fund_scores[r["ticker"]].get("growth_score") or 0,
    ))
    for i, r in enumerate(rows[:top_n], 1):
        r["rank"] = i

    return {
        "generated_at":  datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "universe_size": len(payloads),
        "passing_gate":  len(qualifying),
        "weights":       {"P1": W_P1, "P2": W_P2, "P3": W_P3, "P4": W_P4},
        "gates": {
            "fundamental_score_min": GATE_FUND_MIN,
            "no_loss":               True,
            "exclude_etf":           True,
            "exclude_metadata_only": True,
        },
        "obscurity_proxy": "revenue_percentile_inverted (price coverage 29/81 → revenue used as size)",
        "p3_source":       "predictions.json action+confidence (29 ticker), else 0.5 neutral",
        "p1_source":       "ticker_sentiment.json filtered_score (62 ticker)",
        "excluded":        gate_out,
        "top_picks":       rows[:top_n],
    }


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Hidden Gems — 4-Pillar 합성 + 무명도 ranking")
    ap.add_argument("--top", type=int, default=DEFAULT_TOP_N, help=f"Top N (default {DEFAULT_TOP_N})")
    ap.add_argument("--json", action="store_true", help="JSON 그대로 stdout")
    ap.add_argument("--no-write", action="store_true", help="파일 저장 생략")
    args = ap.parse_args(argv)

    result = find_gems(top_n=args.top)

    if args.json:
        json.dump(result, sys.stdout, indent=2, default=str)
        print()
    else:
        print(f"Universe={result['universe_size']}  passing gates={result['passing_gate']}  "
              f"excluded={len(result['excluded'])}")
        print()
        print(f"{'rank':>4s} {'ticker':<8s} {'score':>6s} {'qual':>6s} {'obs':>6s} "
              f"{'P1':>5s} {'P2':>5s} {'P3':>5s} {'P4':>5s}  {'sector':<22s}  rev")
        print("-" * 130)
        for r in result["top_picks"]:
            s = r["scores"]
            print(f"{r['rank']:>4d} {r['ticker']:<8s} "
                  f"{r['hidden_gems_score']:>6.3f} {s['quality']:>6.3f} {s['obscurity']:>6.3f} "
                  f"{s['P1']:>5.2f} {s['P2']:>5.2f} {s['P3']:>5.2f} {s['P4']:>5.2f}  "
                  f"{(r['metadata']['sector'] or '?')[:22]:<22s}  ${r['metadata']['revenue_b']:.1f}B")
        # Sample reasons for top 3
        print()
        for r in result["top_picks"][:3]:
            print(f"\n[{r['rank']}] {r['ticker']} — {r['entity']}")
            for x in r["reasons"]:    print(f"   + {x}")
            for x in r["risks"]:      print(f"   ! {x}")

    if not args.no_write:
        OUT_PATH.write_text(json.dumps(result, indent=2, default=str))
        print(f"\n[hidden_gems] wrote {OUT_PATH.relative_to(ROOT)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
