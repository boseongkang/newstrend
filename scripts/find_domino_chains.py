"""
find_domino_chains.py — News-driven domino effect detection (Time-Aware v1)
============================================================================
"중국 부동산 → 한국 시멘트" — term burst가 ticker A를 움직이고, A와 강하게
상관된 ticker B가 따라 움직이는 다단 도미노를 surface.

3-hop 그래프 (v1는 Hop 1 + Hop 2):
  Hop 1  term  → ticker_A   (signal_corr.pairs, leading + strict)
  Hop 2  ticker_A → ticker_B (가격 returns Pearson, |corr| ≥ 0.5)
  Hop 3  macro  → sector → ticker  (Phase 5+, 미구현)

Quality 자동 측정 (시간 따른 시스템 성숙도 추적):
  - Grade A: ≥30 stock + ≥10 macro + ≥5 sector → 시스템 성숙
  - Grade B: ≥15 stock + ≥5 macro
  - Grade C: ≥5 stock + ≥3 macro
  - Grade D: 초기 — 데이터 더 필요
  매주 site/data/domino_history.json에 스냅샷 추가 → 진화 가시화.

CLI:
  python scripts/find_domino_chains.py
  python scripts/find_domino_chains.py --no-history
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# ── 입력 ─────────────────────────────────────────────────────────────────────
SIGNAL_CORR_PATH   = ROOT / "site" / "data" / "signal_corr.json"
PRICES_PATH        = ROOT / "site" / "data" / "prices.json"
TRENDS_PATH        = ROOT / "site" / "data" / "trends.json"
FUNDAMENTALS_DIR   = ROOT / "site" / "data" / "fundamentals"

# ── 출력 ─────────────────────────────────────────────────────────────────────
OUT_PATH           = ROOT / "site" / "data" / "domino.json"
HISTORY_PATH       = ROOT / "site" / "data" / "domino_history.json"

# ── 필터 (Hop 1) ─────────────────────────────────────────────────────────────
HOP1_MIN_ABS_CORR  = 0.30
HOP1_MAX_PVAL      = 0.05
HOP1_MIN_HIT_RATE  = 0.50
HOP1_MAX_LAG       = -1     # leading only

# ── 필터 (Hop 2 ticker-ticker price correlation) ────────────────────────────
HOP2_MIN_ABS_CORR  = 0.50   # 강한 가격 동조만
HOP2_MIN_OBS       = 60     # 최소 60일 공통 관측

# ── 활성 단어 임계 ───────────────────────────────────────────────────────────
ACTIVE_Z_THRESHOLD = 1.5

# ── ETF 식별 ─────────────────────────────────────────────────────────────────
ETF_TICKERS = {"SPY","QQQ","IWM","DIA","TLT","HYG","GLD","USO"}

# ── Maturation grading ──────────────────────────────────────────────────────
GRADE_THRESHOLDS = {
    "A": {"stock": 30, "macro": 10, "sector_diversity": 5},
    "B": {"stock": 15, "macro": 5,  "sector_diversity": 3},
    "C": {"stock": 5,  "macro": 3,  "sector_diversity": 1},
    # else: D (초기)
}


# ══════════════════════════════════════════════════════════════════════════════
# 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def _pearson(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 5:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((a - mx) * (b - my) for a, b in zip(xs, ys))
    dx = math.sqrt(sum((a - mx) ** 2 for a in xs))
    dy = math.sqrt(sum((b - my) ** 2 for b in ys))
    if dx < 1e-9 or dy < 1e-9:
        return None
    return round(num / (dx * dy), 4)


def _load_metadata_index() -> dict[str, dict]:
    """ticker → {sector, entity, fund_score} 가능한 만큼 매핑."""
    out: dict[str, dict] = {}
    sys.path.insert(0, str(ROOT / "scripts"))
    try:
        from fundamentals_analyzer import score_ticker
    except ImportError:
        score_ticker = None
    for p in FUNDAMENTALS_DIR.glob("*.json"):
        try:
            payload = json.loads(p.read_text())
        except Exception:
            continue
        tk = (payload.get("ticker") or p.stem).upper()
        meta = payload.get("metadata") or {}
        sector = meta.get("owner_org")
        fund = None
        if score_ticker:
            try:
                s = score_ticker(payload)
                fund = s.get("fundamental_score")
            except Exception:
                pass
        out[tk] = {
            "entity": payload.get("entity"),
            "sector": sector,
            "fund_score": fund,
        }
    return out


# ══════════════════════════════════════════════════════════════════════════════
# Hop 1 — term → ticker (signal_corr 기반)
# ══════════════════════════════════════════════════════════════════════════════

def filter_hop1_pairs(pairs: list[dict]) -> list[dict]:
    out = []
    for p in pairs:
        if p.get("best_lag", 0) > HOP1_MAX_LAG:
            continue
        if abs(p.get("corr") or 0) < HOP1_MIN_ABS_CORR:
            continue
        if (p.get("pval") or 1.0) > HOP1_MAX_PVAL:
            continue
        if (p.get("hit_rate") or 0) < HOP1_MIN_HIT_RATE:
            continue
        out.append({
            "term":       p["term"],
            "ticker":     p["ticker"],
            "corr":       p["corr"],
            "lag":        p["best_lag"],
            "hit_rate":   p["hit_rate"],
            "pval":       p["pval"],
            "confidence": p.get("confidence", 0.0),
            "avg_ret_1d": p.get("avg_ret_1d"),
            "direction":  "bull" if p["corr"] >= 0 else "bear",
            "is_etf":     p["ticker"] in ETF_TICKERS,
        })
    return out


# ══════════════════════════════════════════════════════════════════════════════
# Hop 2 — ticker A → ticker B (price returns 상관)
# ══════════════════════════════════════════════════════════════════════════════

def compute_ticker_correlations(prices_data: dict) -> dict[str, dict[str, float]]:
    """모든 ticker 쌍의 returns Pearson을 강한 것만 dict[A][B] 로 반환."""
    tickers = prices_data.get("tickers", {})
    # 날짜→returns 매핑 — 공통 날짜 빠르게 비교
    series: dict[str, dict[str, float]] = {}
    for tk, data in tickers.items():
        dates = data.get("dates") or []
        rets  = data.get("returns") or []
        if not dates or not rets or len(dates) != len(rets):
            continue
        series[tk] = {d: r for d, r in zip(dates, rets) if r is not None}

    out: dict[str, dict[str, float]] = defaultdict(dict)
    keys = sorted(series.keys())
    for i, a in enumerate(keys):
        for b in keys[i+1:]:
            common = sorted(set(series[a].keys()) & set(series[b].keys()))
            if len(common) < HOP2_MIN_OBS:
                continue
            xs = [series[a][d] for d in common]
            ys = [series[b][d] for d in common]
            r = _pearson(xs, ys)
            if r is None or abs(r) < HOP2_MIN_ABS_CORR:
                continue
            out[a][b] = r
            out[b][a] = r
    return dict(out)


# ══════════════════════════════════════════════════════════════════════════════
# 활성 단어 (오늘 burst)
# ══════════════════════════════════════════════════════════════════════════════

def active_terms_today(trends: dict) -> dict[str, float]:
    """trends.zscores[term] (latest day z-score per term) ≥ threshold 인 단어들.

    Note: trends.today[term] 은 raw count (mention count today), z-score 아님 — 사용 금지.
    """
    zs = trends.get("zscores") or {}
    out: dict[str, float] = {}
    for term, val in zs.items():
        try:
            z = float(val) if val is not None else 0.0
        except (TypeError, ValueError):
            continue
        if z >= ACTIVE_Z_THRESHOLD:
            out[term] = round(z, 3)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# Quality grading
# ══════════════════════════════════════════════════════════════════════════════

def measure_quality(
    stock_count: int,
    macro_count: int,
    sector_diversity: int,
    data_window_days: int,
) -> dict:
    grade = "D"
    for g in ("A", "B", "C"):
        t = GRADE_THRESHOLDS[g]
        if (stock_count >= t["stock"]
                and macro_count >= t["macro"]
                and sector_diversity >= t["sector_diversity"]):
            grade = g
            break

    next_grade = {"D":"C", "C":"B", "B":"A", "A":None}[grade]
    needs = None
    if next_grade:
        t = GRADE_THRESHOLDS[next_grade]
        needs = {
            "stock":            max(0, t["stock"] - stock_count),
            "macro":            max(0, t["macro"] - macro_count),
            "sector_diversity": max(0, t["sector_diversity"] - sector_diversity),
        }

    return {
        "data_window_days": data_window_days,
        "stock_signals":    stock_count,
        "macro_signals":    macro_count,
        "sector_diversity": sector_diversity,
        "grade":            grade,
        "next_grade":       next_grade,
        "needs_for_next":   needs,
    }


# ══════════════════════════════════════════════════════════════════════════════
# History snapshot (주 1회)
# ══════════════════════════════════════════════════════════════════════════════

def append_history(quality: dict, force: bool = False) -> dict:
    """domino_history.json에 주간 스냅샷 추가 (idempotent: 같은 ISO-week 중복 방지)."""
    history = []
    if HISTORY_PATH.exists():
        try:
            history = json.loads(HISTORY_PATH.read_text()).get("snapshots", [])
        except Exception:
            history = []

    now = datetime.now(timezone.utc)
    iso_year, iso_week, _ = now.isocalendar()
    week_key = f"{iso_year}-W{iso_week:02d}"

    if not force and history and history[-1].get("week") == week_key:
        return {"appended": False, "reason": "same_week", "week": week_key}

    snapshot = {
        "week":            week_key,
        "timestamp":       now.isoformat(timespec="seconds"),
        "stock_signals":   quality["stock_signals"],
        "macro_signals":   quality["macro_signals"],
        "sector_diversity": quality["sector_diversity"],
        "grade":           quality["grade"],
    }
    history.append(snapshot)
    HISTORY_PATH.write_text(json.dumps({
        "schema_version": 1,
        "snapshots":      history,
    }, indent=2))
    return {"appended": True, "week": week_key, "total": len(history)}


def project_grade_a_date(history: list[dict]) -> str | None:
    """히스토리에 ≥4 스냅샷이면 stock_signals 선형 추세로 Grade-A 도달 주를 추정."""
    if len(history) < 4:
        return None
    # x = 주 인덱스, y = stock_signals
    ys = [s.get("stock_signals", 0) for s in history[-8:]]
    n = len(ys)
    xs = list(range(n))
    mx = sum(xs) / n; my = sum(ys) / n
    num = sum((x-mx)*(y-my) for x,y in zip(xs,ys))
    den = sum((x-mx)**2 for x in xs)
    if den < 1e-9:
        return None
    slope = num / den
    if slope <= 0:
        return None
    target = GRADE_THRESHOLDS["A"]["stock"]
    weeks_left = max(0, math.ceil((target - ys[-1]) / slope))
    if weeks_left == 0:
        return "achieved"
    from datetime import timedelta
    eta = datetime.now(timezone.utc) + timedelta(weeks=weeks_left)
    return eta.strftime("%Y-%m-%d")


# ══════════════════════════════════════════════════════════════════════════════
# 메인 빌더
# ══════════════════════════════════════════════════════════════════════════════

def build_domino(write_history: bool = True) -> dict:
    if not SIGNAL_CORR_PATH.exists():
        raise FileNotFoundError(f"missing {SIGNAL_CORR_PATH}; run build_signal_corr.py first")
    if not PRICES_PATH.exists():
        raise FileNotFoundError(f"missing {PRICES_PATH}; run fetch_prices_v2.py first")

    sc     = json.loads(SIGNAL_CORR_PATH.read_text())
    prices = json.loads(PRICES_PATH.read_text())
    trends = json.loads(TRENDS_PATH.read_text()) if TRENDS_PATH.exists() else {}

    # Hop 1
    all_pairs = sc.get("pairs", [])
    hop1 = filter_hop1_pairs(all_pairs)
    stock_edges = [e for e in hop1 if not e["is_etf"]]
    macro_edges = [e for e in hop1 if e["is_etf"]]

    # Hop 2 (ticker→ticker)
    tk_corr = compute_ticker_correlations(prices)

    # Metadata (sector / entity / fund)
    meta_idx = _load_metadata_index()

    # 활성 단어
    active = active_terms_today(trends)

    # Hop1 + Hop2 chain 구성
    chains: list[dict] = []
    for e in stock_edges:
        anchor = e["ticker"]
        nbrs = sorted(tk_corr.get(anchor, {}).items(), key=lambda kv: -abs(kv[1]))[:5]
        hop2 = [
            {
                "ticker":          b,
                "ticker_corr":     round(r, 3),
                "implied_dir":     ("bull" if (r > 0) == (e["direction"] == "bull") else "bear"),
                "sector":          (meta_idx.get(b) or {}).get("sector"),
                "entity":          (meta_idx.get(b) or {}).get("entity"),
            } for b, r in nbrs
        ]
        chains.append({
            "term":             e["term"],
            "is_active_today":  e["term"] in active,
            "today_z":          active.get(e["term"]),
            "hop1": {
                "ticker":     anchor,
                "sector":     (meta_idx.get(anchor) or {}).get("sector"),
                "entity":     (meta_idx.get(anchor) or {}).get("entity"),
                "corr":       e["corr"],
                "lag":        e["lag"],
                "hit_rate":   e["hit_rate"],
                "direction":  e["direction"],
                "avg_ret_1d": e["avg_ret_1d"],
                "confidence": e["confidence"],
            },
            "hop2": hop2,
        })

    # Sort: active terms first, then by Hop 1 confidence
    chains.sort(key=lambda c: (-int(c["is_active_today"]), -c["hop1"]["confidence"]))

    # Active inbound (어떤 ticker가 지금 압력 받는지)
    inbound: dict[str, list[dict]] = defaultdict(list)
    for c in chains:
        if c["is_active_today"]:
            inbound[c["hop1"]["ticker"]].append({
                "term":      c["term"],
                "direction": c["hop1"]["direction"],
                "today_z":   c["today_z"],
                "hit_rate":  c["hop1"]["hit_rate"],
            })
    # Hop2 indirect pressure
    for c in chains:
        if c["is_active_today"]:
            for h2 in c["hop2"]:
                inbound[h2["ticker"]].append({
                    "term":      c["term"],
                    "via":       c["hop1"]["ticker"],
                    "direction": h2["implied_dir"],
                    "today_z":   c["today_z"],
                    "indirect":  True,
                })

    pressure_list = sorted(
        ({"ticker": tk,
          "sector": (meta_idx.get(tk) or {}).get("sector"),
          "entity": (meta_idx.get(tk) or {}).get("entity"),
          "fund_score": (meta_idx.get(tk) or {}).get("fund_score"),
          "inbound": evs}
         for tk, evs in inbound.items()),
        key=lambda r: -len(r["inbound"]),
    )

    # Sector diversity (stock_edges affected sectors)
    sectors_hit = {(meta_idx.get(e["ticker"]) or {}).get("sector") for e in stock_edges}
    sectors_hit.discard(None)

    quality = measure_quality(
        stock_count=len(stock_edges),
        macro_count=len(macro_edges),
        sector_diversity=len(sectors_hit),
        data_window_days=sc.get("n_dates", 0),
    )

    # History (week-keyed; idempotent)
    hist_result = None
    if write_history:
        hist_result = append_history(quality)
    history_now = []
    if HISTORY_PATH.exists():
        try:
            history_now = json.loads(HISTORY_PATH.read_text()).get("snapshots", [])
        except Exception:
            history_now = []
    eta_grade_a = project_grade_a_date(history_now)

    return {
        "generated_at":   datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": {
            "signal_corr_updated": sc.get("updated"),
            "n_pairs_total":       len(all_pairs),
            "n_dates":             sc.get("n_dates"),
            "prices_tickers":      len(prices.get("tickers", {})),
            "trends_terms":        len((trends.get("series") or {})),
        },
        "filters": {
            "hop1": {"min_abs_corr": HOP1_MIN_ABS_CORR, "max_pval": HOP1_MAX_PVAL,
                     "min_hit_rate": HOP1_MIN_HIT_RATE, "max_lag": HOP1_MAX_LAG},
            "hop2": {"min_abs_corr": HOP2_MIN_ABS_CORR, "min_obs": HOP2_MIN_OBS},
            "active_z_threshold": ACTIVE_Z_THRESHOLD,
        },
        "active_terms":   active,
        "stock_chains":   [c for c in chains if not c["hop1"]["sector"] is None or True],
        "macro_chains":   [
            {"term": e["term"], "ticker": e["ticker"], "corr": e["corr"], "lag": e["lag"],
             "hit_rate": e["hit_rate"], "direction": e["direction"],
             "is_active_today": e["term"] in active}
            for e in macro_edges
        ],
        "ticker_pressure": pressure_list,
        "quality": {
            **quality,
            "expected_grade_a_date": eta_grade_a,
            "history_weeks":         len(history_now),
        },
        "history_update":  hist_result,
    }


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Time-aware domino chain detection")
    ap.add_argument("--no-history", action="store_true",
                    help="domino_history.json 갱신 생략")
    ap.add_argument("--no-write",   action="store_true",
                    help="domino.json 저장 생략 (stdout만)")
    ap.add_argument("--json",       action="store_true",
                    help="JSON stdout 출력")
    args = ap.parse_args(argv)

    result = build_domino(write_history=not args.no_history)

    q = result["quality"]
    print(f"[domino] grade={q['grade']}  stock={q['stock_signals']}  "
          f"macro={q['macro_signals']}  sector_diversity={q['sector_diversity']}  "
          f"window={q['data_window_days']}d", file=sys.stderr)
    if q.get("expected_grade_a_date"):
        print(f"[domino] projected Grade-A date: {q['expected_grade_a_date']} "
              f"(based on {q.get('history_weeks',0)} weekly snapshots)", file=sys.stderr)
    print(f"[domino] active terms today: {len(result['active_terms'])}", file=sys.stderr)
    print(f"[domino] stock chains: {len(result['stock_chains'])}  "
          f"macro chains: {len(result['macro_chains'])}", file=sys.stderr)
    hu = result.get("history_update") or {}
    if hu.get("appended"):
        print(f"[domino] new weekly snapshot appended ({hu['week']})", file=sys.stderr)

    if args.json:
        json.dump(result, sys.stdout, indent=2, default=str); print()

    if not args.no_write:
        OUT_PATH.write_text(json.dumps(result, indent=2, default=str))
        print(f"[domino] wrote {OUT_PATH.relative_to(ROOT)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
