"""
build_daily_report.py — 일간 섹터 관찰 리포트 생성기.

성격 (주간 검증 리포트와 분리):
- 주간(weekly_narrative) = 측정/검증 — 알파, p값, 검정력. 하루로는 안 움직임.
- 일간(이 파일)        = 관찰/서술 — 섹터 흐름, 뉴스. 예측 주장 없음.
  "오늘 반도체 -2.1%"(사실) O / "XX 매수"(주장) X.

구조 (주간과 동일 원칙):
- 모든 수치는 Python이 결정론적으로 계산해 JSON에 직접 실린다.
- Claude(있다면)는 한 줄 요약 서술만 생성 (200~300자, 관찰만).
- ANTHROPIC_API_KEY 부재/실패 시 데이터-only 강등.
- 서술은 리포트 날짜당 1회만 호출 (이후 회전은 숫자만 갱신, --force로 재생성).

출력:
- site/data/daily_report.json         (최신, 페이지가 읽음)
- site/data/daily_reports/{날짜}.json  (아카이브, 30일 롤링)
- site/data/daily_reports/index.json
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "site" / "data"
OUT_LATEST = DATA / "daily_report.json"
OUT_DIR = DATA / "daily_reports"

ARCHIVE_DAYS = 30
LOW_CONFIDENCE_N = 5   # 섹터 종목 수가 이보다 적으면 평균은 노이즈 — 저신뢰 표시
SURGE_MIN_ARTICLES = 5    # 오늘(D-2) 기사 수 하한 — 적은 n은 배율이 폭발
SURGE_BASELINE_DAYS = 14  # 자기 자신 대비 배율의 기준 창
SURGE_MIN_RATIO = 2.0

ETF_LABELS = {
    "SPY": "S&P 500", "QQQ": "Nasdaq 100", "DIA": "Dow", "IWM": "Russell 2000",
    "GLD": "금", "TLT": "미 장기국채", "HYG": "하이일드 크레딧", "USO": "유가",
}


def _load(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def _sector_map() -> dict[str, str]:
    d = _load(DATA / "tickers.json")
    out = {}
    for sector, tickers in d.items():
        if isinstance(tickers, list):
            for t in tickers:
                out[t] = sector
    return out


def _daily_returns(prices: dict) -> tuple[str | None, dict[str, float]]:
    """마지막 거래일과 그 날의 per-ticker 수익률(%). returns는 소수 비율."""
    tickers = prices.get("tickers") or {}
    last_date = None
    for v in tickers.values():
        ds = v.get("dates") or []
        if ds and (last_date is None or ds[-1] > last_date):
            last_date = ds[-1]
    if not last_date:
        return None, {}
    rets = {}
    for tk, v in tickers.items():
        ds, rs = v.get("dates") or [], v.get("returns") or []
        if ds and rs and ds[-1] == last_date and rs[-1] is not None:
            rets[tk] = round(rs[-1] * 100, 2)
    return last_date, rets


def build_sectors(rets: dict[str, float], smap: dict[str, str]) -> list[dict]:
    by_sector: dict[str, list[tuple[str, float]]] = {}
    for tk, r in rets.items():
        sec = smap.get(tk)
        if not sec or sec == "ETF":
            continue
        by_sector.setdefault(sec, []).append((tk, r))
    out = []
    for sec, items in by_sector.items():
        vals = [r for _, r in items]
        top = max(items, key=lambda x: x[1])
        bot = min(items, key=lambda x: x[1])
        out.append({
            "sector": sec,
            "n": len(items),
            "avg_ret_pct": round(statistics.mean(vals), 2),
            "low_confidence": len(items) < LOW_CONFIDENCE_N,
            "top": {"ticker": top[0], "ret_pct": top[1]},
            "bottom": {"ticker": bot[0], "ret_pct": bot[1]},
        })
    out.sort(key=lambda x: -x["avg_ret_pct"])
    return out


def build_macro(rets: dict[str, float]) -> list[dict]:
    return [
        {"ticker": tk, "label": label, "ret_pct": rets[tk]}
        for tk, label in ETF_LABELS.items() if tk in rets
    ]


def build_movers(rets: dict[str, float], smap: dict[str, str], k: int = 5) -> dict:
    stocks = [(tk, r) for tk, r in rets.items() if smap.get(tk) != "ETF"]
    stocks.sort(key=lambda x: -x[1])
    fmt = lambda pairs: [
        {"ticker": tk, "sector": smap.get(tk, "?"), "ret_pct": r}
        for tk, r in pairs
    ]
    return {"up": fmt(stocks[:k]), "down": fmt(stocks[-k:][::-1])}


def _cum_return_after(prices: dict, ticker: str, after_date: str) -> tuple[float, int] | None:
    """after_date '이후' 거래일들의 누적 수익률(%)과 일수. 이미 확정된 사실."""
    v = (prices.get("tickers") or {}).get(ticker)
    if not v:
        return None
    dates, rets = v.get("dates") or [], v.get("returns") or []
    cum, n = 1.0, 0
    for d, r in zip(dates, rets):
        if d > after_date and r is not None:
            cum *= 1 + r
            n += 1
    return (round((cum - 1) * 100, 2), n) if n else None


def _load_headlines(sent_date: str) -> dict[str, list[dict]]:
    """sentiment_per_day에서 종목별 기사 목록 (text/label/confidence/url)."""
    f = ROOT / "data" / "sentiment_per_day" / f"sentiment_{sent_date}.json"
    d = _load(f)
    out: dict[str, list[dict]] = {}
    for r in d.get("results") or []:
        for tk in r.get("tickers") or []:
            label = r.get("label")
            conf = (r.get("scores") or {}).get(label, 0)
            out.setdefault(tk, []).append({
                "text": (r.get("text") or "")[:120],
                "label": label, "conf": conf, "url": r.get("url"),
            })
    return out


def build_news(smap: dict[str, str], prices: dict) -> dict:
    # ⚠ predictions.json의 news_z_today는 쓰지 않는다: trends.json hot 20
    # 단어의 |z| 합계 = 날짜당 단일 전역값이 전 종목에 복사된 것이라
    # 종목별 급증 지표가 아니다 (docs/ISSUES.md F-new-1).
    ts = _load(DATA / "ticker_sentiment.json")
    dates = ts.get("dates") or []
    sent_date = dates[-1] if dates else None

    # (a) 종목별 기사량 급증 — D-2 완성일의 기사 수를 자기 자신의 직전
    #     14일 평균과 비교. 오늘 5건 미만은 배율이 무의미하므로 제외.
    headlines = _load_headlines(sent_date) if sent_date else {}
    surge = []
    for tk, v in (ts.get("tickers") or {}).items():
        totals = v.get("total") or []
        if len(totals) < SURGE_BASELINE_DAYS + 1:
            continue
        today = totals[-1]
        if today < SURGE_MIN_ARTICLES:
            continue
        baseline = statistics.mean(totals[-(SURGE_BASELINE_DAYS + 1):-1])
        ratio = today / baseline if baseline > 0 else float(today)
        if ratio < SURGE_MIN_RATIO:
            continue
        item = {"ticker": tk, "articles": today,
                "avg_14d": round(baseline, 1),
                "ratio": round(ratio, 1),
                "sector": smap.get(tk, "?"),
                "bullish": (v.get("bullish") or [0])[-1],
                "bearish": (v.get("bearish") or [0])[-1],
                "neutral": (v.get("neutral") or [0])[-1]}

        # ① 대표 헤드라인: 다수 감성 방향에서 confidence 최대 (없으면 전체 최대)
        arts = headlines.get(tk) or []
        if arts:
            major = ("positive" if item["bullish"] > item["bearish"]
                     else "negative" if item["bearish"] > item["bullish"] else None)
            pool = [a for a in arts if a["label"] == major] if major else []
            best = max(pool or arts, key=lambda a: a["conf"])
            item["headline"] = best["text"]
            item["headline_url"] = best["url"]

        # ② 뉴스 이후 가격 반응 — 이미 확정된 사실 (예측 아님).
        #    섹터 대비 초과 병기: 섹터 ex-self 평균(표본<5면 유니버스 ex-self).
        r = _cum_return_after(prices, tk, sent_date)
        if r:
            item["react_pct"], item["react_days"] = r
            sec = smap.get(tk)
            peers = [t for t, s in smap.items()
                     if s == sec and t != tk and s != "ETF"] if sec else []
            if len(peers) < 5:
                peers = [t for t, s in smap.items() if t != tk and s != "ETF"]
            peer_rets = [pr[0] for t in peers
                         if (pr := _cum_return_after(prices, t, sent_date))]
            if peer_rets:
                item["react_excess_pp"] = round(
                    item["react_pct"] - statistics.mean(peer_rets), 2)
        surge.append(item)
    surge.sort(key=lambda x: -x["ratio"])
    # (b) 섹터별 sentiment — D-2 완성일 기준 (클램프된 ticker_sentiment).
    # ③ Δ7d: 자기 자신의 직전 7일 평균 대비 변화 (FinBERT의 금융뉴스 부정
    #   편향 때문에 레벨보다 변화가 해석 가능). baseline 검증(2026-08-06)
    #   결과에 따른 표시 규칙: 직전 7일 중 데이터 ≥5일 AND 당일 기사 ≥10건
    #   일 때만 Δ 표시 — 미달 섹터(Energy/Cons.Def/Health급)는 일간 sd가
    #   0.5~0.65로 신호보다 커서 Δ가 노이즈.
    # ④ 당일 기사 <10건 섹터는 low_confidence로 흐림.
    n_days = len(dates)
    day_cells: dict[str, dict[int, dict]] = {}
    for tk, v in (ts.get("tickers") or {}).items():
        sec = smap.get(tk)
        if not sec or sec == "ETF" or not dates:
            continue
        totals, scores = v.get("total") or [], v.get("score") or []
        for i in range(max(0, n_days - 8), n_days):
            if i < len(totals) and totals[i] > 0:
                cell = day_cells.setdefault(sec, {}).setdefault(
                    i, {"scores": [], "arts": 0, "tickers": 0})
                cell["scores"].append(scores[i])
                cell["arts"] += totals[i]
                cell["tickers"] += 1
    sentiment = []
    for sec, days in day_cells.items():
        today_i = n_days - 1
        if today_i not in days:
            continue
        today = days[today_i]
        today_score = statistics.mean(today["scores"])
        prior = [statistics.mean(days[i]["scores"])
                 for i in range(max(0, n_days - 8), today_i) if i in days]
        low_conf = today["arts"] < 10
        delta = (round(today_score - statistics.mean(prior), 3)
                 if len(prior) >= 5 and not low_conf else None)
        sentiment.append({
            "sector": sec,
            "n_tickers": today["tickers"],
            "total_articles": today["arts"],
            "avg_score": round(today_score, 3),
            "delta_7d": delta,
            "baseline_days": len(prior),
            "low_confidence": low_conf,
        })
    sentiment.sort(key=lambda x: -x["total_articles"])

    return {
        "surge": surge[:8],
        "sentiment_date": sent_date,
        "lag_note": (f"이 섹션 전체(기사량 급증·섹터 sentiment)는 {sent_date} "
                     "기준 (D-2) — 뉴스 아카이브가 완성된 날만 보관하는 설계라 "
                     "이틀 늦다. 오늘 뉴스가 아니다."
                     if sent_date else "sentiment 데이터 없음"),
        "sentiment_by_sector": sentiment,
    }


def build_signals(predictions: dict) -> dict:
    items = [
        {"ticker": p["ticker"], "action": p["action"],
         "confidence": p.get("confidence")}
        for p in predictions.get("predictions") or []
        if p.get("action") in ("BUY", "WATCH")
    ]
    items.sort(key=lambda x: (x["action"] != "BUY", -(x["confidence"] or 0)))
    return {
        "warning": ("이 시스템은 벤치마크를 이기지 못한다는 것이 세 번의 독립 "
                    "검증에서 확인됨. 아래 신호는 검증 대상이지 추천이 아님."),
        "items": items,
    }


# ── 한 줄 요약 (Claude — 관찰만, 숫자는 계산된 값만) ────────────────────
SUMMARY_SCHEMA = {
    "type": "object",
    "properties": {"summary": {
        "type": "string",
        "description": "오늘 시장 관찰 요약, 한국어 200~300자, 2~3문장",
    }},
    "required": ["summary"],
    "additionalProperties": False,
}

SYSTEM_PROMPT = """\
너는 일간 시장 '관찰' 서술자다. 이 리포트는 예측이나 추천이 아니라
오늘 무슨 일이 있었는지의 기록이다.

절대 규칙:
1. 예측/추천/전망 금지. "오를 것", "주목할 만", "매수 기회", "유망" 금지.
   미래 시제 문장 자체를 쓰지 말 것. 오늘 관찰된 사실만.
2. 숫자를 재계산하거나 입력에 없는 수치를 만들지 말 것. 입력 JSON의 값만.
3. sentiment를 언급하면 반드시 이틀 지연(D-2)임을 함께 말할 것.
4. 종목 수가 적은 섹터(low_confidence=true)의 평균을 흐름으로 단정하지 말 것.
5. 숫자 나열이 아니라 "오늘 무슨 일이 있었나"를 읽기 쉽게. 2~3문장, 200~300자."""


def generate_summary(payload: dict) -> dict:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return {"generated": False, "reason": "ANTHROPIC_API_KEY 없음"}
    try:
        import anthropic
    except ImportError:
        return {"generated": False, "reason": "anthropic SDK 미설치"}
    user_content = json.dumps(
        {k: payload[k] for k in ("date", "sectors", "macro_etf", "movers",
                                 "news", "regime")},
        ensure_ascii=False, indent=1)
    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model="claude-opus-5",
            max_tokens=1500,
            system=SYSTEM_PROMPT,
            output_config={"format": {"type": "json_schema",
                                      "schema": SUMMARY_SCHEMA}},
            messages=[{"role": "user", "content": user_content}],
        )
        if resp.stop_reason == "refusal":
            return {"generated": False, "reason": "모델이 요청을 거절함"}
        text = next(b.text for b in resp.content if b.type == "text")
        return {"generated": True, "model": resp.model,
                "text": json.loads(text).get("summary", "")}
    except Exception as e:
        return {"generated": False, "reason": f"API 오류: {e}"}


def rotate_archive() -> list[str]:
    """30일 롤링: 오래된 아카이브 파일 삭제, index 갱신용 목록 반환."""
    files = sorted(OUT_DIR.glob("????-??-??.json"))
    keep = files[-ARCHIVE_DAYS:]
    for f in files[:-ARCHIVE_DAYS]:
        f.unlink()
    return [f.stem for f in keep]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true",
                    help="같은 날짜의 기존 서술이 있어도 Claude 재호출")
    args = ap.parse_args()

    prices = _load(DATA / "prices.json")
    predictions = _load(DATA / "predictions.json")
    if not prices or not predictions:
        print("ERROR: prices.json/predictions.json 없음", file=sys.stderr)
        return 1

    smap = _sector_map()
    date, rets = _daily_returns(prices)
    if not date or not rets:
        print("ERROR: prices.json에서 일간 수익률을 얻지 못함", file=sys.stderr)
        return 1

    mr = predictions.get("market_regime") or {}
    payload = {
        "date": date,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sectors": build_sectors(rets, smap),
        "macro_etf": build_macro(rets),
        "movers": build_movers(rets, smap),
        "news": build_news(smap, prices),
        "regime": {
            "regime": mr.get("regime"),
            "regime_note": mr.get("regime_note"),
            "bull_pct": mr.get("bull_pct"),
            "avg_rsi": mr.get("avg_rsi"),
            "fear_greed": mr.get("fear_greed"),
            "fear_greed_label": mr.get("fear_greed_label"),
        },
        "signals": build_signals(predictions),
    }

    # 서술은 날짜당 1회 (CI가 하루 ~10회 돌아도 API는 한 번만)
    prev = _load(OUT_LATEST)
    prev_sum = prev.get("summary") or {}
    if (not args.force and prev.get("date") == date and prev_sum.get("generated")):
        payload["summary"] = prev_sum
        payload["summary"]["reused"] = True
    else:
        payload["summary"] = generate_summary(payload)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_LATEST.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
    (OUT_DIR / f"{date}.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=1))

    dates = rotate_archive()
    (OUT_DIR / "index.json").write_text(json.dumps(
        {"dates": dates, "updated": payload["generated_at"]},
        ensure_ascii=False, indent=1))

    s = payload["summary"]
    print(f"daily_report: {date} 생성 (sectors={len(payload['sectors'])}, "
          f"summary={'생성됨' if s.get('generated') else '데이터-only: ' + str(s.get('reason'))}"
          f"{' [reused]' if s.get('reused') else ''}, archive={len(dates)}일)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
