"""
weekly_report.py — 주간/월간 신호 성과 리포트
=============================================
매주 일요일 또는 수동 실행:
  1. 지난 N일간 predictions.json 히스토리를 순회
  2. 각 예측의 실제 N일 후 수익률 계산
  3. Action별 성공률 / 평균 수익률 집계
  4. 가장 좋았던 신호 / 가장 나빴던 신호 추출
  5. 단어별 신호 성과 (trump→AAPL 등)

출력: site/data/reports/weekly_report.json + history/<YYYY-MM-DD>.json

사용:
  python scripts/weekly_report.py \\
    --prices     site/data/prices.json \\
    --pred-dir   site/data/predictions_history \\
    --analysis-dir site/data/ticker_analysis \\
    --out-dir    site/data/reports \\
    --lookback-days 7
"""

from __future__ import annotations
import argparse
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path


def load_json(p: Path):
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def compute_return(prices: dict, ticker: str, from_date: str, days: int) -> float:
    """from_date 이후 days일의 수익률 (%)."""
    pdata = prices.get("tickers", {}).get(ticker)
    if not pdata:
        return None
    dates = pdata.get("dates", [])
    closes = pdata.get("closes", [])
    if from_date not in dates:
        return None
    i = dates.index(from_date)
    j = min(i + days, len(dates) - 1)
    if i == j:
        return None
    start = closes[i]
    end = closes[j]
    if start is None or end is None or start == 0:
        return None
    return round((end / start - 1) * 100, 3)


def evaluate_predictions(pred: dict, prices: dict, hold_days: int) -> dict:
    """한 날짜의 predictions.json을 실제 수익률로 평가."""
    updated = pred.get("updated", "")[:10]
    predictions = pred.get("predictions", [])

    results = []
    for p in predictions:
        ticker = p.get("ticker")
        action = p.get("action", "HOLD")
        conf   = p.get("confidence", 0)

        ret = compute_return(prices, ticker, updated, hold_days)
        if ret is None:
            continue

        # 방향 적중 여부
        if action in ("BUY", "WATCH"):
            hit = ret > 0
        elif action in ("SELL", "REDUCE"):
            hit = ret < 0
        else:
            hit = abs(ret) < 2  # HOLD: ±2% 이내

        results.append({
            "date":       updated,
            "ticker":     ticker,
            "action":     action,
            "confidence": conf,
            "actual_ret": ret,
            "hit":        hit,
        })

    return {"date": updated, "results": results}


def aggregate_stats(all_evaluations: list) -> dict:
    """기간 전체 통계 집계."""
    all_results = []
    for ev in all_evaluations:
        all_results.extend(ev.get("results", []))

    if not all_results:
        return {
            "total": 0,
            "overall_hit_rate": 0,
            "by_action": {},
            "best_signals": [],
            "worst_signals": [],
            "buy_watch_hit_rate": 0,
            "buy_watch_avg_ret": 0,
        }

    # Action별 집계
    by_action = {}
    for r in all_results:
        a = r["action"]
        if a not in by_action:
            by_action[a] = {"count": 0, "hits": 0, "returns": [], "confidences": []}
        by_action[a]["count"] += 1
        if r["hit"]:
            by_action[a]["hits"] += 1
        by_action[a]["returns"].append(r["actual_ret"])
        by_action[a]["confidences"].append(r["confidence"])

    # 비율/평균 계산
    for a, d in by_action.items():
        d["hit_rate"] = round(d["hits"] / d["count"], 3) if d["count"] else 0
        d["avg_ret"]  = round(sum(d["returns"]) / len(d["returns"]), 3)
        d["avg_conf"] = round(sum(d["confidences"]) / len(d["confidences"]), 3)
        # 분산
        mean = d["avg_ret"]
        var  = sum((r - mean) ** 2 for r in d["returns"]) / len(d["returns"])
        d["std_ret"] = round(math.sqrt(var), 3)
        # Sharpe (일일 수익률 / 표준편차)
        d["sharpe"]  = round(mean / d["std_ret"], 3) if d["std_ret"] > 0 else 0

    # 전체 hit rate
    total_hits = sum(1 for r in all_results if r["hit"])
    total = len(all_results)

    # Best / Worst 신호
    sorted_by_ret = sorted(all_results, key=lambda x: x["actual_ret"], reverse=True)
    best = sorted_by_ret[:5]
    worst = sorted_by_ret[-5:][::-1]

    # BUY/WATCH만 골라서 best 별도 — 실제 매수 시그널 성과
    buy_signals = [r for r in all_results if r["action"] in ("BUY", "WATCH")]
    buy_hit_rate = (sum(1 for r in buy_signals if r["hit"]) / len(buy_signals)
                    if buy_signals else 0)
    buy_avg_ret  = (sum(r["actual_ret"] for r in buy_signals) / len(buy_signals)
                    if buy_signals else 0)

    return {
        "total":            total,
        "overall_hit_rate": round(total_hits / total, 3) if total else 0,
        "by_action":        by_action,
        "best_signals":     best,
        "worst_signals":    worst,
        "buy_watch_hit_rate": round(buy_hit_rate, 3),
        "buy_watch_avg_ret":  round(buy_avg_ret, 3),
    }


def aggregate_word_signals(analysis_dir: Path) -> dict:
    """ticker_analysis에서 현재 활성화된 검증된 신호 단어들 집계."""
    verified_signals = []
    for json_file in sorted(analysis_dir.glob("*.json")):
        try:
            data = json.loads(json_file.read_text())
        except Exception:
            continue
        ticker = data.get("ticker")
        for word in (data.get("analysis", {}).get("bullish_words", [])[:3]):
            verified_signals.append({
                "ticker":    ticker,
                "word":      word["word"],
                "lag":       word["lead_days"],
                "hit_rate":  word["hit_rate"],
                "test_hit":  word.get("test_hit"),
                "avg_ret":   word["avg_ret_1d"],
                "n_events":  word["n_events"],
                "stability": word.get("stability", 1.0),
                "direction": "bullish",
            })
        for word in (data.get("analysis", {}).get("bearish_words", [])[:3]):
            verified_signals.append({
                "ticker":    ticker,
                "word":      word["word"],
                "lag":       word["lead_days"],
                "hit_rate":  word["hit_rate"],
                "test_hit":  word.get("test_hit"),
                "avg_ret":   word["avg_ret_1d"],
                "n_events":  word["n_events"],
                "stability": word.get("stability", 1.0),
                "direction": "bearish",
            })

    # stability * hit_rate 기준 정렬
    def score(s):
        hr = s["hit_rate"] if s["direction"] == "bullish" else (1 - s["hit_rate"])
        return hr * s["stability"] * math.sqrt(s["n_events"])

    verified_signals.sort(key=score, reverse=True)
    return {"total": len(verified_signals), "top_signals": verified_signals[:20]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prices",        default="site/data/prices.json")
    ap.add_argument("--pred-dir",      default="site/data/predictions_history",
                    help="과거 predictions.json들이 날짜별로 저장된 디렉토리")
    ap.add_argument("--current-pred",  default="site/data/predictions.json",
                    help="오늘의 predictions.json (아카이브할 대상)")
    ap.add_argument("--analysis-dir",  default="site/data/ticker_analysis")
    ap.add_argument("--out-dir",       default="site/data/reports")
    ap.add_argument("--lookback-days", type=int, default=7)
    ap.add_argument("--hold-days",     type=int, default=5,
                    help="각 예측의 N일 후 수익률을 평가")
    args = ap.parse_args()

    # 경로 준비
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    history_dir = out_dir / "history"
    history_dir.mkdir(exist_ok=True)
    pred_hist_dir = Path(args.pred_dir)
    pred_hist_dir.mkdir(parents=True, exist_ok=True)

    # 1) 오늘의 예측을 히스토리에 아카이브
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    current = load_json(Path(args.current_pred))
    if current:
        archive_path = pred_hist_dir / f"{today}.json"
        archive_path.write_text(json.dumps(current, ensure_ascii=False,
                                          separators=(",", ":")))
        print(f"Archived today's prediction → {archive_path}")

    # 2) 지난 N일간 예측 파일 로드
    prices = load_json(Path(args.prices)) or {}

    cutoff = datetime.now(timezone.utc) - timedelta(days=args.lookback_days)
    evaluations = []
    pred_files = sorted(pred_hist_dir.glob("*.json"))

    for f in pred_files:
        try:
            date_str = f.stem
            file_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if file_date < cutoff:
                continue
        except ValueError:
            continue

        pred = load_json(f)
        if pred:
            ev = evaluate_predictions(pred, prices, args.hold_days)
            evaluations.append(ev)

    print(f"Evaluated {len(evaluations)} days of predictions")

    # 3) 집계
    stats = aggregate_stats(evaluations)

    # 4) 현재 검증된 신호 단어
    word_signals = aggregate_word_signals(Path(args.analysis_dir))

    # 5) 리포트 작성
    report = {
        "generated":     datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "lookback_days": args.lookback_days,
        "hold_days":     args.hold_days,
        "evaluations":   evaluations,
        "stats":         stats,
        "word_signals":  word_signals,
    }

    # 주간 리포트 저장
    out_file = out_dir / "weekly_report.json"
    out_file.write_text(json.dumps(report, ensure_ascii=False, separators=(",", ":")))

    # 히스토리에도 타임스탬프 있는 버전 저장
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    hist_file = history_dir / f"{ts}.json"
    hist_file.write_text(json.dumps(report, ensure_ascii=False, separators=(",", ":")))

    # 콘솔 출력
    print(f"\n→ {out_file}")
    print(f"  Period: last {args.lookback_days} days, evaluated after {args.hold_days}d hold")
    print(f"  Total predictions: {stats['total']}")
    print(f"  Overall hit rate:  {stats['overall_hit_rate']:.1%}")
    print(f"  BUY/WATCH hit:     {stats['buy_watch_hit_rate']:.1%}  avg ret: {stats['buy_watch_avg_ret']:+.2f}%")
    print("\n  By action:")
    for a, d in sorted(stats["by_action"].items()):
        print(f"    {a:<8} n={d['count']:>3}  hit={d['hit_rate']:.0%}  "
              f"avg={d['avg_ret']:+.2f}%  sharpe={d['sharpe']:+.2f}")

    if stats["best_signals"]:
        print("\n  Top 3 winners:")
        for s in stats["best_signals"][:3]:
            print(f"    {s['date']} {s['ticker']:<6} {s['action']:<6} "
                  f"ret={s['actual_ret']:+.2f}%  conf={s['confidence']:.2f}")

    if stats["worst_signals"]:
        print("\n  Top 3 losers:")
        for s in stats["worst_signals"][:3]:
            print(f"    {s['date']} {s['ticker']:<6} {s['action']:<6} "
                  f"ret={s['actual_ret']:+.2f}%  conf={s['confidence']:.2f}")

    print(f"\n  Verified word signals: {word_signals['total']}")
    if word_signals["top_signals"]:
        print("  Top 5 most reliable:")
        for s in word_signals["top_signals"][:5]:
            arrow = "▲" if s["direction"] == "bullish" else "▼"
            print(f"    {arrow} {s['word']:<15} → {s['ticker']:<6} "
                  f"lag={s['lag']}d  hit={s['hit_rate']:.0%}  "
                  f"stab={s['stability']:.2f}  n={s['n_events']}")


if __name__ == "__main__":
    main()