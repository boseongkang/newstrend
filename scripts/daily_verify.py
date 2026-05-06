"""
daily_verify.py — 어제 예측 vs 오늘 실제 비교
=============================================
매일 자동 실행:
  1. predictions_history에서 어제 예측 로드
  2. prices.json에서 실제 가격 변화 확인
  3. 신호별 적중 여부 평가
  4. 누적 기록 site/data/verification/daily.json에 추가

평가 기준:
  - BUY/WATCH: 다음날 종가 > 신호일 종가 → HIT
  - SELL/REDUCE: 다음날 종가 < 신호일 종가 → HIT  
  - HOLD: |변화율| < 1.5% → HIT
"""
import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path


def load_predictions(pred_dir: Path) -> dict:
    """모든 predictions_history 로드 → {date: {ticker: prediction}}"""
    result = {}
    if not pred_dir.exists():
        return result
    for f in sorted(pred_dir.glob("*.json")):
        try:
            d = json.loads(f.read_text())
            date = f.stem  # "2026-05-05.json" → "2026-05-05"
            preds = {p["ticker"]: p for p in d.get("predictions", [])}
            result[date] = preds
        except Exception:
            continue
    return result


def evaluate_signal(action: str, ret_pct: float) -> str:
    """신호와 실제 수익률 비교."""
    if action == "BUY" or action == "WATCH":
        return "HIT" if ret_pct > 0 else "MISS"
    if action == "SELL" or action == "REDUCE":
        return "HIT" if ret_pct < 0 else "MISS"
    if action == "HOLD":
        return "HIT" if abs(ret_pct) < 1.5 else "MISS"
    return "SKIP"


def get_close(prices: dict, ticker: str, date: str):
    """특정 날짜 종가."""
    t = prices.get("tickers", {}).get(ticker)
    if not t: return None
    if date not in t.get("dates", []):
        return None
    idx = t["dates"].index(date)
    closes = t.get("closes", [])
    if idx >= len(closes):
        return None
    return closes[idx]


def find_next_trading_day(prices: dict, ticker: str, after_date: str):
    """ticker의 after_date 이후 첫 거래일 (주말/공휴일 건너뜀)."""
    t = prices.get("tickers", {}).get(ticker)
    if not t: return None
    dates = t.get("dates", [])
    for d in dates:
        if d > after_date:
            return d
    return None


def verify_day(signal_date: str, predictions: dict, prices: dict) -> dict:
    """특정 날짜의 모든 신호 검증."""
    results = {
        "signal_date": signal_date,
        "evaluated_at": datetime.now().strftime("%Y-%m-%d"),
        "tickers": [],
        "summary": {"total": 0, "hits": 0, "by_action": {}},
    }

    for ticker, pred in predictions.items():
        action = pred.get("action", "HOLD")
        signal_close = get_close(prices, ticker, signal_date)
        if signal_close is None:
            continue

        # 다음 거래일 찾기
        next_date = find_next_trading_day(prices, ticker, signal_date)
        if next_date is None:
            continue

        next_close = get_close(prices, ticker, next_date)
        if next_close is None:
            continue

        ret_pct = (next_close / signal_close - 1) * 100
        outcome = evaluate_signal(action, ret_pct)

        if outcome == "SKIP":
            continue

        results["tickers"].append({
            "ticker":      ticker,
            "action":      action,
            "confidence":  pred.get("confidence", 0),
            "tier":        pred.get("tier", "—"),
            "signal_close": round(signal_close, 2),
            "next_close":  round(next_close, 2),
            "next_date":   next_date,
            "ret_pct":     round(ret_pct, 2),
            "outcome":     outcome,
        })

        # Summary
        results["summary"]["total"] += 1
        if outcome == "HIT":
            results["summary"]["hits"] += 1

        if action not in results["summary"]["by_action"]:
            results["summary"]["by_action"][action] = {"n": 0, "hits": 0, "avg_ret": 0}
        results["summary"]["by_action"][action]["n"] += 1
        if outcome == "HIT":
            results["summary"]["by_action"][action]["hits"] += 1
        results["summary"]["by_action"][action]["avg_ret"] += ret_pct

    # 평균 계산
    for action_data in results["summary"]["by_action"].values():
        if action_data["n"] > 0:
            action_data["avg_ret"] = round(action_data["avg_ret"] / action_data["n"], 2)
            action_data["hit_rate"] = round(action_data["hits"] / action_data["n"] * 100, 1)

    if results["summary"]["total"] > 0:
        results["summary"]["hit_rate"] = round(
            results["summary"]["hits"] / results["summary"]["total"] * 100, 1
        )
    else:
        results["summary"]["hit_rate"] = 0

    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred-dir", default="site/data/predictions_history")
    ap.add_argument("--prices",   default="site/data/prices.json")
    ap.add_argument("--out",      default="site/data/verification/daily.json")
    args = ap.parse_args()

    prices = json.loads(Path(args.prices).read_text())
    predictions = load_predictions(Path(args.pred_dir))

    if not predictions:
        print("No predictions to verify")
        return

    print(f"Loaded {len(predictions)} prediction days")

    # 모든 날짜에 대해 검증
    all_results = []
    for sig_date in sorted(predictions.keys()):
        result = verify_day(sig_date, predictions[sig_date], prices)
        if result["summary"]["total"] > 0:
            all_results.append(result)

    if not all_results:
        print("No verifiable days yet")
        return

    # 누적 통계
    total = sum(r["summary"]["total"] for r in all_results)
    hits = sum(r["summary"]["hits"] for r in all_results)
    overall_hr = (hits / total * 100) if total > 0 else 0

    # Action별 누적
    action_cum = {}
    for r in all_results:
        for act, data in r["summary"]["by_action"].items():
            if act not in action_cum:
                action_cum[act] = {"n": 0, "hits": 0, "avg_ret_sum": 0}
            action_cum[act]["n"] += data["n"]
            action_cum[act]["hits"] += data["hits"]
            action_cum[act]["avg_ret_sum"] += data["avg_ret"] * data["n"]

    for act, d in action_cum.items():
        if d["n"] > 0:
            d["hit_rate"] = round(d["hits"] / d["n"] * 100, 1)
            d["avg_ret"] = round(d["avg_ret_sum"] / d["n"], 2)
            del d["avg_ret_sum"]

    # 출력
    output = {
        "updated":        datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "evaluation_window": "next-day (1 trading day after signal)",
        "total_days":     len(all_results),
        "total_trades":   total,
        "overall_hit_rate": round(overall_hr, 1),
        "by_action":      action_cum,
        "daily_results":  all_results[-30:],  # 최근 30일만
        "yesterday":      all_results[-1] if all_results else None,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(output, indent=2))

    # 콘솔 출력
    print(f"\n{'='*60}")
    print(f"📊 DAILY VERIFICATION REPORT")
    print(f"{'='*60}")
    print(f"Total days verified:  {len(all_results)}")
    print(f"Total signals:        {total}")
    print(f"Overall hit rate:     {overall_hr:.1f}%")

    print(f"\n📈 By Action:")
    for act in sorted(action_cum.keys()):
        d = action_cum[act]
        print(f"  {act:<8}  n={d['n']:>3}  hit={d['hit_rate']:>5.1f}%  avg={d['avg_ret']:+.2f}%")

    if all_results:
        last = all_results[-1]
        print(f"\n🔔 YESTERDAY ({last['signal_date']}):")
        print(f"  Total: {last['summary']['total']} signals")
        print(f"  Hit rate: {last['summary'].get('hit_rate', 0):.1f}%")

        for t in last["tickers"][:10]:
            mark = "✓" if t["outcome"] == "HIT" else "✗"
            print(f"  {mark} {t['ticker']:<6} {t['action']:<6} "
                  f"${t['signal_close']:>7.2f} → ${t['next_close']:>7.2f}  "
                  f"({t['ret_pct']:+.2f}%)  [{t['tier']}-tier]")

    print(f"\n→ {args.out}")


if __name__ == "__main__":
    main()
