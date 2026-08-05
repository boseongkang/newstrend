"""
build_weekly_narrative.py — 주간 검증 로그 데이터+서술 생성기.

성격: "오늘의 추천"이 아니라 "이번 주 검증 로그". 세 번의 독립 검증에서
엣지 부재가 확인된 시스템이므로 추천이 아니라 측정 기록을 발행한다.

구조로 강제되는 제약:
- 모든 수치는 이 스크립트(Python)가 결정론적으로 계산해 JSON에 직접 실린다.
- Claude(있다면)는 서술 텍스트와 결함 목록의 문장만 생성한다 — 구조화 출력
  스키마로 강제되며, 숫자 재계산/재해석은 시스템 프롬프트로 금지된다.
- ANTHROPIC_API_KEY 부재/실패 시 서술 없이 데이터-only로 강등된다.

출력:
- site/data/weekly_narrative.json          (최신, 페이지가 읽음)
- site/data/weekly_narratives/{ISO주}.json  (아카이브)
- site/data/weekly_narratives/index.json    (아카이브 목록)

주 1회 게이트: 토요일(UTC)에만 생성하고, 같은 ISO 주에 이미 생성했으면
스킵한다. --force 로 오버라이드 (수동 실행/테스트용).
"""
from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "site" / "data"
OUT_LATEST = DATA / "weekly_narrative.json"
OUT_DIR = DATA / "weekly_narratives"
CHECKPOINT_FILE = ROOT / "config" / "checkpoint_registration.json"

LIVE_SENTIMENT_SINCE = "2026-08-06"  # CLAUDE.md 'Sentiment revival boundary'
NW_LAG = 4
HOLD_DAYS = 5
TRADING_DAYS_PER_MONTH = 21


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_week(dt: datetime) -> str:
    y, w, _ = dt.date().isocalendar()
    return f"{y}-W{w:02d}"


def _load(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


# ── 결정론적 수치 계산 ───────────────────────────────────────────────────
def _nw_vif(series: list[float]) -> tuple[float, float]:
    """AR(1) 근사 기반 Newey-West 분산 팽창 계수. (rho1, vif)"""
    n = len(series)
    if n < 3:
        return 0.0, 1.0
    m = statistics.mean(series)
    dev = [x - m for x in series]
    denom = sum(d * d for d in dev)
    if denom == 0:
        return 0.0, 1.0
    rho1 = sum(dev[i] * dev[i + 1] for i in range(n - 1)) / denom
    rho1 = max(0.0, min(rho1, 0.99))
    vif = 1 + 2 * sum((1 - k / (NW_LAG + 1)) * rho1**k for k in range(1, NW_LAG + 1))
    return rho1, vif


def _t_ppf(p: float, df: int) -> float:
    from scipy import stats
    return float(stats.t.ppf(p, df))


def build_status(validation: dict) -> dict:
    gate = validation.get("gate") or {}
    wf = validation.get("walk_forward") or {}
    folds = wf.get("folds") or []
    n = gate.get("paired_n_folds") or len(folds)

    alpha = gate.get("system_alpha_pct")
    p_nw = gate.get("system_alpha_p")
    ci = [None, None]
    if alpha is not None and p_nw not in (None, 0) and n and n > 2:
        # NW p값에서 t → se 역산 → 95% CI (판정용이 아니라 표시용 근사)
        t_abs = abs(_t_ppf(p_nw / 2, n - 1))
        if t_abs > 0:
            se = abs(alpha) / t_abs
            crit = abs(_t_ppf(0.025, n - 1))
            ci = [round(alpha - crit * se, 3), round(alpha + crit * se, 3)]

    live = [
        f for f in folds
        if f.get("test_date", "") >= LIVE_SENTIMENT_SINCE
        and f.get("alpha_portfolio_pct") is not None
    ]
    live_alpha = (
        round(statistics.mean(f["alpha_portfolio_pct"] for f in live), 3)
        if live else None
    )

    return {
        "forward_alpha_pct": alpha,
        "forward_alpha_ci95": ci,
        "forward_alpha_p_nw": p_nw,
        "live_sentiment": {
            "since": LIVE_SENTIMENT_SINCE,
            "n_folds": len(live),
            "alpha_pct": live_alpha,
        },
        "gate": {
            "pass": bool(gate.get("pass")),
            "reason": gate.get("reason"),
            "paired_diff_pp": gate.get("paired_diff_pp"),
            "paired_p": gate.get("paired_p"),
        },
        "n_folds_total": n,
    }


def build_power(validation: dict) -> dict:
    folds = (validation.get("walk_forward") or {}).get("folds") or []
    alphas = [f["alpha_portfolio_pct"] for f in folds
              if f.get("alpha_portfolio_pct") is not None]
    if len(alphas) < 3:
        return {"n_folds": len(alphas), "rows": []}
    sd = statistics.stdev(alphas)
    rho1, vif = _nw_vif(alphas)
    sd_nw = sd * math.sqrt(vif)
    rows = []
    for eff, annual in ((0.2, "~10%/yr"), (0.5, "~28%/yr"), (1.0, "~65%/yr")):
        need = (1.96 * sd_nw / eff) ** 2
        rows.append({
            "effect_pct": eff,
            "annualized": annual,
            "folds_needed": round(need),
            "months_needed": round(need / TRADING_DAYS_PER_MONTH, 1),
        })
    return {
        "n_folds": len(alphas),
        "fold_sd_pct": round(sd, 3),
        "rho1": round(rho1, 3),
        "nw_sd_pct": round(sd_nw, 3),
        "rows": rows,
        "note": ("이 표가 이 리포트의 존재 이유다: 현실적 크기의 엣지"
                 "(연 10~30%)는 이 설계·표본에서 유의성으로 판정할 수 없다. "
                 "그래서 사전 등록 체크포인트는 유의성이 아니라 점추정 부호와 "
                 "기대 가치를 기준으로 한다. 검정력 부족은 '엣지가 없다'의 "
                 "증거가 아니라 '이 방법으로는 알 수 없다'의 증거다."),
    }


def build_checkpoint(status: dict) -> dict | None:
    reg = _load(CHECKPOINT_FILE)
    regs = reg.get("registrations") or []
    active = [r for r in regs if not r.get("superseded_by")]
    if not active:
        return None
    r = active[-1]
    power = r.get("precomputed_power") or {}
    return {
        "id": r.get("id"),
        "registered_at": r.get("registered_at"),
        "decision_date": r.get("decision_date"),
        "elapsed_folds": status["live_sentiment"]["n_folds"],
        "expected_folds": power.get("expected_folds_by_decision_date"),
        "mde_pct": power.get("minimum_detectable_effect_pct_5d"),
        "rule_summary": (
            f"{r.get('decision_date')}에 live-sentiment forward alpha 점추정이 "
            f"양수가 아니면 → {((r.get('decision_rule') or {}).get('then') or '')}"
        ),
    }


def build_raw_signals(week: str) -> dict:
    """이번 ISO 주의 predictions_history에서 참고용 원시 신호 집계."""
    hist_dir = DATA / "predictions_history"
    agg: dict[str, dict] = {}
    for f in sorted(hist_dir.glob("*.json")):
        try:
            d = datetime.fromisoformat(f.stem).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if _iso_week(d) != week:
            continue
        payload = _load(f)
        for p in payload.get("predictions") or []:
            tk = p.get("ticker")
            if not tk or p.get("action") in (None, "HOLD", "WATCH"):
                continue
            cell = agg.setdefault(tk, {"ticker": tk, "action": p["action"],
                                       "confidence": 0.0, "count": 0})
            cell["count"] += 1
            if (p.get("confidence") or 0) > cell["confidence"]:
                cell["confidence"] = p["confidence"]
                cell["action"] = p["action"]
    items = sorted(agg.values(), key=lambda x: (-x["count"], -x["confidence"]))[:15]
    return {
        "warning": ("벤치마크를 이기지 못하는 것으로 확인된 시스템의 출력. "
                    "검증 대상이지 추천이 아님."),
        "items": items,
    }


# ── 서술 생성 (Claude — 텍스트만, 숫자는 위에서 계산된 값만 참조) ────────
NARRATIVE_SCHEMA = {
    "type": "object",
    "properties": {
        "narrative": {
            "type": "string",
            "description": "이번 주 검증 활동 요약 서술 (한국어, 3-6문장)",
        },
    },
    "required": ["narrative"],
    "additionalProperties": False,
}

SYSTEM_PROMPT = """\
너는 정량 트레이딩 시스템의 주간 '검증 로그' 서술자다. 이 시스템은 세 번의
독립 검증(walk-forward, ML forward 반증, 통계 보정 후 재검정)에서 벤치마크를
이기지 못한다는 것이 확인되었고, 이 리포트는 투자 추천이 아니라 측정 기록이다.

절대 규칙 (위반 시 리포트 전체가 무효):
1. "유의하지 않음"을 "개선됨"으로 바꾸지 말 것. 부호가 덜 나빠진 것은 노이즈다.
2. 점추정과 유의성을 항상 구분할 것. 점추정 언급 시 유의하지 않음을 병기.
3. 검정력 부족을 증거 부재와 구분할 것 — "알 수 없다"와 "없다"는 다르다.
4. 숫자를 재계산하거나 새 해석을 추가하지 말 것. 입력 JSON에 있는 값만,
   있는 그대로 서술할 것. 입력에 없는 수치는 쓰지 말 것.
5. 추천/전망/낙관 표현 금지: "기대된다", "개선 추세", "유망", "매수" 등 금지.
   미래에 대한 문장은 사전 등록 체크포인트의 기계적 규칙 인용만 허용.

할 일:
- narrative: 이번 주의 측정 결과를 3-6문장으로. 시스템 성과 자랑이 아니라
  측정 기록이다."""


def generate_narrative(payload: dict) -> dict:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return {"generated": False, "reason": "ANTHROPIC_API_KEY 없음"}
    try:
        import anthropic
    except ImportError:
        return {"generated": False, "reason": "anthropic SDK 미설치"}

    user_content = json.dumps(
        {"computed_numbers": {k: payload[k] for k in
                              ("week", "status", "week_activity", "power",
                               "checkpoint")},},
        ensure_ascii=False, indent=1,
    )
    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model="claude-opus-5",
            max_tokens=4000,
            system=SYSTEM_PROMPT,
            output_config={"format": {"type": "json_schema",
                                      "schema": NARRATIVE_SCHEMA}},
            messages=[{"role": "user", "content": user_content}],
        )
        if resp.stop_reason == "refusal":
            return {"generated": False, "reason": "모델이 요청을 거절함"}
        text = next(b.text for b in resp.content if b.type == "text")
        parsed = json.loads(text)
        return {
            "generated": True,
            "model": resp.model,
            "text": parsed.get("narrative", ""),
            "defects": parsed.get("defects", []),
        }
    except Exception as e:  # 서술은 부가물 — 실패해도 데이터는 발행
        return {"generated": False, "reason": f"API 오류: {e}"}


# ── main ────────────────────────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true",
                    help="요일/주간 게이트 무시하고 즉시 생성")
    args = ap.parse_args()

    now = _now()
    week = _iso_week(now)

    if not args.force:
        if now.isoweekday() != 6:
            print(f"skip: 주간 리포트는 토요일(UTC)에 생성 (오늘 isoweekday="
                  f"{now.isoweekday()}). --force로 오버라이드.")
            return 0
        prev = _load(OUT_LATEST)
        if prev.get("week") == week:
            print(f"skip: {week} 리포트는 이미 생성됨 ({prev.get('generated_at')}). "
                  f"--force로 오버라이드.")
            return 0

    validation = _load(DATA / "validation.json")
    if not validation:
        print("ERROR: validation.json 없음 — 리포트 생성 불가", file=sys.stderr)
        return 1

    status = build_status(validation)
    # weekly_analysis.json은 원시 활동량 입력으로만 사용 — 그 파일의
    # "지난주 계획이 먹혔나" 프레임(캘리브레이션 시대)은 서사에 쓰지 않는다
    wa = _load(DATA / "weekly_analysis.json")
    week_activity = {
        k: (wa.get("week_summary") or {}).get(k)
        for k in ("n", "directional_acc")
    }
    payload = {
        "week": week,
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": status,
        "week_activity": week_activity,
        "power": build_power(validation),
        "checkpoint": build_checkpoint(status),
        "raw_signals": build_raw_signals(week),
    }

    narrative = generate_narrative(payload)
    payload["narrative"] = narrative

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_LATEST.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
    (OUT_DIR / f"{week}.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=1))

    index_path = OUT_DIR / "index.json"
    index = _load(index_path)
    weeks = sorted(set((index.get("weeks") or []) + [week]))
    index_path.write_text(json.dumps(
        {"weeks": weeks, "updated": payload["generated_at"]},
        ensure_ascii=False, indent=1))

    print(f"weekly_narrative: {week} 생성 "
          f"(narrative={'생성됨' if narrative.get('generated') else '데이터-only: ' + str(narrative.get('reason'))}, "
          f"live_folds={status['live_sentiment']['n_folds']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
