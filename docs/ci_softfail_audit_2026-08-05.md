# trend-site.yml `|| true` 감사 (STEP 4-3, 2026-08-05)

61곳 분류. 원칙: soft-fail 스텝은 유지하되, 산출물이 "조용히 stale"이 될 수
있는 경로는 게이트가 잡아야 한다.

## A. 정당 — 인프라/정리/복원 (~32곳, 유지)
worktree remove/prune, git fetch/restore/reset/stash/stash pop, ls/echo,
rsync/cp 레거시 복제, gzip. 실패해도 데이터 정합성에 영향 없음.

## B. soft-fail + 종단 freshness gate 커버 (유지)
- predict.py → `predictions.json` (gate: updated)
- find_hidden_gems → `hidden_gems.json` (gate: generated_at)
- find_domino_chains → `domino.json` (gate: generated_at)
- aggregate_ticker_sentiment → `ticker_sentiment.json` (gate: generated)
- validate.py → `validation.json`, ml_monitor → `ml_monitor.json`,
  experiment_engine → `experiments/results.json` (기존 gate)
- archive_predictions.py — 자체 stale-input 거부 + predictions.json gate가
  상류를 커버

## C. soft-fail + 인라인 stale-input gate 신설 (이번 수정)
- fetch_prices_v2 / analyze_prices: 산출물(prices.json, technical_analysis.json)이
  이 워크플로에서 커밋되지 않아(fetch_prices.yml이 커밋) 종단 gate가 볼 수 없고,
  실패 시 predict가 어제 가격/TA로 "신선해 보이는" 예측을 생성.
  → TA 직후 "Gate — prices/TA regenerated this run" 신설: updated 나이 >2h면 RED.

## D. soft-fail 유지, 커버리지 없음 — 잔여 부채 (후속 후보)
- build_signal_corr, build_fundamentals, analyze_ticker, ticker_weights:
  실패 시 predict가 stale 판을 읽지만 종단 gate 미커버. 파일은 매 런 커밋됨
  → 종단 gate TARGETS에 추가 가능 (다음 라운드).
- backtest v1/v2, daily_verify, weekly_report, macro_themes, tickers.json,
  weekly_analyzer: 표시용/주간물 — 위험도 낮음.
- paper_trade / prediction_tracker / gap_analyzer / feature_engineering /
  ml_baseline: 실패 시 validate.py가 이전 records로 돌지만 validation.json의
  updated는 신선 → gate green인 채 내용만 stale. **구조적 한계**: gate는
  "산출물이 다시 쓰였는가"만 보고 "입력이 신선한가"는 못 본다. 해결하려면
  각 산출물에 입력-워터마크(예: last_record_date) 내장 필요 — STEP 4 범위 밖.
- pip install(42-43): torch 분리(STEP 4-7)와 함께 fail-loud로 전환 검토.

## 요약
- 신설: 인라인 prices/TA gate 1개, 종단 gate TARGETS +4
  (predictions/hidden_gems/domino/ticker_sentiment)
- 제거한 `|| true`: 0 (soft-fail 철학 유지, 게이트로 커버)
- 잔여 부채: D 섹션 — 입력-워터마크 패턴이 근본 해법
