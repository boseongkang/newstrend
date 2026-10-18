# ISSUES — frozen 코드 관련 알려진 결함 (unfreeze 시 재검토)

predict.py와 그 입력 파이프라인은 FROZEN (CLAUDE.md Model Freeze Policy).
여기 기록된 항목은 **지금 수정하지 않는다** — 수정하면 freeze 위반이자
forward 데이터 오염이다. unfreeze 조건 충족 또는 명시적 재설계 결정 시
재검토한다.

## F-new-1: news_z가 전역 지표인데 per-ticker feature로 사용 (2026-08-06)

- `analyze_prices.py:635 load_news_zscores()`: trends.json hot 20 단어의
  |z| **합계** = 날짜당 **단일 값**. 이 동일한 전역 시계열이 모든 종목의
  TA 컬럼 `news_z`에 병합되고, predict.py가 각 종목의 `news_z_today`로
  복사한다.
- 결과: 전 종목 동일값 (2026-08-05 기준 90/90 종목 모두 107.2) →
  **횡단면 변별력 0**. 시계열 변동만 기여.
- ML feature importance에서 news_z 0.6%에 그치는 **구조적 원인**.
- 함수 주석("각 날짜의 전체 뉴스 신호 강도를 하나의 숫자로 요약")상
  전역 지표 자체는 의도된 설계로 보이나, per-ticker 필드(`news_z_today`)에
  실려 종목별 지표처럼 소비/표시되는 것은 의도 불명.
- 처리: **의도된 설계라면** 이름/문서에 "market-wide"임을 명시,
  **아니라면** per-ticker 뉴스 카운트 기반으로 교체 필요. 어느 쪽이든
  frozen이므로 unfreeze 전 수정 불가.
- 소비처 완화 조치 (frozen 아님, 2026-08-06 수정): daily_report 생성기는
  news_z를 쓰지 않고 ticker_sentiment.json의 종목별 기사 수(자기 14일
  평균 대비 배율, D-2)로 교체함.

## F-new-2: RECENT_MINUTES 180→360 + dedup 부재로 news_z 인플레이션 (2026-08-06)

- 2026-08-05 STEP 4에서 collect_continuous의 RECENT_MINUTES를 180→360으로
  확대 (cron 스로틀 구멍 대응 — 그 목적 자체는 유효).
- warehouse 병합(update-warehouse.yml "Merge Live to Daily JSONL")에는
  dedup이 없다 — 기존에도 1.36x 중복 계수가 문서화되어 있음
  ([[project-news-archive-truncation]] side finding).
- 수집 창 2배 → 08-05부터 일자별 기사/단어 카운트가 기저 대비 부풀려짐.
  z_at()의 28일 롤링 평균이 옛 수집량 기준인 동안(**08-05 ~ 09-02 구간**)
  news_z가 구조적으로 높게 나온다.
- predict.py의 news_z_today에 그대로 반영됨 (F-new-1과 결합: 전 종목
  동일한 인플레이션 값).
- **08-05~09-02 구간의 예측 해석 시 주의**: news 채널 기여가 과대평가될
  수 있음. 28일 창이 새 수집량으로 채워지는 09-02 이후 자연 정상화.
- 근본 수정 후보 (frozen 아님, 별도 결정 필요): warehouse 병합에
  URL/id 기반 dedup 추가 — 단 이는 카운트 시계열의 레벨을 바꾸므로
  또 다른 불연속을 만든다. 추가하려면 도입일을 기록하고 trends 신뢰
  구간에 반영할 것.

---

# 잔여 부채 (2026-08-05 softfail 감사 §D 및 2026-08-17 전수 점검에서 정리)

## D-1: 게이트가 입력 신선도를 못 봄 — **FIXED 2026-08-17**

- 문제: freshness gate는 "출력이 다시 쓰였는가"만 검사. 입력이 낡아도
  생성기는 출력을 신선하게 재도장하므로 통과 — sentiment 95일 사망이
  정확히 이 구조로 안 잡혔다. signal_corr / fundamentals / analyze_ticker /
  ticker_weights 4개는 종단 게이트 미포함이기도 했다.
- 수정: 4개 산출물에 `input_watermark`(source / last_record_date /
  record_count, 소스별) 내장 — scripts/input_watermark.py. 신규
  scripts/input_watermark_gate.py가 소스별 허용 지연(trends 2d / prices 4d /
  edgar 120d — 각 한계는 설계 근거 주석과 함께 상수로 정의, STALE_DAYS=1
  vs D-2 클램프 사건 재발 방지)을 검사하고 벗어나면 RED.
  trend-site.yml의 validation freshness gate 직후 신설 스텝으로 실행.
- 검증: `--self-test` 7/7 (장주말 4d 가격 갭·51d된 분기 as_of는 통과 =
  false RED 없음), 정상 트리 green, 인위적 stale 트리 4개 시나리오
  (trends 5d / prices 6d / 워터마크 없는 잔존 파일 / edgar 139d) 전부 RED.
- 남은 후속(범위 밖): paper_trade → validate.py 체인의 입력 워터마크
  (validation.json의 records 신선도) — 아래 D-2와 함께 후속 라운드.

## D-2: 통계 도구 잔여 결함 — OPEN

- naive_baselines McNemar + 사전등록 미비, ml_monitor anchor collision +
  CI/p 기저 불일치, benchmark regime 하드코딩 + 동어반복 self-test.

## D-3: 영구 데이터 갭 — OPEN (복구 불가, 기록 유지)

- 2025-11-15 → 2026-02-02 (80일): warehouse 일별 파일이 기사 대신 토큰
  집계만 보유 — sentiment 영구 계산 불가. 해당 구간 평가 시 제외 필수.

- **2026-09-04 → 2026-09-17 뉴스 기사** (CI-2 교착; NewsAPI lookback 밖, warehouse·Release 모두 없음). 토큰 전용 구간은 2025-11-15 → **2026-02-04(82일)**로 재측정(기존 기록 02-02/80일).

## D-4: 파킹된 회귀/정리 — OPEN

- signal_corr 0-pairs 회귀 (tokenizer-fix-c3 브랜치 ead6c26a에 파킹).
- ticker_to_cik의 ANSS 잔존 (무해하나 매 런 SEC 호출 낭비 + skip 로그).

---

# CI 인프라 이슈 (frozen 아님 — 수정 가능)

## CI-1: GitHub API가 낡은 run 목록/아티팩트를 반환 (2026-08-08 최초 관측)

- 증상: `gh run list` 및 dawidd6/action-download-artifact의 "latest
  successful run" 선택이 간헐적으로 낡은 런을 반환한다. 관측 사례:
  - 2026-08-08: trend-site의 `.[0]` 픽이 2시간 내에 4일 전 런
    (31234965503)과 40일 전 런(31238330329)으로 두 번 해석됨.
  - 2026-08-11: trend-site RED — 12h 신선도 하한을 5회 재시도에도
    통과 못 함 (강화 로직이 설계대로 낡은 데이터 사용을 차단).
  - 2026-08-12: archive-daily가 받은 "latest" warehouse 아티팩트에
    daily/2026-08-10.jsonl이 없었고, live 폴백(98건 전부 08-11자)이
    0건을 만들어 snap-2026-08-10 Release가 영구 누락됨
    (2026-08-15 warehouse에서 수동 복구).
- 수정 이력:
  - trend-site: a79200443 (2026-08-08) — 클라이언트측 createdAt 정렬
    + <12h 하한 + 5회 재시도 + 실패 시 명시적 RED.
  - archive-daily: 2026-08-15 — 같은 로직 이식 + live 폴백 제거(RED로
    전환) + workflow_dispatch `date` 입력(warehouse에서 특정 날짜를
    꺼내 Release 생성/갱신하는 수동 복구 경로).
  - update-warehouse previous-warehouse 픽: 2026-08-15 — 같은 RID 로직
    + 신선도 하한 4h → **6h (2026-10-18, PR #9)**: live 창이 6h
    (RECENT_MINUTES=360)라 base가 6h 이내면 공백이 live 병합으로 전부
    재충전 → 영구 손실 0; 하한을 넘는 base는 손실이 다음 회전의 base로
    고착됨. 4h는 GitHub cron 실발화 간격(3.5~5h)에 걸려 상시 교착 위험
    (→ CI-2) + 날짜 커버리지 가드
    (복원된 warehouse의 최신 daily가 D-2보다 오래되면 RED — 줄 수 floor는
    "오래된 대형 warehouse"를 통과시키므로 별도 필요) + 자기-체인 데드락
    탈출구 `allow_stale_base` dispatch 입력(>floor 장애 후 최초 성공 런은
    반드시 stale base라 RED 교착 — 의식적 수동 수용 경로). 2026-10-18
    PR #9부터는 자기연쇄 교착을 자동 탈출(CI-2 참조).
- 잔여 노출 — **감사 종결, 강화 안 함** (`grep -rn "dawidd6\|gh run list"
  .github/workflows/`):
  - `update-warehouse.yml` live-jsonl / rss-jsonl: 낡은 픽이면 그 회전에
    해당 시간대 기사가 빠지지만, 수집 창 6h × 매시간 회전이라 다음
    정상 픽에서 같은 기사가 다시 병합됨 — 자가 회복, 상태 비누적.
  - `entities.yml` warehouse 픽: 읽기 전용 소비자(분석 출력 재생성).
    낡은 base여도 다음 회전에서 전체 재계산 — 자가 회복.
  - 판단 기준(기록용): 출력이 회전마다 전체 재생성되면 자가 회복 →
    강화 불필요. 출력이 누적 상태(warehouse)거나 하루 1회 append-only
    (Release 스냅샷)면 낡은 픽 = 영구 손실 → 강화 필수. 해당 두 경로는
    모두 강화 완료.

## CI-2: update-warehouse 자기연쇄 stale-base 교착 — 6주 정지 (2026-09-04 → 2026-10-18)

- 사건: 2026-09-04 09:43Z 마지막 스케줄 성공(run 33859763034) 뒤
  10:23~13:23 cron 4회가 GitHub에 의해 드롭(concurrency 그룹은 pending
  1개만 유지). 14:25Z run은 "최신 성공이 4h 플로어보다 오래됨"으로 RED
  (run 33883530214). 그 뒤 매 run이 같은 상태를 상속 — 성공이 생기지
  않으니 플로어를 통과할 성공도 생기지 않음 — 2026-10-18 19:16Z
  `allow_stale_base=true` 수동 dispatch(run 39158023789)까지 ~190회 RED,
  실패 메일 ~100통.
- 원인: CI-1 가드가 "API가 낡은 목록을 반환"과 "자기 실패 연쇄"를 구분할
  수 없었음. 둘 다 "최신 성공 < 플로어"로 보임. 또 명목 매시간 cron은
  실제 5~7회/일(간격 3.5~5h)이라 4h 플로어는 평시에도 걸림.
- 피해: warehouse에 2026-09-04 → 10-17 기사 부재. NewsAPI 무료 플랜
  lookback(~30일) 안인 09-18 이후는 backfill 입력으로 복구 중(청크
  09-18~23 완료 10-18, 09-24~10-05 / 10-06~10-17 예정). **2026-09-04 →
  09-17은 영구 공백** (D-3에 추가). snap-* Release 09-18~10-16은
  archive-daily `date=` dispatch로 재생성 예정.
- 수정 (PR #9, squash 2a2c2af7f, 2026-10-18): "Get previous warehouse run
  id"가 매 시도마다 (1) liveness oracle(`gh run list --limit 10`에 자기
  run id가 보이면 목록은 live), (2) `--status success --limit 20`,
  (3) 무필터 40(교차 확인 + since-run 통계) 3개 호출. 판정 a) 최신 성공이
  플로어 안 → 정상, b) 플로어 밖 + 목록 live → 교착 확정, AUTO_STALE로
  즉시 그 성공 위에 빌드, c) 목록 not live → 5회 재시도 후 RED(CI-1
  보호 유지). 플로어 4h→6h. stale base 수용 시(auto/manual)
  `::warning title=Stale base accepted::` + `STALE_BASE_ACCEPTED=1` +
  `data/warehouse/gaps.jsonl` 1줄(artifact에 포함) + 날짜 커버리지 가드
  warning 강등. fixture 14건 + 브랜치 실물 run 39162216999로 검증.
- 잔여: (a) oracle의 호출(1)과 성공 조회(2)(3)이 별개 응답이라 "(1) live,
  (2) stale" 글리치는 auto-accept 가능 → 호출(3) raw 응답에서 SELF 확인
  (PR B). (b) warehouse가 90일 artifact 체인 단일 사본 → 주간 Release
  스냅샷 + `restore_from_release` (PR B). (c) 매 run 최신 live 슬라이스
  1개만 병합 → 마지막 성공 이후 전부 병합 (PR B). (d) `timeout-minutes`
  없음, `permissions` 기본 write-all (PR B).

## D-5: news_archive 중복 행 ~60% — OPEN (PR B에서 수정)

- 측정 (2026-10-18, data-cache `news_archive/*.jsonl.gz` 335일):
  901,215행 / 고유 norm_url 362,327 → **중복 59.8%** (일별 평균 61.1%,
  2026-08-20: 7,223행 중 고유 2,632).
- 원인: update_corpus.py는 norm_url로 dedup하지만 update-warehouse의
  "Merge Live to Daily JSONL" 단계가 같은 기사를 raw로 다시 append
  (6h 윈도우 중첩 + 매시간 RSS 재다운로드). FinBERT는 dedup하지만 두
  사본의 키가 다름(`newsapi:`+url vs url / RSS sha1 id) → 둘 다 채점 →
  `n_articles_scored` ~2배(08-20: 5,163 vs 2,632). aggregate는 dedup 없음
  → `MIN_ARTICLES=5`는 실제 고유 2~3건.
- F-new-2(news_z 인플레이션)의 근본 원인과 동일 계열. frozen 코드가 아닌
  워크플로/수집 측 결함이므로 수정 가능.
