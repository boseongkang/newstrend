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
- 잔여 노출 (같은 패턴, 미강화 — `grep -rn "dawidd6\|gh run list"
  .github/workflows/`로 감사):
  - `update-warehouse.yml` dawidd6 3곳: previous warehouse(낡은 픽이면
    최근 며칠이 조용히 빠질 수 있음 — floor 체크는 대규모 소실만 잡음),
    live-jsonl, rss-jsonl(낡은 픽이면 해당 시간대 기사 누락, 다음
    회전에서 대체로 자가 회복).
  - `entities.yml` dawidd6 1곳: warehouse 다운로드.
  - 강화 필요성 판단 기준: 출력이 회전마다 재생성되면 자가 회복(낮은
    우선순위), 하루 1회·append-only면 영구 누락(높은 우선순위).
