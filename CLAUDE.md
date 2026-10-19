# newstrend

## Model Freeze Policy (2026-05-25)

**predict.py and all scoring logic are FROZEN.** No changes to:
- `predict.py` (decide_action, score formula, action thresholds)
- Pillar weights, confidence thresholds, drift offsets
- Any parameter that affects prediction output

### Why (updated 2026-08-05 — original numbers superseded)
The freeze was imposed 2026-05-25 on walk-forward stats (acc 44.0% p=0.023,
alpha −1.96% p=0.0002) that STEP 3 later showed were **overstated**: duplicate
weekend folds double-counted trades and iid t-tests ignored ρ1≈0.76 fold
autocorrelation. After dedup + Newey-West the historical OOS record is
*not significant in either direction* (acc p≈0.32, alpha p≈0.19 NW).

The freeze nevertheless stands, on two grounds that survive the corrections:
1. **ml_monitor forward falsification (2026-07-14, numbers as of
   2026-10-18)** — materially independent of both the calibrator taint and
   the STEP 3 statistical issues: RF forward alpha −0.397% (95% CI −0.583 to
   −0.211, t=−4.66), GB −0.500% (CI −0.688 to −0.312, t=−4.64), n=7740 /
   86 folds, frozen hash-verified models, post-2026-05-25 data only
   (`site/data/ml_monitor.json` latest, updated 2026-10-18T21:00:01Z, CI
   run 39165500880). ⚠️ These t/p values are **iid fold t-tests with
   weekend folds included** (ml_monitor.py) — the same two flaws STEP 3
   fixed in walk_forward. An independent recomputation (2026-10-18 audit:
   weekdays only, NW lag 4) still gives RF −0.43% p=0.0015, GB −0.51%
   p=0.0066, so the falsification survives; ml_monitor itself is fixed in
   PR C. The "p=0.0095 / p=0.0033" figures quoted in older notes came from
   the 2026-07-14 run and no longer appear in any output.
2. **Adoption gate FAIL** under the corrected paired NW test: system −
   always-buy = −0.98pp (NW p=0.092; system −0.57%, always-buy +0.41%,
   85 folds, `validation.json` updated 2026-10-18T20:59:59Z) — no evidence
   the system beats the trivial baseline, and the point estimate is
   negative. Caveat (2026-10-18 audit): always-buy is evaluated on the same
   records through `system_fn`, so on BUY/WATCH records both sides are
   identical and the paired difference is carried by SELL records only —
   the gate does not see long-side selection. PR C adds a "long picks −
   universe" line next to it (gate definition itself unchanged).

4 hypotheses tested (momentum, anti-signal, regime-gate, conf-inversion) —
none passed the adoption gate. Single RISK-ON regime dominates the sample;
the one RISK-OFF day (2026-04-17, n=23) is nowhere near a cycle.

### Unrecorded drift channel (found 2026-10-18)
predict.py has not changed since 2026-05-25, but **its inputs are refit
daily on realized outcomes**, so the "frozen" system still drifts:
- `learn_ticker_weights.py` runs before every predict (trend-site.yml
  "Learn ticker weights") and refits per-ticker TA/news hit rates over the
  **entire price history** (in-sample, `range(30, len(dates) − hold_days)`),
  writing `site/data/ticker_weights.json`. predict.py reads
  `news_weight`; when `news_weight ≥ 0.5` and the ticker's news score
  clears `3.0 / news_weight`, a HOLD/WATCH is **forced to BUY with
  conf +0.15** (and symmetrically to SELL/REDUCE with conf +0.10). The set
  of tickers in that branch moves day to day (7–24 tickers; 14 on
  2026-10-18, 20 on 2026-08-25). This applies to **every snapshot since
  2026-05-25**, not just the calibrator window.
- `analyze_ticker.py` (per-ticker word lists / hit rates →
  `ticker_analysis/*.json`) and `build_signal_corr.py` (`signal_corr.json`)
  are likewise refit every run and feed the news bonus.
- Effect on the ML forward test: RF/GB read these only through the
  `confidence` feature (RF 6.3% / GB 9.5% importance), so the forward
  falsification is indirectly exposed but not driven by it.
Consequence: historical walk-forward folds measure a system whose
news-override set changed daily. Any future fixed-system test must pin
`ticker_weights.json`, `ticker_analysis/` and `signal_corr.json` snapshots
alongside predictions (the `input_manifest.json` written since 2026-10-18
records their sha256 for exactly this purpose). Fixing the refit itself is
a scoring change and stays frozen.

### Rules
1. **New hypotheses** must be tested via `walk_forward(records, system_fn=...)` first
2. **Adoption gate**: system alpha > always-buy alpha AND p < 0.05 (OOS)
3. **Only gate-passing hypotheses** get implemented in predict.py
4. **Data collection continues** — target: 1785 test records + at least 1 RISK-OFF cycle
5. `validate.py` runs daily in CI, tracking progress automatically

### Calibrator demoted to shadow (2026-08-05)
The adaptive calibrator (`adaptive_calibration.py` → `pillar_weights.json`)
had been LIVE since 2026-05-15: `mode="active"` fed daily EMA multipliers into
predict.py (sentiment/fundamental/insider weights + news mult), fitted
in-sample on realized outcomes — bypassing rule 1 and making the "frozen"
model's behaviour drift daily. 74/86 walk-forward folds (89.9% of records)
were generated under this drift, so historical walk-forward alpha is the
score of a *changing* system, not the frozen predict.py.

Now demoted: `LIVE_MODE = False` in adaptive_calibration.py writes
`mode="shadow"`, which predict.py treats as no-op (hardcoded weights only).
Weights are still computed and recorded every run for later evaluation.
**Re-activation requires passing the adoption gate (rule 2) via
`walk_forward(records, system_fn=...)` with weights refit only on each
fold's train window.** Flipping `LIVE_MODE` back without that is a freeze
violation.

**Taint boundary:** predict.py evaluation metrics for snapshots
**2026-05-15 → 2026-08-05 are NOT fixed-system forward results** — the
scorer drifted daily in that window. `validation.json` carries a `taint`
block marking affected folds. Measured materiality is low (same-day A/B
2026-08-05: 0/90 action flips, max confidence delta 0.001, because the
multiplied channels — sentiment/news/insider — are largely dark), so the
negative-alpha conclusion likely stands, but treat pooled numbers spanning
the window accordingly. **ml_monitor (RF/GB forward) is materially
independent of this taint**: it evaluates its own frozen models
(hash-verified), and the only calibrator-downstream input is the
`confidence` feature (importance: RF 6.3% rank 6/34, GB 9.5% rank 4/34)
whose drift was ≤0.001 — the forward falsification (RF −0.397% / GB
−0.500% as of 2026-10-18, see "Why" above) is unaffected.

**Additional taint windows (recorded 2026-10-18, `validation.json`
`taint.windows`):**
- `2026-08-19 → 2026-08-23` — sentiment input frozen at 2026-08-16 (local
  FinBERT stalled; index −1 read with no age check).
- `2026-10-18 → FinBERT catch-up` — sentiment pillar dark (09-04 partial
  day, filtered_score null for 90/90), `trends.json` has holes
  (09-05→09-17, 09-25→10-16) so news z-scores run over a non-contiguous
  series, and `ticker_weights.json` was refit on that gappy news history.
  Snapshots in this window are not fixed-system forward results and must
  be excluded from any evaluation.

### Sentiment revival boundary (2026-08-05)
The news archive truncation (2026-05-03 → 2026-08-04, ~97% of articles
lost) was fixed and backfilled on 2026-08-05, and FinBERT rescored 168
days. **The backfill fixes `sentiment_per_day` / `ticker_sentiment` only —
historical predictions were NOT re-generated.** Every prediction snapshot
from 2026-05-03 through 2026-08-04 was made with the sentiment pillar
effectively dark (filtered_score null for ~all tickers) and stays that way
in the record. Therefore:
- **Forward data with a live sentiment pillar starts 2026-08-06** (corrected
  from 2026-08-05). The 08-05 daytime snapshots still had the pillar dark:
  aggregate_ticker_sentiment exported the same-day partial file (4 tickers)
  and predict.py's load_sentiment reads index −1, so filtered_score was null
  for all 90 tickers. Fixed 2026-08-05 (commit 3388cca6c): the aggregator now
  clamps to complete days (≤ D-2), so index −1 lands on a full day. Any
  evaluation of "the system with sentiment" must use snapshots from the first
  CI run after that commit — 2026-08-06 is the clean boundary. The ML forward
  falsification below is evidence against the 4-working-pillar system, not
  the 5-pillar design.
- Sentiment now lags 2 days by design: the archive stores only complete
  days, and with SHIFT_MINUTES=1440 day D's warehouse file keeps growing
  through D+1 — so D-2 is the freshest complete day (a same-day snapshot
  is structurally ~3% of the articles, which is what the original bug
  silently shipped for 95 days).
- **Permanent gap 2025-11-15 → 2026-02-02**: the warehouse's own daily
  files for those 80 days contain token aggregates instead of articles;
  no article-level source exists, so sentiment can never be computed for
  that window.
- **2026-10-18 ~ FinBERT catch-up 전 스냅샷: sentiment pillar DARK
  (평가 시 제외).** update-warehouse 6주 교착(2026-09-04 → 10-18) 동안
  news_archive/sentiment_per_day가 멈췄고, ticker_sentiment.json의 마지막
  날짜는 09-04 **부분일(7건)** — 10-18 스냅샷 90개 전부
  `sentiment.date=2026-09-04, filtered_score=null, total=0`. predict.py
  load_sentiment는 index −1을 age 체크 없이 읽고, aggregate의 D-2 클램프는
  "오늘−2"만 보므로 멈춘 파이프라인이 남긴 부분일을 걸러내지 못한다.
  (같은 유형의 미기록 사례: 08-19→08-23 스냅샷은 sentiment가 08-16에 고정.) trend-site의 "Commit predictions
  history" 단계가 "Sentiment freshness health check"보다 앞이라, 게이트
  RED여도 stale sentiment로 만든 스냅샷은 이미 main에 커밋된 상태다.
  health check를 Generate predictions 앞으로 옮기는 구조 변경은 별도 PR.

### Statistical corrections (STEP 3, 2026-08-05)
Baseline snapshot before these landed: `docs/validation_baseline_2026-08-05.md`.
- walk_forward reads `records_deduped` (one outcome per (ticker, anchor));
  weekend snapshots no longer create duplicate folds (86 → ~60 folds).
- Fold-level t-tests use Newey-West (lag 4) — daily folds with 5-day
  returns have ρ1≈0.76; iid p-values are kept under `*_iid`.
- MIN_TRAIN_DATES=0: the 2026-04-17 RISK-OFF snapshot (n=23) is no longer
  burned; risk_off_n is nonzero for the first time (still ONE day — the
  "RISK-OFF cycle observed" unfreeze condition remains unmet in spirit).
- The adoption gate tests the paired per-fold (system − always-buy)
  difference with a NW t-test instead of the system's own H0:alpha=0.
- benchmark.py headline alpha nets the benchmark leg to average exposure
  (~85% invested vs 100%-invested SPY); raw alpha kept as alpha_raw_pct.

### CI robustness (STEP 4, 2026-08-05)
Silent-failure hardening (commits 3388cca6c, 033666d6b): freshness gate now
also covers predictions/hidden_gems/domino/ticker_sentiment; inline stale-
input gate for prices/TA before predict; predictions_history archiving moved
to a dedicated stale-rejecting step (archive_predictions.py) right after
predict; collect fails RED on 0 articles or missing NEWSAPI_KEY; warehouse
artifact floor+shrink checks; calibration EMA gated to once/UTC-day; torch
split to requirements-local.txt (CI pip fail-loud); per-path git adds;
archive-daily find-path bug fixed (Releases were single-batch fallbacks) and
now archives D-2. Full audit: docs/ci_softfail_audit_2026-08-05.md.

### ML Alpha Signal (2026-05-25) — ⚠️ FALSIFIED FORWARD, see below
Walk-forward ML evaluation showed RF/GBM passing adoption gate with
OOS alpha +1.7~2.0% vs SPY. 5-point audit (look-ahead, paired test,
long-short, autocorrelation, multiple testing) found no evidence of
fake alpha. However: 14 folds, single regime, borderline p-values.

**Forward tracking active** via `ml_monitor.py`. Trains on data <= 2026-05-25,
evaluates only on NEW data after that date. This is the real test.

### ML Forward Alpha Falsification (2026-07-14)
The forward-only tracker (installed 2026-05-25) **falsified** the historical
ML alpha after ~50 days of out-of-sample data. The +1.7~2.0% was a
false positive:
- RF forward alpha (2026-07-14 run): −0.39% — significantly NEGATIVE;
  **as of 2026-10-18: −0.397%**, CI [−0.583, −0.211], t=−4.66, n=7740
- GB forward alpha (2026-07-14 run): −0.59%; **as of 2026-10-18: −0.500%**,
  CI [−0.688, −0.312], t=−4.64
  (source: `site/data/ml_monitor.json` latest, CI run 39165500880; the
  July p-values 0.0095/0.0033 are not reproducible from current output —
  ml_monitor reports iid `alpha_p=0.0`; NW/weekday recomputation in the
  2026-10-18 audit: RF p=0.0015, GB p=0.0066)
- predict.py OOS alpha (2026-07-14): −0.73% (p=0.034) → gate FAIL vs
  always-buy (+0.38%); **as of 2026-10-18: −0.57% (NW p=0.18), paired
  diff −0.98pp (NW p=0.092), always-buy +0.41%**
- Directional accuracy: 48–51% (coin flip)
- Data target MET: 2009/1785 test records (112.5%)
- RISK-OFF cycle: observed=true

**Conclusion:** the historical alpha did not survive forward testing.
Model freeze REMAINS JUSTIFIED — not for lack of data (target met), but
because the edge is disproven. This is the forward tracker working as
designed: it caught an overfit signal that all 5 historical audits missed.

Side warning: `long_ratio 84.6%` — the system is systematically long and
rarely goes short; part of the "alpha" was just bull-market beta.

### Unfreeze Conditions (revised 2026-07-14)
Both must be true simultaneously:
1. Forward-only alpha (post-2026-05-25 data) is **significantly positive**
   (p < 0.05) — the data target alone is NOT sufficient (it is already met)
2. **Both** RF and GB agree on positive forward alpha (not one cherry-picked)
   AND at least 1 RISK-OFF cycle observed (already satisfied)

Given forward alpha is now significantly negative, unfreeze is not on the
table. Until forward alpha flips positive: model frozen, data collection
continues, monitoring automatic.

### Validation
```bash
python3 scripts/validate.py              # full report + validation.json
python3 scripts/validate.py --self-test  # 11 harness self-tests
python3 scripts/ml_monitor.py            # forward alpha tracker
python3 scripts/ml_monitor.py --status   # one-line CI status
python3 scripts/ml_walkforward.py        # ML walk-forward (historical)
```

## Project Structure

- `scripts/predict.py` — prediction generator (FROZEN)
- `scripts/validate.py` — Phase 0 validation entry point
- `scripts/walk_forward.py` — expanding-window OOS evaluation
- `scripts/benchmark.py` — paper-trading alpha vs SPY/QQQ
- `scripts/naive_baselines.py` — always-buy/sell/random comparisons
- `scripts/ml_walkforward.py` — ML walk-forward evaluation (historical)
- `scripts/ml_monitor.py` — forward-only ML alpha tracker (daily CI)
- `site/data/validation.json` — latest validation results
- `site/data/ml_monitor.json` — forward alpha tracking log
