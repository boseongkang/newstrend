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
1. **ml_monitor forward falsification (2026-07-14)** — materially independent
   of both the calibrator taint and the STEP 3 statistical issues: RF forward
   alpha −0.39% (p=0.0095), GB −0.59% (p=0.0033) on frozen, hash-verified
   models over post-2026-05-25 data only.
2. **Adoption gate FAIL** under the corrected paired NW test: system − 
   always-buy = −0.85pp (p=0.28) — no evidence the system beats the trivial
   baseline, and the point estimate is negative.

4 hypotheses tested (momentum, anti-signal, regime-gate, conf-inversion) —
none passed the adoption gate. Single RISK-ON regime dominates the sample;
the one RISK-OFF day (2026-04-17, n=23) is nowhere near a cycle.

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
whose drift was ≤0.001 — the forward falsification (RF −0.39% p=0.0095,
GB −0.59% p=0.0033) is unaffected.

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
- RF forward alpha: **−0.39%** (p=0.0095) — significantly NEGATIVE
- GB forward alpha: **−0.59%** (p=0.0033) — significantly NEGATIVE
- predict.py OOS alpha: **−0.73%** (p=0.034) → gate FAIL vs always-buy (+0.38%)
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
