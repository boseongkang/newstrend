# Validation baseline snapshot — 2026-08-05 (pre-STEP-3)

Frozen copy of `site/data/validation.json` headline numbers **before** the
STEP 3 statistical corrections (dedup / Newey-West / MIN_TRAIN_DATES=0 /
paired gate / benchmark beta adjustment) landed.

Purpose: after STEP 3, the daily numbers move for TWO independent reasons —
(a) the statistical corrections themselves, and (b) the sentiment pillar
revival (news_archive backfill 2026-08-05; predictions made from 2026-08-05
onward see live sentiment, everything earlier was scored with the pillar
effectively dark). This snapshot pins (a)'s starting point so the two
effects can be separated. **This file is a historical record — do not
regenerate or update it.**

Source: `validation.json` updated `2026-08-05T06:55:05Z`
(CI run 30982687227), i.e. sentiment-dead predictions + pre-correction
statistics. Calibrator taint: mode=active window 2026-05-15 → 2026-08-05
(74/86 folds; see `taint` block in validation.json and CLAUDE.md).

## Walk-forward (uncorrected)

| metric | value |
|---|---|
| n_folds | 86 |
| total_test_n | 2,865 |
| pooled_accuracy | 0.4712 |
| p_vs_coin (accuracy) | 0.0021 |
| alpha per-trade mean | −0.7492 % |
| alpha p (iid t-test) | 0.0078 |
| risk_off_n | 0 (2026-04-17 snapshot burned by MIN_TRAIN_DATES=3) |
| regimes_tested | NEUTRAL, RISK-ON |

## Gate (unpaired substitute test)

FAIL: system alpha (−0.75%) vs always-buy alpha (+0.09%), p=0.0078
(p is the system's own H0:alpha=0 test, not a paired difference test).

## Baselines (post-hoc best-baseline selection, unpaired SE)

Edge vs always_sell: −4.29 pp, z=−4.59, p≈0 (best baseline chosen after
seeing results; known winner's-curse comparison — recorded as-is).

## Benchmark (no beta/exposure adjustment)

Period 2026-04-17 → 2026-08-04, SPY +8.46%, QQQ +11.19%:

| strategy | return | avg_invested | alpha vs SPY (raw) |
|---|---|---|---|
| main_system | +7.34% | 84.9% | −1.11 pp |
| conservative | +4.33% | 79.9% | −4.13 pp |
| aggressive | +9.20% | 93.6% | +0.74 pp |

Raw alpha compares a ~85%-invested portfolio against 100%-invested SPY;
mechanical exposure drag ≈ (1−0.849)×8.46 ≈ 1.28 pp over this period.

## Known-invalid aspects captured by this baseline

- 27/86 folds are weekend/holiday calendar re-runs of the same trading day
  (records duplicated 1.44x in pooled stats).
- No autocorrelation correction: fold alpha ρ1≈0.76 (5-day overlapping
  returns on daily folds).
- Reference corrected values measured during the 2026-08-05 audit, on the
  then-current 86-fold data: dedup-by-anchor accuracy p 0.0021→0.0564;
  Newey-West(4) alpha p 0.0078→0.147; paired+NW gate diff p≈0.243.
