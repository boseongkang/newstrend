# newstrend

## Model Freeze Policy (2026-05-25)

**predict.py and all scoring logic are FROZEN.** No changes to:
- `predict.py` (decide_action, score formula, action thresholds)
- Pillar weights, confidence thresholds, drift offsets
- Any parameter that affects prediction output

### Why
Walk-forward OOS evaluation (357 records, 14 folds) shows:
- Accuracy 44.0% — significantly below 50% (p=0.023)
- Alpha -1.96% vs SPY — significantly negative (p=0.0002)
- Loses to always-buy, always-sell, and random baselines

4 hypotheses tested (momentum, anti-signal, regime-gate, conf-inversion) — none passed the adoption gate. The data is insufficient to distinguish signal from noise (single RISK-ON regime, effective n~160).

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
