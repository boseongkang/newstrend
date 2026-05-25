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

### ML Alpha Signal (2026-05-25)
Walk-forward ML evaluation showed RF/GBM passing adoption gate with
OOS alpha +1.7~2.0% vs SPY. 5-point audit (look-ahead, paired test,
long-short, autocorrelation, multiple testing) found no evidence of
fake alpha. However: 14 folds, single regime, borderline p-values.

**Forward tracking active** via `ml_monitor.py`. Trains on data <= 2026-05-25,
evaluates only on NEW data after that date. This is the real test.

### Unfreeze Conditions
Both must be true simultaneously:
1. Forward-only alpha (post-2026-05-25 data) is positive AND p < 0.05
2. At least 1 RISK-OFF cycle observed in forward data

Until then: model frozen, data collection continues, monitoring automatic.

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
