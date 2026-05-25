# Phase 0 Validation Framework

Measurement-only infrastructure for evaluating prediction system performance.
No model or scoring logic is modified (GUARDRAIL 1).

## Quick Start

```bash
# Full validation report (writes site/data/validation.json)
python3 scripts/validate.py

# Self-tests only (11 tests across 3 harnesses)
python3 scripts/validate.py --self-test

# Individual harnesses
python3 scripts/walk_forward.py              # walk-forward OOS evaluation
python3 scripts/walk_forward.py --self-test   # 6 self-tests
python3 scripts/benchmark.py                 # paper-trading alpha vs SPY/QQQ
python3 scripts/benchmark.py --self-test      # 2 self-tests
python3 scripts/naive_baselines.py           # system vs always-buy/sell/random
python3 scripts/naive_baselines.py --self-test # 3 self-tests
```

All commands accept `--json` for machine-readable output.

## Components

### walk_forward.py
Expanding-window out-of-sample evaluation. Train window [0..k] → test fold [k+1].
In Phase 0, the train window enforces temporal ordering only (no parameters are
tuned on it). MIN_TRAIN_DATES=3 burns in the first 3 snapshot dates.

Metrics per fold (test-only): accuracy, Wilson 95% CI, market_up_rate,
edge vs baselines (pp), z-test, p-value, regime label, SPY 5d alpha
(per-trade mean and portfolio-level).

### benchmark.py
Paper-trading strategy returns vs SPY/QQQ buy-and-hold over the identical period.
Every output line includes alpha — absolute return is never shown alone.

### naive_baselines.py
Compares the real system against always-buy, always-sell, and two random
baselines, all evaluated through the same walk-forward engine.
Reports edge = system - best_baseline with z-test and p-value.

### validate.py
Unified entry point. Runs all three harnesses, writes `site/data/validation.json`,
prints a summary with verdict. The CI step (`--ci`) is measurement-only and
always exits 0.

## Self-Tests (GUARDRAIL 3)

Each component includes synthetic-system injection tests:

| Harness | Test | Assertion |
|---------|------|-----------|
| walk_forward | random_system | \|edge\|<10pp, p>0.05 |
| walk_forward | always_buy | edge_vs_buy = 0 |
| walk_forward | always_sell | acc = 1 - market_up_rate |
| walk_forward | perfect_foresight | acc > 99%, p < 0.001 |
| walk_forward | anti_perfect | acc < 1%, p < 0.001 |
| walk_forward | alpha_zero_always_buy | alpha p > 0.05 |
| benchmark | alpha_zero | identical return → alpha = 0 |
| benchmark | positive_alpha | ret = spy + 5% → alpha = +5% |
| naive_baselines | edge_zero_when_same | system = baseline → edge = 0 |
| naive_baselines | perfect_beats_all | perfect > all baselines |
| naive_baselines | random_vs_buy_not_significant | p > 0.05 |

## Regime Handling

All outputs are split by RISK-ON / RISK-OFF. When a regime has n < 30 test
records, it is flagged `[PROVISIONAL]` — conclusions from that regime are
unreliable.

## CI Integration

`trend-site.yml` runs `python scripts/validate.py --ci` after the calibration
pipeline. This writes `validation.json` alongside other site data. It never
gates the build — measurement only.
