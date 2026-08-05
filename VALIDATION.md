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
tuned on it).

STEP 3 corrections (2026-08-05, commit 05a7ce31f):
- Reads `records_deduped` — one outcome per (ticker, anchor). Weekend
  snapshots used to duplicate folds (86 → ~60), silently double-counting
  the same trades in every pooled statistic.
- Fold-level t-tests use Newey-West (lag 4). Daily folds with 5-day
  overlapping returns have ρ1≈0.76; the old iid p-values (kept as `*_iid`)
  overstated significance badly.
- MIN_TRAIN_DATES=0 — the single RISK-OFF snapshot (2026-04-17, n=23) is
  no longer burned in.

Metrics per fold (test-only): accuracy, Wilson 95% CI, market_up_rate,
edge vs baselines (pp), NW t-test p-value, regime label, SPY 5d alpha
(per-trade mean and portfolio-level).

### benchmark.py
Paper-trading strategy returns vs SPY/QQQ buy-and-hold over the identical period.
Every output line includes alpha — absolute return is never shown alone.
Since STEP 3 the headline alpha nets the benchmark leg to the strategy's
average exposure (~85% invested vs a 100%-invested SPY leg); the raw
100%-benchmark number is kept as `alpha_raw_pct`.
Known limitations (open): the regime label is hardcoded rather than derived,
and its two self-tests are tautologies (see below).

### naive_baselines.py
Compares the real system against always-buy, always-sell, and two random
baselines, all evaluated through the same walk-forward engine.
Reports edge = system - best_baseline with z-test and p-value.

### validate.py
Unified entry point. Runs all three harnesses, writes `site/data/validation.json`,
prints a summary with verdict. The CI step (`--ci`) is measurement-only and
always exits 0.

The adoption gate (since STEP 3) tests the *paired* per-fold difference
(system − always-buy) with a Newey-West t-test — not the system's own
H0:alpha=0, which conflated "has alpha" with "beats the trivial baseline".
`validation.json` also carries a `taint` block marking the 2026-05-16 →
2026-08-05 calibrator-live window (see CLAUDE.md).

## Self-Tests (GUARDRAIL 3)

Each component includes synthetic-system injection tests. Honest accounting:
these verify the harness *plumbing* (correct wiring, no sign flips, no
look-ahead in the happy path) — they do NOT certify that the measured metrics
are meaningful. In particular the two benchmark tests are tautologies: they
assert that `a − b` subtracts (identical series → alpha 0; series+5% → alpha
+5%), which can only fail if the arithmetic itself is broken. They provide no
evidence about period alignment, exposure netting, or dividend handling.
A previous self-test bug is also instructive: the null-injection test once
used `permutation(y)`, which inherits market drift and produced "fake alpha"
that looked like a passing detector (fixed 2026-07-14 → `standard_normal`).
Treat a green self-test suite as "not obviously broken", never as "validated".

| Harness | Test | Assertion | Strength |
|---------|------|-----------|----------|
| walk_forward | random_system | \|edge\|<10pp, p>0.05 | real null check |
| walk_forward | always_buy | edge_vs_buy = 0 | consistency |
| walk_forward | always_sell | acc = 1 - market_up_rate | consistency |
| walk_forward | perfect_foresight | acc > 99%, p < 0.001 | detector works |
| walk_forward | anti_perfect | acc < 1%, p < 0.001 | detector works |
| walk_forward | alpha_zero_always_buy | alpha p > 0.05 | real null check |
| benchmark | alpha_zero | identical return → alpha = 0 | tautology |
| benchmark | positive_alpha | ret = spy + 5% → alpha = +5% | tautology |
| naive_baselines | edge_zero_when_same | system = baseline → edge = 0 | consistency |
| naive_baselines | perfect_beats_all | perfect > all baselines | detector works |
| naive_baselines | random_vs_buy_not_significant | p > 0.05 | real null check |

## Regime Handling

All outputs are split by RISK-ON / RISK-OFF. When a regime has n < 30 test
records, it is flagged `[PROVISIONAL]` — conclusions from that regime are
unreliable.

## CI Integration

`trend-site.yml` runs `python scripts/validate.py --ci` after the calibration
pipeline. This writes `validation.json` alongside other site data. It never
gates the build — measurement only.
