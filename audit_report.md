# Newstrend Codebase Audit — 2026-05-08

Read-only review of `scripts/` · `site/` · `config/` · `.github/workflows/` · top-level layout.
**No code modified.** Findings prioritized for the user's review.

---

## TL;DR — Top 10 cross-cutting priorities

| # | Finding | Severity | Impact | Effort |
|---|---|---|---|---|
| 1 | `page_debug/` 54 MB / 126 files **tracked in git**, 0 code references | 🔴 | repo size, clone time | 5 min |
| 2 | `_wh/` 25 MB / 31 files **tracked**, only used as workflow temp path | 🔴 | repo size | 5 min |
| 3 | `backtest.html` nav has 5 corrupted paths `../../../Downloads/...` (escapes site root); also missing GEMS / DOMINO / PORTFOLIO links | 🔴 | live site UX | 15 min |
| 4 | All pages link `./index.html`, but source file is `index_custom.html` — relies on undocumented deploy rename. If rename ever drops, every DASHBOARD nav link 404s. | 🔴 | live site UX | 10 min |
| 5 | `data/warehouse/master.jsonl` gone, but **10 scripts default to it** → silent empty output for any local user who runs them | 🔴 | dev confusion / hidden failures | 30 min (retire scripts) |
| 6 | `daily.yml` workflow runs nightly against a legacy `newscli`/`raw_newsapi` flow that no longer exists; burns Actions minutes | 🔴 | CI cost | 5 min |
| 7 | `configs/alias_en.yml` referenced by `update-warehouse.yml:153` does **not exist** — hourly workflow is mis-configured | 🔴 | hourly CI noise / silent dropouts | 10 min |
| 8 | 24 `data/warehouse/daily/*.jsonl` tracked **despite** `.gitignore data/warehouse/` rule — repo state inconsistent | 🔴 | gitignore semantics broken | 10 min |
| 9 | ~33 dead Python scripts (~5,000 LOC ≈ 35–40 % of `scripts/`) — most cluster around the legacy `master.jsonl` / `trend_runner.yaml` / matplotlib-viz era. Bulk retirement is one PR. | 🟡 | maintenance surface | 1 h |
| 10 | ~3–4 k LOC of duplicate CSS + JS across 12 HTML pages (`<style>` blocks, `:root` palette, `nav`, `.section`, `escapeHtml`, `fetchJSON`). Already caused the GEMS/DOMINO/RISING propagation bugs above. | 🟡 | future-edit ergonomics | 2 h |

---

## Recommended phased cleanup

| Phase | Goal | Items | Risk | Estimated |
|---|---|---|---|---|
| **Phase A** — Bulk untrack | Reclaim ~80 MB, fix gitignore semantics | #1, #2, #8, plus `wh_art/` `daily/` `master.jsonl` `config/extra_noise.txt.bak` | very low (no code touched) | 20 min |
| **Phase B** — Live-site fixes | Stop user-facing 404s | #3, #4, plus add `index.html` symlink/copy or rename source | low | 30 min |
| **Phase C** — Workflow trim | Stop wasted CI runs, fix race | #6, #7, plus `entities.yml` cron conflict, missing `concurrency:` on `update-warehouse.yml` | medium (verify nothing depends on `daily.yml` artifacts) | 30 min |
| **Phase D** — Dead-script retirement | Shrink `scripts/` by ~35 % | #5, #9 — move to `scripts/legacy/` or `git rm` outright | medium (no callers found, but local devs might) | 1 h |
| **Phase E** — Refactor | Long-term ergonomics | #10, plus extract `scripts/_stats.py` shared helpers, dedup `requirements.txt`, rewrite `README.md` | medium (broad-touch) | 4–6 h |

Each phase is independently shippable; I'd run A → B → C in one session and D → E later after watching for regressions.

---

# Detailed findings by domain

## Domain 1 — `scripts/` (71 files, ~13.6 k LOC)

### Dead scripts (not invoked by any workflow / CLI / other script)

🟡 The following scripts have **no caller** in `.github/workflows/`, other scripts, `*.sh`, or `src/news_trend/`:

```
add_silver_to_warehouse.py    aggregate.py                    analyze_pairs.py
auto_pipeline.py              backfill_auto_pipeline.py        bursts.py
clean_keywords_report.py      cooccur_graph.py                 cumulative_report.py
entities_extract.py           entity_trend.py                  fetch_prices.py
fetch_releases.py             filter_tokens_csv.py             join_prices.py
keyword_trends.py             make_articles_json.py            make_report.py
make_report_data.py           merge_live_to_daily.py           move_live_into_days.py
ngrams_trend.py               organize_live_by_date.py         postcheck_trend_pivot.py
prepare_trending_dir.py       publisher_sentiment.py           render_trend_site.py
reorg_live_to_ymd.py          rising_from_tokens_csv.py        run_trends_existing.py
site_from_run.py              split_tokens_by_day.py           trend_cumulative.py
trend_topics_from_master.py   trending_terms.py                viz_rising_results.py
viz_warehouse.py              viz_words.py                     warehouse_build.py
assert_freshness.py
```

Recommended: bulk-move to `scripts/legacy/` with a one-paragraph note in each, OR `git rm` outright. They cluster around three retired sub-systems:
1. `data/warehouse/master.jsonl` era (master.jsonl no longer maintained).
2. `config/trend_runner.yaml` era (runner not invoked).
3. matplotlib/streamlit local viz tools never wired into the site.

### Critical: stale `master.jsonl` references (🔴, system-wide)

10 scripts default to `data/warehouse/master.jsonl` which the current pipeline does not produce:
`bursts.py`, `cooccur_graph.py`, `entities_extract.py`, `entity_trend.py`, `keyword_trends.py`, `publisher_sentiment.py`, `run_trends_existing.py`, `warehouse_build.py`, `render_trend_site.py`, `update_corpus.py` (note: `update_corpus.py` IS invoked by `update-warehouse.yml`, but it falls through to a code path that handles missing master gracefully — confirm).

Anyone who runs these locally gets silent empty output. Recommend: retire 9 of 10; keep `update_corpus.py` and confirm graceful degradation.

### Duplicated logic worth extracting (🟡, system-wide)

| Helper | Files (count) | Suggested home |
|---|---|---|
| `zscore_at` / `zscore_series` (28-day rolling) | `backtest.py`, `backtest_v2.py`, `analyze_pairs.py`, `analyze_ticker.py`, `build_signal_corr.py`, `learn_ticker_weights.py`, `macro_themes.py`, `predict.py`, `make_trends_json.py` (**8+**) | `scripts/_stats.py` |
| `binomial_pvalue` + `erf_approx` (Abramowitz coefficients verbatim) | `backtest.py`, `backtest_v2.py`, `analyze_pairs.py`, `analyze_ticker.py` (**4**) | same |
| `rsi_at` (hand-rolled RSI vs `analyze_prices.calc_rsi` already producing series) | `backtest.py`, `backtest_v2.py` (**2**) | reuse pre-computed `technical_analysis.json` series |
| Tokenize + stopword filter | `aggregate.py`, `aggregate_from_warehouse.py`, `cumulative_report.py`, `trend_cumulative.py`, `trending_terms.py`, `csv_to_tokens.py`, `extract_terms.py`, `make_trends_json.py`, others (**8+**, with subtly different stop sets) | `scripts/_text.py` + `config/stopwords.txt` (already partial) |
| Publisher extraction (dict / str / list fallback) | `cumulative_report.py`, `make_report_data.py`, `warehouse_build.py`, `sentiment_finbert.py`, `site_from_run.py`, `aggregate_from_warehouse.py` (**6**) | one helper |
| Hit-rate evaluation (BUY → ret>0, SELL → ret<0, etc.) | `learn_ticker_weights.py`, `daily_verify.py`, `weekly_report.py`, `backtest.py` (**4**, with different thresholds 1.5 % vs 2.0 %) | one evaluator with explicit threshold arg |
| Ticker alias loading + regex compilation | `sentiment_finbert.py`, `build_signal_corr.py`, others (**3+**, with slight rule differences) | central alias module |

Removing these duplicates would delete ~150–200 LOC and prevent silent formula drift.

### O(N²) hotspots in **active** scripts (🟡, local)

- `backtest.py:184, 201` — `p_dates.index(d)` inside per-day loop.
- `backtest_v2.py:255, 267` — same pattern × 3 market tickers per day.
- `learn_ticker_weights.py:48, 51, 153–172` — `dates.index(date)` inside outer per-day loop.
- `daily_verify.py:48, 65` — `dates.index(d)` linear search per day.
- `analyze_ticker.py:188–230` — `compute_stats` runs on train, test, full set per term (3× redundant).
- `analyze_ticker.py:105` — `zscore_series` recomputed for every (term, lag) instead of once per term.
- `backtest.py:90` — `zscore_at` recomputed for every (word, ticker) instead of once per word.
- `macro_themes.py:300–307` — per-(date, word) `zscore_at`; ~180 k calls × O(28) = wasted ~5 M ops per run.

Per-script fix: pre-compute `{date:idx}` once per ticker; pre-compute z-score series once per word. Each individual fix is 5 lines.

### Confirmed bugs (🟡)

- `predict.py:683` | `news["best_conf"] = max((w.get("expected_ret",0) for w in active_bull), default=0) / 5` — `expected_ret` can be negative; current `max(... default=0)` silently floors to 0. Should use `abs()` or proper conf score.
- `learn_ticker_weights.py:189` | `to_weight(hr, n)` returns 0 when `hr < 0.5`; throws away valid bearish signals.
- `analyze_prices.py:202` | `signal_line = ema(macd_line, signal_n)` — when `macd_line` contains `None` from holiday gaps, `ema()` overwrites `prev` with None, effectively resetting the EMA. Filter or skip None-as-overwrite.
- `aggregate.py:148` | check-then-increment pattern verified safe on re-read; **not a bug**, false positive.

### Deprecation / hygiene (🟢)

- `assert_freshness.py:7` and `cumulative_report.py:150` use `datetime.utcnow()` (deprecated 3.12+).
- `build_static_ui.py:67` parses dates as plain strings (lexicographic sort) — works for ISO 8601 only.
- Lazy imports inside hot functions: `import gzip`, `import re as _re`, `from glob import glob`, `import sys`, `import pandas as pd`, `import orjson` appear inside function bodies in active scripts. Hoist to module level.
- Unused imports: `cumulative_report.py:1` (`math`, `csv`), `extract_terms.py:1` (`sys`), `clean_keywords_report.py:1` (`math`).
- `fetch_prices.py` (legacy 125-line) still on disk; superseded by `fetch_prices_v2.py`. Delete.

**Severity counts (Domain 1):** 🔴 1 · 🟡 ~75 · 🟢 ~35 · ⚪ ~10

---

## Domain 2 — `site/` (12 HTML pages, ~6.2 k LOC)

### Critical broken nav (🔴)

- `site/backtest.html:121–127` | 5 nav links point at `../../../Downloads/predict.html`, `../../../Downloads/report.html`, `.../portfolio.html`, `.../rising.html`, `.../macro.html`. They escape the site root → 404 on live.
- `site/backtest.html` nav | Missing GEMS, DOMINO, and PORTFOLIO links entirely (recent additions never propagated here because of the corruption above).
- `site/index_custom.html:223–235` | Nav block omits `<a href="./rising.html">RISING</a>` (10 links instead of 11). Likely dropped accidentally during GEMS/DOMINO insert.
- All 12 pages | Nav links to `./index.html`; source file is `index_custom.html`. Relies on a deploy-step rename. If the rename ever breaks, **every page** loses its DASHBOARD link.

### Massive CSS/JS duplication (🟡, ~3–4 k LOC)

| Block | Pages | Approx LOC duplicated |
|---|---|---|
| `:root{--bg0…--cyan…}` palette + reset + scrollbar | all 12 | ~10 × 12 = 120 |
| `.unified-header` block | 8 inlined + 4 variants | ~40 × 8 = 320 |
| `header` / `nav` / `.brand` styling | all 12 | ~30 × 12 = 360 |
| `.section` / `.sec-hdr` / `.sec-title` / `.sec-meta` | all 12 | ~25 × 12 = 300 |
| `.empty` + `.spinner` + `@keyframes spin` | most pages | ~15 × 8 = 120 |
| `escapeHtml`, `fmt2`, `sign`, `pct`, `fetchJSON('./data/X?v=…')` | all pages | ~20 × 12 = 240 |
| Trailing duplicate `<script>` IIFE that resets `#unified-ts` (already set in init) | 7 pages | ~5 × 7 = 35 |

**Total extractable**: ≈ 1,500 lines into `site/css/{base,header,components}.css` + `site/js/util.js`. Full HTML page sizes drop ~35–40 %.

### Variable/value drift (🟡, cross-file)

- `predict.html`, `report.html`, `rising.html`, `signals.html`, `ticker_detail.html` use `--font-mono`/`--font-sans` for the same purpose other pages call `--fm`/`--fs`. Pick one.
- `predict.html:14`, `signals.html:15`, `report.html:14` declare `--purple:#b39ddb` while other pages use `#b380ff`. Pick one.
- `domino.html:135`, `hidden_gems.html:135` use `<header>` (no `unified-header` class) while siblings use `<header class="unified-header">`. Standardize.

### Stale data refs (🟢)

`site/data/`-located JSONs that **no page fetches**:
`articles.json` (958 B), `publishers.json`, `words.json`, `rising.json`, `backtest.json`, `ta_summary.json`, `technical_analysis.json` (3 MB), `ticker_sentiment.json`, `prices_meta.json`. Either consume these or stop emitting from the pipeline.

`site/entities/report.html` references 4 PNGs all sized 3,925 B (placeholders) and 2 zero-byte CSVs — stale debug artifact directory not linked from any nav.

### CDN coupling (🟡)

- Chart.js is loaded from `https://cdn.jsdelivr.net/npm/chart.js@4.4.6/...` on 5 pages, while `site/vendor/chart.umd.min.js` (200 KB vendored copy) already exists. Switch to local vendor for offline + reproducibility.
- D3 v7 in `domino.html` is also loaded from CDN; consider vendoring.

### Other UX (🟢)

- Browser-side Anthropic API call in `report.html:411–444` will always fail (no key, no proxy). Hide the button or pre-bake `brief.json`.
- Inline `onclick=` handlers in markup (`portfolio.html:228`, `ticker_detail.html:489`, `index_custom.html:348`) mixed with addEventListener style elsewhere. Migrate for CSP-friendliness.

**Severity counts (Domain 2):** 🔴 9 · 🟡 23 · 🟢 15

---

## Domain 3 — config / configs / workflows / structure / data / deps

### `configs/` is a typo (🟡)

- `configs/` (sibling of canonical `config/`) contains only `stopwords_en.txt` (referenced by `update-warehouse.yml:152`). Move file → `config/stopwords_en.txt`, delete `configs/`, fix workflow.

### Missing referenced file (🔴)

- `update-warehouse.yml:153` passes `--alias configs/alias_en.yml` to `extract_terms.py`. **The file does not exist.** Hourly workflow runs with this misconfiguration silently. Fix: create the file (empty/seeded), drop the flag, or make `extract_terms.py` no-op on missing path.

### Stale config files (🟡)

- `config/extra_noise.txt.bak` — backup, 0 references. Delete.
- `config/topic_lexicon.yaml` — 0 references. Delete or revive.
- `config/trend_runner.yaml` + `config/publisher_blacklist.txt` + `config/publisher_weights.json` — referenced only by orphaned scripts (`run_trends_existing.py`, `trend_topics_from_master.py`). Retire together.

### Top-level junk dirs (🔴, big size win)

| Path | Size | Tracked files | References | Verdict |
|---|---|---|---|---|
| `page_debug/` | **54 MB** | **126** | 0 | git rm -r --cached + .gitignore |
| `_wh/` | 25 MB | 31 | only as workflow temp | git rm -r --cached + .gitignore |
| `wh_art/` | 288 KB | 2 | 0 | git rm -r --cached + .gitignore |
| `daily/` | 92 KB | 1 | 0 | git rm -r --cached + .gitignore |
| `master.jsonl` (top-level) | 92 KB | 1 | 0 | git rm + .gitignore |
| `data/warehouse/daily/*.jsonl` | — | **24** | gitignored but **still tracked** | inconsistency — pick one and align |

Total reclaimable: ~80 MB.

### `.gitignore` hygiene (🟢)

- `run/` listed 3 times — collapse to one.
- `data/live_newsapi/` and `data/warehouse/` lines redundant with `data/**`.
- `!data/README.md` whitelisted but file doesn't exist.
- Missing entries for the junk dirs above.

### `.github/workflows/` — full inventory

| Workflow | Schedule | Status |
|---|---|---|
| `collect_continuous.yml` | every 30 min | active (NewsAPI ingest) |
| `collect_rss.yml` | every 4 h | active (RSS) |
| `update-warehouse.yml` | hourly :23 | **central hub**, but missing `concurrency:` block |
| `cache-tokens.yml` | after update-warehouse | active |
| `archive-daily.yml` | 00:08 UTC | active (GH Releases) |
| `entities.yml` | 07:25 UTC | **stale-ish** — uploads artifact no consumer reads, **races trend-site.yml** |
| `fetch_prices.yml` | 21:30 weekdays | active |
| `trend-site.yml` | 07:25 UTC + workflow_run | active (Pages deploy) |
| `daily.yml` | 09:00 UTC | **DEAD** — runs against `newscli`/`raw_newsapi` flow that no longer exists |

🔴 **Schedule conflict:** `entities.yml` and `trend-site.yml` both fire at `25 7 * * *`, both download warehouse artifact, both render entities. Move one or delete `entities.yml`.

🔴 **`daily.yml`** burns nightly Actions minutes for nothing. Delete.

🟡 **`update-warehouse.yml`** has no `concurrency:` group; back-to-back hourly runs could overlap on shared paths.

🟡 **`entities.yml`** uploads `entities-report` artifact with 7-day retention but no `workflow_run` consumer. Either wire it in or delete.

### `data/` layout (🔴)

- `data/warehouse/daily/*.jsonl` — 24 files **tracked** despite `data/warehouse/` being in `.gitignore`. Fix:
  - `git rm --cached data/warehouse/daily/*.jsonl` (preferred — they live on artifacts and `data-cache` branch), OR
  - remove the gitignore line if intent is to keep them in-tree.

### Deps (🟡)

- `requirements.txt` has duplicates: `pandas` ×3, `numpy` ×3, `feedparser` ×2, `requests` ×2, `pyyaml` ×2, `python-dateutil` ×2, `tqdm` ×2, `beautifulsoup4` ×2, `lxml` ×2, `orjson` ×2, `yfinance` ×2.
- `pyproject.toml` and `requirements.txt` overlap inconsistently (`python-dotenv` and `trafilatura` only in pyproject; `typer` only in requirements).

### Top-level files (🟡)

- `README.md` documents the legacy `newscli` / `raw_newsapi` flow. **Out of date by ~3–6 months.** No mention of `update-warehouse`, `trend-site`, FinBERT, predictions, `data-cache` branch, fundamentals, hidden gems, domino. Rewrite or replace with a 1-paragraph pointer.
- `pyproject.toml` author placeholder `you@example.com`. Cosmetic.

**Severity counts (Domain 3):** 🔴 6 · 🟡 17 · 🟢 9 · ⚪ 3

---

## Aggregate severity tally

- 🔴 **critical**: ~16
- 🟡 **improvement**: ~115
- 🟢 **nitpick**: ~59
- ⚪ **intentional / OK**: ~13

≈ **200 distinct findings**. The bulk (115 🟡) are dead code / duplication / refactor opportunities — not urgent, but together they're 35–40 % of the codebase.

---

## Patterns observed (multi-domain)

1. **"master.jsonl era" residue.** A whole generation of scripts (10+) and one config family (`trend_runner.yaml`, `topic_lexicon.yaml`, `publisher_*.json`) was built around a warehouse format that the current pipeline doesn't maintain. Retiring all of them in one move would shrink scripts/ by ~35 %.
2. **Helper drift.** Same numeric helper (`zscore_at`, `binomial_pvalue`, `rsi_at`, tokenize, publisher-extract) reimplemented 4–8× across active scripts. No silent contradictions today, but slight differences (e.g. z-score `std<0.5` floor vs `<0.3`) make signals from different scripts not strictly comparable.
3. **Copy-paste UI.** `<style>` and `<nav>` blocks are duplicated verbatim across 12 HTML pages, which is exactly why GEMS / DOMINO / RISING propagation has bugs in `backtest.html` and `index_custom.html`. Until extracted to shared CSS/JS, every nav change costs 12 edits and one inevitable miss.
4. **Tracked junk.** ~80 MB of `_wh/`, `wh_art/`, `daily/`, `page_debug/`, top-level `master.jsonl` are tracked with no readers. `.gitignore` updates would have prevented this; today they slow every clone.
5. **Workflow drift.** Two workflows (`daily.yml`, `entities.yml`) burn CI minutes producing artifacts no one consumes. One scheduling race (`entities.yml` ↔ `trend-site.yml`). One missing concurrency guard (`update-warehouse.yml`).

---

## What's good (do not touch)

- Recently shipped Pillar 4 stack — `sec_edgar_fetcher.py`, `build_fundamentals.py`, `fundamentals_analyzer.py`, `find_hidden_gems.py`, `find_domino_chains.py`, `fetch_prices_v2.py` — all clean, well-documented, internally consistent.
- `predict.py` core scoring logic (RSI/BB/MACD/sentiment/fundamental layers) — sound architecture; only minor `expected_ret/5` confidence proxy issue.
- `archive-daily.yml` (GitHub Releases per-day) — durable backup, keep.
- `cache-tokens.yml` — clean rolling window on `data-cache` branch, keep.
- `data/prices/` and `data/sec_cache/` directories — correctly ignored, ledger-style append-only.
- Memory files (`project_*.md`) — actively used, accurate, good documentation discipline.

---

## Suggested next session

The user has audit data; concrete change is **out of scope for this read-only review**. When ready, propose Phase A → B → C as a single PR (low risk, big visible win), and revisit D/E after observing for one full pipeline cycle (~24 h).
