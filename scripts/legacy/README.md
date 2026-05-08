# scripts/legacy

Retired scripts preserved for git history / archaeology. **Not invoked
by any workflow, script, or `src/` module** as of 2026-05-08
(`audit_report.md` Phase D).

These cluster around three retired sub-systems:

1. **`data/warehouse/master.jsonl` era.** `bursts`, `cooccur_graph`,
   `entities_extract`, `entity_trend`, `keyword_trends`,
   `publisher_sentiment`, `run_trends_existing`, `warehouse_build`,
   `render_trend_site`, `trend_topics_from_master`,
   `trend_cumulative`, `trending_terms`, `cumulative_report`,
   `make_report*`, `clean_keywords_report` — all defaulted to the
   master.jsonl format the current pipeline no longer maintains.
2. **`config/trend_runner.yaml` era.** `auto_pipeline`,
   `backfill_auto_pipeline`, `prepare_trending_dir`,
   `organize_live_by_date`, `reorg_live_to_ymd`,
   `move_live_into_days`, `merge_live_to_daily` — orchestrators for
   a runner that is no longer invoked.
3. **Local matplotlib / streamlit viz.** `viz_rising_results`,
   `viz_warehouse`, `viz_words`, `ngrams_trend`,
   `postcheck_trend_pivot` — never wired into the static site.
4. **v1 superseded.** `fetch_prices` → use `fetch_prices_v2`.
   `aggregate` → use `aggregate_from_warehouse`. `site_from_run`,
   `make_articles_json`, `make_report`, `make_report_data`,
   `rising_from_tokens_csv`, `split_tokens_by_day`,
   `filter_tokens_csv`, `join_prices`, `analyze_pairs`,
   `add_silver_to_warehouse`, `assert_freshness`,
   `entities_extract`, `fetch_releases` — superseded by current
   pipeline equivalents.

If you need to revive one, copy back to `scripts/` and audit its
data dependencies first. The files have not been edited; they are
preserved as-was at retirement time.
