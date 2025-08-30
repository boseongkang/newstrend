# news trend  

08/24 Jetbrain student license renwel not working -> cant work


A Python-first, package-style starter to *ingest daily US news, deduplicate them, and analyze trends*.  
Now also includes **quick view & daily report generation**.

---

## Quickstart  

```bash
# Create virtual environment & install dependencies
python -m venv .venv && source .venv/bin/activate
pip install -e .
pip install pydantic
pip install timedelta

# Copy env template & set NEWSAPI_KEY (optional)
cp .env.example .env
# edit .env and add: NEWSAPI_KEY=your_api_key
```

## Ingest today's news (RSS + NewsAPI if key present)
`newscli ingest --country us --rss --newsapi` 

## Deduplicate today's file into silver dataset
`newscli dedup --date today`

### Example: look at yesterday's ingested raw newsapi
`python src/news_trend/quickview.py --date YYYY-MM-DD --indir data --kind raw_newsapi --top 30 --min-len 3`

This will show:

- total articles
- top publishers
- top words & bigrams
- sample articles

### Example: generate report from deduplicated (silver) data
`python src/news_trend/report.py --date YYYY-MM-DD --indir data --kind silver_newsapi --outdir reports`

### Cron Example (everyday automation)
`5 8 * * * cd /path/to/news-trend-python-starter && . .venv/bin/activate && newscli ingest --country us --rss --newsapi && newscli dedup --date today && python src/news_trend/report.py --date $(date +\%F) --indir data --kind silver_newsapi --outdir reports >> logs.txt 2>&1`

## Structure
- src/news_trend/: Python package (ingest, dedup, quickview, report, utils)
- data/raw/YYYY-MM-DD.jsonl: raw ingested articles
- data/silver/YYYY-MM-DD.jsonl: cleaned & deduplicated articles
- reports/YYYY-MM-DD.md: generated daily reports

## 1. Ingest
`newscli ingest --country us --rss --newsapi`

## 2. Deduplicate
`newscli dedup --date today`

## 3. Quickview on raw data
`python src/news_trend/quickview.py --date today --indir data --kind raw_newsapi`

## 4. Generate report from deduplicated (silver) data
`python src/news_trend/report.py --date today --indir data --kind silver_newsapi --outdir reports`


## 08/19 update
- Time-sliced NewsAPI ingest
- Daily HTML report


## Commands (daily pipeline)

```bash
# 1) Ingest (yesterday, time-sliced inside newscli / or your ingest script)
newscli ingest --newsapi --date yesterday

# 2) Report 
python src/news_trend/report.py --date yesterday --indir data --kind raw --outdir reports --top 30
# python src/news_trend/report.py --date yesterday --indir data --kind silver_newsapi --outdir reports --top 30
```

## 08/22 update 
### Continuous live collection using GitHub Actions (NEWSAPI)
This repo includes a 30-minute interval workflow that collects news data and commits them to the repo as newline-delimited JSON.<br>
Files are written under data/live_newsapi/ with names like YYYY-MM-DDTHH-MMZ.jsonl.

### Verify
'Actions' - 'collect-live' - click recent workflow <br>
The results are as follows <br>
[LIVE] NewsAPI -> data/live_newsapi/2025-08-22T21-39Z.jsonl (n rows)

## 08/30 update
## Word Trends (cumulative)
Generate “top words” and 14-day trends from the deduplicated warehouse:

```
python scripts/viz_words.py \
  --master data/warehouse/master.jsonl \
  --outdir reports/words \
  --top 30 \
  --days 14 \
  --min-len 3 \
  --drop-content \
  --extra-stop "chars,nbsp,amp,apos,mdash,ndash,inc,com,report,reports,shares"
```

## What the command does

- **Input**: data/warehouse/master.jsonl (all deduped articles).
- **Window**: keeps only the most recent --days (default: 14).
- **Text selection**: with --drop-content, only title + description are used (article body is ignored).
- This reduces boilerplate/noise that appears in bodies and surfaces headline topics.
- **Normalization**: lowercasing, basic cleaning, tokenization.
- **Filtering**:
- --min-len: drop tokens shorter than N characters (e.g., 3).
- Stopwords = built-in list plus --extra-stop (comma-separated, case-insensitive).
- **Outputs** (written to --outdir, e.g., reports/words/):
- top_words.png – bar chart of the overall top N words.
- top_words_trend.png – line chart of daily counts for those words over the last N days.
- top_words.csv – total counts.
- top_words_trend.csv – daily counts per word.