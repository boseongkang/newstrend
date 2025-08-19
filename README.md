# news trend  

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

