# news trend 

A Python-first, package-style starter to *ingest daily US news and deduplicate them for trend analysis.

## Quickstart

```bash 
python -m venv .venv && source .venv/bin/activate
pip install -e .
pip install pydantic

cp .env.example .env  # (optional) add NEWSAPI_KEY=...

# Ingest today's news (RSS + NewsAPI if key present)
newscli ingest --country us --rss --newsapi

# Deduplicate today's file
newscli dedup --date today

# Check outputs
ls -lh data/raw
ls -lh data/silver
```

## Cron example for everyday
```
5 8 * * * cd /path/to/news-trend-python-starter && . .venv/bin/activate && newscli ingest --country us --rss --newsapi && newscli dedup --date today >> logs.txt 2>&1
```

## Structure
- `src/news_trend/`: Python package (ingest, dedup, utils)
- `data/raw/YYYY-MM-DD.jsonl`: raw articles
- `data/silver/YYYY-MM-DD.jsonl`: cleaned & deduped
