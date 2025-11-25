#!/usr/bin/env bash
set -euo pipefail

cd /Users/mymac/Desktop/newstrend
. .venv/bin/activate

mkdir -p site/data

python scripts/make_trends_json.py \
  --silver-dir data/silver \
  --out site/data/trends.json \
  --last-days 90 \
  --topk 200 \
  --min-len 4

python scripts/make_articles_json.py \
  --silver-dir data/silver \
  --out site/data/articles.json \
  --last-days 90