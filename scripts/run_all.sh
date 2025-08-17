#!/usr/bin/env bash
set -euo pipefail
DATE=${1:-today}
newscli ingest --country us --rss --newsapi --date "$DATE"
newscli dedup --date "$DATE"
