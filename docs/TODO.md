# TODO

## hidden_gems: rank boundary noise (2026-05-11)

Observed during 5/9 → 5/11 snapshot comparison: the 4th-ranked pick swapped
(FTNT ↔ VRTX) while both held identical `hidden_gems_score = 0.511`. Score gap
between rank 4 and rank 5 in either snapshot is ~0.001–0.005, well within
day-to-day input jitter (FinBERT daily, predictions.json hourly).

Consider exposing **Top 8 or Top 10** instead of Top 4 so the displayed cohort
is robust to sub-0.01 score wobble. Keeping Top 4 invites users to interpret
boundary swaps as new signals when they are noise.

Not urgent — fix when next touching `scripts/find_hidden_gems.py` ranking or
the site page (`site/hidden_gems.html` filter defaults).
