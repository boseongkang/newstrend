"""
build_signal_corr.py  v3
trends.json + prices.json → signal_corr.json

Upgrades over v2:
  - Train / test split (70/30) — reports both train corr and test hit_rate
  - Composite confidence score: corr × hit_rate × sqrt(n/10) × consistency × source_proxy
  - Rolling 21-day correlation — detects whether signal is stable or decaying
  - Co-occurrence clusters — finds word groups that spike together
  - p-value approximation filter (n ≥ 5 required)
  - Source proxy weighting from trends.json
"""

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path


# ── Statistical helpers ───────────────────────────────────────────────────────

def pearson(xs, ys):
    n = len(xs)
    if n < 5:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((a - mx) * (b - my) for a, b in zip(xs, ys))
    dx  = math.sqrt(sum((a - mx) ** 2 for a in xs))
    dy  = math.sqrt(sum((b - my) ** 2 for b in ys))
    if dx < 1e-9 or dy < 1e-9:
        return None
    return round(num / (dx * dy), 4)


def p_value_approx(r, n):
    """Two-tailed p-value approximation via t-distribution (df = n-2)."""
    if r is None or n < 4 or abs(r) >= 1.0:
        return 1.0
    t = r * math.sqrt(n - 2) / math.sqrt(1 - r ** 2 + 1e-12)
    # Rough approximation using logistic transform calibrated on t-dist
    x = abs(t) / math.sqrt(n - 2)
    p = 2 * (1 / (1 + math.exp(6 * (x - 0.5))))
    return round(min(1.0, max(0.0, p)), 4)


def zscore_series(counts, window=28):
    result = []
    for i, c in enumerate(counts):
        if i < 3:
            result.append(0.0)
            continue
        hist = counts[max(0, i - window): i]
        mean = sum(hist) / len(hist)
        std  = math.sqrt(sum((x - mean) ** 2 for x in hist) / len(hist))
        result.append(round((c - mean) / std, 3) if std >= 0.5 else 0.0)
    return result


def rolling_corr(xs, ys, window=21):
    """Compute rolling Pearson correlations. Returns list of (corr | None)."""
    result = []
    for i in range(len(xs)):
        if i < window - 1:
            result.append(None)
            continue
        wx = xs[i - window + 1: i + 1]
        wy = ys[i - window + 1: i + 1]
        result.append(pearson(wx, wy))
    return result


def corr_trend(rolling):
    """Is the rolling correlation strengthening (+1), stable (0), or decaying (-1)?"""
    valid = [r for r in rolling if r is not None]
    if len(valid) < 4:
        return 0
    recent = valid[-3:]
    older  = valid[-6:-3] if len(valid) >= 6 else valid[:3]
    r_now  = sum(abs(x) for x in recent) / len(recent)
    r_old  = sum(abs(x) for x in older)  / len(older)
    if r_now > r_old + 0.08:
        return 1
    if r_now < r_old - 0.08:
        return -1
    return 0


def confidence_score(corr, hit_rate, n_events, consistency, source_proxy):
    """Composite 0-1 confidence: penalizes low n, low consistency, single-source spikes."""
    if corr is None:
        return 0.0
    # n penalty: approaches 1 as n→∞, 0.5 at n=10
    n_factor = math.sqrt(n_events / 10.0) if n_events > 0 else 0
    n_factor = min(n_factor, 1.5) / 1.5

    # hit_rate > 0.5 is meaningful, < 0.5 is noise
    hit_factor = max(0, (hit_rate - 0.45) / 0.55)

    c = abs(corr) * 0.35 + hit_factor * 0.30 + n_factor * 0.20 + \
        consistency * 0.10 + source_proxy * 0.05
    return round(min(1.0, c), 3)


# ── Main build ────────────────────────────────────────────────────────────────

def build_corr(trends_path, prices_path,
               top_terms, min_corr, min_events, lag_range, min_conf):

    T = json.loads(Path(trends_path).read_text())
    P = json.loads(Path(prices_path).read_text())

    t_dates  = T["dates"]
    t_series = T["series"]
    t_cons   = T.get("consistency", {})
    t_proxy  = T.get("source_proxy", {})

    p_tickers = P["tickers"]

    # Top terms by total frequency
    totals = {t: sum(v) for t, v in t_series.items()}
    top = sorted(totals, key=totals.get, reverse=True)[:top_terms]

    t_date_idx = {d: i for i, d in enumerate(t_dates)}

    # ── Train / test split ────────────────────────────────────────────────────
    n = len(t_dates)
    split = int(n * 0.70)
    train_dates = set(t_dates[:split])
    test_dates  = set(t_dates[split:])
    print(f"  Train: {len(train_dates)} days  |  Test: {len(test_dates)} days  (70/30 split)")

    pairs = []
    term_best = {}

    for ticker, pdata in p_tickers.items():
        p_dates   = pdata["dates"]
        p_returns = pdata["returns"]
        p_ret_idx = {d: i for i, d in enumerate(p_dates)}

        common_all   = sorted(set(t_dates) & set(p_dates))
        common_train = [d for d in common_all if d in train_dates]
        common_test  = [d for d in common_all if d in test_dates]

        if len(common_all) < 10:
            continue

        for term in top:
            counts = t_series[term]
            zs     = zscore_series(counts)
            cons   = t_cons.get(term, 0.5)
            proxy  = t_proxy.get(term, 0.5)

            # ── Lag correlation on TRAIN set ──────────────────────────────────
            best_lag, best_corr = 0, 0.0
            lag_corrs = {}

            for lag in range(-lag_range, lag_range + 1):
                xs, ys = [], []
                for d in common_train:
                    ti = t_date_idx[d]
                    pi = p_ret_idx.get(d)
                    if pi is None:
                        continue
                    pi_target = pi - lag
                    if pi_target < 1 or pi_target >= len(p_returns):
                        continue
                    ret = p_returns[pi_target]
                    if ret is None:
                        continue
                    xs.append(zs[ti])
                    ys.append(ret)

                c = pearson(xs, ys)
                if c is not None:
                    lag_corrs[str(lag)] = c
                    if abs(c) > abs(best_corr):
                        best_corr = c
                        best_lag  = lag

            if abs(best_corr) < min_corr:
                continue

            # ── p-value filter ────────────────────────────────────────────────
            n_train_pts = sum(
                1 for d in common_train
                if p_ret_idx.get(d) is not None and
                   0 < p_ret_idx[d] - best_lag < len(p_returns) and
                   p_returns[p_ret_idx[d] - best_lag] is not None
            )
            pval = p_value_approx(best_corr, n_train_pts)
            if pval > 0.20:  # relaxed during data accumulation phase
                continue

            # ── TEST set: forward hit-rate validation ─────────────────────────
            test_rets_1d = []
            for d in common_test:
                ti = t_date_idx[d]
                if zs[ti] < 2.0:
                    continue
                pi = p_ret_idx.get(d)
                if pi is None:
                    continue
                pi_lag = pi - best_lag
                if 1 <= pi_lag < len(p_returns) and p_returns[pi_lag] is not None:
                    test_rets_1d.append(p_returns[pi_lag])

            # ── ALL-data event analysis ───────────────────────────────────────
            events_1d, events_5d = [], []
            for i, d in enumerate(t_dates):
                if zs[i] < 2.0:
                    continue
                pi = p_ret_idx.get(d)
                if pi is None:
                    continue
                if pi + 1 < len(p_returns) and p_returns[pi + 1] is not None:
                    events_1d.append(p_returns[pi + 1])
                w5 = [p_returns[pi + k] for k in range(1, 6)
                      if pi + k < len(p_returns) and p_returns[pi + k] is not None]
                if w5:
                    events_5d.append(sum(w5) / len(w5))

            n_events = len(events_1d)
            if n_events < min_events:
                continue

            hit_rate   = round(sum(1 for r in events_1d if r > 0) / n_events, 3)
            avg_ret_1d = round(sum(events_1d) / n_events * 100, 3)
            avg_ret_5d = round(sum(events_5d) / len(events_5d) * 100, 3) if events_5d else None

            # ── Test validation ───────────────────────────────────────────────
            test_hit = round(sum(1 for r in test_rets_1d if r > 0) / len(test_rets_1d), 3) \
                       if test_rets_1d else None
            test_n   = len(test_rets_1d)

            # ── Rolling correlation ───────────────────────────────────────────
            xs_all, ys_all = [], []
            for d in common_all:
                ti = t_date_idx[d]
                pi = p_ret_idx.get(d)
                if pi is None:
                    continue
                pi_t = pi - best_lag
                if 1 <= pi_t < len(p_returns) and p_returns[pi_t] is not None:
                    xs_all.append(zs[ti])
                    ys_all.append(p_returns[pi_t])

            rolling = rolling_corr(xs_all, ys_all, window=min(21, len(xs_all) // 2))
            c_trend = corr_trend(rolling)

            # ── Composite confidence ──────────────────────────────────────────
            conf = confidence_score(best_corr, hit_rate, n_events, cons, proxy)
            if conf < min_conf:
                continue

            # ── Signal type ───────────────────────────────────────────────────
            if best_lag <= -2:  stype = "leading"
            elif best_lag == -1: stype = "leading_1d"
            elif best_lag == 0:  stype = "coincident"
            elif best_lag >= 2:  stype = "lagging"
            else:                stype = "lagging_1d"

            pair = {
                "term":          term,
                "ticker":        ticker,
                "best_lag":      best_lag,
                "corr":          best_corr,          # train-set correlation
                "pval":          pval,
                "hit_rate":      hit_rate,            # all-data hit rate
                "test_hit":      test_hit,            # held-out test hit rate
                "test_n":        test_n,
                "n_events":      n_events,
                "avg_ret_1d":    avg_ret_1d,
                "avg_ret_5d":    avg_ret_5d,
                "confidence":    conf,               # composite 0-1 score
                "corr_trend":    c_trend,            # +1 strengthening, 0 stable, -1 decaying
                "consistency":   cons,
                "source_proxy":  proxy,
                "signal_type":   stype,
                "lag_corrs":     lag_corrs,
            }
            pairs.append(pair)

            if term not in term_best or conf > term_best[term]["confidence"]:
                term_best[term] = {
                    "best_ticker": ticker, "best_corr": best_corr,
                    "best_lag": best_lag,  "confidence": conf,
                    "signal_type": stype,
                }

    # Sort by composite confidence descending
    pairs.sort(key=lambda x: x["confidence"], reverse=True)

    return {
        "updated":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "n_dates":    len(t_dates),
        "n_train":    len(train_dates),
        "n_test":     len(test_dates),
        "n_pairs":    len(pairs),
        "pairs":      pairs,
        "term_stats": term_best,
    }


# ── Co-occurrence cluster detection ─────────────────────────────────────────

def find_cooccurrence_clusters(T, min_corr=0.70, top_n=20):
    """Find groups of words that spike together (co-occurrence correlation ≥ min_corr)."""
    t_dates  = T["dates"]
    t_series = T["series"]
    totals   = {t: sum(v) for t, v in t_series.items()}
    top = sorted(totals, key=totals.get, reverse=True)[:top_n]

    clusters = []
    seen = set()

    for i, t1 in enumerate(top):
        for t2 in top[i+1:]:
            z1 = zscore_series(t_series[t1])
            z2 = zscore_series(t_series[t2])
            c  = pearson(z1, z2)
            if c is not None and c >= min_corr:
                key = tuple(sorted([t1, t2]))
                if key not in seen:
                    seen.add(key)
                    clusters.append({"terms": list(key), "co_corr": c})

    clusters.sort(key=lambda x: x["co_corr"], reverse=True)
    return clusters[:30]


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trends",     default="site/data/trends.json")
    ap.add_argument("--prices",     default="site/data/prices.json")
    ap.add_argument("--out",        default="site/data/signal_corr.json")
    ap.add_argument("--top-terms",  type=int,   default=200)
    ap.add_argument("--min-corr",   type=float, default=0.25)
    ap.add_argument("--min-events", type=int,   default=3)
    ap.add_argument("--min-conf",   type=float, default=0.10)
    ap.add_argument("--lag-range",  type=int,   default=5)
    args = ap.parse_args()

    print(f"Loading trends : {args.trends}")
    print(f"Loading prices : {args.prices}")

    T = json.loads(Path(args.trends).read_text())
    result = build_corr(
        args.trends, args.prices,
        args.top_terms, args.min_corr, args.min_events,
        args.lag_range, args.min_conf,
    )

    # Co-occurrence clusters
    clusters = find_cooccurrence_clusters(T, min_corr=0.70)
    result["cooccurrence"] = clusters

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")

    print(f"\n→ {args.out}")
    print(f"  pairs      : {result['n_pairs']}")
    print(f"  n_dates    : {result['n_dates']}  (train={result['n_train']}, test={result['n_test']})")
    print(f"  clusters   : {len(clusters)}")

    print(f"\n  Top 15 by confidence score:")
    for p in result["pairs"][:15]:
        trend_sym = "↑" if p["corr_trend"] == 1 else ("↓" if p["corr_trend"] == -1 else "→")
        test_str  = f"test_hit={p['test_hit']:.0%}" if p["test_hit"] is not None else "no test data"
        print(f"    {p['term']:<18} {p['ticker']:<6} "
              f"lag={p['best_lag']:+d}  corr={p['corr']:+.3f}  "
              f"conf={p['confidence']:.3f}  {test_str}  "
              f"1d={p['avg_ret_1d']:+.2f}%  {trend_sym}  [{p['signal_type']}]")

    print(f"\n  Leading signals (lag ≤ -1):")
    for p in [x for x in result["pairs"] if x["best_lag"] <= -1][:8]:
        print(f"    {p['term']:<18} {p['ticker']:<6} "
              f"lag={p['best_lag']:+d}  conf={p['confidence']:.3f}  "
              f"hit={p['hit_rate']:.0%}  1d={p['avg_ret_1d']:+.2f}%")

    if clusters:
        print(f"\n  Top co-occurrence clusters:")
        for c in clusters[:5]:
            print(f"    {' + '.join(c['terms']):<30}  co_corr={c['co_corr']:+.3f}")


if __name__ == "__main__":
    main()