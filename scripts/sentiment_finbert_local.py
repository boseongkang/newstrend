"""
sentiment_finbert_local.py — local M3-MPS-accelerated FinBERT backfill.

Hybrid pattern: GitHub Actions collects raw news to data-cache; FinBERT scoring
runs locally on M3 MPS (~10x faster than CI CPU); CI just restores + aggregates.

Workflow:
    # one-time worktree setup (or pass --setup)
    git fetch origin data-cache
    git worktree add /tmp/newstrend-cache origin/data-cache

    # backfill last 30 days, then push to data-cache
    python scripts/sentiment_finbert_local.py --window-days 30 --commit

Daily use after backfill:
    # fills any missing recent days (typically just today), pushes
    python scripts/sentiment_finbert_local.py --commit
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCORE_SCRIPT = ROOT / "scripts" / "sentiment_finbert.py"
DEFAULT_CACHE_DIR = Path("/tmp/newstrend-cache")
DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})")
# Prefer repo-local .venv (has torch/transformers installed); fall back to current python.
VENV_PYTHON = ROOT / ".venv" / "bin" / "python"
PYTHON_BIN = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable


def setup_worktree(cache_dir: Path) -> None:
    """Create a git worktree of origin/data-cache at cache_dir if missing."""
    if cache_dir.exists() and (cache_dir / ".git").exists():
        # Already a worktree — just pull latest
        print(f"[setup] worktree exists at {cache_dir}, fetching latest...")
        subprocess.run(["git", "fetch", "origin", "data-cache"], cwd=ROOT, check=True)
        subprocess.run(["git", "pull", "--ff-only", "origin", "data-cache"],
                       cwd=cache_dir, check=False)
        return
    if cache_dir.exists():
        print(f"[setup] ERROR: {cache_dir} exists but is not a git worktree", file=sys.stderr)
        sys.exit(1)
    print(f"[setup] fetching origin/data-cache and creating worktree at {cache_dir}")
    subprocess.run(["git", "fetch", "origin", "data-cache"], cwd=ROOT, check=True)
    subprocess.run(
        ["git", "worktree", "add", str(cache_dir), "origin/data-cache"],
        cwd=ROOT, check=True,
    )
    # Detached HEAD is fine for read-only; for --commit we need a branch
    subprocess.run(["git", "checkout", "-B", "data-cache"], cwd=cache_dir, check=False)


def list_news_days(cache_dir: Path) -> list[tuple[str, Path]]:
    """Return [(date, path)] for raw daily news files, ascending by date.

    On the data-cache branch, raw news lives at data/news_archive/{date}.jsonl
    (preserved by trend-site.yml's "Preserve raw news to data-cache" step).
    """
    news_dir = cache_dir / "data" / "news_archive"
    if not news_dir.is_dir():
        print(f"[error] {news_dir} not found — is the worktree on data-cache?",
              file=sys.stderr)
        sys.exit(1)
    out: list[tuple[str, Path]] = []
    for p in sorted(news_dir.iterdir()):
        if not p.is_file() or p.suffix != ".jsonl":
            continue
        if "_tokens" in p.name:
            continue
        m = DATE_RE.match(p.name)
        if not m:
            continue
        out.append((m.group(1), p))
    return out


def ensure_today_in_news(cache_dir: Path, news: list[tuple[str, Path]],
                         max_attempts: int = 3, sleep_s: int = 30) -> list[tuple[str, Path]]:
    """If the latest news file isn't today (UTC), retry fetch.

    trend-site.yml's "Preserve raw news to data-cache" step pushes today's
    raw JSONL asynchronously from CI; launchd fires us at a fixed hour and
    can race ahead of that push, leaving today out of `news_archive/`. The
    script would then declare missing=0 and move on, leaving sentiment one
    day behind until tomorrow's cycle. Retry the fetch a few times to give
    trend-site a chance to land today's file before we plan the window.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    latest = news[-1][0] if news else None
    if latest == today:
        return news
    for attempt in range(1, max_attempts + 1):
        print(f"[fetch-retry] latest news = {latest}, expected {today} — "
              f"trend-site may not have pushed yet (attempt {attempt}/{max_attempts})")
        time.sleep(sleep_s)
        subprocess.run(["git", "fetch", "origin", "data-cache"],
                       cwd=ROOT, check=False, capture_output=True)
        subprocess.run(["git", "pull", "--ff-only", "origin", "data-cache"],
                       cwd=cache_dir, check=False, capture_output=True)
        news = list_news_days(cache_dir)
        latest = news[-1][0] if news else None
        if latest == today:
            print(f"[fetch-retry] picked up {today} after attempt {attempt}")
            return news
    print(f"[fetch-retry] warning: today's news ({today}) still missing after "
          f"{max_attempts} attempts — deferring to next cycle", file=sys.stderr)
    return news


def cached_dates(cache_dir: Path) -> set[str]:
    sent_dir = cache_dir / "data" / "sentiment_per_day"
    if not sent_dir.is_dir():
        return set()
    out: set[str] = set()
    for p in sent_dir.glob("sentiment_*.json"):
        m = re.match(r"sentiment_(\d{4}-\d{2}-\d{2})\.json$", p.name)
        if m and p.stat().st_size > 0:
            out.add(m.group(1))
    return out


def score_one_day(input_jsonl: Path, output_json: Path, batch_size: int) -> tuple[bool, float]:
    """Subprocess sentiment_finbert.py for one day. Returns (success, elapsed_s)."""
    output_json.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    proc = subprocess.run(
        [PYTHON_BIN, str(SCORE_SCRIPT),
         "--input", str(input_jsonl),
         "--output", str(output_json),
         "--batch-size", str(batch_size)],
        cwd=ROOT,
        capture_output=True, text=True,
    )
    elapsed = time.time() - t0
    if proc.returncode != 0:
        print(f"  [error] sentiment_finbert.py exited {proc.returncode}", file=sys.stderr)
        if proc.stderr:
            print(f"  stderr: {proc.stderr[-400:]}", file=sys.stderr)
        return False, elapsed
    return True, elapsed


def write_local_runs(cache_dir: Path, *, last_exit: int, days_scored: int,
                     days_failed: int, days_skipped: int,
                     elapsed_seconds: float, window_days: int) -> None:
    """Write data/local_runs.json — the launchd-side metadata the system_health
    page reads to detect stale/failed runs. Lives on data-cache alongside
    sentiment_per_day so it ships with the same push."""
    out = cache_dir / "data" / "local_runs.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_run_at":     datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "last_exit":       last_exit,
        "days_scored":     days_scored,
        "days_failed":     days_failed,
        "days_skipped":    days_skipped,
        "elapsed_seconds": round(elapsed_seconds, 1),
        "window_days":     window_days,
    }
    out.write_text(json.dumps(payload, indent=2))
    print(f"[local_runs] {out.relative_to(cache_dir)}: scored={days_scored} skipped={days_skipped} exit={last_exit}")


def commit_and_push(cache_dir: Path) -> None:
    """Stage data/sentiment_per_day + data/local_runs.json, commit, push origin data-cache."""
    print(f"[commit] staging changes in {cache_dir}")
    # Make sure we're on a branch (worktree may be detached).
    subprocess.run(["git", "checkout", "-B", "data-cache"], cwd=cache_dir, check=False)
    # data-cache branch removes .gitignore; stage tracked files explicitly.
    subprocess.run(["git", "rm", "-f", "--cached", ".gitignore"],
                   cwd=cache_dir, check=False, capture_output=True)
    (cache_dir / ".gitignore").unlink(missing_ok=True)
    subprocess.run(["git", "add", "data/sentiment_per_day/", "data/local_runs.json"],
                   cwd=cache_dir, check=True)
    diff = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=cache_dir)
    if diff.returncode == 0:
        print("[commit] no changes to push")
        return
    today = time.strftime("%Y-%m-%d")
    subprocess.run(
        ["git", "commit", "-m", f"sentiment_per_day local backfill: {today}"],
        cwd=cache_dir, check=True,
    )
    # Retry-on-rebase: trend-site CI pushes prices to data-cache during our
    # ~20min scoring window, which would otherwise reject our push. Sentiment
    # and prices touch disjoint paths so rebase is conflict-free.
    for attempt in range(1, 4):
        push = subprocess.run(["git", "push", "origin", "data-cache"], cwd=cache_dir)
        if push.returncode == 0:
            print("[commit] pushed to origin/data-cache")
            return
        if attempt == 3:
            print("[commit] push still failing after 2 rebase attempts; giving up", file=sys.stderr)
            sys.exit(1)
        print(f"[commit] push rejected (attempt {attempt}); fetching + rebasing on origin/data-cache")
        subprocess.run(["git", "fetch", "origin", "data-cache"], cwd=cache_dir, check=True)
        subprocess.run(["git", "rebase", "origin/data-cache"], cwd=cache_dir, check=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR,
                    help="git worktree of data-cache branch (default: /tmp/newstrend-cache)")
    ap.add_argument("--window-days", type=int, default=30,
                    help="how many most-recent days to keep filled (default 30)")
    ap.add_argument("--batch-size", type=int, default=32,
                    help="FinBERT batch size (default 32; 16 if memory-constrained)")
    ap.add_argument("--setup", action="store_true",
                    help="create / fast-forward the worktree before scoring")
    ap.add_argument("--commit", action="store_true",
                    help="git add/commit/push data/sentiment_per_day to origin/data-cache")
    args = ap.parse_args()

    cache_dir: Path = args.cache_dir.resolve()
    if args.setup or not cache_dir.exists():
        setup_worktree(cache_dir)

    news = list_news_days(cache_dir)
    if not news:
        print("[error] no news_archive daily files found", file=sys.stderr)
        sys.exit(1)
    news = ensure_today_in_news(cache_dir, news)
    target = news[-args.window_days:]                         # most recent N days
    cached = cached_dates(cache_dir)
    missing = [(d, p) for d, p in target if d not in cached]
    missing.sort(key=lambda x: x[0], reverse=True)            # newest first

    print(f"[plan] window={args.window_days}d  news_archive={len(news)}  "
          f"target={len(target)}  cached={len(cached & {d for d,_ in target})}  "
          f"missing={len(missing)}")
    days_skipped = len(target) - len(missing)
    if not missing:
        print("[plan] all target days already cached — nothing to do")
        if args.commit:
            write_local_runs(cache_dir, last_exit=0, days_scored=0, days_failed=0,
                             days_skipped=days_skipped, elapsed_seconds=0.0,
                             window_days=args.window_days)
            commit_and_push(cache_dir)
        return

    sent_dir = cache_dir / "data" / "sentiment_per_day"
    total_t0 = time.time()
    n_ok = 0
    n_fail = 0
    for i, (date, in_path) in enumerate(missing, start=1):
        out_path = sent_dir / f"sentiment_{date}.json"
        print(f"[{i:>2}/{len(missing)}] scoring {date} ...", end=" ", flush=True)
        ok, dt = score_one_day(in_path, out_path, args.batch_size)
        if ok:
            n_ok += 1
            print(f"{dt:5.1f}s  →  {out_path.relative_to(cache_dir)}")
        else:
            n_fail += 1
            print(f"FAILED ({dt:.1f}s)")

    total = time.time() - total_t0
    avg = total / max(1, n_ok)
    print(f"\n[done] scored {n_ok}/{len(missing)}  failed={n_fail}  "
          f"total={total/60:.1f}min  avg={avg:.1f}s/day")

    if args.commit:
        write_local_runs(cache_dir, last_exit=(0 if n_fail == 0 else 1),
                         days_scored=n_ok, days_failed=n_fail,
                         days_skipped=days_skipped, elapsed_seconds=total,
                         window_days=args.window_days)
        # Always commit — even if 0 new sentiment files, local_runs.json
        # timestamp moved and the system_health page reads it.
        commit_and_push(cache_dir)
        if n_fail > 0:
            sys.exit(1)


if __name__ == "__main__":
    main()
