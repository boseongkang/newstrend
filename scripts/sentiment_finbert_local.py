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
import re
import subprocess
import sys
import time
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


def commit_and_push(cache_dir: Path) -> None:
    """Stage data/sentiment_per_day, commit, push origin data-cache."""
    print(f"[commit] staging changes in {cache_dir}")
    # Make sure we're on a branch (worktree may be detached).
    subprocess.run(["git", "checkout", "-B", "data-cache"], cwd=cache_dir, check=False)
    # data-cache branch removes .gitignore; stage the sentiment dir explicitly.
    subprocess.run(["git", "rm", "-f", "--cached", ".gitignore"],
                   cwd=cache_dir, check=False, capture_output=True)
    (cache_dir / ".gitignore").unlink(missing_ok=True)
    subprocess.run(["git", "add", "data/sentiment_per_day/"], cwd=cache_dir, check=True)
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
    target = news[-args.window_days:]                         # most recent N days
    cached = cached_dates(cache_dir)
    missing = [(d, p) for d, p in target if d not in cached]
    missing.sort(key=lambda x: x[0], reverse=True)            # newest first

    print(f"[plan] window={args.window_days}d  news_archive={len(news)}  "
          f"target={len(target)}  cached={len(cached & {d for d,_ in target})}  "
          f"missing={len(missing)}")
    if not missing:
        print("[plan] all target days already cached — nothing to do")
        if args.commit:
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

    if args.commit and n_ok > 0:
        commit_and_push(cache_dir)


if __name__ == "__main__":
    main()
