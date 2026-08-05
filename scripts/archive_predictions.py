"""
Archive today's predictions.json into predictions_history/ — immediately
after predict.py, not as a weekly_report.py side effect.

Why a dedicated step (STEP 4, 2026-08-05):
- weekly_report.py used to do this archiving as a side effect, several soft-
  fail (`|| true`) steps after predict.py. If predict.py crashed silently,
  weekly_report archived the previous run's stale predictions.json under
  TODAY's date, corrupting the history record that daily_verify / walk-forward
  evaluate against.
- daily_verify.py runs before weekly_report in CI, so today's snapshot was
  invisible to it.

Stale-input rejection: refuses (exit 1) when predictions.json's `updated`
timestamp is older than --max-age-hours. The archive filename comes from the
`updated` timestamp's UTC date, never from wall-clock "today".
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

MAX_AGE_HOURS = 6


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred", default="site/data/predictions.json")
    ap.add_argument("--out-dir", default="site/data/predictions_history")
    ap.add_argument("--max-age-hours", type=float, default=MAX_AGE_HOURS,
                    help="refuse to archive when `updated` is older than this")
    args = ap.parse_args()

    p = Path(args.pred)
    if not p.exists():
        print(f"REFUSED: {p} does not exist (predict.py did not run?)", file=sys.stderr)
        return 1
    try:
        data = json.loads(p.read_text())
    except Exception as e:
        print(f"REFUSED: {p} unreadable ({e})", file=sys.stderr)
        return 1

    ts = data.get("updated", "")
    try:
        updated = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        print(f"REFUSED: {p} has no parseable `updated` timestamp ({ts!r})", file=sys.stderr)
        return 1

    now = datetime.now(timezone.utc)
    age_h = (now - updated).total_seconds() / 3600
    if age_h > args.max_age_hours:
        print(f"REFUSED: {p} is stale — updated={ts} ({age_h:.1f}h old > "
              f"{args.max_age_hours}h). Not archiving a stale snapshot.", file=sys.stderr)
        return 1

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    day = updated.strftime("%Y-%m-%d")
    out_path = out_dir / f"{day}.json"
    out_path.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")))
    print(f"Archived {p} (updated={ts}, {age_h:.1f}h old) → {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
