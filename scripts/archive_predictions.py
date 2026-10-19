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
    ap.add_argument("--manifest", default="",
                    help="input manifest written just before predict.py "
                         "(site/data/input_manifest.json); archived next to the "
                         "snapshot as <day>.manifest.json so every snapshot "
                         "carries the exact state of its inputs")
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

    # Input manifest: keep the record of what predict.py actually read
    # (sentiment date / non-null count, fundamentals & insider generated_at,
    # prices/TA bar dates, ticker_weights sha256) next to the snapshot. A
    # missing or unreadable manifest is a warning, not a refusal — the
    # snapshot itself is still fresh and valid.
    if args.manifest:
        mp = Path(args.manifest)
        if not mp.exists():
            print(f"::warning::input manifest {mp} missing — snapshot archived without it")
        else:
            try:
                man = json.loads(mp.read_text())
                man["snapshot_updated"] = ts
                man["snapshot_file"] = out_path.name
                man_path = out_dir / f"{day}.manifest.json"
                man_path.write_text(json.dumps(man, ensure_ascii=False, indent=1, sort_keys=True))
                print(f"Archived input manifest → {man_path}")
            except Exception as e:
                print(f"::warning::input manifest {mp} unreadable ({e}) — snapshot archived without it")
    return 0


if __name__ == "__main__":
    sys.exit(main())
