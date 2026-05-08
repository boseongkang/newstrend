from __future__ import annotations
import argparse, subprocess, shlex, sys, json
from pathlib import Path
import datetime as dt

try:
    import yaml
    import pandas as pd
except Exception:
    print("Please `pip install pyyaml pandas`", file=sys.stderr); sys.exit(1)

def run_cmd(cmd: str) -> int:
    print(f"\n$ {cmd}")
    proc = subprocess.Popen(shlex.split(cmd))
    return proc.wait()

def ensure_outdirs_in_cmd(cmd: str):
    parts = shlex.split(cmd)
    for i, tok in enumerate(parts):
        if tok == "--outdir" and i + 1 < len(parts):
            Path(parts[i + 1]).mkdir(parents=True, exist_ok=True)

def must_exist(path: str) -> bool:
    return bool(path) and Path(path).exists()

def infer_date_range(master_path: Path, lookback_days: int) -> tuple[str, str]:
    if not master_path.exists():
        end = dt.date.today()
        start = end - dt.timedelta(days=max(lookback_days - 1, 0))
        return (start.isoformat(), end.isoformat())
    df = pd.read_json(master_path, lines=True)
    col = None
    for c in ("date", "publishedAt", "publish_date"):
        if c in df.columns:
            col = c
            break
    if col is None or df.empty:
        end = dt.date.today()
        start = end - dt.timedelta(days=max(lookback_days - 1, 0))
        return (start.isoformat(), end.isoformat())
    s = pd.to_datetime(df[col], errors="coerce").dt.date.dropna()
    if s.empty:
        end = dt.date.today()
        start = end - dt.timedelta(days=max(lookback_days - 1, 0))
        return (start.isoformat(), end.isoformat())
    max_d = s.max()
    min_d = s.min()
    start = max_d - dt.timedelta(days=max(lookback_days - 1, 0))
    if start < min_d:
        start = min_d
    return (start.isoformat(), max_d.isoformat())

def build_raw_inputs(raw_dir: Path, start: str|None, end: str|None) -> str:
    files = []
    if raw_dir.exists():
        if start and end:
            try:
                sd = dt.date.fromisoformat(start)
                ed = dt.date.fromisoformat(end)
            except Exception:
                sd = ed = None
            if sd and ed:
                cur = sd
                while cur <= ed:
                    p = raw_dir / f"{cur.isoformat()}.jsonl"
                    if p.exists():
                        files.append(str(p))
                    cur += dt.timedelta(days=1)
        if not files:
            for p in sorted(raw_dir.glob("*.jsonl")):
                files.append(str(p))
    return " ".join(files)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))

    outroot = Path(cfg.get("outdir", "reports/auto_trends_existing")); outroot.mkdir(parents=True, exist_ok=True)
    lookback_days = int(cfg.get("lookback_days", 30))

    master_path_cfg = cfg.get("master_path", "data/warehouse/master.jsonl")
    master_path = Path(master_path_cfg)

    start_cfg = str(cfg.get("start", "") or "").strip().lower()
    end_cfg   = str(cfg.get("end", "") or "").strip().lower()
    if start_cfg in ("", "auto") or end_cfg in ("", "auto"):
        auto_start, auto_end = infer_date_range(master_path, lookback_days)
        start = auto_start if start_cfg in ("", "auto") else cfg.get("start")
        end   = auto_end   if end_cfg   in ("", "auto") else cfg.get("end")
    else:
        start, end = cfg.get("start"), cfg.get("end")

    agg_dir = Path(cfg.get("aggregate_dir", ""))
    tokens_csv   = (agg_dir / "tokens_by_day.csv").as_posix() if agg_dir else ""
    articles_csv = (agg_dir / "articles_by_day.csv").as_posix() if agg_dir else ""

    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    outdir = (outroot / ts); outdir.mkdir(parents=True, exist_ok=True)

    raw_inputs = build_raw_inputs(Path("data/raw_newsapi"), start or None, end or None)

    placeholders = {
        "tokens_csv": tokens_csv if tokens_csv and Path(tokens_csv).exists() else "",
        "articles_csv": articles_csv if articles_csv and Path(articles_csv).exists() else "",
        "start": start or "",
        "end": end or "",
        "outdir": outdir.as_posix(),
        "raw_inputs": raw_inputs,
    }

    def ok_to_run(cmd_fmt: str) -> tuple[bool, str]:
        cmd = cmd_fmt.format(**placeholders)
        parts = shlex.split(cmd)
        for i, tok in enumerate(parts):
            if tok == "--master" and i + 1 < len(parts):
                if not must_exist(parts[i + 1]):
                    return False, f"skip (missing master): {parts[i + 1]}"
        if "--inputs" in parts and not raw_inputs:
            return False, "skip (no raw_inputs)"
        for i, tok in enumerate(parts):
            if tok == "--tokens" and i + 1 < len(parts):
                if not must_exist(parts[i + 1]):
                    return False, f"skip (missing tokens_by_day.csv): {parts[i + 1]}"
        return True, cmd

    print(json.dumps({
        "resolved_start": placeholders["start"],
        "resolved_end": placeholders["end"],
        "lookback_days": lookback_days,
        "outdir": placeholders["outdir"]
    }))

    for job in cfg.get("jobs", []):
        name = job.get("name", "job")
        cmd_tmpl = job["cmd"]
        print(f"\n=== [{name}] ===")
        ok, cmd = ok_to_run(cmd_tmpl)
        if not ok:
            print(cmd)
            continue
        ensure_outdirs_in_cmd(cmd)
        rc = run_cmd(cmd)
        if rc != 0:
            print(f"Command failed ({rc}): {cmd}")
            sys.exit(rc)

    print("\nDone.")

if __name__ == "__main__":
    main()