import argparse, subprocess, shlex, sys
from pathlib import Path
import datetime as dt

try:
    import yaml
except Exception:
    print("Please `pip install pyyaml`", file=sys.stderr); sys.exit(1)

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


def build_raw_inputs(raw_dir: Path, start: str | None, end: str | None) -> str:
    files = []
    sd = dt.date.fromisoformat(start) if start else None
    ed = dt.date.fromisoformat(end) if end else None
    if raw_dir.exists():
        for p in sorted(raw_dir.glob("*.jsonl")):
            # 파일명이 YYYY-MM-DD.jsonl 형식이라고 가정
            try:
                d = dt.date.fromisoformat(p.stem)
            except Exception:
                continue
            if sd and d < sd:
                continue
            if ed and d > ed:
                continue
            files.append(str(p))
    return " ".join(files)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    agg_dir = Path(cfg.get("aggregate_dir", ""))
    start   = cfg.get("start", "")
    end     = cfg.get("end", "")
    outroot = Path(cfg["outdir"]); outroot.mkdir(parents=True, exist_ok=True)

    tokens_csv   = (agg_dir / "tokens_by_day.csv").as_posix()
    articles_csv = (agg_dir / "articles_by_day.csv").as_posix()

    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    outdir = (outroot / ts); outdir.mkdir(parents=True, exist_ok=True)

    raw_inputs = build_raw_inputs(Path("data/raw_newsapi"), start or None, end or None)

    extra_stop = ""
    extra_path = Path("config/extra_noise.txt")
    if extra_path.exists():
        words = [ln.strip() for ln in extra_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
        extra_stop = ",".join(sorted(set(w.lower() for w in words)))

    placeholders = {
        "tokens_csv": tokens_csv if Path(tokens_csv).exists() else "",
        "articles_csv": articles_csv if Path(articles_csv).exists() else "",
        "start": start or "",
        "end": end or "",
        "outdir": outdir.as_posix(),
        "raw_inputs": raw_inputs,
        "extra_stop": extra_stop,
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