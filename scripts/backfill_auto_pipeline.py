import datetime, subprocess, sys, pathlib

root = pathlib.Path(__file__).resolve().parents[1]

start = datetime.date(2025, 10, 28)
end = datetime.date(2025, 11, 23)

d = start
while d <= end:
    ds = d.isoformat()
    print("=== auto_pipeline", ds, "===")
    r = subprocess.run(
        [
            sys.executable,
            "scripts/auto_pipeline.py",
            "--date", ds,
            "--rawdir", "data/raw",
            "--silverdir", "data/silver",
            "--reports", "reports",
        ],
        cwd=root,
    )
    if r.returncode != 0:
        print("FAILED on", ds, "code", r.returncode)
        break
    d += datetime.timedelta(days=1)
