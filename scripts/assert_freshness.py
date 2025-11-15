import sys, datetime
if len(sys.argv) < 2 or not sys.argv[1]:
    print("No latest date provided", file=sys.stderr)
    sys.exit(1)
latest = sys.argv[1]
dt = datetime.date.fromisoformat(latest)
today = datetime.datetime.utcnow().date()
delta = (today - dt).days
print(f"freshness Δdays={delta}")
if delta > 2:
    print(f"Data too old: {latest} (Δ{delta}d)", file=sys.stderr)
    sys.exit(1)