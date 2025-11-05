import argparse, json, pathlib, time
import yfinance as yf

def load_tickers(map_path=None, list_path=None):
    t = set()
    if map_path:
        d = json.loads(pathlib.Path(map_path).read_text())
        for k in d.keys():
            t.add(k.upper())
    if list_path:
        for line in pathlib.Path(list_path).read_text().splitlines():
            line=line.strip()
            if not line or line.startswith("#"): continue
            for part in line.replace(",", " ").split():
                t.add(part.upper())
    if not t:
        raise SystemExit("no tickers from --map/--list")
    return sorted(t)

def fetch_meta(ticker, retry=2, sleep_ms=150):
    last_err = None
    for _ in range(retry):
        try:
            tk = yf.Ticker(ticker)
            try:
                info = tk.get_info()
            except Exception:
                info = tk.info
            sector = None
            exch = None
            if isinstance(info, dict):
                sector = info.get("sector") or info.get("industry") or None
                exch = info.get("exchange") or info.get("fullExchangeName") or info.get("market") or None
            if not sector:
                try:
                    ap = tk.get_asset_profile()
                    if isinstance(ap, dict):
                        sector = ap.get("sector") or sector
                except Exception:
                    pass
            return sector or "Unknown", str(exch or "").strip()
        except Exception as e:
            last_err = e
            time.sleep(sleep_ms/1000.0)
    return "Unknown", ""

def is_nasdaq(exch):
    s = str(exch).lower()
    return ("nasdaq" in s) or ("nms" in s) or ("ngs" in s)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--map")
    ap.add_argument("--list")
    ap.add_argument("--out", required=True)
    ap.add_argument("--nasdaq-only", type=int, default=1)
    ap.add_argument("--sleep-ms", type=int, default=120)
    a = ap.parse_args()

    tickers = load_tickers(a.map, a.list)
    out = {}
    for t in tickers:
        sector, exch = fetch_meta(t, retry=3, sleep_ms=a.sleep_ms)
        if a.nasdaq_only and not is_nasdaq(exch):
            continue
        out.setdefault(sector, []).append(t)

    for k in list(out.keys()):
        out[k] = sorted(set(out[k]))

    p = pathlib.Path(a.out)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(out))
    print(f"[ok] wrote -> {p} sectors={len(out)}")

if __name__ == "__main__":
    main()