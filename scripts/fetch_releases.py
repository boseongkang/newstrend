import argparse, os, re, sys, json, time, pathlib, datetime, urllib.request

def get(url, token=None):
    req = urllib.request.Request(url, headers={"Accept":"application/vnd.github+json"})
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read().decode())

def download(url, out, token=None):
    out.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req) as r, open(out, "wb") as f:
        while True:
            b = r.read(1024*64)
            if not b: break
            f.write(b)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--out", default="data/releases")
    ap.add_argument("--pattern", default=r"news_\d{4}-\d{2}-\d{2}\.jsonl\.gz$")
    ap.add_argument("--since", default="")
    ap.add_argument("--max-pages", type=int, default=10)
    args=ap.parse_args()

    token = os.environ.get("GITHUB_TOKEN","")
    outdir = pathlib.Path(args.out)
    pat = re.compile(args.pattern)
    since_dt = None
    if args.since:
        since_dt = datetime.datetime.fromisoformat(args.since)

    page = 1
    downloaded = 0
    while page <= args.max_pages:
        url = f"https://api.github.com/repos/{args.repo}/releases?per_page=100&page={page}"
        rels = get(url, token)
        if not rels: break
        for rel in rels:
            pub = rel.get("published_at") or rel.get("created_at")
            if pub and since_dt:
                try:
                    pub_dt = datetime.datetime.fromisoformat(pub.replace("Z","+00:00"))
                    if pub_dt < since_dt:
                        continue
                except Exception:
                    pass
            for a in rel.get("assets",[]):
                name = a.get("name","")
                if not pat.match(name): continue
                dest = outdir/name
                if dest.exists() and dest.stat().st_size>0:
                    continue
                url = a.get("browser_download_url")
                if not url: continue
                print(f"downloading {name}...")
                try:
                    download(url, dest, token)
                    downloaded += 1
                except Exception as e:
                    print(f"failed {name}: {e}", file=sys.stderr)
                    if dest.exists(): dest.unlink()
                    time.sleep(1)
        page += 1
    print(f"done. downloaded={downloaded}")

if __name__=="__main__":
    main()