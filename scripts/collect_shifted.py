import os, json, urllib.parse, urllib.request, time, sys

key = os.environ["NEWSAPI_KEY"]
query = os.environ.get("QUERY", "news")
frm = os.environ["FROM_ISO"]
to = os.environ["TO_ISO"]
pages = int(os.environ.get("MAX_PAGES", "2"))
tmp = os.environ["OUTFILE_TMP"]

base = "https://newsapi.org/v2/everything"
total = 0

with open(tmp, "w", encoding="utf-8") as f:
    for p in range(1, pages + 1):
        params = dict(
            q=query,
            **{"from": frm},
            to=to,
            sortBy="publishedAt",
            pageSize="100",
            page=str(p),
            apiKey=key,
        )
        url = base + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"User-Agent": "gh-actions"})
        try:
            with urllib.request.urlopen(req, timeout=40) as r:
                data = json.loads(r.read().decode("utf-8"))
        except Exception:
            break
        if data.get("status") != "ok":
            break
        arts = data.get("articles") or []
        if not arts:
            break
        for a in arts:
            f.write(json.dumps(a, ensure_ascii=False) + "\n")
            total += 1
        time.sleep(0.3)

if total == 0:
    try:
        os.remove(tmp)
    except FileNotFoundError:
        pass
