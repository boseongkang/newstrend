import os, sys, json, time, urllib.parse, urllib.request

key = os.environ.get("NEWSAPI_KEY", "")
if not key:
    sys.exit(0)

q = os.environ.get("QUERY", "news")
frm = os.environ["FROM_ISO"]
to = os.environ["TO_ISO"]
pages = int(os.environ.get("MAX_PAGES", "3"))
outfile = os.environ["OUTFILE_TMP"]

base = "https://newsapi.org/v2/everything"
page_size = 100

total = 0
with open(outfile, "w", encoding="utf-8") as w:
    for page in range(1, pages + 1):
        params = {
            "q": q,
            "from": frm,
            "to": to,
            "sortBy": "publishedAt",
            "pageSize": page_size,
            "page": page,
            "language": "en",
            "apiKey": key,
        }
        url = base + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"User-Agent": "news-collector/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=40) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception:
            break
        if data.get("status") != "ok":
            break
        arts = data.get("articles", [])
        if not arts:
            break
        for a in arts:
            w.write(json.dumps(a, ensure_ascii=False) + "\n")
        total += len(arts)
        if len(arts) < page_size:
            break
        time.sleep(0.6)

if total == 0:
    try:
        os.remove(outfile)
    except FileNotFoundError:
        pass
