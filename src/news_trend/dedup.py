from .utils import normalize_title, make_id

def dedup_rows(rows: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for r in rows:
        publisher = r.get("publisher") or ""
        title_norm = normalize_title(r.get("title") or "")
        date_key = (r.get("published_at") or "")[:10]
        aid = make_id(publisher, title_norm, date_key)
        if aid in seen:
            continue
        seen.add(aid)
        r["title_norm"] = title_norm
        r["article_id"] = aid
        out.append(r)
    return out
