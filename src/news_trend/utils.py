import re, unicodedata, hashlib

def normalize_title(title: str) -> str:
    if not title:
        return ""
    t = unicodedata.normalize("NFKC", title).lower().strip()
    t = re.sub(r"\s*[-–—:]\s*.*$", "", t)  # strip suffix like " - Reuters"
    t = re.sub(r"\s+", " ", t)
    return t

def make_id(publisher: str, title_norm: str, date_str: str) -> str:
    raw = f"{publisher}|{title_norm}|{date_str}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()

def save_jsonl(path: str, rows: list[dict]) -> None:
    import os, json
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def load_jsonl(path: str):
    import json
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)
