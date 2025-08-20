from __future__ import annotations
import json, re, base64, io
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timezone

WORD_RE = re.compile(r"[A-Za-z]{3,}")

def _load_jsonl(path: Path) -> pd.DataFrame:
    recs = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            recs.append(json.loads(line))
    return pd.DataFrame(recs)

def _png_b64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=150)
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()

def analyze_hourly(
    date: str,
    indir: str = "data/silver_newsapi",
    outdir: str = "reports/hourly",
    top_k_publishers: int = 10,
    top_k_words: int = 30,
) -> Path:
    day = date
    inpath = Path(indir) / f"{day}.jsonl"
    assert inpath.exists(), f"missing: {inpath}"
    df = _load_jsonl(inpath)

    if not df.empty:
        df["ts"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
        df["hour"] = df["ts"].dt.floor("H")
    else:
        df["hour"] = pd.NaT

    by_hour = df.groupby("hour").size().reset_index(name="count").sort_values("hour")
    fig1, ax1 = plt.subplots(figsize=(9,3))
    ax1.plot(by_hour["hour"].dt.strftime("%H:%M"), by_hour["count"])
    ax1.set_title(f"Articles by hour — {day}")
    ax1.set_xlabel("Hour (UTC)")
    ax1.set_ylabel("Articles")
    img_hour = _png_b64(fig1)

    pub = df["publisher"].dropna()
    top_pub = pub.value_counts().head(top_k_publishers)
    fig2, ax2 = plt.subplots(figsize=(6,4))
    top_pub.iloc[::-1].plot(kind="barh", ax=ax2)
    ax2.set_title("Top publishers")
    ax2.set_xlabel("Articles")
    img_pub = _png_b64(fig2)

    texts = (df["title"].fillna("").astype(str) + " " + df["description"].fillna("").astype(str))
    words = []
    for t in texts:
        words.extend(WORD_RE.findall(t.lower()))
    ws = pd.Series(words)
    stop = set(["the","and","for","with","that","from","this","have","has","are","was","were","will","not","you","your","they","been","into","but","about","over"])
    ws = ws[~ws.isin(stop)]
    topw = ws.value_counts().head(top_k_words)
    fig3, ax3 = plt.subplots(figsize=(6,7))
    topw.iloc[::-1].plot(kind="barh", ax=ax3)
    ax3.set_title("Top words")
    ax3.set_xlabel("Count")
    img_words = _png_b64(fig3)

    outd = Path(outdir); outd.mkdir(parents=True, exist_ok=True)
    html_path = outd / f"{day}.html"
    html = f"""<!doctype html>
<html lang="en"><meta charset="utf-8"><title>Hourly report {day}</title>
<style>
body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:24px}}
h1{{margin:0 0 12px}}
.grid{{display:grid;grid-template-columns:1fr;gap:20px;max-width:1000px}}
.card{{border:1px solid #eee;border-radius:12px;padding:16px}}
img{{max-width:100%;height:auto;border-radius:8px;display:block}}
.badge{{display:inline-block;padding:4px 10px;border-radius:999px;background:#f5f7ff;border:1px solid #e3e8ff}}
</style>
<h1>Hourly Report — <span class="badge">{day}</span></h1>
<div class="grid">
  <div class="card"><h2>Articles by hour (UTC)</h2><img src="data:image/png;base64,{img_hour}"></div>
  <div class="card"><h2>Top publishers</h2><img src="data:image/png;base64,{img_pub}"></div>
  <div class="card"><h2>Top words</h2><img src="data:image/png;base64,{img_words}"></div>
</div>
</html>"""
    html_path.write_text(html, encoding="utf-8")
    print(f"[OK] hourly report -> {html_path}")
    return html_path
