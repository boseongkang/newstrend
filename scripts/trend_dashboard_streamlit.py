import argparse
from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

@st.cache_data
def load_data(rising_dir: Path):
    rising = pd.read_csv(rising_dir/"rising_terms_top.csv")
    bursty = pd.read_csv(rising_dir/"bursty_terms_top.csv")
    ts = pd.read_csv(rising_dir/"trend_selected_timeseries.csv", parse_dates=["date"])
    return rising, bursty, ts

def get_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--rising-dir", required=False, default="reports/auto_trends_existing/20250924_234146Z/rising_csv")
    args, _ = parser.parse_known_args()
    return args

args = get_args()
rising_dir = Path(args.rising_dir)

st.set_page_config(page_title="News Trends Dashboard", layout="wide")
st.title("📈 News Trends Dashboard")

rising, bursty, ts = load_data(rising_dir)

mode = st.sidebar.radio("Ranking", ["Rising (slope)", "Bursty (z-score)"])
topn = st.sidebar.slider("Top N", 5, 200, 30, step=5)
query = st.sidebar.text_input("Search term", "")

rank_df = (rising if mode.startswith("Rising") else bursty).copy()
score_col = "norm_slope" if mode.startswith("Rising") else "max_z"
rank_df = rank_df.sort_values(score_col, ascending=False).head(topn)
if query.strip():
    rank_df = rank_df[rank_df["term"].str.contains(query, case=False, na=False)]
st.dataframe(rank_df.reset_index(drop=True))

terms = st.multiselect("Select terms", rank_df["term"].tolist(), default=rank_df["term"].tolist()[:5])
if terms:
    fig = go.Figure()
    for t in terms:
        cols = [c for c in ts.columns if c==t or c==f"{t}_sma7" or c==f"{t}_z"]
        if not cols:
            continue
        df = ts[["date"]+cols].sort_values("date")
        for c in cols:
            fig.add_trace(go.Scatter(x=df["date"], y=df[c], mode="lines", name=c))
    fig.update_layout(height=500, margin=dict(l=10,r=10,t=30,b=10))
    st.plotly_chart(fig, use_container_width=True)