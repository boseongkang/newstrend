"""
ml_baseline.py — benchmark 4 regressors on the 5-day forward return.

Inputs:
  - site/data/ml_features.csv (produced by feature_engineering.py)

Output:
  - site/data/ml_baseline.json

Models:
  1. LinearRegression
  2. Ridge(alpha=1.0)
  3. RandomForestRegressor(n_estimators=100, min_samples_leaf=3)
  4. GradientBoostingRegressor(n_estimators=100, max_depth=3)

Pipeline:
  - drop rows without fwd_5d_return
  - chronological split: oldest 70 % train, newest 30 % test
  - one-hot encode {action, ta_*, sector, regime, dow}
  - median-impute then standard-scale numerics

Gating:
  - n_train < MIN_DATA (100) → mode="advisory"; metrics still
    computed and recorded but the run is flagged so downstream
    consumers know not to weight the recommendation heavily.
"""

from __future__ import annotations

import json
import warnings
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# Skip-noise: empty-feature imputer warnings during early data-collection mode.
warnings.filterwarnings("ignore", message="Skipping features without any observed values")
warnings.filterwarnings("ignore", message="X does not have valid feature names")
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "site" / "data"
FEATURES_CSV = DATA / "ml_features.csv"
OUT_JSON = DATA / "ml_baseline.json"

TARGET = "fwd_5d_return"
MIN_DATA = 100              # below this n_train, mode=advisory
TRAIN_FRAC = 0.70

CATEGORICAL_COLS = [
    "action", "ta_trend", "ta_macd_bias",
    "ta_bb_position", "ta_volatility",
    "sector", "regime", "dow",
]
NUMERIC_COLS = [
    "confidence",
    "ta_rsi14", "ta_hv20", "ta_atr14",
    "news_best_conf", "news_z",
    "sent_filtered", "sent_total", "sent_bullish_ratio",
    "fund_quality", "fund_growth", "fund_health", "fund_score",
    "ins_p_score", "ins_score", "ins_cluster_size", "ins_n_buyers", "ins_net_buy_value",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _make_pipeline(model):
    pre = ColumnTransformer(
        transformers=[
            ("num", Pipeline([("impute", SimpleImputer(strategy="median")),
                              ("scale", StandardScaler())]), NUMERIC_COLS),
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAL_COLS),
        ],
        remainder="drop",
    )
    return Pipeline([("pre", pre), ("model", model)])


def _directional_accuracy(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    mask = (y_true != 0)
    if not mask.any():
        return float("nan")
    return float(np.mean(np.sign(y_true[mask]) == np.sign(y_pred[mask])))


def _evaluate(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    return {
        "mae_pct": round(float(mean_absolute_error(y_true, y_pred)) * 100, 4),
        "rmse_pct": round(float(np.sqrt(mean_squared_error(y_true, y_pred))) * 100, 4),
        "r2": round(float(r2_score(y_true, y_pred)), 4),
        "directional_accuracy": round(_directional_accuracy(y_true, y_pred), 4),
    }


def _rf_feature_importance(pipe, feature_names: list[str]) -> dict:
    model = pipe.named_steps["model"]
    if not hasattr(model, "feature_importances_"):
        return {}
    importances = model.feature_importances_
    pairs = sorted(zip(feature_names, importances), key=lambda x: -x[1])
    return {name: round(float(imp), 4) for name, imp in pairs[:15]}


def run() -> None:
    if not FEATURES_CSV.exists():
        raise SystemExit(f"Missing {FEATURES_CSV} — run feature_engineering.py first")

    df = pd.read_csv(FEATURES_CSV)
    df = df[df[TARGET].notna()].copy()

    n_total = len(df)
    df = df.sort_values("snap_date").reset_index(drop=True)
    split_idx = max(int(round(n_total * TRAIN_FRAC)), 1)
    train_df = df.iloc[:split_idx]
    test_df = df.iloc[split_idx:]

    n_train, n_test = len(train_df), len(test_df)
    mode = "advisory" if n_train < MIN_DATA else "active"

    payload: dict = {
        "updated": _now_iso(),
        "mode": mode,
        "config": {"min_data": MIN_DATA, "train_frac": TRAIN_FRAC, "target": TARGET},
        "data_summary": {
            "n_total_with_target": n_total,
            "n_train": n_train,
            "n_test": n_test,
            "train_date_range": [train_df["snap_date"].min() if n_train else None,
                                 train_df["snap_date"].max() if n_train else None],
            "test_date_range": [test_df["snap_date"].min() if n_test else None,
                                test_df["snap_date"].max() if n_test else None],
            "n_features_categorical": len(CATEGORICAL_COLS),
            "n_features_numeric": len(NUMERIC_COLS),
        },
        "models": {},
    }

    # If we lack any test rows we cannot benchmark — bail with what we have.
    if n_train < 5 or n_test < 1:
        payload["error"] = f"insufficient_data n_train={n_train} n_test={n_test}"
        OUT_JSON.write_text(json.dumps(payload, indent=2, default=str))
        print(f"Wrote {OUT_JSON}  (mode={mode}, error={payload['error']})")
        return

    X_train = train_df[CATEGORICAL_COLS + NUMERIC_COLS]
    y_train = train_df[TARGET].to_numpy(dtype=float)
    X_test = test_df[CATEGORICAL_COLS + NUMERIC_COLS]
    y_test = test_df[TARGET].to_numpy(dtype=float)

    models = {
        "LinearRegression":           LinearRegression(),
        "Ridge":                      Ridge(alpha=1.0, random_state=42),
        "RandomForest":               RandomForestRegressor(n_estimators=100, min_samples_leaf=3, random_state=42, n_jobs=-1),
        "GradientBoosting":           GradientBoostingRegressor(n_estimators=100, max_depth=3, random_state=42),
    }

    for name, model in models.items():
        try:
            pipe = _make_pipeline(model)
            pipe.fit(X_train, y_train)
            train_pred = pipe.predict(X_train)
            test_pred = pipe.predict(X_test)
            entry = {
                "train": _evaluate(y_train, train_pred),
                "test": _evaluate(y_test, test_pred),
            }
            if name == "RandomForest":
                fitted_pre = pipe.named_steps["pre"]
                ohe_names = list(fitted_pre.named_transformers_["cat"].get_feature_names_out(CATEGORICAL_COLS))
                feature_names = NUMERIC_COLS + ohe_names
                entry["top_feature_importance"] = _rf_feature_importance(pipe, feature_names)
            payload["models"][name] = entry
        except Exception as e:
            payload["models"][name] = {"error": f"{type(e).__name__}: {e}"}

    # Pick the winner on test directional accuracy as primary metric;
    # advisory until n_train ≥ MIN_DATA.
    valid = {n: v["test"] for n, v in payload["models"].items()
             if "test" in v and v["test"]["directional_accuracy"] == v["test"]["directional_accuracy"]}
    if valid:
        best_name = max(valid, key=lambda k: valid[k]["directional_accuracy"])
        payload["winner"] = {"model": best_name, **valid[best_name]}

    OUT_JSON.write_text(json.dumps(payload, indent=2, default=str))
    print(f"Wrote {OUT_JSON}  (mode={mode})")
    print(f"  n_train={n_train} n_test={n_test}")
    for name, info in payload["models"].items():
        if "test" in info:
            t = info["test"]
            print(f"  {name:18s} test mae={t['mae_pct']:.3f}% rmse={t['rmse_pct']:.3f}% r2={t['r2']:+.3f} dir_acc={t['directional_accuracy']:.3f}")
        elif "error" in info:
            print(f"  {name:18s} ERROR: {info['error']}")
    if "winner" in payload:
        w = payload["winner"]
        print(f"  → winner: {w['model']} dir_acc={w['directional_accuracy']:.3f}")


if __name__ == "__main__":
    run()
