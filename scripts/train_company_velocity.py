"""Train supervised regressors to predict company filing velocity.

Builds a company-year panel from the SQLite DB and trains RandomForest and
XGBoost regressors to predict next-year filings from lagged filing and
inventor-count features.

Run: python scripts/train_company_velocity.py
"""
import os
import json
import sqlite3
from pathlib import Path

import pandas as pd
import numpy as np

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import LabelEncoder
import joblib

try:
    import xgboost as xgb
except Exception:
    xgb = None

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "patents.db"
OUT_DIR = ROOT / "models"
OUT_DIR.mkdir(exist_ok=True)


def query_db(sql: str) -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(sql, conn)


def build_dataset():
    # Get per-company, per-year filings 2010-2024
    df = query_db("""
        SELECT c.company_id, c.name, c.country, p.year, COUNT(DISTINCT r.patent_id) AS filings
        FROM relationships r
        JOIN companies c ON r.company_id = c.company_id
        JOIN patents p ON r.patent_id = p.patent_id
        WHERE p.year BETWEEN 2010 AND 2024
        GROUP BY c.company_id, p.year
    """)

    if df.empty:
        raise RuntimeError("No data found in DB for years 2010-2024.")

    filing_pivot = df.pivot_table(index=["company_id", "name", "country"], columns="year", values="filings", aggfunc="sum", fill_value=0)
    filing_pivot.columns = [f"filings_{int(c)}" for c in filing_pivot.columns]
    filing_pivot = filing_pivot.reset_index()

    # Inventor counts per company per year
    inv = query_db("""
        SELECT c.company_id, p.year, COUNT(DISTINCT r.inventor_id) AS inventors
        FROM relationships r
        JOIN companies c ON r.company_id = c.company_id
        JOIN patents p ON r.patent_id = p.patent_id
        WHERE p.year BETWEEN 2010 AND 2024
        GROUP BY c.company_id, p.year
    """)
    inv_pivot = inv.pivot_table(index=["company_id"], columns="year", values="inventors", aggfunc="sum", fill_value=0)
    inv_pivot.columns = [f"inventors_{int(c)}" for c in inv_pivot.columns]
    inv_pivot = inv_pivot.reset_index()

    df_full = filing_pivot.merge(inv_pivot, on="company_id", how="left").fillna(0)

    # Build one row per company-year with lagged features and next-year target.
    years = list(range(2012, 2020))
    rows = []
    for _, row in df_full.iterrows():
        company_id = row["company_id"]
        name = row["name"]
        country = row["country"]

        for year in years:
            current = int(row.get(f"filings_{year}", 0))
            prev1 = int(row.get(f"filings_{year - 1}", 0))
            prev2 = int(row.get(f"filings_{year - 2}", 0))
            inv_current = int(row.get(f"inventors_{year}", 0))
            inv_prev1 = int(row.get(f"inventors_{year - 1}", 0))
            inv_prev2 = int(row.get(f"inventors_{year - 2}", 0))
            target = int(row.get(f"filings_{year + 1}", 0))

            rows.append({
                "company_id": company_id,
                "name": name,
                "country": country,
                "year": year,
                "filings_current": current,
                "filings_lag1": prev1,
                "filings_lag2": prev2,
                "inventors_current": inv_current,
                "inventors_lag1": inv_prev1,
                "inventors_lag2": inv_prev2,
                "filings_growth": (current - prev1) / (prev1 + 1),
                "inventor_growth": (inv_current - inv_prev1) / (inv_prev1 + 1),
                "target_next_year": target,
            })

    panel = pd.DataFrame(rows)
    panel = panel[panel["year"] <= 2018].copy()
    panel = panel[(panel[["filings_lag1", "filings_lag2"]].sum(axis=1) > 0) | (panel["filings_current"] > 0)]

    # Label encode country
    le = LabelEncoder()
    panel["country_code"] = le.fit_transform(panel["country"].astype(str))

    # Remove rows with no signal in the target; keep zero targets only if history exists.
    panel = panel[(panel["target_next_year"] >= 0)].copy()

    feature_cols = [
        "filings_current",
        "filings_lag1",
        "filings_lag2",
        "inventors_current",
        "inventors_lag1",
        "inventors_lag2",
        "filings_growth",
        "inventor_growth",
        "country_code",
        "year",
    ]

    return panel, feature_cols, le


def train_and_save():
    df, features, le = build_dataset()

    X = df[features].values
    y = df["target_next_year"].values

    if len(df) < 20:
        raise RuntimeError("Insufficient rows after panel construction.")

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
    )

    # Random Forest
    rf = RandomForestRegressor(n_estimators=300, random_state=42)
    rf.fit(X_train, y_train)
    y_pred_rf = rf.predict(X_test)

    results = {}
    results["rf"] = {
        "rmse": float(np.sqrt(mean_squared_error(y_test, y_pred_rf))),
        "mae": float(mean_absolute_error(y_test, y_pred_rf)),
        "r2": float(r2_score(y_test, y_pred_rf)),
    }

    best_model = rf

    # XGBoost if available
    if xgb is not None:
        xg = xgb.XGBRegressor(
            n_estimators=300,
            random_state=42,
            verbosity=0,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.9,
            colsample_bytree=0.9,
        )
        xg.fit(X_train, y_train)
        y_pred_xg = xg.predict(X_test)
        results["xgboost"] = {
            "rmse": float(np.sqrt(mean_squared_error(y_test, y_pred_xg))),
            "mae": float(mean_absolute_error(y_test, y_pred_xg)),
            "r2": float(r2_score(y_test, y_pred_xg)),
        }
        # choose lower RMSE
        if results["xgboost"]["rmse"] < results["rf"]["rmse"]:
            best_model = xg

    # Save model and metadata
    model_path = OUT_DIR / "company_velocity_model.joblib"
    joblib.dump(best_model, model_path)

    meta = {
        "features": features,
        "label_encoder_classes": le.classes_.tolist(),
        "metrics": results,
    }
    (OUT_DIR / "company_velocity_metadata.json").write_text(json.dumps(meta, indent=2))

    print(f"Rows: train={len(y_train)}, test={len(y_test)}")
    print("Training complete. Models and metadata saved to:", OUT_DIR)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    train_and_save()
