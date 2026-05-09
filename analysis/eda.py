"""
analysis/eda.py — Load, clean, feature engineering, EDA
Run: python -m analysis.eda
"""

import logging
import pandas as pd
import numpy as np
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.db import get_conn

logger = logging.getLogger(__name__)

# -------------------------------------------------------
# 1. Load from DB
# -------------------------------------------------------

def load_data() -> pd.DataFrame:
    """Load valid apartments from DB into DataFrame."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id, listing_id, city, district, district_raw,
                    area_m2, price_vnd, price_per_m2,
                    bedrooms, posted_date, scraped_at
                FROM apartments
                WHERE is_valid = TRUE
                ORDER BY posted_date DESC;
            """)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
    df = pd.DataFrame([dict(r) for r in rows], columns=cols)
    logger.info(f"Loaded {len(df)} valid records from DB")
    return df


# -------------------------------------------------------
# 2. Clean
# -------------------------------------------------------

def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Remove remaining outliers (IQR on price_per_m2)
    - Drop rows missing critical fields
    - Fix dtypes
    """
    original_len = len(df)

    # drop missing critical fields
    df = df.dropna(subset=["area_m2", "district"])

    # fix dtypes
    df["posted_date"] = pd.to_datetime(df["posted_date"], errors="coerce")
    df["area_m2"]     = pd.to_numeric(df["area_m2"],     errors="coerce")
    df["bedrooms"]    = pd.to_numeric(df["bedrooms"],     errors="coerce").astype("Int64")

    # IQR outlier removal on price_per_m2 (only for rows that have price)
    priced = df["price_per_m2"].notna()
    price_series = df.loc[priced, "price_per_m2"].astype(float)
    Q1 = float(price_series.quantile(0.25))
    Q3 = float(price_series.quantile(0.75))
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR

    outlier_mask = priced & ((df["price_per_m2"] < lower) | (df["price_per_m2"] > upper))
    df = df[~outlier_mask]

    removed = original_len - len(df)
    logger.info(f"Clean: removed {removed} rows (IQR outliers + missing) → {len(df)} remaining")
    logger.info(f"  IQR bounds: {lower:,.0f} — {upper:,.0f} VND/m²")

    return df.reset_index(drop=True)


# -------------------------------------------------------
# 3. Feature Engineering
# -------------------------------------------------------

def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived columns:
    - price_per_m2_m  : price/m² in triệu (easier to read)
    - price_bil       : total price in tỷ
    - size_bucket     : small / medium / large
    - segment         : affordable / mid-range / luxury
    - posted_month    : YYYY-MM for time series
    """
    # normalize units
    df["price_per_m2_m"] = (df["price_per_m2"] / 1_000_000).round(1)   # triệu/m²
    df["price_bil"]      = (df["price_vnd"]     / 1_000_000_000).round(2) # tỷ

    # size bucket
    df["size_bucket"] = pd.cut(
        df["area_m2"],
        bins=[0, 50, 80, float("inf")],
        labels=["small (<50m²)", "medium (50–80m²)", "large (>80m²)"]
    )

    # segment by price/m² in triệu
    df["segment"] = pd.cut(
        df["price_per_m2_m"],
        bins=[0, 40, 80, float("inf")],
        labels=["affordable (<40tr/m²)", "mid-range (40–80tr/m²)", "luxury (>80tr/m²)"]
    )

    # time series
    df["posted_month"] = df["posted_date"].dt.to_period("M").astype(str)

    logger.info("Feature engineering done")
    return df


# -------------------------------------------------------
# 4. EDA — Business Questions
# -------------------------------------------------------

def district_affordability(df: pd.DataFrame) -> pd.DataFrame:
    """
    Q: Which districts are most/least affordable?
    Metric: median price/m² (triệu) per district
    """
    priced = df[df["price_per_m2_m"].notna()]
    result = (
        priced.groupby(["city", "district"])
        .agg(
            listings        = ("id", "count"),
            avg_price_m2    = ("price_per_m2_m", "mean"),
            median_price_m2 = ("price_per_m2_m", "median"),
            min_price_m2    = ("price_per_m2_m", "min"),
            max_price_m2    = ("price_per_m2_m", "max"),
        )
        .round(1)
        .reset_index()
        .sort_values("median_price_m2")
    )
    return result


def price_volatility(df: pd.DataFrame) -> pd.DataFrame:
    """
    Q: Which districts have most price variance?
    Metric: coefficient of variation (CV = stddev / mean)
    """
    priced = df[df["price_per_m2_m"].notna()]
    result = (
        priced.groupby(["city", "district"])
        .agg(
            listings = ("id", "count"),
            mean     = ("price_per_m2_m", "mean"),
            std      = ("price_per_m2_m", "std"),
        )
        .round(2)
        .reset_index()
    )
    result["cv"] = (result["std"] / result["mean"] * 100).round(1)  # %
    result = result[result["listings"] >= 3].sort_values("cv", ascending=False)
    return result


def segment_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Q: What's the market split between affordable / mid-range / luxury?
    """
    priced = df[df["segment"].notna()]
    result = (
        priced.groupby(["city", "segment"])
        .agg(count=("id", "count"))
        .reset_index()
    )
    result["pct"] = (
        result.groupby("city")["count"]
        .transform(lambda x: (x / x.sum() * 100).round(1))
    )
    return result


def size_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Q: What apartment sizes are most common?
    """
    result = (
        df.groupby(["city", "size_bucket"])
        .agg(count=("id", "count"))
        .reset_index()
    )
    result["pct"] = (
        result.groupby("city")["count"]
        .transform(lambda x: (x / x.sum() * 100).round(1))
    )
    return result


def price_trend(df: pd.DataFrame) -> pd.DataFrame:
    """
    Q: How has median price/m² changed over time?
    """
    priced = df[df["price_per_m2_m"].notna() & df["posted_month"].notna()]
    result = (
        priced.groupby(["city", "posted_month"])
        .agg(
            listings        = ("id", "count"),
            median_price_m2 = ("price_per_m2_m", "median"),
        )
        .round(1)
        .reset_index()
        .sort_values("posted_month")
    )
    return result


# -------------------------------------------------------
# 5. Full pipeline
# -------------------------------------------------------

def run_eda() -> dict:
    """Run full EDA pipeline, return dict of DataFrames."""
    df_raw   = load_data()
    df_clean = clean(df_raw)
    df       = feature_engineering(df_clean)

    results = {
        "df":                   df,
        "affordability":        district_affordability(df),
        "volatility":           price_volatility(df),
        "segment_distribution": segment_distribution(df),
        "size_distribution":    size_distribution(df),
        "price_trend":          price_trend(df),
    }
    return results


# -------------------------------------------------------
# Quick test
# -------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    results = run_eda()
    df      = results["df"]

    print(f"\n{'='*50}")
    print(f"Dataset: {len(df)} records | {df['city'].value_counts().to_dict()}")
    print(f"{'='*50}")

    print("\n--- District Affordability (top 5 cheapest) ---")
    print(results["affordability"].head(5).to_string(index=False))

    print("\n--- Price Volatility (top 5 most volatile) ---")
    print(results["volatility"].head(5).to_string(index=False))

    print("\n--- Segment Distribution ---")
    print(results["segment_distribution"].to_string(index=False))

    print("\n--- Size Distribution ---")
    print(results["size_distribution"].to_string(index=False))

    print("\n--- Price Trend ---")
    print(results["price_trend"].to_string(index=False))
