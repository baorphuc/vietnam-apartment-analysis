"""
etl/quality.py — Data quality checks
Flags invalid listings before DB insert
"""

import logging
import numpy as np
from typing import Optional

logger = logging.getLogger(__name__)

# -------------------------------------------------------
# Thresholds
# -------------------------------------------------------
MIN_AREA_M2       = 15       # drop nếu < 15m²
MAX_AREA_M2       = 500      # drop nếu > 500m² (bất thường cho căn hộ)
MIN_PRICE_PER_M2  = 5_000_000    # 5 triệu/m² — quá rẻ, likely data lỗi
P99_MULTIPLIER    = 1.0          # dùng percentile 99 từ batch hiện tại
MAX_BEDROOMS      = 10


def check(record: dict) -> dict:
    """
    Run quality checks on a transformed record.
    Adds/updates: is_valid (bool), invalid_reason (str | None)
    Returns the record with quality fields set.
    """
    reasons = []

    area_m2      = record.get("area_m2")
    price_vnd    = record.get("price_vnd")
    price_per_m2 = record.get("price_per_m2")
    district     = record.get("district")
    bedrooms     = record.get("bedrooms")

    # 1. Area checks
    if area_m2 is None:
        reasons.append("missing_area")
    elif area_m2 < MIN_AREA_M2:
        reasons.append(f"area<{MIN_AREA_M2}m2")
    elif area_m2 > MAX_AREA_M2:
        reasons.append(f"area>{MAX_AREA_M2}m2")

    # 2. Price checks (only if price exists — negotiable is allowed)
    if price_per_m2 is not None and price_per_m2 < MIN_PRICE_PER_M2:
        reasons.append(f"price_per_m2<{MIN_PRICE_PER_M2:,}")

    # 3. Missing district
    if not district:
        reasons.append("missing_district")

    # 4. Bedroom sanity
    if bedrooms is not None and bedrooms > MAX_BEDROOMS:
        reasons.append(f"bedrooms>{MAX_BEDROOMS}")

    record["is_valid"]      = len(reasons) == 0
    record["invalid_reason"] = ", ".join(reasons) if reasons else None

    return record


def flag_price_outliers(records: list[dict]) -> list[dict]:
    """
    Flag records where price_per_m2 > P99 of the current batch.
    Must be called AFTER all records are transformed (needs full batch).
    """
    prices = [
        r["price_per_m2"]
        for r in records
        if r.get("price_per_m2") is not None and r.get("is_valid", True)
    ]

    if not prices:
        return records

    p99 = float(np.percentile(prices, 99))
    logger.info(f"P99 price/m²: {p99:,.0f} VND")

    for r in records:
        if r.get("price_per_m2") and r["price_per_m2"] > p99:
            r["is_valid"] = False
            existing = r.get("invalid_reason") or ""
            r["invalid_reason"] = (existing + ", price>P99").lstrip(", ")

    return records


def quality_report(records: list[dict]) -> dict:
    """
    Print and return a summary quality report for a batch.
    """
    total    = len(records)
    valid    = sum(1 for r in records if r.get("is_valid"))
    invalid  = total - valid

    # count by reason
    reason_counts: dict[str, int] = {}
    for r in records:
        if r.get("invalid_reason"):
            for reason in r["invalid_reason"].split(", "):
                reason_counts[reason] = reason_counts.get(reason, 0) + 1

    report = {
        "total":          total,
        "valid":          valid,
        "invalid":        invalid,
        "invalid_pct":    round(invalid / total * 100, 1) if total else 0,
        "reason_counts":  reason_counts,
    }

    logger.info("=" * 40)
    logger.info(f"Quality Report")
    logger.info(f"  Total    : {total}")
    logger.info(f"  Valid    : {valid}")
    logger.info(f"  Invalid  : {invalid} ({report['invalid_pct']}%)")
    for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
        logger.info(f"    {reason}: {count}")
    logger.info("=" * 40)

    return report


def run_quality_pipeline(records: list[dict]) -> tuple[list[dict], dict]:
    """
    Full quality pipeline:
    1. Per-record checks
    2. Batch P99 outlier flagging
    3. Report

    Returns: (records_with_quality_flags, report)
    """
    # step 1 — per record
    records = [check(r) for r in records]

    # step 2 — batch outlier (P99)
    records = flag_price_outliers(records)

    # step 3 — report
    report = quality_report(records)

    return records, report


# -------------------------------------------------------
# Quick test
# -------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    sample = [
        {"listing_id": "1", "area_m2": 70.0,  "price_vnd": 2_700_000_000, "price_per_m2": 38_571_428, "district": "District 7",   "bedrooms": 2},
        {"listing_id": "2", "area_m2": 68.0,  "price_vnd": 4_100_000_000, "price_per_m2": 60_294_117, "district": "Binh Tan",      "bedrooms": 2},
        {"listing_id": "3", "area_m2": 70.0,  "price_vnd": None,          "price_per_m2": None,        "district": "Thu Duc City",  "bedrooms": 2},
        {"listing_id": "4", "area_m2": 10.0,  "price_vnd": 500_000_000,   "price_per_m2": 50_000_000, "district": "District 1",    "bedrooms": 1},   # area < 15
        {"listing_id": "5", "area_m2": None,  "price_vnd": 3_000_000_000, "price_per_m2": None,        "district": None,            "bedrooms": None}, # missing area + district
        {"listing_id": "6", "area_m2": 80.0,  "price_vnd": 50_000_000_000,"price_per_m2": 625_000_000,"district": "Hoan Kiem",     "bedrooms": 3},   # P99 outlier
    ]

    results, report = run_quality_pipeline(sample)

    print("\nPer-record results:")
    for r in results:
        status = "✓ valid" if r["is_valid"] else f"✗ invalid ({r['invalid_reason']})"
        print(f"  [{r['listing_id']}] {status}")
