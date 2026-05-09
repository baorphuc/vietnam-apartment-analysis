"""
etl/loader.py — Insert cleaned + quality-checked records into PostgreSQL
Handles: duplicate skipping, price history tracking, scrape logging
"""

import logging
from datetime import datetime
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.db import get_conn

logger = logging.getLogger(__name__)


# -------------------------------------------------------
# Main insert
# -------------------------------------------------------

def insert_listings(records: list[dict], log_id: Optional[int] = None) -> dict:
    """
    Insert a batch of quality-checked records into apartments table.
    - Skips duplicates (ON CONFLICT DO NOTHING on source_url)
    - Tracks price changes in price_history
    - Updates scrape_logs if log_id provided

    Returns: {"inserted": int, "skipped": int, "invalid": int}
    """
    inserted = 0
    skipped  = 0
    invalid  = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            for record in records:

                # skip invalid records — still count them
                if not record.get("is_valid", True):
                    invalid += 1
                    _insert_invalid(cur, record)
                    continue

                try:
                    cur.execute("""
                        INSERT INTO apartments (
                            listing_id, source_url, title,
                            city, district_raw, district,
                            area_m2, price_vnd, price_per_m2,
                            bedrooms, posted_date, scraped_at,
                            is_valid, invalid_reason
                        ) VALUES (
                            %(listing_id)s, %(source_url)s, %(title)s,
                            %(city)s, %(district_raw)s, %(district)s,
                            %(area_m2)s, %(price_vnd)s, %(price_per_m2)s,
                            %(bedrooms)s, %(posted_date)s, %(scraped_at)s,
                            TRUE, NULL
                        )
                        ON CONFLICT (source_url) DO NOTHING
                        RETURNING id;
                    """, record)

                    row = cur.fetchone()
                    if row:
                        inserted += 1
                        # track price history for new inserts
                        if record.get("price_vnd"):
                            _insert_price_history(cur, row["id"], record)
                    else:
                        skipped += 1
                        # check if price changed for existing listing
                        _update_price_if_changed(cur, record)

                except Exception as e:
                    logger.warning(f"Insert failed for {record.get('listing_id')}: {e}")
                    conn.rollback()
                    skipped += 1
                    continue

        conn.commit()

    result = {"inserted": inserted, "skipped": skipped, "invalid": invalid}
    logger.info(f"Loader: inserted={inserted}, skipped={skipped}, invalid={invalid}")

    if log_id:
        _update_scrape_log(log_id, inserted, skipped, invalid)

    return result


def _insert_invalid(cur, record: dict):
    """Insert invalid record with is_valid=FALSE for audit trail."""
    try:
        cur.execute("""
            INSERT INTO apartments (
                listing_id, source_url, title,
                city, district_raw, district,
                area_m2, price_vnd, price_per_m2,
                bedrooms, posted_date, scraped_at,
                is_valid, invalid_reason
            ) VALUES (
                %(listing_id)s, %(source_url)s, %(title)s,
                %(city)s, %(district_raw)s, %(district)s,
                %(area_m2)s, %(price_vnd)s, %(price_per_m2)s,
                %(bedrooms)s, %(posted_date)s, %(scraped_at)s,
                FALSE, %(invalid_reason)s
            )
            ON CONFLICT (source_url) DO NOTHING;
        """, record)
    except Exception as e:
        logger.debug(f"Invalid insert skipped: {e}")


def _insert_price_history(cur, apartment_id: int, record: dict):
    cur.execute("""
        INSERT INTO price_history (apartment_id, price_vnd, price_per_m2)
        VALUES (%s, %s, %s);
    """, (apartment_id, record.get("price_vnd"), record.get("price_per_m2")))


def _update_price_if_changed(cur, record: dict):
    """If listing already exists and price changed, log to price_history."""
    try:
        cur.execute("""
            SELECT id, price_vnd FROM apartments WHERE source_url = %s;
        """, (record["source_url"],))
        row = cur.fetchone()
        if not row:
            return

        apartment_id  = row["id"]
        existing_price = row["price_vnd"]
        new_price      = record.get("price_vnd")

        if new_price and existing_price != new_price:
            logger.info(f"Price change detected for {record['listing_id']}: {existing_price} → {new_price}")
            _insert_price_history(cur, apartment_id, record)

            # update main record price
            cur.execute("""
                UPDATE apartments
                SET price_vnd = %s, price_per_m2 = %s, scraped_at = NOW()
                WHERE id = %s;
            """, (new_price, record.get("price_per_m2"), apartment_id))
    except Exception as e:
        logger.debug(f"Price update skipped: {e}")


# -------------------------------------------------------
# Scrape log helpers
# -------------------------------------------------------

def start_scrape_log(city: str) -> int:
    """Create a scrape_logs entry, return log id."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO scrape_logs (city, status)
                VALUES (%s, 'running')
                RETURNING id;
            """, (city,))
            log_id = cur.fetchone()["id"]
        conn.commit()
    logger.info(f"Scrape log started: id={log_id}, city={city}")
    return log_id


def finish_scrape_log(log_id: int, inserted: int, skipped: int, invalid: int, status: str = "done"):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE scrape_logs
                SET finished_at    = NOW(),
                    total_inserted = %s,
                    total_skipped  = %s,
                    total_invalid  = %s,
                    total_scraped  = %s,
                    status         = %s
                WHERE id = %s;
            """, (inserted, skipped, invalid, inserted + skipped + invalid, status, log_id))
        conn.commit()
    logger.info(f"Scrape log finished: id={log_id}, status={status}")


def _update_scrape_log(log_id: int, inserted: int, skipped: int, invalid: int):
    finish_scrape_log(log_id, inserted, skipped, invalid)


# -------------------------------------------------------
# Quick test
# -------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    from datetime import datetime

    sample = [
        {
            "listing_id": "TEST001", "source_url": "https://batdongsan.com.vn/test001",
            "title": "Test Căn Hộ Q7", "city": "HCMC",
            "district_raw": "Quận 7 (P. Tân Mỹ mới)", "district": "District 7",
            "area_m2": 70.0, "price_vnd": 2_700_000_000, "price_per_m2": 38_571_428,
            "bedrooms": 2, "posted_date": "2026-05-09",
            "scraped_at": datetime.now().isoformat(),
            "is_valid": True, "invalid_reason": None,
        },
        {
            "listing_id": "TEST002", "source_url": "https://batdongsan.com.vn/test002",
            "title": "Test Invalid Area", "city": "HCMC",
            "district_raw": "Quận 1", "district": "District 1",
            "area_m2": 10.0, "price_vnd": 500_000_000, "price_per_m2": 50_000_000,
            "bedrooms": 1, "posted_date": "2026-05-09",
            "scraped_at": datetime.now().isoformat(),
            "is_valid": False, "invalid_reason": "area<15m2",
        },
    ]

    log_id = start_scrape_log("HCMC")
    result = insert_listings(sample, log_id=log_id)
    finish_scrape_log(log_id, **result)
    print(f"\nResult: {result}")
