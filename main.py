"""
main.py — Full ETL pipeline entry point
Usage:
    python main.py              # scrape both HCMC + HN
    python main.py --city HCMC  # scrape one city only
    python main.py --pages 5    # limit pages per city (for testing)
"""

import argparse
import logging
from datetime import datetime

from scraper.bds_scraper import BDSScraper, CITY_CONFIGS
from etl.transformer import transform
from etl.quality import run_quality_pipeline
from etl.loader import insert_listings, start_scrape_log, finish_scrape_log

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"scrape_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    ]
)
logger = logging.getLogger(__name__)


def run_pipeline(city: str, max_pages: int = None):
    logger.info(f"{'='*50}")
    logger.info(f"Starting pipeline — city={city}")
    logger.info(f"{'='*50}")

    # ── 1. Scrape ──────────────────────────────────────
    scraper = BDSScraper(sleep_min=2.0, sleep_max=4.0)

    if max_pages:
        CITY_CONFIGS[city]["total_pages"] = max_pages

    log_id = start_scrape_log(city)

    try:
        raw_listings = scraper.scrape_city(city)
        logger.info(f"[{city}] Scraped {len(raw_listings)} raw listings")
    except Exception as e:
        logger.error(f"[{city}] Scrape failed: {e}")
        finish_scrape_log(log_id, 0, 0, 0, status="error")
        return

    if not raw_listings:
        logger.warning(f"[{city}] No listings scraped, aborting.")
        finish_scrape_log(log_id, 0, 0, 0, status="error")
        return

    # ── 2. Transform ───────────────────────────────────
    transformed = []
    for raw in raw_listings:
        record = transform(raw)
        if record:
            transformed.append(record)

    logger.info(f"[{city}] Transformed: {len(transformed)}/{len(raw_listings)}")

    # ── 3. Quality ─────────────────────────────────────
    checked, report = run_quality_pipeline(transformed)
    logger.info(f"[{city}] Quality: valid={report['valid']}, invalid={report['invalid']}")

    # ── 4. Load ────────────────────────────────────────
    result = insert_listings(checked, log_id=log_id)
    logger.info(f"[{city}] Load result: {result}")

    return result


def main():
    parser = argparse.ArgumentParser(description="Vietnam Apartment Scraper Pipeline")
    parser.add_argument("--city",  choices=["HCMC", "HN", "both"], default="both")
    parser.add_argument("--pages", type=int, default=None, help="Max pages per city (default: all)")
    args = parser.parse_args()

    cities = ["HCMC", "HN"] if args.city == "both" else [args.city]

    for city in cities:
        run_pipeline(city, max_pages=args.pages)

    logger.info("Pipeline complete.")


if __name__ == "__main__":
    main()
