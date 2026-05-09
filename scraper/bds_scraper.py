"""
scraper/bds_scraper.py — batdongsan.com.vn scraper
Scrapes căn hộ listings for HCMC and HN
"""

import re
import logging
from datetime import datetime, date
from dataclasses import dataclass, field
from typing import Optional

from bs4 import BeautifulSoup
from scraper.base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://batdongsan.com.vn"

CITY_CONFIGS = {
    "HCMC": {
        "url": f"{BASE_URL}/ban-can-ho-chung-cu-tp-hcm",
        "total_pages": 20,   # adjust after checking actual page count
    },
    "HN": {
        "url": f"{BASE_URL}/ban-can-ho-chung-cu-ha-noi",
        "total_pages": 20,
    },
}


@dataclass
class RawListing:
    listing_id:   str
    source_url:   str
    title:        Optional[str]
    city:         str
    district_raw: Optional[str]
    price_raw:    Optional[str]       # e.g. "5.2 tỷ", "Giá thỏa thuận"
    area_raw:     Optional[str]       # e.g. "73 m²"
    bedrooms_raw: Optional[str]       # e.g. "2"
    posted_date:  Optional[str]       # e.g. "08/05/2026"
    scraped_at:   datetime = field(default_factory=datetime.now)


class BDSScraper(BaseScraper):

    def scrape_city(self, city: str) -> list[RawListing]:
        """Scrape all pages for a city. Returns list of RawListing."""
        config = CITY_CONFIGS[city]
        all_listings: list[RawListing] = []

        for page in range(1, config["total_pages"] + 1):
            url = f"{config['url']}/p{page}" if page > 1 else config["url"]
            logger.info(f"[{city}] Page {page}/{config['total_pages']} — {url}")

            listings = self._scrape_page(url, city)
            if not listings:
                logger.info(f"[{city}] No listings on page {page}, stopping.")
                break

            all_listings.extend(listings)
            logger.info(f"[{city}] Page {page} — {len(listings)} listings scraped (total: {len(all_listings)})")

        return all_listings

    def _scrape_page(self, url: str, city: str) -> list[RawListing]:
        """Scrape a single listing page. Returns list of RawListing."""
        resp = self.get(url)
        if resp is None:
            return []

        if self.needs_selenium(resp):
            logger.warning(f"Page may need Selenium: {url}")
            # TODO: plug in selenium_fallback here
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("div.js__card.js__card-full-web")

        if not cards:
            # fallback selector
            cards = soup.select("div.js__card-listing")

        return [
            listing
            for card in cards
            if (listing := self._parse_card(card, city)) is not None
        ]

    def _parse_card(self, card, city: str) -> Optional[RawListing]:
        """Parse a single listing card. Returns RawListing or None."""
        try:
            # --- listing_id + source_url ---
            anchor = card.select_one("a.js__product-link-for-product-id")
            if not anchor:
                return None

            listing_id = anchor.get("data-product-id", "").strip()
            href = anchor.get("href", "").strip()
            source_url = f"{BASE_URL}{href}" if href.startswith("/") else href

            if not listing_id:
                return None

            # --- title ---
            title_el = card.select_one(".pr-title.js__card-title")
            title = title_el.get_text(strip=True) if title_el else None

            # --- price ---
            price_el = card.select_one(".re__card-config-price")
            price_raw = price_el.get_text(strip=True) if price_el else None

            # --- area ---
            area_el = card.select_one(".re__card-config-area")
            area_raw = area_el.get_text(strip=True) if area_el else None

            # --- bedrooms ---
            bedroom_el = card.select_one(".re__card-config-bedroom span")
            bedrooms_raw = bedroom_el.get_text(strip=True) if bedroom_el else None

            # --- district ---
            # Structure: <div class="re__card-location"><span>·</span><span>Quận 7 (P. Tân Mỹ mới)</span></div>
            location_el = card.select_one(".re__card-location")
            district_raw = None
            if location_el:
                spans = location_el.select("span")
                # last span is the actual location text
                for span in reversed(spans):
                    text = span.get_text(strip=True)
                    if text and text != "·":
                        district_raw = text
                        break

            # --- posted_date ---
            date_el = card.select_one(".re__card-published-info-published-at")
            posted_date = None
            if date_el:
                posted_date = date_el.get("aria-label", "").strip()  # format: "08/05/2026"

            return RawListing(
                listing_id=listing_id,
                source_url=source_url,
                title=title,
                city=city,
                district_raw=district_raw,
                price_raw=price_raw,
                area_raw=area_raw,
                bedrooms_raw=bedrooms_raw,
                posted_date=posted_date,
            )

        except Exception as e:
            logger.warning(f"Failed to parse card: {e}")
            return None


# -------------------------------------------------------
# Quick test — run: python -m scraper.bds_scraper
# -------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    scraper = BDSScraper(sleep_min=2.0, sleep_max=4.0)

    # Test: scrape page 1 HCMC only
    listings = scraper._scrape_page(CITY_CONFIGS["HCMC"]["url"], city="HCMC")

    print(f"\nScraped {len(listings)} listings\n")
    for l in listings[:3]:
        print(f"  ID       : {l.listing_id}")
        print(f"  Title    : {l.title}")
        print(f"  Price    : {l.price_raw}")
        print(f"  Area     : {l.area_raw}")
        print(f"  Bedrooms : {l.bedrooms_raw}")
        print(f"  District : {l.district_raw}")
        print(f"  Date     : {l.posted_date}")
        print(f"  URL      : {l.source_url}")
        print()
