"""
scraper/base.py — Base scraper class
Uses curl_cffi to bypass Cloudflare protection
"""

import time
import random
import logging
from curl_cffi import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class BaseScraper:
    def __init__(
        self,
        sleep_min: float = 2.0,
        sleep_max: float = 5.0,
        timeout: int = 15,
    ):
        self.sleep_min = sleep_min
        self.sleep_max = sleep_max
        self.timeout = timeout
        # impersonate Chrome 124 — bypasses Cloudflare TLS fingerprint
        self.session = requests.Session(impersonate="chrome124")

    def get(self, url: str):
        self._sleep()
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            logger.info(f"OK {resp.status_code} — {url}")
            return resp
        except Exception as e:
            logger.warning(f"Request failed — {url} — {e}")
            return None

    def _sleep(self):
        t = random.uniform(self.sleep_min, self.sleep_max)
        logger.debug(f"Sleeping {t:.2f}s")
        time.sleep(t)

    def needs_selenium(self, response) -> bool:
        return len(response.text) < 5000
