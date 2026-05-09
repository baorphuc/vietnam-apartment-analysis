"""
scraper/base.py — Base scraper class
Handles: retry, random sleep, rotating user-agent, session management
"""

import time
import random
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
]


class BaseScraper:
    def __init__(
        self,
        sleep_min: float = 2.0,
        sleep_max: float = 5.0,
        max_retries: int = 3,
        timeout: int = 15,
    ):
        self.sleep_min = sleep_min
        self.sleep_max = sleep_max
        self.timeout = timeout
        self.session = self._build_session(max_retries)

    # ------------------------------------------------------------------
    # Session setup
    # ------------------------------------------------------------------
    def _build_session(self, max_retries: int) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=max_retries,
            backoff_factor=1.5,          # wait 1.5, 3, 6 seconds between retries
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _get_headers(self) -> dict:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://batdongsan.com.vn/",
        }

    # ------------------------------------------------------------------
    # Core fetch
    # ------------------------------------------------------------------
    def get(self, url: str) -> requests.Response | None:
        """
        GET with rotating headers + random sleep.
        Returns Response or None on failure.
        """
        self._sleep()
        try:
            resp = self.session.get(url, headers=self._get_headers(), timeout=self.timeout)
            resp.raise_for_status()
            logger.info(f"OK {resp.status_code} — {url}")
            return resp
        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP error {e.response.status_code} — {url}")
        except requests.exceptions.ConnectionError:
            logger.warning(f"Connection error — {url}")
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout — {url}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed — {url} — {e}")
        return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _sleep(self):
        t = random.uniform(self.sleep_min, self.sleep_max)
        logger.debug(f"Sleeping {t:.2f}s")
        time.sleep(t)

    def needs_selenium(self, response: requests.Response) -> bool:
        """
        Heuristic: nếu response HTML quá ngắn hoặc không có listing → cần Selenium.
        Override trong subclass nếu cần logic riêng.
        """
        return len(response.text) < 5000
