import random
import time
import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Only desktop Windows/Mac Chrome/Firefox — never mobile.
# fake_useragent.random can return mobile UAs which trigger the mobile
# version of b2b-center.ru (different HTML, no firm links).
_DESKTOP_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
]


class AntiBlock:
    def __init__(self, base_delay: float = 3.0):
        self.base_delay = base_delay
        self.session = requests.Session()

        retry = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        # Fixed desktop UA for the whole session — never mobile
        self._ua = random.choice(_DESKTOP_AGENTS)

        self._warmed_up = False
        self.warn_callback = None

    def _headers_for(self, url: str, referer: str = "https://www.b2b-center.ru/") -> dict:
        return {
            "User-Agent": self._ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": referer,
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }

    def warmup(self, search_url: str):
        """
        Visit homepage → search page to establish a real session with cookies.
        Must be called once before scraping firm pages.
        """
        if self._warmed_up:
            return

        steps = [
            ("https://www.b2b-center.ru/", "https://www.google.com/"),
            (search_url, "https://www.b2b-center.ru/"),
        ]

        for target_url, referer in steps:
            try:
                logger.info("Прогрев сессии: %s", target_url)
                self.session.get(
                    target_url,
                    headers=self._headers_for(target_url, referer=referer),
                    timeout=20,
                )
                time.sleep(random.uniform(1.5, 3.0))
            except Exception as e:
                logger.warning("Ошибка прогрева на %s: %s", target_url, e)

        self._warmed_up = True
        logger.info("Прогрев сессии завершён")

    def get(self, url: str, referer: str = "https://www.b2b-center.ru/") -> str:
        headers = self._headers_for(url, referer=referer)
        try:
            response = self.session.get(url, headers=headers, timeout=20)
        except requests.exceptions.ConnectionError as e:
            logger.error("Ошибка соединения %s: %s", url, e)
            raise

        if response.status_code in (429, 503):
            wait = random.uniform(45, 90)
            msg = f"Получен статус {response.status_code}. Пауза {wait:.0f} сек..."
            logger.warning(msg)
            if self.warn_callback:
                self.warn_callback(msg)
            time.sleep(wait)
            response = self.session.get(url, headers=self._headers_for(url, referer=referer), timeout=20)

        if response.status_code == 404:
            raise requests.exceptions.HTTPError(f"404 Not Found: {url}", response=response)

        if response.status_code == 403:
            raise requests.exceptions.HTTPError(f"403 Forbidden: {url}", response=response)

        response.raise_for_status()
        return response.text

    def delay(self):
        jitter = self.base_delay * random.uniform(0.7, 1.3)
        time.sleep(jitter)
