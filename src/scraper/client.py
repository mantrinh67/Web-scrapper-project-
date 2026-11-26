import logging
import random
import time
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from playwright.sync_api import sync_playwright
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config.settings import (
    DEFAULT_DELAY_MAX,
    DEFAULT_DELAY_MIN,
    DEFAULT_HEADERS,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
)
from src.utils.logger import setup_logger

logger = setup_logger("playwright_client")


class RetriableHttpError(Exception):
    """Exception raised when a Playwright API response indicates a transient error that can be retried."""
    pass


class HttpClient:
    """Playwright-backed APIRequestContext client with exponential backoff and jitter delay."""

    def __init__(self, headers: Optional[Dict[str, str]] = None):
        self.headers = headers or DEFAULT_HEADERS.copy()

    @staticmethod
    def _sleep_jitter() -> None:
        delay = random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX)
        time.sleep(delay)

    @retry(
        retry=retry_if_exception_type((RetriableHttpError, Exception)),
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1.5, min=2, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Performs a GET request using Playwright's native APIRequestContext with automatic retry."""
        self._sleep_jitter()
        full_url = f"{url}?{urlencode(params)}" if params else url

        with sync_playwright() as p:
            request_ctx = p.request.new_context(
                extra_http_headers=self.headers,
                timeout=REQUEST_TIMEOUT * 1000,
            )
            try:
                response = request_ctx.get(full_url)
                if response.status in (429, 500, 502, 503, 504):
                    raise RetriableHttpError(
                        f"Transient status code {response.status} received from {full_url}"
                    )
                if not response.ok:
                    logger.error(f"Playwright request error {response.status} for URL: {full_url}")
                    raise RuntimeError(f"HTTP {response.status}: {response.status_text}")

                return response.json()
            finally:
                request_ctx.dispose()
