import random
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional

from playwright.sync_api import BrowserContext, Page, Playwright, sync_playwright

from config.settings import (
    BROWSER_PROFILE_DIR,
    DEFAULT_DELAY_MAX,
    DEFAULT_DELAY_MIN,
    DEFAULT_HEADLESS,
)
from src.models.target import CrawlResult, CrawlTarget
from src.scraper.checkpoint import CheckpointManager
from src.storage.bronze_writer import BronzeWriter
from src.utils.logger import setup_logger

logger = setup_logger("base_scraper")


class BaseScraper(ABC):
    """Abstract Base Class for all Playwright-based e-commerce scrapers in the ingestion pipeline."""

    def __init__(
        self,
        checkpoint_mgr: Optional[CheckpointManager] = None,
        bronze_writer: Optional[BronzeWriter] = None,
        profile_dir: Optional[Path] = None,
        headless: bool = DEFAULT_HEADLESS,
    ):
        self.checkpoint_mgr = checkpoint_mgr or CheckpointManager()
        self.bronze_writer = bronze_writer or BronzeWriter()
        self.profile_dir = profile_dir or BROWSER_PROFILE_DIR
        self.profile_dir.mkdir(parents=True, exist_ok=True)
        self.headless = headless

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Name of the platform source (e.g., 'shopee', 'tiki')."""
        pass

    def launch_persistent_context(
        self,
        playwright: Playwright,
        headless: Optional[bool] = None,
        use_dedicated_subdir: bool = False,
    ) -> BrowserContext:
        """Launches a Playwright Chromium/Chrome persistent context with anti-automation flags."""
        use_headless = self.headless if headless is None else headless
        user_data_path = (
            self.profile_dir / self.source_name
            if use_dedicated_subdir
            else self.profile_dir
        )
        user_data_path.mkdir(parents=True, exist_ok=True)

        launch_kwargs = {
            "user_data_dir": str(user_data_path),
            "headless": use_headless,
            "args": ["--disable-blink-features=AutomationControlled"],
            "locale": "vi-VN",
            "timezone_id": "Asia/Ho_Chi_Minh",
            "viewport": {"width": 1366, "height": 768},
        }
        try:
            return playwright.chromium.launch_persistent_context(
                channel="chrome",
                **launch_kwargs,
            )
        except Exception:
            return playwright.chromium.launch_persistent_context(**launch_kwargs)

    @staticmethod
    def sleep_jitter() -> None:
        """Sleeps for a randomized human-like jitter interval."""
        delay = random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX)
        time.sleep(delay)

    @staticmethod
    def scroll_page(page: Page, steps: int = 6, delta_y: int = 900, wait_ms: int = 1000) -> None:
        """Scrolls down a Playwright page incrementally to trigger lazy-loaded elements and XHR calls."""
        for _ in range(steps):
            page.mouse.wheel(0, delta_y)
            page.wait_for_timeout(wait_ms)

    @abstractmethod
    def scrape_target(
        self,
        target: CrawlTarget,
        max_pages: Optional[int] = None,
        force_restart: bool = False,
        **kwargs,
    ) -> CrawlResult:
        """Executes Playwright crawl for a single target and persists raw records into Bronze storage."""
        pass

    def scrape_targets(
        self,
        targets: List[CrawlTarget],
        max_pages: Optional[int] = None,
        force_restart: bool = False,
        **kwargs,
    ) -> List[CrawlResult]:
        """Iteratively executes Playwright crawl for a list of targets."""
        results: List[CrawlResult] = []
        for target in targets:
            logger.info(f"--- Starting Playwright crawl for target: {target.display_name} ---")
            res = self.scrape_target(
                target=target,
                max_pages=max_pages,
                force_restart=force_restart,
                **kwargs,
            )
            results.append(res)
        return results
