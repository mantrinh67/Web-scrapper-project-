from typing import Optional
from src.scraper.base_scraper import BaseScraper
from src.scraper.checkpoint import CheckpointManager
from src.scraper.shopee_scraper import ShopeeScraper
from src.scraper.tiki_scraper import TikiScraper
from src.storage.bronze_writer import BronzeWriter


class ScraperFactory:
    """Factory for instantiating Playwright-based platform scrapers."""

    @staticmethod
    def create(
        source: str,
        checkpoint_mgr: Optional[CheckpointManager] = None,
        bronze_writer: Optional[BronzeWriter] = None,
        headless: bool = True,
    ) -> BaseScraper:
        source_clean = source.strip().lower()
        if source_clean == "shopee":
            return ShopeeScraper(
                checkpoint_mgr=checkpoint_mgr,
                bronze_writer=bronze_writer,
                headless=headless,
            )
        elif source_clean == "tiki":
            return TikiScraper(
                checkpoint_mgr=checkpoint_mgr,
                bronze_writer=bronze_writer,
                headless=headless,
            )
        else:
            raise ValueError(f"Unsupported scraper source: '{source}'. Supported: 'shopee', 'tiki'")
