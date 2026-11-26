from typing import Dict, List, Optional

from src.core.category_manager import CategoryManager
from src.core.factory import ScraperFactory
from src.models.target import CrawlResult, CrawlTarget
from src.scraper.checkpoint import CheckpointManager
from src.storage.bronze_writer import BronzeWriter
from src.utils.logger import setup_logger

logger = setup_logger("orchestrator")


class CrawlerOrchestrator:
    """Coordinates category search, target resolution, and multi-platform scraping."""

    def __init__(
        self,
        category_mgr: Optional[CategoryManager] = None,
        checkpoint_mgr: Optional[CheckpointManager] = None,
        bronze_writer: Optional[BronzeWriter] = None,
    ):
        self.category_mgr = category_mgr or CategoryManager()
        self.checkpoint_mgr = checkpoint_mgr or CheckpointManager()
        self.bronze_writer = bronze_writer or BronzeWriter()

    def search_categories(self, query: str, source: Optional[str] = None) -> List[CrawlTarget]:
        """Searches catalog for categories matching query string."""
        return self.category_mgr.search(query=query, source=source)

    def list_all_categories(self, source: Optional[str] = None) -> List[CrawlTarget]:
        """Lists all registered categories in catalog."""
        return self.category_mgr.list_all(source=source)

    def resolve_targets(
        self,
        query: str,
        source: str = "shopee",
        allow_custom_fallback: bool = True,
    ) -> List[CrawlTarget]:
        """Resolves matching catalog categories; falls back to custom keyword target if none match."""
        matches = self.category_mgr.search(query=query, source=source)
        if matches:
            return matches

        if allow_custom_fallback:
            logger.info(f"Khong tim thay danh muc co san cho '{query}'. Tao custom target tim kiem.")
            custom_target = self.category_mgr.create_custom_target(query=query, source=source)
            return [custom_target]

        return []

    def crawl_targets(
        self,
        targets: List[CrawlTarget],
        max_pages: int = 2,
        headless: bool = True,
        force_restart: bool = False,
    ) -> List[CrawlResult]:
        """Executes crawl pipeline grouped by platform source."""
        # Group targets by platform
        by_source: Dict[str, List[CrawlTarget]] = {}
        for t in targets:
            by_source.setdefault(t.source.lower(), []).append(t)

        all_results: List[CrawlResult] = []
        for src, src_targets in by_source.items():
            scraper = ScraperFactory.create(
                source=src,
                checkpoint_mgr=self.checkpoint_mgr,
                bronze_writer=self.bronze_writer,
                headless=headless,
            )
            results = scraper.scrape_targets(
                targets=src_targets,
                max_pages=max_pages,
                force_restart=force_restart,
            )
            all_results.extend(results)

        return all_results
