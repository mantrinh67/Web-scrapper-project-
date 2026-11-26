from pathlib import Path
from typing import Any, Dict, Optional
from tqdm import tqdm

from playwright.sync_api import sync_playwright

from config.settings import DEFAULT_HEADLESS
from src.models.target import CrawlResult, CrawlTarget
from src.scraper.base_scraper import BaseScraper
from src.scraper.checkpoint import CheckpointManager
from src.scraper.pages.base_page import BasePage
from src.scraper.pages.tiki_page import TikiSearchPage
from src.storage.bronze_writer import BronzeWriter
from src.utils.logger import setup_logger

logger = setup_logger("tiki_scraper")


class TikiScraper(BaseScraper):
    """Playwright POM-based Tiki scraper applying QA Automation Tester patterns:
    - Page Object Model (`TikiSearchPage`)
    - Network synchronization via `page.expect_response(...)`
    - Automatic Playwright Trace recording on zero-result diagnostics
    """

    def __init__(
        self,
        checkpoint_mgr: Optional[CheckpointManager] = None,
        bronze_writer: Optional[BronzeWriter] = None,
        profile_dir: Optional[Path] = None,
        headless: bool = DEFAULT_HEADLESS,
    ):
        super().__init__(
            checkpoint_mgr=checkpoint_mgr,
            bronze_writer=bronze_writer,
            profile_dir=profile_dir,
            headless=headless,
        )

    @property
    def source_name(self) -> str:
        return "tiki"

    def scrape_target(
        self,
        target: CrawlTarget,
        max_pages: Optional[int] = None,
        force_restart: bool = False,
        headless: Optional[bool] = None,
        enable_trace: bool = False,
        **kwargs,
    ) -> CrawlResult:
        """Executes Playwright POM crawl for Tiki target with Tracing & Bronze persistence."""
        use_headless = self.headless if headless is None else headless
        target_key = str(target.category_id) if target.category_id else target.target_id

        if force_restart:
            self.checkpoint_mgr.reset(target_key)

        progress = self.checkpoint_mgr.get_progress(target_key)
        start_page = progress["last_page"] + 1

        if progress.get("completed", False) and not force_restart:
            logger.info(
                f"Target '{target.name}' ({target_key}) is already marked completed. "
                "Use --reset-checkpoint to restart."
            )
            return CrawlResult(
                target=target,
                status="already_completed",
                ingested_records=0,
                last_page=progress["last_page"],
            )

        logger.info(
            f"Starting Playwright POM Tiki crawl for '{target.name}' (ID: {target_key}) "
            f"from page {start_page} (Max pages: {max_pages or 'All'}, Headless: {use_headless})..."
        )

        current_page = start_page
        total_ingested = 0
        pbar = tqdm(desc=f"Tiki POM: {target.name[:18]}", unit="page", initial=start_page - 1)
        partition_dir_str = None
        save_trace = enable_trace

        with sync_playwright() as p:
            context = self.launch_persistent_context(
                playwright=p,
                headless=use_headless,
                use_dedicated_subdir=True,
            )
            BasePage.start_tracing(context)
            page = context.pages[0] if context.pages else context.new_page()
            tiki_page = TikiSearchPage(page)

            try:
                while True:
                    if max_pages and current_page >= (start_page + max_pages):
                        logger.info(f"Reached configured limit of {max_pages} pages.")
                        break

                    self.sleep_jitter()
                    products = tiki_page.open_page_and_capture(target=target, page_num=current_page)

                    if not products:
                        if current_page == 1:
                            tiki_page.capture_diagnostic_screenshot(tag=f"tiki_empty_{target_key}")
                            save_trace = True
                        logger.info(f"No more products found at page {current_page}. Marking completed.")
                        self.checkpoint_mgr.update_progress(
                            target_key=target_key,
                            page=current_page,
                            added_records=0,
                            completed=True,
                        )
                        break

                    output_file = self.bronze_writer.write_page(
                        target=target,
                        records=products,
                        page_number=current_page,
                    )
                    partition_dir_str = str(output_file.parent)

                    page_count = len(products)
                    total_ingested += page_count

                    self.checkpoint_mgr.update_progress(
                        target_key=target_key,
                        page=current_page,
                        added_records=page_count,
                        completed=False,
                    )

                    pbar.update(1)
                    pbar.set_postfix({"records": total_ingested})
                    current_page += 1

            finally:
                BasePage.stop_tracing(context, trace_name=f"tiki_{target_key}", save=save_trace)
                pbar.close()
                context.close()

        logger.info(
            f"Finished Playwright POM Tiki crawl for '{target.name}'. Total records ingested: {total_ingested}."
        )
        return CrawlResult(
            target=target,
            status="success",
            ingested_records=total_ingested,
            last_page=current_page - 1,
            partition_dir=partition_dir_str,
        )

    def scrape_category(
        self,
        category_id: int,
        max_pages: Optional[int] = None,
        force_restart: bool = False,
        headless: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Backward compatible helper method."""
        from src.core.category_manager import CategoryManager
        cm = CategoryManager()
        target = cm.get_by_id(category_id, source="tiki")
        if not target:
            target = CrawlTarget(
                target_id=f"tiki_{category_id}",
                name=f"Tiki Category {category_id}",
                source="tiki",
                keyword=str(category_id),
                category_id=category_id,
            )
        res = self.scrape_target(
            target=target,
            max_pages=max_pages,
            force_restart=force_restart,
            headless=headless,
        )
        return {
            "category_id": category_id,
            "status": res.status,
            "ingested_records": res.ingested_records,
            "last_page": res.last_page,
            "bronze_partition": res.partition_dir,
        }
