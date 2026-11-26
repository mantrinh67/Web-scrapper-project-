import time
from pathlib import Path
from typing import Any, Dict, Optional
from tqdm import tqdm

from playwright.sync_api import sync_playwright

from config.settings import (
    DEFAULT_HEADLESS,
    SHOPEE_BASE_URL,
    SHOPEE_COOKIES,
)
from src.models.target import CrawlResult, CrawlTarget
from src.scraper.base_scraper import BaseScraper
from src.scraper.checkpoint import CheckpointManager
from src.scraper.pages.base_page import BasePage
from src.scraper.pages.shopee_page import ShopeeSearchPage
from src.storage.bronze_writer import BronzeWriter
from src.utils.logger import setup_logger

logger = setup_logger("shopee_scraper")


class ShopeeScraper(BaseScraper):
    """Playwright POM-based Shopee scraper applying QA Automation Tester patterns:
    - Page Object Model (`ShopeeSearchPage`)
    - Network synchronization via `page.expect_response(...)`
    - Automatic Screenshot + Playwright Trace (`logs/traces/*.zip`) on errors/gates
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
        return "shopee"

    def login_session(self) -> None:
        """Opens a visible Chrome window for user to log into Shopee and save persistent session."""
        login_url = f"{SHOPEE_BASE_URL}/buyer/login"
        logger.info("=" * 60)
        logger.info("Khoi chay Google Chrome de luu phien dang nhap Shopee...")
        logger.info("Vui long quet ma QR qua ung dung Shopee hoac nhap OTP tren trinh duyet.")
        logger.info(f"Ho so trinh duyet se duoc luu vinh vien tai: {self.profile_dir}")
        logger.info("=" * 60)

        with sync_playwright() as p:
            context = self.launch_persistent_context(playwright=p, headless=False)
            page = context.pages[0] if context.pages else context.new_page()

            if SHOPEE_COOKIES:
                pw_cookies = [
                    {"name": k, "value": v, "domain": ".shopee.vn", "path": "/"}
                    for k, v in SHOPEE_COOKIES.items()
                ]
                try:
                    context.add_cookies(pw_cookies)
                except Exception:
                    pass

            page.goto(login_url, wait_until="domcontentloaded")
            print("\n>>> Trinh duyet Chrome da mo trang dang nhap Shopee.")
            print(">>> Hay dang nhap tai khoan Shopee (quet ma QR hoac OTP).")
            print(">>> He thong se tu dong theo doi va nhan dien khi dang nhap thanh cong.\n")

            start_time = time.time()
            logged_in = False
            while time.time() - start_time < 300:
                current_url = page.url
                cookies = context.cookies()
                cookie_names = [c.get("name") for c in cookies]

                if ("SPC_U" in cookie_names or "SPC_ST" in cookie_names) and "buyer/login" not in current_url:
                    logger.info(f"Phat hien dang nhap thanh cong! (URL: {current_url}). Dang doi 5s de dong bo session...")
                    page.wait_for_timeout(5000)
                    logged_in = True
                    break
                time.sleep(2)

            if not logged_in:
                logger.info(f"Hoan tat phien dang nhap. URL hien tai: {page.url}")
            else:
                logger.info("Da luu phien dang nhap vao browser profile thanh cong!")

            context.close()

    def scrape_target(
        self,
        target: CrawlTarget,
        max_pages: Optional[int] = None,
        force_restart: bool = False,
        headless: Optional[bool] = None,
        enable_trace: bool = False,
        **kwargs,
    ) -> CrawlResult:
        """Executes Playwright POM crawl for a Shopee target with Tracing & Bronze persistence."""
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
            f"Starting Playwright POM Shopee crawl for '{target.name}' (Keyword: '{target.keyword}', ID: {target_key}) "
            f"from page {start_page} (Max pages: {max_pages or 'All'}, Headless: {use_headless})..."
        )

        current_page = start_page
        total_ingested = 0
        pbar = tqdm(desc=f"Shopee POM: {target.name[:16]}", unit="page", initial=start_page - 1)
        partition_dir_str = None
        save_trace = enable_trace

        with sync_playwright() as p:
            context = self.launch_persistent_context(playwright=p, headless=use_headless)
            BasePage.start_tracing(context)

            if SHOPEE_COOKIES:
                pw_cookies = [
                    {"name": k, "value": v, "domain": ".shopee.vn", "path": "/"}
                    for k, v in SHOPEE_COOKIES.items()
                ]
                try:
                    context.add_cookies(pw_cookies)
                except Exception as e:
                    logger.debug(f"Cookie injection note: {e}")

            page = context.pages[0] if context.pages else context.new_page()
            shopee_page = ShopeeSearchPage(page)

            try:
                while True:
                    if max_pages and current_page >= (start_page + max_pages):
                        logger.info(f"Reached configured limit of {max_pages} pages.")
                        break

                    self.sleep_jitter()
                    if current_page == start_page:
                        final_products = shopee_page.open_search_page_and_capture(
                            target=target,
                            page_num=current_page,
                            use_headless=use_headless,
                        )
                    else:
                        # Prefer clicking real UI pagination button like a QA tester; fallback to URL nav
                        final_products = shopee_page.click_next_page_and_capture()
                        if not final_products:
                            final_products = shopee_page.open_search_page_and_capture(
                                target=target,
                                page_num=current_page,
                                use_headless=use_headless,
                            )

                    if not final_products:
                        if current_page == 1:
                            save_trace = True
                            logger.warning(
                                f"Chua lay duoc san pham tai trang {current_page} cho '{target.name}'. "
                                "Da luu Trace & Screenshot tai logs/traces/ de debug."
                            )
                        else:
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
                        records=final_products,
                        page_number=current_page,
                    )
                    partition_dir_str = str(output_file.parent)

                    page_count = len(final_products)
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
                BasePage.stop_tracing(context, trace_name=f"shopee_{target_key}", save=save_trace)
                pbar.close()
                context.close()

        logger.info(
            f"Finished Playwright POM Shopee crawl for '{target.name}'. Total records ingested: {total_ingested}."
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
        target = cm.get_by_id(category_id, source="shopee")
        if not target:
            target = CrawlTarget(
                target_id=f"shopee_{category_id}",
                name=f"Shopee Category {category_id}",
                source="shopee",
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
