import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote
from tqdm import tqdm

from playwright.sync_api import sync_playwright

from config.settings import (
    BROWSER_PROFILE_DIR,
    BROWSER_TIMEOUT_MS,
    DEFAULT_DELAY_MAX,
    DEFAULT_DELAY_MIN,
    DEFAULT_HEADLESS,
    SHOPEE_BASE_URL,
    SHOPEE_COOKIES,
    SHOPEE_SEARCH_URL,
)
from src.models.target import CrawlResult, CrawlTarget
from src.scraper.base_scraper import BaseScraper
from src.scraper.checkpoint import CheckpointManager
from src.storage.bronze_writer import BronzeWriter
from src.utils.logger import setup_logger

logger = setup_logger("shopee_scraper")


class ShopeeScraper(BaseScraper):
    """Playwright-based Shopee scraper with session persistence, WAF bypass, and Bronze ingestion."""

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
        **kwargs,
    ) -> CrawlResult:
        """Scrapes listing pages for a single CrawlTarget using Playwright."""
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
            f"Starting Shopee ingestion for '{target.name}' (Keyword: '{target.keyword}', ID: {target_key}) "
            f"from page {start_page} (Max pages: {max_pages or 'All'}, Headless: {use_headless})..."
        )

        current_page = start_page
        total_ingested = 0
        pbar = tqdm(desc=f"Shopee: {target.name[:20]}", unit="page", initial=start_page - 1)
        partition_dir_str = None

        with sync_playwright() as p:
            context = self.launch_persistent_context(playwright=p, headless=use_headless)

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

            captured_api_items: List[Dict[str, Any]] = []

            def on_response(resp):
                if any(k in resp.url for k in ("search/search_items", "search_items", "rcmd_items")):
                    try:
                        data = resp.json()
                        items = data.get("items")
                        if not items and isinstance(data.get("data"), dict):
                            items = data["data"].get("items")
                        if items and isinstance(items, list):
                            captured_api_items.extend(items)
                            logger.info(f"Captured {len(items)} items from Shopee API ({resp.url.split('?')[0]})")
                    except Exception:
                        pass

            page.on("response", on_response)

            try:
                while True:
                    if max_pages and current_page >= (start_page + max_pages):
                        logger.info(f"Reached configured limit of {max_pages} pages.")
                        break

                    shopee_page_index = current_page - 1
                    target_url = f"{SHOPEE_SEARCH_URL}?keyword={quote(target.keyword)}&page={shopee_page_index}"

                    captured_api_items.clear()

                    # Jitter delay
                    delay = random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX)
                    time.sleep(delay)

                    try:
                        page.goto(target_url, timeout=BROWSER_TIMEOUT_MS, wait_until="domcontentloaded")
                        try:
                            page.wait_for_load_state("networkidle", timeout=8000)
                        except Exception:
                            pass
                    except Exception as e:
                        logger.error(f"Error navigating to {target_url}: {e}")
                        break

                    # Check if redirected to login or verification page
                    page_body_text = page.evaluate("() => document.body ? document.body.innerText : ''")
                    needs_auth = (
                        "buyer/login" in page.url
                        or "/verify/" in page.url
                        or "Có vẻ như bạn chưa đăng nhập" in page_body_text
                        or "Cần đăng nhập" in page_body_text
                    )
                    if needs_auth:
                        if not use_headless:
                            logger.warning(
                                f"Shopee yeu cau dang nhap/xac thuc tai URL: {page.url}. "
                                "Vui long hoan tat dang nhap hoac xac thuc ngay tren cua so Chrome dang mo (toi da 120s)..."
                            )
                            wait_start = time.time()
                            while time.time() - wait_start < 120:
                                page.wait_for_timeout(2000)
                                cur_url = page.url
                                if "buyer/login" not in cur_url and "/verify/" not in cur_url:
                                    logger.info("Da vuot qua buoc dang nhap/xac thuc! Dang tai lai trang tim kiem...")
                                    page.wait_for_timeout(3000)
                                    page.goto(target_url, timeout=BROWSER_TIMEOUT_MS, wait_until="domcontentloaded")
                                    try:
                                        page.wait_for_load_state("networkidle", timeout=8000)
                                    except Exception:
                                        pass
                                    break
                        else:
                            logger.warning(
                                f"Shopee yeu cau dang nhap/xac thuc (URL: {page.url})! "
                                "Vui long chay kem cờ '--head' de dang nhap truc tiep tren cua so Chrome."
                            )
                            break

                    # Scroll down to trigger lazy loading of products
                    for _ in range(6):
                        page.mouse.wheel(0, 900)
                        page.wait_for_timeout(1200)

                    # Extract products: prefer captured API items, fallback to DOM
                    final_products = []
                    if captured_api_items:
                        final_products = list(captured_api_items)
                    else:
                        dom_products = page.evaluate("""
                            () => {
                                const list = [];
                                const seen = new Set();
                                const anchors = Array.from(
                                    document.querySelectorAll('a[data-sqe="link"], a[href*="-i."], a[href*="/product/"], [data-sqe="item"] a')
                                );
                                anchors.forEach((a, idx) => {
                                    const text = (a.innerText || '').trim();
                                    const href = a.href;
                                    if (text && href && !seen.has(href)) {
                                        seen.add(href);
                                        list.push({
                                            dom_id: idx,
                                            title: text.replace(/\\n+/g, ' | '),
                                            link: href,
                                            extracted_at: new Date().toISOString()
                                        });
                                    }
                                });
                                return list;
                            }
                        """)
                        if dom_products:
                            logger.info(f"Extracted {len(dom_products)} items via DOM fallback.")
                        final_products = dom_products

                    if not final_products:
                        logger.warning(f"Current page URL when 0 products found: {page.url}")
                        if current_page == 1:
                            logger.warning(
                                f"Chua lay duoc san pham tai trang {current_page} cho '{target.name}'. "
                                "He thong KHONG danh dau hoan thanh de ban co the thu lai sau."
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

                    # Write raw items to Bronze Storage
                    output_file = self.bronze_writer.write_page(
                        target=target,
                        records=final_products,
                        page_number=current_page,
                    )
                    partition_dir_str = str(output_file.parent)

                    page_count = len(final_products)
                    total_ingested += page_count

                    # Update Checkpoint
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
                try:
                    page.remove_listener("response", on_response)
                except Exception:
                    pass
                pbar.close()
                context.close()

        logger.info(
            f"Finished scraping target '{target.name}'. Total records ingested: {total_ingested}."
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
