import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlencode
from tqdm import tqdm

from playwright.sync_api import sync_playwright

from config.settings import (
    BROWSER_TIMEOUT_MS,
    DEFAULT_HEADLESS,
    DEFAULT_PAGE_LIMIT,
    TIKI_LISTING_URL,
)
from src.models.target import CrawlResult, CrawlTarget
from src.scraper.base_scraper import BaseScraper
from src.scraper.checkpoint import CheckpointManager
from src.storage.bronze_writer import BronzeWriter
from src.utils.logger import setup_logger

logger = setup_logger("tiki_scraper")


class TikiScraper(BaseScraper):
    """Playwright-based Tiki e-commerce scraper using browser navigation, network API interception,
    Playwright request context, and DOM extraction fallback.
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

    @staticmethod
    def _build_web_url(target: CrawlTarget, page_num: int) -> str:
        """Builds Tiki frontend URL for browser navigation."""
        if target.category_id:
            slug = target.url_key or "danh-muc"
            return f"https://tiki.vn/{slug}/c{target.category_id}?page={page_num}"
        return f"https://tiki.vn/search?q={quote(target.keyword)}&page={page_num}"

    def scrape_target(
        self,
        target: CrawlTarget,
        max_pages: Optional[int] = None,
        force_restart: bool = False,
        headless: Optional[bool] = None,
        **kwargs,
    ) -> CrawlResult:
        """Scrapes Tiki product listings using Playwright browser automation + API interception."""
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
            f"Starting Playwright Tiki ingestion for '{target.name}' (ID: {target_key}) "
            f"from page {start_page} (Max pages: {max_pages or 'All'}, Headless: {use_headless})..."
        )

        current_page = start_page
        total_ingested = 0
        pbar = tqdm(desc=f"Tiki (PW): {target.name[:18]}", unit="page", initial=start_page - 1)
        partition_dir_str = None

        with sync_playwright() as p:
            context = self.launch_persistent_context(
                playwright=p,
                headless=use_headless,
                use_dedicated_subdir=True,
            )
            page = context.pages[0] if context.pages else context.new_page()

            captured_api_items: List[Dict[str, Any]] = []

            def on_response(resp):
                if "api/personalish/v1/blocks/listings" in resp.url or "api/v2/products" in resp.url:
                    try:
                        data = resp.json()
                        items = data.get("data", [])
                        if isinstance(items, list) and items:
                            captured_api_items.extend(items)
                            logger.info(f"Captured {len(items)} items from Tiki network API.")
                    except Exception:
                        pass

            page.on("response", on_response)

            try:
                while True:
                    if max_pages and current_page >= (start_page + max_pages):
                        logger.info(f"Reached configured limit of {max_pages} pages.")
                        break

                    captured_api_items.clear()
                    self.sleep_jitter()

                    # 1. Navigate browser page and intercept XHR API response
                    web_url = self._build_web_url(target, current_page)
                    try:
                        page.goto(web_url, timeout=BROWSER_TIMEOUT_MS, wait_until="domcontentloaded")
                        self.scroll_page(page, steps=4, delta_y=800, wait_ms=800)
                    except Exception as e:
                        logger.warning(f"Browser navigation warning on {web_url}: {e}")

                    products: List[Dict[str, Any]] = []
                    if captured_api_items:
                        products = list(captured_api_items)

                    # 2. If browser XHR wasn't captured yet, fetch via Playwright's BrowserContext.request
                    if not products:
                        params: Dict[str, Any] = {
                            "limit": DEFAULT_PAGE_LIMIT,
                            "page": current_page,
                        }
                        if target.category_id:
                            params["category"] = target.category_id
                            if target.url_key:
                                params["urlKey"] = target.url_key
                        else:
                            params["q"] = target.keyword

                        api_url = f"{TIKI_LISTING_URL}?{urlencode(params)}"
                        try:
                            api_resp = context.request.get(
                                api_url,
                                headers={"Referer": web_url, "Accept": "application/json"},
                                timeout=BROWSER_TIMEOUT_MS,
                            )
                            if api_resp.ok:
                                payload = api_resp.json()
                                products = payload.get("data", []) or []
                        except Exception as e:
                            logger.warning(f"Playwright context.request fallback note: {e}")

                    # 3. DOM extraction fallback via Playwright page.evaluate
                    if not products:
                        dom_items = page.evaluate("""
                            () => {
                                const results = [];
                                const cards = Array.from(
                                    document.querySelectorAll('a.product-item, [data-view-id="product_list_item"]')
                                );
                                cards.forEach((card, idx) => {
                                    const href = card.href || (card.querySelector('a') ? card.querySelector('a').href : '');
                                    const titleEl = card.querySelector('h3, .name, [class*="name"]');
                                    const priceEl = card.querySelector('.price-discount__price, [class*="price"]');
                                    const name = titleEl ? titleEl.innerText.trim() : card.innerText.split('\\n')[0].trim();
                                    const rawPrice = priceEl ? parseInt(priceEl.innerText.replace(/[^0-9]/g, ''), 10) || 0 : 0;
                                    const idMatch = href.match(/-p(\\d+)\\.html/);
                                    const pid = idMatch ? parseInt(idMatch[1], 10) : (idx + 1);
                                    if (name) {
                                        results.push({
                                            id: pid,
                                            name: name,
                                            price: rawPrice,
                                            original_price: rawPrice,
                                            url_path: href.replace('https://tiki.vn/', '')
                                        });
                                    }
                                });
                                return results;
                            }
                        """)
                        if dom_items:
                            logger.info(f"Extracted {len(dom_items)} Tiki items via Playwright DOM fallback.")
                            products = dom_items

                    if not products:
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
                        records=products,
                        page_number=current_page,
                    )
                    partition_dir_str = str(output_file.parent)

                    page_count = len(products)
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
            f"Finished Playwright scraping target '{target.name}'. Total records ingested: {total_ingested}."
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
