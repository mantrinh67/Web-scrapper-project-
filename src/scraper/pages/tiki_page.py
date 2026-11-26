from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlencode

from playwright.sync_api import Page, Response

from config.settings import DEFAULT_PAGE_LIMIT, TIKI_LISTING_URL
from src.models.target import CrawlTarget
from src.scraper.pages.base_page import BasePage
from src.utils.logger import setup_logger

logger = setup_logger("pom_tiki_page")


class TikiSearchPage(BasePage):
    """Page Object Model (POM) for Tiki Category & Search Listing Pages."""

    # Semantic Playwright Locators
    SEARCH_INPUT = 'input[data-view-id="main_search_form_input"]'
    PRODUCT_CARD = 'a.product-item, [data-view-id="product_list_item"]'
    NEXT_PAGE_BTN = 'a[data-view-id="product_list_pagination_item"]:has-text(">"), a.arrow.next'

    def __init__(self, page: Page):
        super().__init__(page)

    @staticmethod
    def _is_listing_api(resp: Response) -> bool:
        return (
            resp.status == 200
            and ("api/personalish/v1/blocks/listings" in resp.url or "api/v2/products" in resp.url)
        )

    @staticmethod
    def build_url(target: CrawlTarget, page_num: int) -> str:
        if target.category_id:
            slug = target.url_key or "danh-muc"
            return f"https://tiki.vn/{slug}/c{target.category_id}?page={page_num}"
        return f"https://tiki.vn/search?q={quote(target.keyword)}&page={page_num}"

    def search_via_ui(self, keyword: str) -> List[Dict[str, Any]]:
        """Simulates a real QA user typing into Tiki's search bar and capturing the XHR API response."""
        self.navigate("https://tiki.vn/")

        def _submit():
            self.human_type(self.SEARCH_INPUT, keyword)
            self.page.keyboard.press("Enter")

        resp = self.expect_api_response(_submit, self._is_listing_api, timeout_ms=15000)
        return self._extract_items_from_response(resp)

    def open_page_and_capture(self, target: CrawlTarget, page_num: int) -> List[Dict[str, Any]]:
        """Navigates to Tiki page with Playwright `expect_response` synchronization,
        falling back to BrowserContext API request and DOM locator extraction.
        """
        url = self.build_url(target, page_num)
        resp = self.expect_api_response(
            action_fn=lambda: self.navigate(url),
            url_predicate=self._is_listing_api,
            timeout_ms=12000,
        )
        items = self._extract_items_from_response(resp)
        self.smooth_scroll(steps=4, delta_y=800, wait_ms=600)

        if items:
            logger.info(f"[TikiPOM] Synchronized {len(items)} items via expect_response (page {page_num}).")
            return items

        # Fallback 1: Playwright BrowserContext.request (shares browser cookies & headers)
        params: Dict[str, Any] = {"limit": DEFAULT_PAGE_LIMIT, "page": page_num}
        if target.category_id:
            params["category"] = target.category_id
            if target.url_key:
                params["urlKey"] = target.url_key
        else:
            params["q"] = target.keyword

        api_url = f"{TIKI_LISTING_URL}?{urlencode(params)}"
        try:
            api_resp = self.page.context.request.get(
                api_url,
                headers={"Referer": url, "Accept": "application/json"},
                timeout=self.timeout_ms,
            )
            if api_resp.ok:
                data = api_resp.json().get("data", []) or []
                if data:
                    logger.info(f"[TikiPOM] Captured {len(data)} items via Playwright context.request.")
                    return data
        except Exception as e:
            logger.debug(f"[TikiPOM] context.request note: {e}")

        # Fallback 2: Playwright Locator DOM extraction
        dom_items = self.extract_dom_products()
        if dom_items:
            logger.info(f"[TikiPOM] Extracted {len(dom_items)} items via Playwright DOM locators.")
        return dom_items

    @staticmethod
    def _extract_items_from_response(resp: Optional[Response]) -> List[Dict[str, Any]]:
        if resp is None:
            return []
        try:
            payload = resp.json()
            items = payload.get("data", [])
            return items if isinstance(items, list) else []
        except Exception:
            return []

    def extract_dom_products(self) -> List[Dict[str, Any]]:
        """Extracts product cards from live DOM using Playwright Locators."""
        try:
            self.page.locator(self.PRODUCT_CARD).first.wait_for(state="attached", timeout=4000)
        except Exception:
            return []

        return self.page.evaluate("""
            (selector) => {
                const results = [];
                const cards = Array.from(document.querySelectorAll(selector));
                cards.forEach((card, idx) => {
                    const href = card.href || (card.querySelector('a') ? card.querySelector('a').href : '');
                    const titleEl = card.querySelector('h3, .name, [class*="name"]');
                    const priceEl = card.querySelector('.price-discount__price, [class*="price"]');
                    const name = titleEl ? titleEl.innerText.trim() : (card.innerText || '').split('\\n')[0].trim();
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
        """, self.PRODUCT_CARD)
