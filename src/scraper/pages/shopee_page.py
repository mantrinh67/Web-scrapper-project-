import time
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from playwright.sync_api import Page, Response

from config.settings import SHOPEE_BASE_URL, SHOPEE_SEARCH_URL
from src.models.target import CrawlTarget
from src.scraper.pages.base_page import BasePage
from src.utils.logger import setup_logger

logger = setup_logger("pom_shopee_page")


class ShopeeSearchPage(BasePage):
    """Page Object Model (POM) for Shopee Search & Listing Pages following QA Tester patterns:
    - Uses `page.expect_response(...)` synchronized with UI navigation / pagination clicks.
    - Detects Login / CAPTCHA gates transparently (never bypasses CAPTCHA; waits for user in --head mode).
    - Captures screenshots for QA diagnostics when zero items are rendered.
    """

    SEARCH_INPUT = 'input.shopee-searchbar-input__input, input[aria-label*="Tìm"], input[placeholder*="Tìm"]'
    PRODUCT_LINK = 'a[data-sqe="link"], a[href*="-i."], a[href*="/product/"], [data-sqe="item"] a'
    NEXT_PAGE_BTN = 'button.shopee-icon-button--right, a.shopee-icon-button--right'

    def __init__(self, page: Page):
        super().__init__(page)

    @staticmethod
    def _is_search_api(resp: Response) -> bool:
        return resp.status == 200 and any(
            k in resp.url for k in ("search/search_items", "search_items", "rcmd_items")
        )

    @staticmethod
    def _parse_api_response(resp: Optional[Response]) -> List[Dict[str, Any]]:
        if resp is None:
            return []
        try:
            data = resp.json()
            items = data.get("items")
            if not items and isinstance(data.get("data"), dict):
                items = data["data"].get("items")
            return items if isinstance(items, list) else []
        except Exception:
            return []

    def is_auth_or_captcha_gate(self) -> bool:
        """Detects whether Shopee redirected to login or user-verification page."""
        url = self.page.url
        if "buyer/login" in url or "/verify/" in url:
            return True
        try:
            body_text = self.page.evaluate("() => document.body ? document.body.innerText : ''")
            return (
                "Có vẻ như bạn chưa đăng nhập" in body_text
                or "Cần đăng nhập" in body_text
                or "Xác minh" in body_text
            )
        except Exception:
            return False

    def wait_for_manual_verification(self, max_wait_sec: int = 120) -> bool:
        """In visible `--head` mode, pauses cleanly so the human user can complete login/CAPTCHA."""
        logger.warning(
            f"[ShopeePOM] Phat hien cong dang nhap/xac thuc ({self.page.url}). "
            f"Vui long thao tac truc tiep tren cua so Chrome (cho toi da {max_wait_sec}s)..."
        )
        start = time.time()
        while time.time() - start < max_wait_sec:
            self.page.wait_for_timeout(2000)
            if not self.is_auth_or_captcha_gate():
                logger.info("[ShopeePOM] Nguoi dung da hoan tat dang nhap/xac thuc!")
                self.page.wait_for_timeout(2000)
                return True
        return False

    def open_search_page_and_capture(
        self,
        target: CrawlTarget,
        page_num: int,
        use_headless: bool = True,
    ) -> List[Dict[str, Any]]:
        """Opens Shopee search page using Playwright `expect_response` synchronization."""
        shopee_page_idx = page_num - 1
        target_url = f"{SHOPEE_SEARCH_URL}?keyword={quote(target.keyword)}&page={shopee_page_idx}"

        resp = self.expect_api_response(
            action_fn=lambda: self.navigate(target_url),
            url_predicate=self._is_search_api,
            timeout_ms=12000,
        )

        # Check login/CAPTCHA gate
        if self.is_auth_or_captcha_gate():
            self.capture_diagnostic_screenshot(tag=f"shopee_gate_p{page_num}")
            if not use_headless and self.wait_for_manual_verification(max_wait_sec=120):
                resp = self.expect_api_response(
                    action_fn=lambda: self.navigate(target_url),
                    url_predicate=self._is_search_api,
                    timeout_ms=12000,
                )
            else:
                return []

        items = self._parse_api_response(resp)
        self.smooth_scroll(steps=5, delta_y=900, wait_ms=900)

        if items:
            logger.info(f"[ShopeePOM] Synchronized {len(items)} items via expect_response (page {page_num}).")
            return items

        # Fallback: DOM extraction via Playwright Locators
        dom_items = self.extract_dom_products()
        if dom_items:
            logger.info(f"[ShopeePOM] Extracted {len(dom_items)} items via Playwright DOM locators.")
            return dom_items

        self.capture_diagnostic_screenshot(tag=f"shopee_empty_p{page_num}")
        return []

    def click_next_page_and_capture(self) -> List[Dict[str, Any]]:
        """Clicks Shopee's real pagination Next button using POM Locator + `expect_response`."""
        next_btn = self.page.locator(self.NEXT_PAGE_BTN).first
        if next_btn.count() == 0 or not next_btn.is_visible():
            return []

        resp = self.expect_api_response(
            action_fn=lambda: next_btn.click(),
            url_predicate=self._is_search_api,
            timeout_ms=12000,
        )
        items = self._parse_api_response(resp)
        self.smooth_scroll(steps=5, delta_y=900, wait_ms=800)
        return items or self.extract_dom_products()

    def extract_dom_products(self) -> List[Dict[str, Any]]:
        """Extracts product cards via semantic Playwright DOM locators."""
        try:
            self.page.locator(self.PRODUCT_LINK).first.wait_for(state="attached", timeout=4000)
        except Exception:
            return []

        return self.page.evaluate("""
            (selector) => {
                const list = [];
                const seen = new Set();
                const anchors = Array.from(document.querySelectorAll(selector));
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
        """, self.PRODUCT_LINK)
