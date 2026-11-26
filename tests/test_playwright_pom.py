import json
import unittest
from playwright.sync_api import sync_playwright

from src.models.target import CrawlTarget
from src.scraper.pages.shopee_page import ShopeeSearchPage
from src.scraper.pages.tiki_page import TikiSearchPage


class TestPlaywrightPOM(unittest.TestCase):
    """QA Automation / SDET E2E Test Suite verifying Playwright Page Object Models
    using `page.route()` network interception mocking (zero external network required).
    """

    def test_tiki_pom_expect_response_and_dom_fallback(self) -> None:
        mock_api_payload = {
            "data": [
                {
                    "id": 990001,
                    "name": "Laptop Gaming ASUS ROG Strix",
                    "price": 25990000,
                    "original_price": 29990000,
                    "quantity_sold": {"value": 120},
                },
                {
                    "id": 990002,
                    "name": "Laptop Dell XPS 13",
                    "price": 31500000,
                    "original_price": 34000000,
                    "quantity_sold": {"value": 85},
                },
            ]
        }
        mock_html = """
        <html><body>
          <h1>Tiki Mock Listing</h1>
          <a class="product-item" href="https://tiki.vn/laptop-asus-p990001.html">
            <h3>Laptop Gaming ASUS ROG Strix</h3>
            <div class="price-discount__price">25.990.000 ₫</div>
          </a>
          <script>
            fetch("https://tiki.vn/api/personalish/v1/blocks/listings?category=1846&page=1");
          </script>
        </body></html>
        """

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            # QA Tester Pattern: Mock both HTML shell and XHR API via page.route()
            page.route("https://tiki.vn/laptop-may-vi-tinh-linh-kien/c1846*", lambda route: route.fulfill(
                status=200, content_type="text/html; charset=utf-8", body=mock_html
            ))
            page.route("**/api/personalish/v1/blocks/listings*", lambda route: route.fulfill(
                status=200, content_type="application/json", body=json.dumps(mock_api_payload)
            ))

            tiki_pom = TikiSearchPage(page)
            target = CrawlTarget(
                target_id="tiki_1846",
                name="Laptop - Máy Vi Tính",
                source="tiki",
                keyword="laptop",
                category_id=1846,
                url_key="laptop-may-vi-tinh-linh-kien",
            )

            items = tiki_pom.open_page_and_capture(target=target, page_num=1)
            self.assertEqual(len(items), 2)
            self.assertEqual(items[0]["id"], 990001)
            self.assertEqual(items[1]["name"], "Laptop Dell XPS 13")

            # Verify DOM fallback Locator extraction independently
            dom_items = tiki_pom.extract_dom_products()
            self.assertEqual(len(dom_items), 1)
            self.assertEqual(dom_items[0]["id"], 990001)
            self.assertEqual(dom_items[0]["price"], 25990000)

            context.close()
            browser.close()

    def test_shopee_pom_expect_response_and_pagination_click(self) -> None:
        mock_page1_api = {
            "items": [
                {"itemid": 555001, "shopid": 88801, "item_card_displayed_asset": {"name": "MacBook Air M3"}},
            ]
        }
        mock_page2_api = {
            "items": [
                {"itemid": 555002, "shopid": 88802, "item_card_displayed_asset": {"name": "ThinkPad X1 Carbon"}},
            ]
        }
        mock_html = """
        <html><body>
          <div data-sqe="item">
            <a data-sqe="link" href="https://shopee.vn/MacBook-Air-M3-i.88801.555001">MacBook Air M3</a>
          </div>
          <button class="shopee-icon-button--right" onclick="fetch('https://shopee.vn/api/v4/search/search_items?keyword=laptop&page=1')">Next</button>
          <script>
            fetch("https://shopee.vn/api/v4/search/search_items?keyword=laptop&page=0");
          </script>
        </body></html>
        """

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            page.route("https://shopee.vn/search*", lambda route: route.fulfill(
                status=200, content_type="text/html; charset=utf-8", body=mock_html
            ))
            page.route("**/api/v4/search/search_items*page=0*", lambda route: route.fulfill(
                status=200, content_type="application/json", body=json.dumps(mock_page1_api)
            ))
            page.route("**/api/v4/search/search_items*page=1*", lambda route: route.fulfill(
                status=200, content_type="application/json", body=json.dumps(mock_page2_api)
            ))

            shopee_pom = ShopeeSearchPage(page)
            target = CrawlTarget(
                target_id="shopee_11035954",
                name="Máy Tính & Laptop",
                source="shopee",
                keyword="laptop",
                category_id=11035954,
            )

            # 1. Verify Page 1 synchronized via expect_response
            p1_items = shopee_pom.open_search_page_and_capture(target=target, page_num=1, use_headless=True)
            self.assertEqual(len(p1_items), 1)
            self.assertEqual(p1_items[0]["itemid"], 555001)

            # 2. Verify clicking real pagination Next button captures Page 2 API
            p2_items = shopee_pom.click_next_page_and_capture()
            self.assertEqual(len(p2_items), 1)
            self.assertEqual(p2_items[0]["itemid"], 555002)

            # 3. Verify DOM fallback Locator extraction
            dom_items = shopee_pom.extract_dom_products()
            self.assertEqual(len(dom_items), 1)
            self.assertIn("555001", dom_items[0]["link"])

            context.close()
            browser.close()


if __name__ == "__main__":
    unittest.main()
