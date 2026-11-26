from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from playwright.sync_api import BrowserContext, Page, Response

from config.settings import BROWSER_TIMEOUT_MS, LOGS_DIR
from src.utils.logger import setup_logger

logger = setup_logger("pom_base_page")

TRACES_DIR = LOGS_DIR / "traces"


class BasePage:
    """Page Object Model (POM) Base Class applying QA Automation / SDET best practices:
    - Auto-waiting Playwright Locators instead of hardcoded sleeps.
    - Explicit network synchronization via `page.expect_response(...)`.
    - Built-in Screenshot & Playwright Trace diagnostics on errors.
    """

    def __init__(self, page: Page, timeout_ms: int = BROWSER_TIMEOUT_MS):
        self.page = page
        self.timeout_ms = timeout_ms
        self.page.set_default_timeout(timeout_ms)

    def navigate(self, url: str, wait_until: str = "domcontentloaded") -> None:
        logger.info(f"[POM] Navigating to: {url}")
        self.page.goto(url, timeout=self.timeout_ms, wait_until=wait_until)

    def smooth_scroll(self, steps: int = 5, delta_y: int = 850, wait_ms: int = 700) -> None:
        """Scrolls incrementally like a real user to trigger lazy-rendered product cards."""
        for _ in range(steps):
            self.page.mouse.wheel(0, delta_y)
            self.page.wait_for_timeout(wait_ms)

    def human_type(self, selector: str, text: str, delay_ms: int = 75) -> None:
        """Locates an input using Playwright auto-wait and types character-by-character."""
        locator = self.page.locator(selector).first
        locator.wait_for(state="visible", timeout=10000)
        locator.click()
        locator.fill("")
        locator.type(text, delay=delay_ms)

    def expect_api_response(
        self,
        action_fn: Callable[[], None],
        url_predicate: Callable[[Response], bool],
        timeout_ms: int = 15000,
    ) -> Optional[Response]:
        """Synchronizes a UI action with the expected XHR/Fetch API response (SDET pattern)."""
        try:
            with self.page.expect_response(url_predicate, timeout=timeout_ms) as resp_info:
                action_fn()
            return resp_info.value
        except Exception as e:
            logger.debug(f"[POM] expect_response timeout or note: {e}")
            return None

    def capture_diagnostic_screenshot(self, tag: str = "diagnostic") -> Optional[Path]:
        """Saves a full-page diagnostic screenshot into logs/traces/ for QA debugging."""
        try:
            TRACES_DIR.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_path = TRACES_DIR / f"{tag}_{ts}.png"
            self.page.screenshot(path=str(out_path), full_page=False)
            logger.info(f"[POM] Saved diagnostic screenshot: {out_path}")
            return out_path
        except Exception as e:
            logger.debug(f"Could not capture screenshot: {e}")
            return None

    @staticmethod
    def start_tracing(context: BrowserContext) -> None:
        """Starts Playwright Trace Viewer recording (screenshots + DOM snapshots + network)."""
        try:
            context.tracing.start(screenshots=True, snapshots=True, sources=False)
        except Exception as e:
            logger.debug(f"Tracing start note: {e}")

    @staticmethod
    def stop_tracing(context: BrowserContext, trace_name: str, save: bool = True) -> Optional[Path]:
        """Stops Playwright tracing and optionally writes .zip viewable via `playwright show-trace`."""
        try:
            if save:
                TRACES_DIR.mkdir(parents=True, exist_ok=True)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                out_path = TRACES_DIR / f"{trace_name}_{ts}.zip"
                context.tracing.stop(path=str(out_path))
                logger.info(f"[POM] Saved Playwright Trace: {out_path} (View via: playwright show-trace {out_path})")
                return out_path
            else:
                context.tracing.stop()
                return None
        except Exception as e:
            logger.debug(f"Tracing stop note: {e}")
            return None
