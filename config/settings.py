from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = BASE_DIR / "config"
CATEGORIES_FILE = CONFIG_DIR / "categories.json"
DATA_DIR = BASE_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
LOGS_DIR = BASE_DIR / "logs"
CHECKPOINT_FILE = DATA_DIR / "checkpoint.json"
BROWSER_PROFILE_DIR = DATA_DIR / "browser_profile"

# E-Commerce Base Endpoints
SHOPEE_BASE_URL = "https://shopee.vn"
SHOPEE_SEARCH_URL = "https://shopee.vn/search"
TIKI_LISTING_URL = "https://tiki.vn/api/personalish/v1/blocks/listings"

# Browser & Crawling Settings
DEFAULT_PAGE_LIMIT = 50
DEFAULT_DELAY_MIN = 2.0
DEFAULT_DELAY_MAX = 4.0
DEFAULT_HEADLESS = True
BROWSER_TIMEOUT_MS = 45000

# Default HTTP Headers (used by Playwright API requests & Vercel Tiki cloud fetch)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://shopee.vn/",
}

# Optional manual cookies (Default: empty, managed by browser_profile or Web UI)
SHOPEE_COOKIES = {}
