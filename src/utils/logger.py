import logging
import os
import sys
import tempfile
from pathlib import Path
from config.settings import LOGS_DIR


def setup_logger(name: str = "tiki_scraper") -> logging.Logger:
    """Configures structured logger printing to console and rotating log file (with Vercel /tmp fallback)."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter(
            fmt="[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Console Handler (Standard Output - always works on Local & Vercel)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)

        # File Handler (UTF-8) with fallback to /tmp on read-only serverless filesystems
        try:
            if os.environ.get("VERCEL"):
                log_file = Path(tempfile.gettempdir()) / "crawler.log"
            else:
                LOGS_DIR.mkdir(parents=True, exist_ok=True)
                log_file = LOGS_DIR / "crawler.log"
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setFormatter(formatter)
            file_handler.setLevel(logging.INFO)
            logger.addHandler(file_handler)
        except Exception:
            pass

    return logger
