import json
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Union

from config.settings import CATEGORIES_FILE
from src.models.target import CrawlTarget
from src.utils.logger import setup_logger

logger = setup_logger("category_manager")


def strip_accents(text: str) -> str:
    """Removes Vietnamese diacritics and converts to lower case for fuzzy matching."""
    text = text.lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = text.replace("đ", "d")
    return text.strip()


class CategoryManager:
    """Manages catalog of e-commerce categories, fuzzy search, and dynamic target creation."""

    def __init__(self, config_path: Path = CATEGORIES_FILE):
        self.config_path = config_path
        self._registry: Dict[str, CrawlTarget] = {}
        self._load()

    def _load(self) -> None:
        """Loads categories from JSON file into registry."""
        if not self.config_path.exists():
            logger.warning(f"Categories file not found at {self.config_path}")
            return

        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            for source, items in data.items():
                for item in items:
                    target = CrawlTarget(
                        target_id=item["target_id"],
                        name=item["name"],
                        source=source,
                        keyword=item["keyword"],
                        category_id=item.get("category_id"),
                        url_path=item.get("url_path"),
                        url_key=item.get("url_key"),
                        metadata=item.get("metadata", {}),
                    )
                    self._registry[target.target_id] = target

            logger.info(f"Loaded {len(self._registry)} categories into registry.")
        except Exception as e:
            logger.error(f"Error loading categories file: {e}")

    def list_all(self, source: Optional[str] = None) -> List[CrawlTarget]:
        """Lists all registered targets, optionally filtered by platform source."""
        targets = list(self._registry.values())
        if source:
            source = source.lower()
            targets = [t for t in targets if t.source.lower() == source]
        return targets

    def get_by_id(self, target_id: Union[int, str], source: Optional[str] = None) -> Optional[CrawlTarget]:
        """Finds a target by target_id or numeric category_id."""
        str_id = str(target_id).strip()
        # Direct lookup
        if str_id in self._registry:
            return self._registry[str_id]

        for target in self._registry.values():
            if source and target.source.lower() != source.lower():
                continue
            if str(target.category_id) == str_id:
                return target
            if target.target_id == f"{source}_{str_id}":
                return target

        return None

    def search(self, query: str, source: Optional[str] = None) -> List[CrawlTarget]:
        """Searches categories by matching keyword or name (accent-insensitive)."""
        normalized_query = strip_accents(query)
        query_tokens = [tok for tok in re.split(r"\s+", normalized_query) if tok]

        matches: List[CrawlTarget] = []
        for target in self._registry.values():
            if source and target.source.lower() != source.lower():
                continue

            target_text = f"{target.name} {target.keyword} {target.url_path or ''} {target.url_key or ''}"
            normalized_target = strip_accents(target_text)

            # Check if all query tokens are present in target text
            if all(token in normalized_target for token in query_tokens):
                matches.append(target)

        return matches

    def create_custom_target(self, query: str, source: str = "shopee") -> CrawlTarget:
        """Dynamically creates a custom CrawlTarget based on user search query."""
        clean_keyword = query.strip()
        slug = re.sub(r"[^\w\s-]", "", strip_accents(clean_keyword))
        slug = re.sub(r"[\s_-]+", "_", slug).strip("_")

        target_id = f"{source}_custom_{slug}"
        return CrawlTarget(
            target_id=target_id,
            name=f"Tìm kiếm: '{clean_keyword}'",
            source=source,
            keyword=clean_keyword,
            category_id=None,
            url_path=None,
            url_key=slug,
            metadata={"is_custom_query": True},
        )
