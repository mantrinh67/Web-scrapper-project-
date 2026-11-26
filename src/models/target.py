from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class CrawlTarget(BaseModel):
    """Represents a crawling target, either a formal Category or a custom Keyword Search."""
    target_id: str = Field(description="Unique identifier, e.g. shopee_11036030 or query_ban_phim_co")
    name: str = Field(description="Human-readable name, e.g. 'Điện Thoại & Phụ Kiện' or 'Bàn Phím Cơ'")
    source: str = Field(default="shopee", description="Platform source: 'shopee' or 'tiki'")
    keyword: str = Field(description="Search keyword or query parameter used during crawl")
    category_id: Optional[int] = Field(default=None, description="Official Category ID if available")
    url_path: Optional[str] = Field(default=None, description="Platform URL slug or path")
    url_key: Optional[str] = Field(default=None, description="Tiki urlKey or category slug")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Custom metadata and tags")

    @property
    def display_name(self) -> str:
        cat_str = f" [ID: {self.category_id}]" if self.category_id else " [Custom Query]"
        return f"[{self.source.upper()}] {self.name}{cat_str} (Keyword: '{self.keyword}')"


class CrawlResult(BaseModel):
    """Holds execution result of a single crawl operation."""
    target: CrawlTarget
    status: str = Field(description="'success', 'partial', 'already_completed', or 'failed'")
    ingested_records: int = 0
    last_page: int = 0
    partition_dir: Optional[str] = None
    error_message: Optional[str] = None
