from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class SilverProduct(BaseModel):
    """Unified Silver Schema for normalized e-commerce product records across platforms."""

    source: str = Field(description="E-commerce platform name: 'shopee' or 'tiki'")
    product_id: int = Field(description="Platform-unique product identifier")
    shop_id: int = Field(default=0, description="Platform seller or shop identifier")
    shop_name: Optional[str] = Field(default=None, description="Shop name or seller type")
    name: str = Field(description="Cleaned product title")
    brand: str = Field(default="Unknown", description="Normalized brand name")
    category_id: Optional[int] = Field(default=None, description="Source category ID")
    price: float = Field(description="Current selling price in VND")
    original_price: float = Field(description="Original list price before discount in VND")
    discount_rate: float = Field(default=0.0, description="Discount percentage (0 to 100)")
    rating_average: float = Field(default=0.0, description="Average star rating (0.0 to 5.0)")
    review_count: int = Field(default=0, description="Total number of user ratings/reviews")
    quantity_sold: int = Field(default=0, description="Historical quantity sold")
    location: Optional[str] = Field(default=None, description="Shop location or product origin")
    product_url: str = Field(description="Direct canonical URL to product listing")
    ingest_date: str = Field(description="Partition date YYYY-MM-DD")
    ingested_at: str = Field(description="ISO-8601 ingestion timestamp from Bronze metadata")
    page: int = Field(default=1, description="Page number where record was captured")


class BaseTransformer(ABC):
    """Abstract base class for transforming raw Bronze JSON records into Unified Silver Schema."""

    source_name: str = "unknown"

    @abstractmethod
    def transform_record(
        self,
        bronze_record: Dict[str, Any],
        ingest_date: str,
    ) -> Optional[SilverProduct]:
        """Transforms a single Bronze envelope record ({'_metadata': ..., 'raw_data': ...})
        into a validated SilverProduct instance. Returns None if record is invalid/corrupted.
        """
        raise NotImplementedError

    def transform_batch(
        self,
        bronze_records: List[Dict[str, Any]],
        ingest_date: str,
    ) -> List[SilverProduct]:
        """Transforms a list of Bronze records, skipping invalid entries safely."""
        results: List[SilverProduct] = []
        for rec in bronze_records:
            item = self.transform_record(rec, ingest_date=ingest_date)
            if item is not None:
                results.append(item)
        return results
