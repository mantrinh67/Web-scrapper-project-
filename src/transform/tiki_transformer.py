from typing import Any, Dict, Optional

from src.transform.base_transformer import BaseTransformer, SilverProduct
from src.utils.logger import setup_logger

logger = setup_logger("tiki_transformer")


class TikiTransformer(BaseTransformer):
    """Normalizes Tiki Bronze JSON records into the Unified Silver Schema."""

    source_name = "tiki"

    def transform_record(
        self,
        bronze_record: Dict[str, Any],
        ingest_date: str,
    ) -> Optional[SilverProduct]:
        meta = bronze_record.get("_metadata") or {}
        raw = bronze_record.get("raw_data") or {}

        product_id = raw.get("id")
        if not product_id:
            logger.debug("Skipping Tiki record without valid id.")
            return None

        name = str(raw.get("name") or "").strip()
        if not name:
            return None

        # Extract nested amplitude metadata when available
        vis_info = raw.get("visible_impression_info") or {}
        amplitude = vis_info.get("amplitude") or {}

        brand = (
            raw.get("brand_name")
            or amplitude.get("brand_name")
            or "Unknown"
        )
        if not brand or str(brand).strip() in ("", "OEM", "No Brand"):
            brand = "Unknown"
        else:
            brand = str(brand).strip()

        price = round(float(raw.get("price") or 0.0), 2)
        raw_orig = raw.get("original_price") or raw.get("list_price") or price
        original_price = round(float(raw_orig), 2) if raw_orig else price
        if original_price < price or original_price <= 0:
            original_price = price

        raw_discount = float(raw.get("discount_rate") or 0.0)
        if raw_discount > 0:
            discount_rate = round(raw_discount, 2)
        elif original_price > price and original_price > 0:
            discount_rate = round((1.0 - (price / original_price)) * 100.0, 2)
        else:
            discount_rate = 0.0

        rating_average = round(float(raw.get("rating_average") or 0.0), 2)
        review_count = int(raw.get("review_count") or amplitude.get("number_of_reviews") or 0)

        # Quantity sold can be a dict {"text": "Đã bán 6201", "value": 6201}, int, or None
        qty_obj = raw.get("quantity_sold")
        if isinstance(qty_obj, dict):
            quantity_sold = int(qty_obj.get("value") or 0)
        elif isinstance(qty_obj, (int, float)):
            quantity_sold = int(qty_obj)
        else:
            quantity_sold = int(amplitude.get("all_time_quantity_sold") or 0)

        shop_id = int(raw.get("seller_id") or amplitude.get("seller_id") or 0)
        shop_name = raw.get("seller_name") or amplitude.get("seller_type")
        location = amplitude.get("origin")

        category_id = meta.get("category_id")
        ingested_at = str(meta.get("ingested_at") or "")
        page = int(meta.get("page") or 1)

        url_path = str(raw.get("url_path") or "").lstrip("/")
        product_url = f"https://tiki.vn/{url_path}" if url_path else f"https://tiki.vn/p{int(product_id)}.html"

        return SilverProduct(
            source=self.source_name,
            product_id=int(product_id),
            shop_id=shop_id,
            shop_name=shop_name,
            name=name,
            brand=brand,
            category_id=int(category_id) if category_id is not None else None,
            price=price,
            original_price=original_price,
            discount_rate=discount_rate,
            rating_average=rating_average,
            review_count=review_count,
            quantity_sold=quantity_sold,
            location=location,
            product_url=product_url,
            ingest_date=ingest_date,
            ingested_at=ingested_at,
            page=page,
        )
