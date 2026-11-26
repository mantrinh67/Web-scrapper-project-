import re
from typing import Any, Dict, Optional

from src.transform.base_transformer import BaseTransformer, SilverProduct
from src.utils.logger import setup_logger

logger = setup_logger("shopee_transformer")

SHOPEE_PRICE_SCALE = 100_000.0


class ShopeeTransformer(BaseTransformer):
    """Flattens deeply nested Shopee Bronze records and normalizes price scale (/ 100,000)."""

    source_name = "shopee"

    def transform_record(
        self,
        bronze_record: Dict[str, Any],
        ingest_date: str,
    ) -> Optional[SilverProduct]:
        meta = bronze_record.get("_metadata") or {}
        raw = bronze_record.get("raw_data") or {}

        item_data = raw.get("item_data") or {}
        item_basic = raw.get("item_basic") or {}
        asset = raw.get("item_card_displayed_asset") or {}

        # 1. Extract product_id and shop_id
        product_id = (
            raw.get("itemid")
            or item_data.get("itemid")
            or item_basic.get("itemid")
        )
        shop_id = (
            raw.get("shopid")
            or item_data.get("shopid")
            or item_basic.get("shopid")
            or 0
        )

        # Fallback for DOM-extracted links if any: e.g. ...-i.12345.67890
        if not product_id and raw.get("link"):
            match = re.search(r"-i\.(\d+)\.(\d+)", str(raw.get("link", "")))
            if match:
                shop_id = int(match.group(1))
                product_id = int(match.group(2))

        if not product_id:
            logger.debug("Skipping Shopee record without valid itemid.")
            return None

        # 2. Extract product name
        name = (
            asset.get("name")
            or item_basic.get("name")
            or raw.get("title")
            or ""
        ).strip()
        if not name:
            return None

        # 3. Extract brand
        global_brand = item_data.get("global_brand") or {}
        brand_info = asset.get("brand_info") or {}
        brand = (
            global_brand.get("display_name")
            or brand_info.get("brand_name")
            or item_data.get("brand")
            or item_basic.get("brand")
            or "Unknown"
        )
        if not brand or str(brand).strip() in ("", "0", "None", "No Brand"):
            brand = "Unknown"
        else:
            brand = str(brand).strip()

        # 4. Extract and normalize prices (Shopee stores VND * 100,000)
        price_card = item_data.get("item_card_display_price") or {}
        raw_price = (
            price_card.get("price")
            or item_basic.get("price")
            or 0
        )
        raw_orig_price = (
            price_card.get("original_price")
            or price_card.get("strikethrough_price")
            or item_basic.get("price_before_discount")
            or raw_price
        )

        price = round(float(raw_price) / SHOPEE_PRICE_SCALE, 2) if raw_price else 0.0
        original_price = (
            round(float(raw_orig_price) / SHOPEE_PRICE_SCALE, 2)
            if raw_orig_price
            else price
        )
        if original_price < price or original_price <= 0:
            original_price = price

        # 5. Discount rate (%)
        raw_discount = price_card.get("discount") or item_basic.get("raw_discount") or 0
        if original_price > price and original_price > 0:
            discount_rate = round((1.0 - (price / original_price)) * 100.0, 2)
        elif raw_discount:
            discount_rate = round(float(raw_discount), 2)
        else:
            discount_rate = 0.0

        # 6. Rating and review counts
        rating_obj = item_data.get("item_rating") or item_basic.get("item_rating") or {}
        rating_average = round(float(rating_obj.get("rating_star") or 0.0), 2)
        rating_counts = rating_obj.get("rating_count")
        if isinstance(rating_counts, list) and len(rating_counts) > 0:
            review_count = int(rating_counts[0] or 0)
        else:
            review_count = 0

        # 7. Sold count
        sold_obj = item_data.get("item_card_display_sold_count") or {}
        quantity_sold = int(
            sold_obj.get("historical_sold_count")
            or sold_obj.get("monthly_sold_count")
            or item_basic.get("historical_sold")
            or item_basic.get("sold")
            or 0
        )

        # 8. Shop info & Location
        shop_data = item_data.get("shop_data") or {}
        shop_name = shop_data.get("shop_name") or item_basic.get("shop_name")
        location = asset.get("shop_location") or item_basic.get("shop_location")

        # 9. Category & Metadata
        category_id = meta.get("category_id") or item_data.get("catid") or item_basic.get("catid")
        ingested_at = str(meta.get("ingested_at") or "")
        page = int(meta.get("page") or 1)
        product_url = raw.get("link") or f"https://shopee.vn/product/{int(shop_id)}/{int(product_id)}"

        return SilverProduct(
            source=self.source_name,
            product_id=int(product_id),
            shop_id=int(shop_id),
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
