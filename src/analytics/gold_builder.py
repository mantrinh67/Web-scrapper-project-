from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import polars as pl

from config.settings import GOLD_DIR, SILVER_DIR
from src.core.category_manager import CategoryManager
from src.utils.logger import setup_logger

logger = setup_logger("gold_builder")

KEYWORD_TO_GROUP = {
    "dien thoai": "Điện Thoại & Smartphone",
    "laptop": "Laptop & Máy Vi Tính",
    "tai nghe": "Thiết Bị Số & Âm Thanh",
    "tai nghe loa": "Thiết Bị Số & Âm Thanh",
    "sach": "Nhà Sách",
    "do gia dung": "Điện Gia Dụng",
    "noi com dien": "Điện Gia Dụng",
    "my pham": "Làm Đẹp & Mỹ Phẩm",
    "quan ao nam": "Thời Trang Nam",
    "quan ao nu": "Thời Trang Nữ",
    "giay nam": "Giày Dép Nam",
    "giay nu": "Giày Dép Nữ",
    "dong ho": "Đồng Hồ",
}


class GoldBuilder:
    """Builds Gold-layer business intelligence and analytical tables from Silver Parquet data."""

    def __init__(
        self,
        silver_dir: Path = SILVER_DIR,
        gold_dir: Path = GOLD_DIR,
        category_mgr: Optional[CategoryManager] = None,
    ):
        self.silver_dir = silver_dir
        self.gold_dir = gold_dir
        self.gold_dir.mkdir(parents=True, exist_ok=True)
        self.category_mgr = category_mgr or CategoryManager()

    def _build_category_lookup_df(self) -> pl.DataFrame:
        """Creates a lookup DataFrame mapping (source, category_id) -> (category_name, category_group)."""
        rows: List[Dict[str, Any]] = []
        for target in self.category_mgr.list_all():
            if target.category_id is not None:
                group_name = KEYWORD_TO_GROUP.get(target.keyword, target.name)
                rows.append({
                    "source": target.source.lower(),
                    "category_id": int(target.category_id),
                    "category_name": target.name,
                    "category_group": group_name,
                })
        return pl.DataFrame(rows)

    def load_silver_enriched(self) -> Optional[pl.DataFrame]:
        """Loads all Silver Parquet files and enriches them with human-readable category names and groups."""
        parquet_files = sorted(self.silver_dir.rglob("*.parquet"))
        if not parquet_files:
            logger.warning(f"No Silver Parquet files found in {self.silver_dir}")
            return None

        silver_df = pl.concat([pl.read_parquet(p) for p in parquet_files], how="vertical_relaxed")
        lookup_df = self._build_category_lookup_df()

        enriched = silver_df.join(
            lookup_df,
            on=["source", "category_id"],
            how="left",
        ).with_columns([
            pl.col("category_name").fill_null("Khác"),
            pl.col("category_group").fill_null("Khác"),
            (pl.col("price") * pl.col("quantity_sold")).round(0).alias("estimated_revenue"),
        ])
        return enriched

    def build_category_brand_stats(self, df: pl.DataFrame) -> Path:
        """TASK-10: Aggregates price, discount, rating, and sales metrics by category and brand."""
        con = duckdb.connect()
        con.register("silver_enriched", df.to_arrow())
        stats_df = con.sql("""
            SELECT
                source,
                category_group,
                category_name,
                brand,
                COUNT(*) AS product_count,
                ROUND(MIN(price), 0) AS min_price,
                ROUND(AVG(price), 0) AS avg_price,
                ROUND(MEDIAN(price), 0) AS median_price,
                ROUND(MAX(price), 0) AS max_price,
                ROUND(AVG(discount_rate), 2) AS avg_discount_rate,
                ROUND(MAX(discount_rate), 2) AS max_discount_rate,
                SUM(quantity_sold) AS total_quantity_sold,
                ROUND(SUM(estimated_revenue), 0) AS total_estimated_revenue,
                ROUND(AVG(CASE WHEN rating_average > 0 THEN rating_average ELSE NULL END), 2) AS avg_rating
            FROM silver_enriched
            GROUP BY source, category_group, category_name, brand
            ORDER BY source, category_group, total_quantity_sold DESC, product_count DESC
        """).pl()

        out_path = self.gold_dir / "category_brand_stats.parquet"
        stats_df.write_parquet(out_path, compression="zstd")
        logger.info(f"Wrote category_brand_stats ({stats_df.height} rows) to {out_path}")
        return out_path

    def build_best_sellers(self, df: pl.DataFrame, top_n_per_source: int = 20) -> Path:
        """TASK-11a: Ranks top best-selling products per platform and per category."""
        con = duckdb.connect()
        con.register("silver_enriched", df.to_arrow())
        best_df = con.sql(f"""
            WITH ranked AS (
                SELECT
                    source,
                    category_group,
                    category_name,
                    product_id,
                    name,
                    brand,
                    price,
                    original_price,
                    discount_rate,
                    quantity_sold,
                    estimated_revenue,
                    rating_average,
                    review_count,
                    product_url,
                    ROW_NUMBER() OVER (
                        PARTITION BY source
                        ORDER BY quantity_sold DESC, review_count DESC, price ASC
                    ) AS rank_in_source,
                    ROW_NUMBER() OVER (
                        PARTITION BY source, category_group
                        ORDER BY quantity_sold DESC, review_count DESC, price ASC
                    ) AS rank_in_category
                FROM silver_enriched
            )
            SELECT *
            FROM ranked
            WHERE rank_in_source <= {int(top_n_per_source)}
            ORDER BY source, rank_in_source
        """).pl()

        out_path = self.gold_dir / "best_sellers.parquet"
        best_df.write_parquet(out_path, compression="zstd")
        logger.info(f"Wrote best_sellers ({best_df.height} rows) to {out_path}")
        return out_path

    def build_platform_price_comparison(self, df: pl.DataFrame) -> Path:
        """TASK-11b: Compares price level, discount depth, and sales volume across Shopee vs Tiki per industry group."""
        con = duckdb.connect()
        con.register("silver_enriched", df.to_arrow())
        comp_df = con.sql("""
            SELECT
                category_group,
                source,
                COUNT(*) AS product_count,
                COUNT(DISTINCT brand) AS distinct_brands,
                ROUND(MIN(price), 0) AS min_price,
                ROUND(AVG(price), 0) AS avg_price,
                ROUND(MEDIAN(price), 0) AS median_price,
                ROUND(MAX(price), 0) AS max_price,
                ROUND(AVG(discount_rate), 2) AS avg_discount_rate,
                SUM(quantity_sold) AS total_quantity_sold,
                ROUND(AVG(quantity_sold), 1) AS avg_sold_per_product,
                ROUND(AVG(CASE WHEN rating_average > 0 THEN rating_average ELSE NULL END), 2) AS avg_rating
            FROM silver_enriched
            GROUP BY category_group, source
            ORDER BY category_group, source
        """).pl()

        out_path = self.gold_dir / "platform_price_comparison.parquet"
        comp_df.write_parquet(out_path, compression="zstd")
        logger.info(f"Wrote platform_price_comparison ({comp_df.height} rows) to {out_path}")
        return out_path

    def run(self, top_n_best_sellers: int = 20) -> Dict[str, Any]:
        """Executes all Gold analytical aggregations and saves Parquet tables."""
        df = self.load_silver_enriched()
        if df is None or df.is_empty():
            return {"status": "empty", "tables": {}}

        stats_path = self.build_category_brand_stats(df)
        best_path = self.build_best_sellers(df, top_n_per_source=top_n_best_sellers)
        comp_path = self.build_platform_price_comparison(df)

        return {
            "status": "success",
            "silver_input_rows": df.height,
            "tables": {
                "category_brand_stats": str(stats_path),
                "best_sellers": str(best_path),
                "platform_price_comparison": str(comp_path),
            },
        }
