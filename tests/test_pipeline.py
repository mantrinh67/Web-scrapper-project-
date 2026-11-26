import json
import tempfile
import unittest
from pathlib import Path

import polars as pl

from config import settings
from src.analytics.gold_builder import GoldBuilder
from src.core.category_manager import CategoryManager
from src.models.target import CrawlTarget
from src.scraper.checkpoint import CheckpointManager
from src.storage.bronze_writer import BronzeWriter
from src.transform.shopee_transformer import ShopeeTransformer
from src.transform.silver_writer import SilverPipeline
from src.transform.tiki_transformer import TikiTransformer


class TestMedallionPipeline(unittest.TestCase):
    """Comprehensive automated unit test suite across Bronze, Silver, and Gold layers (TASK-12)."""

    def test_01_settings_and_category_manager(self):
        """Verifies legacy CATEGORIES is removed and CategoryManager loads 17 catalog targets."""
        self.assertFalse(hasattr(settings, "CATEGORIES"))
        cm = CategoryManager()
        all_targets = cm.list_all()
        self.assertEqual(len(all_targets), 17)
        shopee_phone = cm.get_by_id(11036030, source="shopee")
        self.assertIsNotNone(shopee_phone)
        self.assertEqual(shopee_phone.keyword, "dien thoai")

    def test_02_checkpoint_manager_lifecycle(self):
        """Verifies CheckpointManager tracks page progress, record counts, and resets cleanly."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            cp_file = Path(tmp_dir) / "checkpoint.json"
            cp = CheckpointManager(cp_file)

            cp.update_progress("11036030", page=1, added_records=60, completed=False)
            cp.update_progress("11036030", page=2, added_records=60, completed=True)

            prog = cp.get_progress("11036030")
            self.assertEqual(prog["last_page"], 2)
            self.assertEqual(prog["total_records"], 120)
            self.assertTrue(prog["completed"])

            cp.reset("11036030")
            self.assertEqual(cp.get_progress("11036030")["last_page"], 0)

    def test_03_bronze_writer_lazy_partitioning(self):
        """Verifies BronzeWriter refuses empty records without creating empty folders."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            bw = BronzeWriter(base_dir=Path(tmp_dir))
            target = CrawlTarget(
                target_id="shopee_999",
                name="Test Target",
                source="shopee",
                keyword="test",
                category_id=999,
            )
            part_dir = bw.get_partition_dir(target, ingest_date="2026-11-19", create=False)
            self.assertFalse(part_dir.exists())

            with self.assertRaises(ValueError):
                bw.write_page(target, records=[], page_number=1, ingest_date="2026-11-19")
            self.assertFalse(part_dir.exists())

            out_file = bw.write_page(
                target,
                records=[{"itemid": 101, "name": "Valid Item"}],
                page_number=1,
                ingest_date="2026-11-19",
            )
            self.assertTrue(out_file.exists())

    def test_04_shopee_and_tiki_transformers(self):
        """Verifies Shopee (/100,000 price scaling, garbage removal) and Tiki (nested sold dict & null safety)."""
        st = ShopeeTransformer()
        tt = TikiTransformer()

        shopee_raw = {
            "_metadata": {"source": "shopee", "ingested_at": "2026-10-15T10:00:00Z", "category_id": 11036030, "page": 1},
            "raw_data": {
                "itemid": 50768610437,
                "shopid": 1906961054,
                "json_data": "HEAVY_BASE64_BLOB",
                "ads_data_tms": "ADS_TRACKING",
                "item_card_displayed_asset": {"name": "OPPO Find X9", "shop_location": "Hà Nội"},
                "item_data": {
                    "global_brand": {"display_name": "OPPO"},
                    "item_card_display_price": {"price": 1740000000000, "original_price": 2000000000000},
                    "item_rating": {"rating_star": 4.9, "rating_count": [550, 5, 1, 4, 14, 526]},
                    "item_card_display_sold_count": {"historical_sold_count": 2651},
                },
            },
        }
        s_prod = st.transform_record(shopee_raw, ingest_date="2026-10-15")
        self.assertIsNotNone(s_prod)
        self.assertEqual(s_prod.price, 17400000.0)
        self.assertEqual(s_prod.original_price, 20000000.0)
        self.assertEqual(s_prod.discount_rate, 13.0)
        self.assertEqual(s_prod.review_count, 550)
        self.assertEqual(s_prod.quantity_sold, 2651)
        self.assertNotIn("json_data", s_prod.model_dump())

        tiki_raw = {
            "_metadata": {"source": "tiki", "ingested_at": "2026-10-15T08:00:00Z", "category_id": 1789, "page": 1},
            "raw_data": {
                "id": 278628866,
                "name": "Apple iPhone 17 Pro Max",
                "brand_name": "Apple",
                "price": 33350000,
                "original_price": 33350000,
                "rating_average": 5,
                "review_count": 73,
                "quantity_sold": {"text": "Đã bán 6201", "value": 6201},
            },
        }
        t_prod = tt.transform_record(tiki_raw, ingest_date="2026-10-15")
        self.assertIsNotNone(t_prod)
        self.assertEqual(t_prod.quantity_sold, 6201)
        self.assertEqual(t_prod.brand, "Apple")

    def test_05_silver_deduplication_and_gold_builder(self):
        """Verifies end-to-end Silver deduplication and Gold analytical tables generation."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            silver_dir = tmp_path / "silver"
            gold_dir = tmp_path / "gold"

            tt = TikiTransformer()
            st = ShopeeTransformer()

            rec_old = tt.transform_record({
                "_metadata": {"source": "tiki", "ingested_at": "2026-10-15T08:00:00Z", "category_id": 1789, "page": 1},
                "raw_data": {"id": 100, "name": "iPhone Old", "brand_name": "Apple", "price": 25000000, "quantity_sold": {"value": 10}},
            }, "2026-10-15")
            rec_new = tt.transform_record({
                "_metadata": {"source": "tiki", "ingested_at": "2026-10-15T09:00:00Z", "category_id": 1789, "page": 2},
                "raw_data": {"id": 100, "name": "iPhone Latest", "brand_name": "Apple", "price": 24000000, "quantity_sold": {"value": 50}},
            }, "2026-10-15")
            rec_shopee = st.transform_record({
                "_metadata": {"source": "shopee", "ingested_at": "2026-10-15T10:00:00Z", "category_id": 11036030, "page": 1},
                "raw_data": {
                    "itemid": 200,
                    "shopid": 1,
                    "item_card_displayed_asset": {"name": "Samsung S25"},
                    "item_data": {
                        "global_brand": {"display_name": "Samsung"},
                        "item_card_display_price": {"price": 2000000000000},
                        "item_card_display_sold_count": {"historical_sold_count": 100},
                    },
                },
            }, "2026-10-15")

            deduped_tiki = SilverPipeline.deduplicate([rec_old, rec_new])
            self.assertEqual(len(deduped_tiki), 1)
            self.assertEqual(deduped_tiki[0].name, "iPhone Latest")
            self.assertEqual(deduped_tiki[0].price, 24000000.0)

            pipeline = SilverPipeline(bronze_dir=tmp_path / "bronze", silver_dir=silver_dir)
            pipeline.write_parquet(deduped_tiki, "2026-10-15", "tiki")
            pipeline.write_parquet([rec_shopee], "2026-10-15", "shopee")

            gold = GoldBuilder(silver_dir=silver_dir, gold_dir=gold_dir)
            res = gold.run(top_n_best_sellers=5)
            self.assertEqual(res["status"], "success")
            self.assertEqual(res["silver_input_rows"], 2)

            comp_df = pl.read_parquet(gold_dir / "platform_price_comparison.parquet")
            self.assertEqual(comp_df.height, 2)
            self.assertTrue((comp_df["category_group"] == "Điện Thoại & Smartphone").all())


if __name__ == "__main__":
    unittest.main()
