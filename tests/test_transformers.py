import unittest
from src.transform.shopee_transformer import ShopeeTransformer
from src.transform.tiki_transformer import TikiTransformer
from src.transform.silver_writer import SilverPipeline


class TestTransformers(unittest.TestCase):
    def setUp(self) -> None:
        self.shopee_tf = ShopeeTransformer()
        self.tiki_tf = TikiTransformer()

    def test_shopee_transformer_nested_api_and_price_scaling(self) -> None:
        bronze_record = {
            "_metadata": {
                "source": "shopee",
                "ingested_at": "2026-10-15T13:12:52+00:00",
                "category_id": 11036030,
                "page": 1,
            },
            "raw_data": {
                "itemid": 50768610437,
                "shopid": 1906961054,
                "ads_data_tms": "BLOATED_BASE64_STRING",
                "json_data": "BLOATED_JSON_DATA",
                "item_card_displayed_asset": {
                    "name": "  Điện thoại OPPO Find X9 - 16/512  ",
                    "shop_location": "Thành phố Hà Nội",
                },
                "item_data": {
                    "itemid": 50768610437,
                    "shopid": 1906961054,
                    "global_brand": {"display_name": "OPPO"},
                    "shop_data": {"shop_name": "OPPO Official Store"},
                    "item_card_display_price": {
                        "price": 1740000000000,          # 17,400,000 VND * 100,000
                        "original_price": 2000000000000, # 20,000,000 VND * 100,000
                        "discount": 13,
                    },
                    "item_card_display_sold_count": {
                        "historical_sold_count": 350,
                    },
                    "item_rating": {
                        "rating_star": 4.92,
                        "rating_count": [120, 1, 0, 1, 8, 110],
                    },
                },
            },
        }

        product = self.shopee_tf.transform_record(bronze_record, ingest_date="2026-10-15")
        self.assertIsNotNone(product)
        self.assertEqual(product.source, "shopee")
        self.assertEqual(product.product_id, 50768610437)
        self.assertEqual(product.shop_id, 1906961054)
        self.assertEqual(product.name, "Điện thoại OPPO Find X9 - 16/512")
        self.assertEqual(product.brand, "OPPO")
        self.assertEqual(product.price, 17400000.0)
        self.assertEqual(product.original_price, 20000000.0)
        self.assertEqual(product.discount_rate, 13.0)
        self.assertEqual(product.quantity_sold, 350)
        self.assertEqual(product.rating_average, 4.92)
        self.assertEqual(product.review_count, 120)
        self.assertEqual(product.location, "Thành phố Hà Nội")
        self.assertEqual(product.product_url, "https://shopee.vn/product/1906961054/50768610437")

    def test_shopee_transformer_dom_fallback(self) -> None:
        dom_record = {
            "_metadata": {"source": "shopee", "category_id": 11036030, "page": 2},
            "raw_data": {
                "dom_id": 5,
                "title": "Tai nghe Bluetooth Sony WH-1000XM5",
                "link": "https://shopee.vn/Tai-nghe-Sony-i.123456.987654321",
            },
        }
        product = self.shopee_tf.transform_record(dom_record, ingest_date="2026-10-15")
        self.assertIsNotNone(product)
        self.assertEqual(product.shop_id, 123456)
        self.assertEqual(product.product_id, 987654321)
        self.assertEqual(product.name, "Tai nghe Bluetooth Sony WH-1000XM5")

    def test_tiki_transformer_extraction(self) -> None:
        tiki_record = {
            "_metadata": {
                "source": "tiki",
                "ingested_at": "2026-10-15T08:37:19+00:00",
                "category_id": 1789,
                "page": 1,
            },
            "raw_data": {
                "id": 278505394,
                "name": "Điện Thoại Samsung Galaxy A17 LTE",
                "brand_name": "Samsung",
                "price": 5990000,
                "original_price": 6990000,
                "discount_rate": 14,
                "rating_average": 5.0,
                "review_count": 11,
                "quantity_sold": {"text": "Đã bán 324", "value": 324},
                "seller_id": 1,
                "url_path": "dien-thoai-samsung-galaxy-a17-p278505394.html",
                "visible_impression_info": {
                    "amplitude": {
                        "origin": "Việt Nam",
                        "seller_type": "OFFICIAL_STORE",
                    }
                },
            },
        }

        product = self.tiki_tf.transform_record(tiki_record, ingest_date="2026-10-15")
        self.assertIsNotNone(product)
        self.assertEqual(product.source, "tiki")
        self.assertEqual(product.product_id, 278505394)
        self.assertEqual(product.brand, "Samsung")
        self.assertEqual(product.price, 5990000.0)
        self.assertEqual(product.original_price, 6990000.0)
        self.assertEqual(product.discount_rate, 14.0)
        self.assertEqual(product.quantity_sold, 324)
        self.assertEqual(product.location, "Việt Nam")

    def test_deduplication_keeps_latest_record(self) -> None:
        base_rec = {
            "_metadata": {
                "source": "tiki",
                "ingested_at": "2026-10-15T08:00:00+00:00",
                "category_id": 1789,
                "page": 1,
            },
            "raw_data": {
                "id": 1001,
                "name": "iPhone 17",
                "price": 30000000,
                "original_price": 30000000,
                "quantity_sold": {"value": 10},
            },
        }
        newer_rec = {
            "_metadata": {
                "source": "tiki",
                "ingested_at": "2026-10-15T10:00:00+00:00",
                "category_id": 1789,
                "page": 2,
            },
            "raw_data": {
                "id": 1001,
                "name": "iPhone 17",
                "price": 28500000,
                "original_price": 30000000,
                "quantity_sold": {"value": 25},
            },
        }

        p1 = self.tiki_tf.transform_record(base_rec, ingest_date="2026-10-15")
        p2 = self.tiki_tf.transform_record(newer_rec, ingest_date="2026-10-15")
        deduped = SilverPipeline.deduplicate([p1, p2])

        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0].price, 28500000.0)
        self.assertEqual(deduped[0].quantity_sold, 25)


if __name__ == "__main__":
    unittest.main()
