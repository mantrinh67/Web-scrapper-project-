import unittest
from src.core.category_manager import CategoryManager, strip_accents


class TestCategoryManager(unittest.TestCase):
    def setUp(self) -> None:
        self.manager = CategoryManager()

    def test_strip_accents_vietnamese(self) -> None:
        raw = "Điện Thoại & Phụ Kiện Chính Hãng Đẹp"
        self.assertEqual(strip_accents(raw), "dien thoai & phu kien chinh hang dep")

    def test_list_all_and_filter_by_source(self) -> None:
        all_cats = self.manager.list_all()
        shopee_cats = self.manager.list_all(source="shopee")
        tiki_cats = self.manager.list_all(source="tiki")

        self.assertGreater(len(all_cats), 0)
        self.assertEqual(len(all_cats), len(shopee_cats) + len(tiki_cats))
        self.assertTrue(all(c.source == "shopee" for c in shopee_cats))
        self.assertTrue(all(c.source == "tiki" for c in tiki_cats))

    def test_get_by_id_numeric_and_composite(self) -> None:
        cat_num = self.manager.get_by_id(11036030, source="shopee")
        self.assertIsNotNone(cat_num)
        self.assertEqual(cat_num.category_id, 11036030)

        cat_comp = self.manager.get_by_id("tiki_1789")
        self.assertIsNotNone(cat_comp)
        self.assertEqual(cat_comp.source, "tiki")
        self.assertEqual(cat_comp.category_id, 1789)

        self.assertIsNone(self.manager.get_by_id(999999999))

    def test_search_accent_insensitive(self) -> None:
        matches_accented = self.manager.search("Điện Thoại", source="shopee")
        matches_plain = self.manager.search("dien thoai", source="shopee")

        self.assertGreaterEqual(len(matches_accented), 1)
        self.assertEqual(
            [m.target_id for m in matches_accented],
            [m.target_id for m in matches_plain],
        )
        self.assertEqual(matches_plain[0].category_id, 11036030)

    def test_create_custom_target(self) -> None:
        target = self.manager.create_custom_target("Bàn Phím Cơ Không Dây", source="shopee")
        self.assertEqual(target.target_id, "shopee_custom_ban_phim_co_khong_day")
        self.assertEqual(target.source, "shopee")
        self.assertEqual(target.keyword, "Bàn Phím Cơ Không Dây")
        self.assertIsNone(target.category_id)
        self.assertTrue(target.metadata.get("is_custom_query"))


if __name__ == "__main__":
    unittest.main()
