import tempfile
import unittest
from pathlib import Path

from src.scraper.checkpoint import CheckpointManager


class TestCheckpointManager(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.checkpoint_file = Path(self.temp_dir.name) / "test_checkpoint.json"
        self.manager = CheckpointManager(filepath=self.checkpoint_file)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_initial_empty_progress(self) -> None:
        prog = self.manager.get_progress(11036030)
        self.assertEqual(prog["last_page"], 0)
        self.assertEqual(prog["total_records"], 0)
        self.assertFalse(prog["completed"])

    def test_update_and_persist_progress(self) -> None:
        self.manager.update_progress(11036030, page=1, added_records=60, completed=False)
        self.manager.update_progress(11036030, page=2, added_records=55, completed=True)

        # Reload from disk to verify persistence
        reloaded = CheckpointManager(filepath=self.checkpoint_file)
        prog = reloaded.get_progress(11036030)
        self.assertEqual(prog["last_page"], 2)
        self.assertEqual(prog["total_records"], 115)
        self.assertTrue(prog["completed"])
        self.assertIn("updated_at", prog)

    def test_composite_key_resolution(self) -> None:
        self.manager.update_progress("shopee_11035954", page=3, added_records=180, completed=False)
        prog_by_int = self.manager.get_progress(11035954)
        self.assertEqual(prog_by_int["last_page"], 3)
        self.assertEqual(prog_by_int["total_records"], 180)

    def test_reset_single_and_all(self) -> None:
        self.manager.update_progress(11036030, page=1, added_records=60)
        self.manager.update_progress(1789, page=2, added_records=80)

        self.manager.reset(11036030)
        self.assertEqual(self.manager.get_progress(11036030)["last_page"], 0)
        self.assertEqual(self.manager.get_progress(1789)["last_page"], 2)

        self.manager.reset()
        self.assertEqual(self.manager.get_progress(1789)["last_page"], 0)


if __name__ == "__main__":
    unittest.main()
