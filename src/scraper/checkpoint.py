import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Union
from config.settings import CHECKPOINT_FILE


class CheckpointManager:
    """Manages crawl state per target/category to ensure resume capability and idempotency."""

    def __init__(self, filepath: Path = CHECKPOINT_FILE):
        self.filepath = filepath
        try:
            self.filepath.parent.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass
        self.state: Dict[str, Any] = self._load()

    def _load(self) -> Dict[str, Any]:
        if self.filepath.exists():
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save(self) -> None:
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2, ensure_ascii=False)

    def _normalize_key(self, key: Union[int, str]) -> str:
        return str(key).strip()

    def get_progress(self, target_key: Union[int, str]) -> Dict[str, Any]:
        key = self._normalize_key(target_key)
        # Check direct key
        if key in self.state:
            return self.state[key]
        # Check if key is raw integer string matching a composite key like shopee_11036030
        for k, v in self.state.items():
            if k.endswith(f"_{key}") or k == key:
                return v
        return {"last_page": 0, "total_records": 0, "completed": False}

    def update_progress(
        self,
        target_key: Union[int, str],
        page: int,
        added_records: int,
        completed: bool = False,
    ) -> None:
        key = self._normalize_key(target_key)
        current = self.get_progress(key)
        current["last_page"] = page
        current["total_records"] = current.get("total_records", 0) + added_records
        current["completed"] = completed
        current["updated_at"] = datetime.now(timezone.utc).isoformat()
        self.state[key] = current
        self._save()

    def reset(self, target_key: Optional[Union[int, str]] = None) -> None:
        if target_key is not None:
            key = self._normalize_key(target_key)
            keys_to_delete = [k for k in self.state if k == key or k.endswith(f"_{key}")]
            for k in keys_to_delete:
                del self.state[k]
        else:
            self.state = {}
        self._save()
