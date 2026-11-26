import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from config.settings import BRONZE_DIR
from src.models.target import CrawlTarget
from src.utils.logger import setup_logger

logger = setup_logger("bronze_writer")


class BronzeWriter:
    """Handles Medallion Bronze layer ingestion with Hive partitioning and audit metadata."""

    def __init__(self, base_dir: Path = BRONZE_DIR):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def get_partition_dir(
        self,
        target: CrawlTarget,
        ingest_date: Optional[str] = None,
        create: bool = False,
    ) -> Path:
        """Constructs Hive-style partition directory path without creating it prematurely:
        data/bronze/ingest_date=YYYY-MM-DD/source={source}/category_id={id}/
        """
        if not ingest_date:
            ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        clean_id = target.category_id if target.category_id is not None else target.target_id
        partition_dir = (
            self.base_dir
            / f"ingest_date={ingest_date}"
            / f"source={target.source}"
            / f"category_id={clean_id}"
        )
        if create:
            partition_dir.mkdir(parents=True, exist_ok=True)
        return partition_dir

    def write_page(
        self,
        target: CrawlTarget,
        records: List[Dict[str, Any]],
        page_number: int,
        ingest_date: Optional[str] = None,
    ) -> Path:
        """Wraps non-empty records with Bronze audit metadata and writes to a JSONL partition file."""
        if not records:
            raise ValueError(f"Refusing to create partition or write empty records for '{target.name}' page {page_number}.")

        partition_dir = self.get_partition_dir(target, ingest_date=ingest_date, create=True)
        output_file = partition_dir / f"page_{page_number:04d}.jsonl"

        now_iso = datetime.now(timezone.utc).isoformat()
        enriched_records = []

        for item in records:
            # Avoid double wrapping if already enriched
            if "_metadata" in item and "raw_data" in item:
                enriched_records.append(item)
            else:
                enriched_records.append({
                    "_metadata": {
                        "source": target.source,
                        "ingested_at": now_iso,
                        "target_id": target.target_id,
                        "category_id": target.category_id,
                        "target_name": target.name,
                        "keyword": target.keyword,
                        "page": page_number,
                    },
                    "raw_data": item,
                })

        with open(output_file, "w", encoding="utf-8") as f:
            for rec in enriched_records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        return output_file
