import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import polars as pl

from config.settings import BRONZE_DIR, SILVER_DIR
from src.transform.base_transformer import BaseTransformer, SilverProduct
from src.transform.shopee_transformer import ShopeeTransformer
from src.transform.tiki_transformer import TikiTransformer
from src.utils.logger import setup_logger

logger = setup_logger("silver_pipeline")


class SilverPipeline:
    """Orchestrates Bronze-to-Silver transformation, deduplication, and Parquet storage."""

    def __init__(
        self,
        bronze_dir: Path = BRONZE_DIR,
        silver_dir: Path = SILVER_DIR,
    ):
        self.bronze_dir = bronze_dir
        self.silver_dir = silver_dir
        self.transformers: Dict[str, BaseTransformer] = {
            "shopee": ShopeeTransformer(),
            "tiki": TikiTransformer(),
        }

    @staticmethod
    def deduplicate(products: List[SilverProduct]) -> List[SilverProduct]:
        """Deduplicates records by composite key (source, product_id), keeping the latest ingested_at."""
        sorted_items = sorted(
            products,
            key=lambda x: (x.ingested_at or "", x.page),
            reverse=True,
        )
        seen: set[tuple[str, int]] = set()
        unique_items: List[SilverProduct] = []
        for item in sorted_items:
            key = (item.source, item.product_id)
            if key not in seen:
                seen.add(key)
                unique_items.append(item)
        # Restore deterministic ordering by (source, category_id, page, product_id)
        unique_items.sort(key=lambda x: (x.source, x.category_id or 0, x.page, x.product_id))
        return unique_items

    def write_parquet(
        self,
        products: List[SilverProduct],
        ingest_date: str,
        source: str,
    ) -> Optional[Path]:
        """Writes a deduplicated list of SilverProduct records to a Hive-partitioned Parquet file."""
        if not products:
            return None

        partition_dir = (
            self.silver_dir
            / f"ingest_date={ingest_date}"
            / f"source={source}"
        )
        partition_dir.mkdir(parents=True, exist_ok=True)
        output_file = partition_dir / "products.parquet"

        rows = [p.model_dump() for p in products]
        df = pl.DataFrame(rows)
        df.write_parquet(output_file, compression="zstd")
        logger.info(
            f"Wrote {len(products)} Silver records to {output_file} "
            f"({output_file.stat().st_size / 1024:.2f} KB)."
        )
        return output_file

    def run(self, target_ingest_date: Optional[str] = None) -> Dict[str, Any]:
        """Scans Bronze partitions, transforms records per (ingest_date, source),
        deduplicates by (source, product_id), and writes compressed Parquet files.
        """
        if not self.bronze_dir.exists():
            logger.warning(f"Bronze directory {self.bronze_dir} does not exist.")
            return {"processed_files": 0, "total_raw": 0, "total_silver": 0, "outputs": []}

        grouped_records: Dict[tuple[str, str], List[SilverProduct]] = {}
        processed_files = 0
        total_raw = 0

        for jsonl_path in sorted(self.bronze_dir.rglob("*.jsonl")):
            rel_parts = jsonl_path.relative_to(self.bronze_dir).parts
            # Extract ingest_date from path part 'ingest_date=YYYY-MM-DD'
            ingest_date = "unknown"
            source_from_path = None
            for part in rel_parts:
                if part.startswith("ingest_date="):
                    ingest_date = part.split("=", 1)[1]
                elif part.startswith("source="):
                    source_from_path = part.split("=", 1)[1]

            if target_ingest_date and ingest_date != target_ingest_date:
                continue

            lines = [
                line.strip()
                for line in jsonl_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            processed_files += 1
            total_raw += len(lines)

            for line in lines:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                meta = record.get("_metadata") or {}
                source = str(meta.get("source") or source_from_path or "").lower()
                transformer = self.transformers.get(source)
                if not transformer:
                    logger.warning(f"No transformer registered for source '{source}' in {jsonl_path}")
                    continue

                silver_item = transformer.transform_record(record, ingest_date=ingest_date)
                if silver_item is not None:
                    grouped_records.setdefault((ingest_date, source), []).append(silver_item)

        outputs: List[str] = []
        total_silver = 0
        dedup_dropped = 0

        for (ingest_date, source), items in grouped_records.items():
            raw_count = len(items)
            deduped = self.deduplicate(items)
            dedup_dropped += raw_count - len(deduped)
            total_silver += len(deduped)
            out_path = self.write_parquet(deduped, ingest_date=ingest_date, source=source)
            if out_path:
                outputs.append(str(out_path))

        return {
            "processed_files": processed_files,
            "total_raw": total_raw,
            "total_silver": total_silver,
            "duplicates_removed": dedup_dropped,
            "outputs": outputs,
        }
