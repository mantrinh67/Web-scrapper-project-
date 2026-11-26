import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from config.settings import DATA_DIR, GOLD_DIR, SILVER_DIR
from src.core.category_manager import CategoryManager
from src.scraper.checkpoint import CheckpointManager
from src.utils.logger import setup_logger

logger = setup_logger("web_snapshot")

SNAPSHOT_FILE = DATA_DIR / "web_snapshot.json"


def build_and_save_web_snapshot(output_path: Path = SNAPSHOT_FILE) -> Dict[str, Any]:
    """Exports Silver products, Gold analytical tables, and Category checkpoints into a portable
    JSON snapshot (`data/web_snapshot.json`) for instant cloud rendering on Vercel.
    """
    import polars as pl

    cm = CategoryManager()
    cp = CheckpointManager()

    categories: List[Dict[str, Any]] = []
    for t in cm.list_all():
        key = str(t.category_id) if t.category_id else t.target_id
        prog = cp.get_progress(key)
        categories.append({
            "target_id": t.target_id,
            "name": t.name,
            "source": t.source,
            "keyword": t.keyword,
            "category_id": t.category_id,
            "url_key": t.url_key,
            "url_path": t.url_path,
            "last_page": prog.get("last_page", 0),
            "total_records": prog.get("total_records", 0),
            "completed": prog.get("completed", False),
        })

    silver_files = sorted(SILVER_DIR.rglob("products.parquet")) if SILVER_DIR.exists() else []
    silver_products: List[Dict[str, Any]] = []
    if silver_files:
        df = pl.read_parquet(silver_files).sort("quantity_sold", descending=True)
        silver_products = df.to_dicts()

    gold_data: Dict[str, List[Dict[str, Any]]] = {}
    for table_name in ("platform_price_comparison", "best_sellers", "category_brand_stats"):
        fp = GOLD_DIR / f"{table_name}.parquet"
        if fp.exists():
            gold_data[table_name] = pl.read_parquet(fp).to_dicts()
        else:
            gold_data[table_name] = []

    snapshot: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "categories": categories,
        "silver_products": silver_products,
        "gold": gold_data,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2, default=str)

    logger.info(
        f"Exported web_snapshot.json ({len(silver_products)} Silver items, "
        f"{output_path.stat().st_size:,} bytes) to {output_path}"
    )
    return snapshot
