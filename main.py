import argparse
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.analytics.gold_builder import GoldBuilder
from src.core.category_manager import CategoryManager
from src.core.orchestrator import CrawlerOrchestrator
from src.models.target import CrawlTarget
from src.scraper.checkpoint import CheckpointManager
from src.scraper.shopee_scraper import ShopeeScraper
from src.storage.bronze_writer import BronzeWriter
from src.transform.silver_writer import SilverPipeline
from src.utils.logger import setup_logger

logger = setup_logger("cli")


def print_targets_table(targets: list[CrawlTarget], title: str = "DANH SÁCH DANH MỤC") -> None:
    """Helper to display formatted targets list."""
    print(f"\n=== {title} ({len(targets)}) ===")
    if not targets:
        print("  (Không tìm thấy danh mục nào)")
        print("=" * 40 + "\n")
        return

    print(f"{'STT':<4} | {'Nguồn':<7} | {'ID':<10} | {'Tên Danh Mục':<30} | {'Từ khóa'}")
    print("-" * 75)
    for idx, t in enumerate(targets, start=1):
        cat_id_str = str(t.category_id) if t.category_id else "Custom"
        print(f"{idx:<4} | {t.source.upper():<7} | {cat_id_str:<10} | {t.name:<30} | {t.keyword}")
    print("=" * 75 + "\n")


def interactive_selection(orchestrator: CrawlerOrchestrator, default_source: str) -> list[CrawlTarget]:
    """Interactive CLI menu to search, filter and pick targets to crawl."""
    print("\n--- CHẾ ĐỘ CHỌN LỌC DANH MỤC ĐỂ CÀO ---")
    query = input(">> Nhập từ khóa tìm kiếm danh mục (để trống để xem tất cả): ").strip()

    if query:
        matches = orchestrator.search_categories(query=query, source=default_source)
    else:
        matches = orchestrator.list_all_categories(source=default_source)

    if not matches and query:
        print(f"\n[!] Không tìm thấy danh mục nào trong hệ thống khớp với '{query}'.")
        choice = input(f">> Bạn có muốn cào tự do theo từ khóa '{query}' không? (y/n) [y]: ").strip().lower()
        if choice in ("", "y", "yes"):
            custom_target = orchestrator.category_mgr.create_custom_target(query=query, source=default_source)
            return [custom_target]
        return []

    print_targets_table(matches, title=f"KẾT QUẢ TÌM KIẾM CHO '{query or 'TẤT CẢ'}'")
    print("Các tùy chọn:")
    print("  - Nhập số thứ tự (ví dụ: '1' hoặc '1, 2, 3') để chọn danh mục muốn cào.")
    print("  - Nhập 'all' để cào TẤT CẢ các danh mục tìm thấy trên.")
    print("  - Nhập 'q' để hủy.")

    sel = input("\n>> Lựa chọn của bạn: ").strip()
    if not sel or sel.lower() == "q":
        return []

    if sel.lower() == "all":
        return matches

    selected: list[CrawlTarget] = []
    for part in sel.split(","):
        part = part.strip()
        if part.isdigit():
            idx = int(part) - 1
            if 0 <= idx < len(matches):
                selected.append(matches[idx])
            else:
                print(f"[!] Bỏ qua số STT không hợp lệ: {part}")

    return selected


def main():
    parser = argparse.ArgumentParser(
        description="E-Commerce Medallion Pipeline (Bronze Layer) - OOP-driven Search & Ingestion"
    )
    parser.add_argument(
        "--source",
        type=str,
        choices=["shopee", "tiki"],
        default="shopee",
        help="Nền tảng thương mại điện tử cần cào (Mặc định: shopee)",
    )
    parser.add_argument(
        "--search",
        type=str,
        help="Tìm kiếm danh mục theo từ khóa và hiển thị kết quả (không cào)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Liệt kê tất cả các danh mục có sẵn trong hệ thống",
    )
    parser.add_argument(
        "--select",
        action="store_true",
        help="Mở menu tương tác để tìm kiếm và chọn lọc danh mục muốn cào",
    )
    parser.add_argument(
        "--query",
        type=str,
        help="Tìm kiếm danh mục phù hợp và tự động cào các danh mục đó (nếu không có danh mục sẵn, tự động cào theo keyword)",
    )
    parser.add_argument(
        "--category",
        type=int,
        help="Chỉ định ID danh mục cụ thể (vd: 11036030 cho Shopee Điện Thoại)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Cào toàn bộ danh mục có sẵn trong catalog của nền tảng đã chọn",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=2,
        help="Số trang tối đa cần cào cho mỗi danh mục/từ khóa (Mặc định: 2)",
    )
    parser.add_argument(
        "--head",
        action="store_true",
        help="Mở giao diện trình duyệt trực quan thay vì chạy ngầm (headless)",
    )
    parser.add_argument(
        "--login",
        action="store_true",
        help="Mở trình duyệt để đăng nhập Shopee và lưu phiên làm việc (session)",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Hiển thị trạng thái tiến trình cào hiện tại (checkpoint)",
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Xóa checkpoint để cào lại từ trang đầu tiên",
    )
    parser.add_argument(
        "--transform-silver",
        action="store_true",
        help="Chạy pipeline chuẩn hóa dữ liệu từ tầng Bronze (JSONL) sang tầng Silver (Parquet)",
    )
    parser.add_argument(
        "--build-gold",
        action="store_true",
        help="Xây dựng các bảng phân tích tầng Gold (Best Sellers, So sánh giá, Thống kê thương hiệu)",
    )
    parser.add_argument(
        "--web",
        action="store_true",
        help="Khởi chạy giao diện Web UI Dashboard trực quan trên trình duyệt (http://127.0.0.1:8080)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Cổng chạy giao diện Web UI (Mặc định: 8080)",
    )

    args = parser.parse_args()

    if args.web:
        from src.web.server import start_web_server
        start_web_server(port=args.port)
        return

    checkpoint_mgr = CheckpointManager()
    category_mgr = CategoryManager()
    bronze_writer = BronzeWriter()
    orchestrator = CrawlerOrchestrator(
        category_mgr=category_mgr,
        checkpoint_mgr=checkpoint_mgr,
        bronze_writer=bronze_writer,
    )

    # 1. Login flow
    if args.login:
        shopee = ShopeeScraper(checkpoint_mgr=checkpoint_mgr, bronze_writer=bronze_writer)
        shopee.login_session()
        return

    # 1b. Silver transformation pipeline & 1c. Gold analytics builder
    ran_post_pipeline = False
    if args.transform_silver:
        pipeline = SilverPipeline()
        summary = pipeline.run()
        print("\n=== KẾT QUẢ CHUẨN HÓA TẦNG SILVER (PARQUET) ===")
        print(f"- Số file Bronze JSONL đã xử lý : {summary['processed_files']}")
        print(f"- Tổng bản ghi thô đầu vào      : {summary['total_raw']}")
        print(f"- Số bản ghi trùng đã loại bỏ   : {summary['duplicates_removed']}")
        print(f"- Tổng bản ghi sạch tầng Silver : {summary['total_silver']}")
        for out in summary["outputs"]:
            print(f"  -> Parquet: {out}")
        print("=" * 48 + "\n")
        ran_post_pipeline = True

    if args.build_gold:
        gold = GoldBuilder(category_mgr=category_mgr)
        res = gold.run()
        print("\n=== KẾT QUẢ XÂY DỰNG TẦNG GOLD (ANALYTICS) ===")
        print(f"- Trạng thái               : {res['status'].upper()}")
        print(f"- Số bản ghi Silver đầu vào: {res.get('silver_input_rows', 0)}")
        for name, path in res.get("tables", {}).items():
            print(f"  -> [{name}]: {path}")
        print("=" * 48 + "\n")
        ran_post_pipeline = True

    if ran_post_pipeline and not (args.category or args.query or args.all or args.select or args.status or args.search or args.list):
        return

    # 2. Status view
    if args.status:
        all_targets = orchestrator.list_all_categories(source=args.source)
        print(f"\n=== TRẠNG THÁI TIẾN TRÌNH CÀO (CHECKPOINT - {args.source.upper()}) ===")
        for t in all_targets:
            key = str(t.category_id) if t.category_id else t.target_id
            prog = checkpoint_mgr.get_progress(key)
            status_text = "ĐÃ HOÀN THÀNH" if prog.get("completed") else "Chưa hoàn thành"
            print(
                f"- [{t.source.upper()}] {t.name:<30} (ID: {key:<12}): "
                f"Trang cuối = {prog.get('last_page', 0):<3} | "
                f"Đã cào = {prog.get('total_records', 0):<4} sp | "
                f"Trạng thái = {status_text}"
            )
        print("=" * 65 + "\n")
        return

    # 3. Reset checkpoint
    if args.reset_checkpoint:
        if args.category:
            checkpoint_mgr.reset(args.category)
            logger.info(f"Đã đặt lại checkpoint cho danh mục ID {args.category}.")
        else:
            checkpoint_mgr.reset()
            logger.info("Đã đặt lại toàn bộ checkpoint.")
        if not args.category and not args.query and not args.all and not args.select:
            return

    # 4. Search only
    if args.search:
        matches = orchestrator.search_categories(query=args.search, source=args.source)
        print_targets_table(matches, title=f"KẾT QUẢ TÌM KIẾM DANH MỤC CHO '{args.search}'")
        return

    # 5. List all categories
    if args.list:
        targets = orchestrator.list_all_categories(source=args.source)
        print_targets_table(targets, title=f"TOÀN BỘ DANH MỤC ({args.source.upper()})")
        return

    # 6. Resolve targets to crawl
    targets_to_crawl: list[CrawlTarget] = []

    if args.select:
        targets_to_crawl = interactive_selection(orchestrator, default_source=args.source)
    elif args.query:
        # Step 1: Find matching categories. Step 2: Crawl matching categories (or custom query)
        targets_to_crawl = orchestrator.resolve_targets(
            query=args.query,
            source=args.source,
            allow_custom_fallback=True,
        )
        print_targets_table(
            targets_to_crawl,
            title=f"CÁC DANH MỤC/MỤC TIÊU PHÙ HỢP ĐƯỢC CHỌN TỰ ĐỘNG CHO '{args.query}'",
        )
    elif args.category:
        target = category_mgr.get_by_id(args.category, source=args.source)
        if not target:
            # Fallback on the fly target
            target = CrawlTarget(
                target_id=f"{args.source}_{args.category}",
                name=f"Danh mục ID {args.category}",
                source=args.source,
                keyword=str(args.category),
                category_id=args.category,
            )
        targets_to_crawl.append(target)
    elif args.all:
        targets_to_crawl = orchestrator.list_all_categories(source=args.source)
    else:
        logger.info(
            "Không có tùy chọn cào cụ thể. Sử dụng một trong các cách sau:\n"
            "  1. Tìm kiếm danh mục:               python main.py --search <từ_khóa>\n"
            "  2. Cào theo tìm kiếm danh mục:      python main.py --query <từ_khóa> --max-pages 2\n"
            "  3. Menu tương tác chọn danh mục:    python main.py --select\n"
            "  4. Cào theo ID danh mục:            python main.py --category <id> --max-pages 2\n"
            "  5. Xem tất cả danh mục:             python main.py --list\n"
            "  6. Xem trạng thái tiến độ:          python main.py --status"
        )
        return

    if not targets_to_crawl:
        logger.warning("Không có mục tiêu nào được chọn để cào. Kết thúc.")
        return

    # Execute crawl pipeline
    logger.info(f"Bắt đầu cào {len(targets_to_crawl)} mục tiêu với max_pages={args.max_pages}...")
    results = orchestrator.crawl_targets(
        targets=targets_to_crawl,
        max_pages=args.max_pages,
        headless=not args.head,
        force_restart=args.reset_checkpoint,
    )

    # Summary
    print("\n=== TỔNG KẾT KẾT QUẢ CÀO DỮ LIỆU ===")
    for res in results:
        print(
            f"- [{res.target.source.upper()}] {res.target.name}: "
            f"Trạng thái = {res.status.upper()} | "
            f"Số bản ghi đã thu thập = {res.ingested_records} | "
            f"Trang cuối = {res.last_page}"
        )
        if res.partition_dir:
            print(f"  -> Lưu tại: {res.partition_dir}")
    print("=" * 45 + "\n")


if __name__ == "__main__":
    main()
