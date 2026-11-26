# E-Commerce Data Engineering Pipeline: Medallion Architecture (Bronze – Silver – Gold)

Dự án Data Engineering phục vụ Portfolio: Thu thập, làm sạch, chuẩn hóa và phân tích dữ liệu thương mại điện tử đa nền tảng (**Shopee** & **Tiki**) theo kiến trúc **Medallion Architecture (Bronze – Silver – Gold)**, thiết kế hoàn toàn theo hướng đối tượng (**OOP**) và nguyên lý **SOLID**.

---

## 1. Kiến trúc Tổng thể (End-to-End Medallion Pipeline)

```text
[ User CLI / main.py ]
        │
        ▼
[ CrawlerOrchestrator ] ──(1. Tìm kiếm / Chọn lọc)──> [ CategoryManager ]
        │                                                    │
        ▼ (2. Khởi tạo Scraper qua Factory)                  ▼
[ ScraperFactory ] ──> [ BaseScraper (ABC) ] <──── [ CrawlTarget (Model) ]
                            │        ▲
               ┌────────────┴────────┴────────────┐
               ▼                                  ▼
      [ ShopeeScraper ]                    [ TikiScraper ]
     (Playwright + Chrome)                (REST API + Retry)
               │                                  │
               └────────────┬─────────────────────┘
                            ▼
  ╔═══════════════════════════════════════════════════════════════════╗
  ║ TẦNG BRONZE (Raw Data Lake - JSONL + Hive Partitioning)           ║
  ║ data/bronze/ingest_date=YYYY-MM-DD/source={source}/category_id=X/ ║
  ╚═════════════════════════╦═════════════════════════════════════════╝
                            │
                            ▼ (3. Làm phẳng JSON, Chia giá /100k, Lọc rác, Khử trùng)
                  [ SilverPipeline (OOP) ]
                  ├── ShopeeTransformer(BaseTransformer)
                  └── TikiTransformer(BaseTransformer)
                            │
                            ▼
  ╔═══════════════════════════════════════════════════════════════════╗
  ║ TẦNG SILVER (Cleaned & Deduplicated - ZSTD Parquet, giảm ~98%)    ║
  ║ data/silver/ingest_date=YYYY-MM-DD/source={source}/products.parquet║
  ╚═════════════════════════╦═════════════════════════════════════════╝
                            │
                            ▼ (4. Tổng hợp BI / Analytics với Polars & DuckDB)
                     [ GoldBuilder ]
                            │
                            ▼
  ╔═══════════════════════════════════════════════════════════════════╗
  ║ TẦNG GOLD (Business & Analytical Data Marts - Parquet)            ║
  ║ ├── data/gold/category_brand_stats.parquet                        ║
  ║ ├── data/gold/best_sellers.parquet                                ║
  ║ └── data/gold/platform_price_comparison.parquet                   ║
  ╚═══════════════════════════════════════════════════════════════════╝
```

---

## 2. Cấu trúc Thư mục

```text
Crawl/
├── config/
│   ├── categories.json          # Danh bạ 17 danh mục chuẩn cho Shopee & Tiki
│   └── settings.py              # Cấu hình đường dẫn (Bronze/Silver/Gold), timeout, retry
├── data/
│   ├── bronze/                  # Raw JSONL phân vùng theo chuẩn Hive
│   ├── silver/                  # Cleaned & Deduplicated Parquet (Unified Schema 18 cột)
│   ├── gold/                    # Bảng tổng hợp BI / Analytics Parquet
│   ├── browser_profile/         # Lưu phiên đăng nhập & session cookies Shopee
│   └── checkpoint.json          # Trạng thái tiến độ cào (Idempotent / Resume)
├── logs/
│   └── crawler.log              # Log lịch sử vận hành toàn pipeline
├── src/
│   ├── core/
│   │   ├── category_manager.py  # Quản lý, tìm kiếm danh mục không dấu & tạo custom target
│   │   ├── factory.py           # ScraperFactory khởi tạo scraper theo sàn
│   │   └── orchestrator.py      # Điều phối tìm kiếm & cào đa mục tiêu
│   ├── models/
│   │   └── target.py            # Pydantic models: CrawlTarget, CrawlResult
│   ├── scraper/
│   │   ├── base_scraper.py      # Abstract Base Class cho Scraper
│   │   ├── checkpoint.py        # Quản lý checkpoint tiến độ
│   │   ├── client.py            # HTTP Client với Tenacity retry & jitter delay
│   │   ├── shopee_scraper.py    # Shopee Scraper (Playwright + Network API Interception)
│   │   └── tiki_scraper.py      # Tiki Scraper (REST API)
│   ├── storage/
│   │   └── bronze_writer.py     # Ghi dữ liệu thô JSONL kèm Audit Metadata
│   ├── transform/
│   │   ├── base_transformer.py  # Unified Schema `SilverProduct` (18 cột) & BaseTransformer
│   │   ├── shopee_transformer.py# Làm phẳng JSON Shopee, chia hệ số giá /100,000, lọc rác
│   │   ├── tiki_transformer.py  # Chuẩn hóa dữ liệu Tiki về SilverProduct
│   │   └── silver_writer.py     # SilverPipeline: Khử trùng theo (source, product_id) & ghi Parquet
│   ├── analytics/
│   │   └── gold_builder.py      # GoldBuilder (Polars + DuckDB): Sinh 3 bảng phân tích Gold
│   └── utils/
│       └── logger.py            # Cấu hình logging Console + File UTF-8
├── tests/
│   ├── test_category_manager.py # Unit tests tìm kiếm & quản lý danh mục
│   ├── test_checkpoint_manager.py# Unit tests lưu vết & khôi phục checkpoint
│   ├── test_transformers.py     # Unit tests chuẩn hóa Shopee/Tiki & khử trùng lặp
│   └── test_pipeline.py         # Integration tests toàn luồng Bronze -> Silver -> Gold
├── main.py                      # CLI Entrypoint hợp nhất
├── requirements.txt             # Danh sách thư viện phụ thuộc
└── README.md
```

---

## 3. Hướng dẫn Sử dụng CLI

### Bước 1: Tìm kiếm & Chọn lọc danh mục
```bash
# Tìm kiếm danh mục theo từ khóa (hỗ trợ có dấu & không dấu):
python main.py --search "dien thoai"
python main.py --search "laptop" --source tiki

# Xem toàn bộ danh bạ danh mục có sẵn:
python main.py --list
python main.py --list --source tiki

# Mở menu tương tác để chọn danh mục muốn cào:
python main.py --select
```

### Bước 2: Cào dữ liệu vào Tầng Bronze
```bash
# Đăng nhập Shopee lần đầu (hoặc khi hết hạn cookie):
python main.py --login

# Tìm danh mục phù hợp với từ khóa và cào ngay:
python main.py --query "laptop" --source shopee --max-pages 2 --head
python main.py --query "laptop" --source tiki --max-pages 2

# Cào theo từ khóa tự do bất kỳ:
python main.py --query "bàn phím cơ" --source shopee --max-pages 2 --head

# Kiểm tra trạng thái tiến độ cào (Checkpoint):
python main.py --status
python main.py --status --source tiki
```

### Bước 3: Chạy Pipeline Chuẩn hóa Tầng Silver & Tổng hợp Tầng Gold
```bash
# Chạy liên hoàn cả Tầng Silver và Tầng Gold trong 1 lệnh:
python main.py --transform-silver --build-gold

# Hoặc chạy từng tầng riêng biệt:
python main.py --transform-silver
python main.py --build-gold
```

### Bước 4: Chạy Bộ Kiểm thử Tự động (18 Unit & Integration Tests)
```bash
python -m unittest discover -s tests -v
```
