import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any, Dict, List

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config.settings import DATA_DIR, DEFAULT_HEADERS, TIKI_LISTING_URL
from src.core.category_manager import CategoryManager
from src.transform.tiki_transformer import TikiTransformer
from src.web.server import DashboardRequestHandler, HTML_DASHBOARD

BUNDLED_SNAPSHOT = DATA_DIR / "web_snapshot.json"
TMP_SNAPSHOT = Path("/tmp/web_snapshot.json")
TMP_LOGS = Path("/tmp/vercel_crawler.log")


def _append_cloud_log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [VERCEL-CLOUD] {msg}\n"
    try:
        with open(TMP_LOGS, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass


def _load_snapshot() -> Dict[str, Any]:
    if TMP_SNAPSHOT.exists():
        try:
            return json.loads(TMP_SNAPSHOT.read_text(encoding="utf-8"))
        except Exception:
            pass
    if BUNDLED_SNAPSHOT.exists():
        try:
            return json.loads(BUNDLED_SNAPSHOT.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"categories": [], "silver_products": [], "gold": {}}


def _save_tmp_snapshot(snapshot: Dict[str, Any]) -> None:
    try:
        TMP_SNAPSHOT.write_text(
            json.dumps(snapshot, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
    except Exception:
        pass


def _recompute_gold_from_silver_dicts(
    products: List[Dict[str, Any]],
    categories: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """Lightweight pure-Python Gold analytical aggregator for Vercel Serverless."""
    cat_name_map = {
        (str(c.get("source", "")).lower(), c.get("category_id")): c.get("name")
        for c in categories
        if c.get("category_id") is not None
    }
    group_map = {
        11036030: "Điện Thoại & Smartphone",
        1789: "Điện Thoại & Smartphone",
        11035954: "Laptop & Máy Vi Tính",
        1846: "Laptop & Máy Vi Tính",
        11035953: "Thiết Bị Số & Âm Thanh",
        1815: "Thiết Bị Số & Âm Thanh",
        11036132: "Điện Gia Dụng",
        1882: "Điện Gia Dụng",
        11037494: "Nhà Sách",
        8322: "Nhà Sách",
        11036467: "Làm Đẹp & Mỹ Phẩm",
        1520: "Làm Đẹp & Mỹ Phẩm",
    }

    # 1. Best Sellers (top 20 per source)
    by_source: Dict[str, List[Dict[str, Any]]] = {}
    for p in products:
        src = str(p.get("source") or "tiki").lower()
        by_source.setdefault(src, []).append(p)

    best_sellers: List[Dict[str, Any]] = []
    for src, items in sorted(by_source.items()):
        sorted_items = sorted(
            items,
            key=lambda x: (int(x.get("quantity_sold") or 0), int(x.get("review_count") or 0)),
            reverse=True,
        )
        for rank, item in enumerate(sorted_items[:20], start=1):
            cid = item.get("category_id")
            price = float(item.get("price") or 0)
            sold = int(item.get("quantity_sold") or 0)
            best_sellers.append({
                "source": src,
                "rank_in_source": rank,
                "category_group": group_map.get(cid, cat_name_map.get((src, cid), f"Danh mục {cid}")),
                "brand": item.get("brand") or "Unknown",
                "name": item.get("name"),
                "price": price,
                "original_price": float(item.get("original_price") or price),
                "discount_rate": float(item.get("discount_rate") or 0),
                "quantity_sold": sold,
                "estimated_revenue": round(price * sold, 0),
                "rating_average": float(item.get("rating_average") or 0),
                "product_url": item.get("product_url"),
            })

    # 2. Platform Price Comparison
    comp_buckets: Dict[tuple, List[Dict[str, Any]]] = {}
    for p in products:
        src = str(p.get("source") or "tiki").lower()
        cid = p.get("category_id")
        grp = group_map.get(cid, cat_name_map.get((src, cid), "Khác"))
        comp_buckets.setdefault((grp, src), []).append(p)

    platform_comparison: List[Dict[str, Any]] = []
    for (grp, src), items in sorted(comp_buckets.items()):
        prices = sorted(float(x.get("price") or 0) for x in items)
        discounts = [float(x.get("discount_rate") or 0) for x in items]
        solds = [int(x.get("quantity_sold") or 0) for x in items]
        ratings = [float(x.get("rating_average") or 0) for x in items if float(x.get("rating_average") or 0) > 0]
        brands = {str(x.get("brand") or "Unknown") for x in items}
        n = len(prices)
        median_p = prices[n // 2] if n else 0
        platform_comparison.append({
            "category_group": grp,
            "source": src,
            "product_count": n,
            "distinct_brands": len(brands),
            "min_price": round(min(prices), 0) if prices else 0,
            "avg_price": round(sum(prices) / n, 0) if n else 0,
            "median_price": round(median_p, 0),
            "max_price": round(max(prices), 0) if prices else 0,
            "avg_discount_rate": round(sum(discounts) / n, 2) if n else 0,
            "total_quantity_sold": sum(solds),
            "avg_sold_per_product": round(sum(solds) / n, 1) if n else 0,
            "avg_rating": round(sum(ratings) / len(ratings), 2) if ratings else 0,
        })

    # 3. Category Brand Stats
    brand_buckets: Dict[tuple, List[Dict[str, Any]]] = {}
    for p in products:
        src = str(p.get("source") or "tiki").lower()
        cid = p.get("category_id")
        cname = cat_name_map.get((src, cid), f"Category {cid}")
        bname = str(p.get("brand") or "Unknown")
        brand_buckets.setdefault((src, cid, cname, bname), []).append(p)

    brand_stats: List[Dict[str, Any]] = []
    for (src, cid, cname, bname), items in brand_buckets.items():
        prices = [float(x.get("price") or 0) for x in items]
        discounts = [float(x.get("discount_rate") or 0) for x in items]
        solds = [int(x.get("quantity_sold") or 0) for x in items]
        n = len(prices)
        brand_stats.append({
            "source": src,
            "category_id": cid,
            "category_name": cname,
            "brand": bname,
            "product_count": n,
            "avg_price": round(sum(prices) / n, 0) if n else 0,
            "min_price": round(min(prices), 0) if prices else 0,
            "max_price": round(max(prices), 0) if prices else 0,
            "avg_discount_rate": round(sum(discounts) / n, 2) if n else 0,
            "max_discount_rate": round(max(discounts), 2) if discounts else 0,
            "total_quantity_sold": sum(solds),
        })
    brand_stats.sort(key=lambda x: (x["source"], -x["total_quantity_sold"]))

    return {
        "platform_price_comparison": platform_comparison,
        "best_sellers": best_sellers,
        "category_brand_stats": brand_stats,
    }


class VercelServerlessHandler(BaseHTTPRequestHandler):
    """Vercel Python Serverless Entrypoint (`api/index.py`)."""

    def _send_json(self, data: Dict[str, Any], status: int = 200) -> None:
        body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str) -> None:
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        path = urllib.parse.urlparse(self.path).path
        snap = _load_snapshot()

        if path in ("/", "/index.html"):
            self._send_html(HTML_DASHBOARD)
            return

        if path == "/api/categories":
            self._send_json({"categories": snap.get("categories", [])})
            return

        if path == "/api/status":
            logs_txt = (
                TMP_LOGS.read_text(encoding="utf-8")
                if TMP_LOGS.exists()
                else "[VERCEL CLOUD READY] Nền tảng Medallion Data Platform đã sẵn sàng trên Vercel.\n"
                     "- Bạn có thể cào trực tiếp Tiki theo Danh mục hoặc Từ khóa bất kỳ ngay trên Cloud.\n"
                     "- Dữ liệu Silver (240+ SP) và Gold BI đã được nạp sẵn sàng."
            )
            self._send_json({
                "job": {"is_running": False, "current_task": "Idle", "started_at": None, "last_result": None, "last_error": None},
                "logs": logs_txt,
                "categories": snap.get("categories", []),
            })
            return

        if path == "/api/silver":
            self._send_json({"products": snap.get("silver_products", [])})
            return

        if path == "/api/gold":
            self._send_json(snap.get("gold", {}))
            return

        self._send_json({"error": "Not found"}, status=404)

    def do_POST(self) -> None:
        path = urllib.parse.urlparse(self.path).path
        content_len = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_len).decode("utf-8") if content_len > 0 else "{}"
        try:
            payload = json.loads(raw_body) if raw_body else {}
        except Exception:
            payload = {}

        snap = _load_snapshot()

        if path == "/api/crawl":
            source = str(payload.get("source") or "tiki").lower()
            mode = str(payload.get("mode") or "category")
            category_id = payload.get("category_id")
            query = str(payload.get("query") or "").strip()
            max_pages = min(int(payload.get("max_pages") or 1), 3)

            if source == "shopee":
                _append_cloud_log(
                    "Lưu ý: Shopee yêu cầu trình duyệt Chrome cục bộ có lưu phiên đăng nhập (chạy `python app.py` trên máy). "
                    "Hệ thống hiển thị tập dữ liệu Shopee đã thu thập sẵn trong kho Silver & Gold."
                )
                self._send_json({"status": "completed", "note": "Shopee uses pre-ingested snapshot on Vercel Cloud."})
                return

            # Live Tiki Crawl directly from Vercel Serverless Function
            cm = CategoryManager()
            target = None
            if mode == "category" and category_id:
                target = cm.get_by_id(int(category_id), source="tiki")
            elif mode == "query" and query:
                matches = cm.search(query, source="tiki")
                target = matches[0] if matches else cm.create_custom_target(query, source="tiki")
            else:
                all_tiki = cm.list_all(source="tiki")
                target = all_tiki[0] if all_tiki else None

            if not target:
                self._send_json({"error": "Không tìm thấy mục tiêu cào hợp lệ."}, status=400)
                return

            _append_cloud_log(f"Bắt đầu cào trực tiếp Tiki cho '{target.name}' (Max pages: {max_pages})...")
            tf = TikiTransformer()
            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            now_iso = datetime.now(timezone.utc).isoformat()
            new_silver: List[Dict[str, Any]] = []

            for page_num in range(1, max_pages + 1):
                params: Dict[str, Any] = {"limit": 40, "page": page_num}
                if target.category_id:
                    params["category"] = target.category_id
                    if target.url_key:
                        params["urlKey"] = target.url_key
                else:
                    params["q"] = target.keyword

                url = f"{TIKI_LISTING_URL}?{urllib.parse.urlencode(params)}"
                req = urllib.request.Request(url, headers=DEFAULT_HEADERS)
                try:
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        data = json.loads(resp.read().decode("utf-8"))
                        items = data.get("data", []) or []
                        for raw_item in items:
                            envelope = {
                                "_metadata": {
                                    "source": "tiki",
                                    "ingested_at": now_iso,
                                    "category_id": target.category_id,
                                    "page": page_num,
                                },
                                "raw_data": raw_item,
                            }
                            sp = tf.transform_record(envelope, ingest_date=today_str)
                            if sp is not None:
                                new_silver.append(sp.model_dump())
                except Exception as e:
                    _append_cloud_log(f"Lỗi khi tải trang {page_num}: {e}")
                    break

            # Deduplicate & merge with existing snapshot
            existing_products = snap.get("silver_products", [])
            merged_by_key = {(p.get("source"), p.get("product_id")): p for p in existing_products}
            for np in new_silver:
                merged_by_key[(np.get("source"), np.get("product_id"))] = np

            updated_products = sorted(
                merged_by_key.values(),
                key=lambda x: int(x.get("quantity_sold") or 0),
                reverse=True,
            )
            snap["silver_products"] = updated_products

            # Update category checkpoint in snapshot
            for c in snap.get("categories", []):
                if c.get("category_id") == target.category_id and c.get("source") == "tiki":
                    c["last_page"] = int(c.get("last_page") or 0) + max_pages
                    c["total_records"] = int(c.get("total_records") or 0) + len(new_silver)

            snap["gold"] = _recompute_gold_from_silver_dicts(updated_products, snap.get("categories", []))
            _save_tmp_snapshot(snap)
            _append_cloud_log(
                f"Hoàn tất cào '{target.name}': Thu thập thêm {len(new_silver)} sản phẩm. "
                f"Tổng kho Silver hiện tại: {len(updated_products)} sản phẩm."
            )
            self._send_json({"status": "completed", "ingested": len(new_silver), "total_silver": len(updated_products)})
            return

        if path == "/api/run-etl":
            snap["gold"] = _recompute_gold_from_silver_dicts(
                snap.get("silver_products", []),
                snap.get("categories", []),
            )
            _save_tmp_snapshot(snap)
            _append_cloud_log("Đã chạy cập nhật lại toàn bộ các bảng phân tích Tầng Silver & Gold.")
            self._send_json({"status": "completed"})
            return

        if path == "/api/reset-checkpoint":
            if TMP_SNAPSHOT.exists():
                TMP_SNAPSHOT.unlink()
            _append_cloud_log("Đã khôi phục dữ liệu và checkpoint về trạng thái gốc.")
            self._send_json({"status": "reset_ok"})
            return

        if path == "/api/login-shopee":
            _append_cloud_log("Đăng nhập Shopee bằng QR Chrome chỉ khả dụng khi chạy `python app.py` trên máy cá nhân.")
            self._send_json({"status": "local_only"})
            return

        self._send_json({"error": "Not found"}, status=404)


# Vercel Python runtime looks for `handler` subclassing `BaseHTTPRequestHandler`
handler = VercelServerlessHandler
