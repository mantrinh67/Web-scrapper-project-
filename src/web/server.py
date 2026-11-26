import json
import threading
import traceback
import webbrowser
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from config.settings import GOLD_DIR, LOGS_DIR, SILVER_DIR
from src.core.category_manager import CategoryManager
from src.models.target import CrawlTarget
from src.scraper.checkpoint import CheckpointManager
from src.storage.bronze_writer import BronzeWriter
from src.utils.logger import setup_logger

logger = setup_logger("web_server")


class JobState:
    """Thread-safe state tracker for background crawling and ETL jobs."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.is_running: bool = False
        self.current_task: str = "Idle"
        self.started_at: Optional[str] = None
        self.last_result: Optional[Dict[str, Any]] = None
        self.last_error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "is_running": self.is_running,
                "current_task": self.current_task,
                "started_at": self.started_at,
                "last_result": self.last_result,
                "last_error": self.last_error,
            }


JOB_STATE = JobState()
_SHARED_CATEGORY_MANAGER: Optional[CategoryManager] = None


def _get_category_manager() -> CategoryManager:
    global _SHARED_CATEGORY_MANAGER
    if _SHARED_CATEGORY_MANAGER is None:
        _SHARED_CATEGORY_MANAGER = CategoryManager()
    return _SHARED_CATEGORY_MANAGER

HTML_DASHBOARD = """<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>E-Commerce Medallion Data Platform (Shopee & Tiki)</title>
  <style>
    :root {
      --bg: #0f172a;
      --card: #1e293b;
      --card-border: #334155;
      --text: #f8fafc;
      --muted: #94a3b8;
      --primary: #3b82f6;
      --primary-hover: #2563eb;
      --shopee: #ee4d2d;
      --tiki: #1a94ff;
      --success: #10b981;
      --warning: #f59e0b;
      --danger: #ef4444;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.5;
      padding-bottom: 48px;
    }
    header {
      background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
      border-bottom: 1px solid var(--card-border);
      padding: 18px 28px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      position: sticky;
      top: 0;
      z-index: 50;
    }
    .brand-title {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .brand-title h1 { font-size: 1.25rem; font-weight: 700; }
    .badge {
      padding: 4px 10px;
      border-radius: 999px;
      font-size: 0.75rem;
      font-weight: 600;
      text-transform: uppercase;
    }
    .badge-shopee { background: rgba(238, 77, 45, 0.2); color: #ff785a; border: 1px solid #ee4d2d; }
    .badge-tiki { background: rgba(26, 148, 255, 0.2); color: #60b5ff; border: 1px solid #1a94ff; }
    .badge-idle { background: rgba(16, 185, 129, 0.15); color: #34d399; border: 1px solid #10b981; }
    .badge-running { background: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid #f59e0b; animation: pulse 1.5s infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.55; } }

    .container { max-width: 1360px; margin: 24px auto; padding: 0 20px; }
    .tabs {
      display: flex;
      gap: 8px;
      margin-bottom: 22px;
      border-bottom: 1px solid var(--card-border);
      padding-bottom: 10px;
    }
    .tab-btn {
      background: transparent;
      color: var(--muted);
      border: 1px solid transparent;
      padding: 10px 18px;
      border-radius: 8px;
      cursor: pointer;
      font-weight: 600;
      font-size: 0.92rem;
      transition: all 0.15s;
    }
    .tab-btn:hover { color: var(--text); background: rgba(255,255,255,0.04); }
    .tab-btn.active {
      background: var(--primary);
      color: white;
    }
    .tab-panel { display: none; }
    .tab-panel.active { display: block; }

    .grid-2 {
      display: grid;
      grid-template-columns: 440px 1fr;
      gap: 22px;
    }
    @media (max-width: 1024px) { .grid-2 { grid-template-columns: 1fr; } }

    .card {
      background: var(--card);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 20px;
      margin-bottom: 20px;
    }
    .card h2 {
      font-size: 1.05rem;
      margin-bottom: 16px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-bottom: 1px solid rgba(255,255,255,0.07);
      padding-bottom: 10px;
    }

    .form-group { margin-bottom: 15px; }
    .form-group label {
      display: block;
      font-size: 0.85rem;
      font-weight: 600;
      color: #cbd5e1;
      margin-bottom: 6px;
    }
    input[type="text"], input[type="number"], select {
      width: 100%;
      padding: 10px 12px;
      border-radius: 8px;
      border: 1px solid #475569;
      background: #0f172a;
      color: var(--text);
      font-size: 0.92rem;
    }
    input:focus, select:focus { outline: 2px solid var(--primary); border-color: transparent; }

    .checkbox-row {
      display: flex;
      align-items: center;
      gap: 10px;
      margin-bottom: 10px;
      font-size: 0.88rem;
      color: #e2e8f0;
      cursor: pointer;
    }
    .checkbox-row input { width: 17px; height: 17px; accent-color: var(--primary); }

    .btn-group { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 16px; }
    .btn {
      padding: 10px 16px;
      border-radius: 8px;
      border: none;
      font-weight: 600;
      font-size: 0.9rem;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      transition: all 0.15s;
    }
    .btn:disabled { opacity: 0.5; cursor: not-allowed; }
    .btn-primary { background: var(--primary); color: white; flex: 1; }
    .btn-primary:hover:not(:disabled) { background: var(--primary-hover); }
    .btn-shopee { background: var(--shopee); color: white; }
    .btn-success { background: var(--success); color: white; }
    .btn-outline {
      background: transparent;
      border: 1px solid #475569;
      color: var(--text);
    }
    .btn-outline:hover:not(:disabled) { background: rgba(255,255,255,0.06); }

    .log-box {
      background: #090d16;
      border: 1px solid #334155;
      border-radius: 8px;
      padding: 12px;
      font-family: 'Consolas', 'Courier New', monospace;
      font-size: 0.8rem;
      color: #a7f3d0;
      height: 250px;
      overflow-y: auto;
      white-space: pre-wrap;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.86rem;
    }
    th, td {
      padding: 10px 12px;
      text-align: left;
      border-bottom: 1px solid rgba(255,255,255,0.07);
    }
    th {
      background: rgba(15, 23, 42, 0.6);
      color: var(--muted);
      font-weight: 600;
      position: sticky;
      top: 0;
    }
    tr:hover td { background: rgba(255,255,255,0.02); }
    .table-scroll { max-height: 440px; overflow-y: auto; border-radius: 8px; }
    .price { color: #38bdf8; font-weight: 600; }
    .discount { color: #f43f5e; font-weight: 600; }
    .sold { color: #34d399; font-weight: 600; }
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 14px;
      margin-bottom: 20px;
    }
    .kpi-card {
      background: rgba(15, 23, 42, 0.65);
      border: 1px solid var(--card-border);
      border-radius: 10px;
      padding: 14px 16px;
    }
    .kpi-card .label { font-size: 0.78rem; color: var(--muted); }
    .kpi-card .value { font-size: 1.4rem; font-weight: 700; margin-top: 4px; }
    .hint { font-size: 0.78rem; color: var(--muted); margin-top: 4px; }
  </style>
</head>
<body>
  <header>
    <div class="brand-title">
      <h1>🛒 E-Commerce Medallion Data Platform</h1>
      <span class="badge badge-shopee">Shopee</span>
      <span class="badge badge-tiki">Tiki</span>
    </div>
    <div id="jobStatusBadge" class="badge badge-idle">● Sẵn sàng (Idle)</div>
  </header>

  <div class="container">
    <div class="tabs">
      <button class="tab-btn active" onclick="switchTab('crawl')">1. Lấy Dữ Liệu Tự Động</button>
      <button class="tab-btn" onclick="switchTab('silver')">2. Danh Sách Sản Phẩm</button>
      <button class="tab-btn" onclick="switchTab('gold')">3. Báo Cáo Thống Kê & So Sánh</button>
    </div>

    <!-- TAB 1: NON-TECH SIMPLE CRAWL CENTER -->
    <div id="tab-crawl" class="tab-panel active">
      <div class="grid-2">
        <!-- LEFT SIMPLE FORM -->
        <div>
          <div class="card">
            <h2>✨ Chọn Thông Tin Cần Lấy Dữ Liệu</h2>

            <div class="form-group">
              <label>Bước 1: Chọn Sàn Thương Mại Điện Tử</label>
              <select id="sourceSelect" onchange="onSourceChange()">
                <option value="tiki">🔵 Tiki (Lấy dữ liệu trực tiếp nhanh 100%)</option>
                <option value="shopee">🟠 Shopee (Chạy tự động trên máy cục bộ / Dữ liệu mẫu trên Web)</option>
              </select>
              <div id="sourceHint" class="hint">Tiki hỗ trợ lấy dữ liệu mới trực tiếp theo thời gian thực ngay trên trình duyệt.</div>
            </div>

            <div class="form-group">
              <label>Bước 2: Bạn Muốn Lấy Theo Cách Nào?</label>
              <select id="modeSelect" onchange="onModeChange()">
                <option value="category">Chọn theo Danh mục ngành hàng có sẵn</option>
                <option value="query">Tự gõ tên sản phẩm muốn tìm (VD: bàn phím cơ, tai nghe...)</option>
              </select>
            </div>

            <div class="form-group" id="categoryGroup">
              <label>Chọn Danh Mục Sản Phẩm</label>
              <select id="categorySelect"></select>
            </div>

            <div class="form-group" id="queryGroup" style="display:none;">
              <label>Gõ Tên Sản Phẩm Bạn Muốn Tìm</label>
              <input type="text" id="queryInput" placeholder="VD: laptop gaming, bàn phím cơ, tai nghe bluetooth..." />
            </div>

            <div class="form-group">
              <label>Bước 3: Chọn Số Lượng Sản Phẩm Muốn Lấy</label>
              <select id="limitItemsSelect">
                <option value="20">20 sản phẩm (Rất nhanh ~3 giây)</option>
                <option value="40" selected>40 sản phẩm (Tiêu chuẩn ~5 giây)</option>
                <option value="80">80 sản phẩm (~10 giây)</option>
                <option value="120">120 sản phẩm (~15 giây)</option>
              </select>
            </div>

            <div class="btn-group">
              <button id="startCrawlBtn" class="btn btn-primary" style="padding: 14px 18px; font-size: 1rem;" onclick="startCrawlJob()">
                🚀 Bắt Đầu Lấy Dữ Liệu
              </button>
            </div>

            <!-- FRIENDLY RESULT BANNER -->
            <div id="friendlyBanner" style="display:none; margin-top:14px; padding:14px; border-radius:10px; background:rgba(16,185,129,0.12); border:1px solid #10b981;">
              <div id="friendlyBannerMsg" style="font-size:0.9rem; color:#ecfdf5; font-weight:600; margin-bottom:10px;"></div>
              <div style="display:flex; gap:8px;">
                <button class="btn btn-success" style="flex:1; padding:8px;" onclick="switchTab('silver')">📋 Xem Danh Sách Sản Phẩm →</button>
                <button class="btn btn-outline" style="padding:8px 12px;" onclick="downloadSilverCSV()">📥 Tải Excel (CSV)</button>
              </div>
            </div>

            <!-- ADVANCED OPTIONS (COLLAPSED FOR NON-TECH USERS) -->
            <details style="margin-top:18px; border-top:1px solid rgba(255,255,255,0.08); padding-top:12px;">
              <summary style="cursor:pointer; font-size:0.84rem; color:#94a3b8; font-weight:600;">
                ⚙️ Tùy chọn nâng cao (Dành cho kỹ thuật viên)
              </summary>
              <div style="margin-top:12px;">
                <label class="checkbox-row">
                  <input type="checkbox" id="showHeadCheck" />
                  <span>Mở cửa sổ trình duyệt Chrome khi chạy cục bộ (<code>--head</code>)</span>
                </label>
                <label class="checkbox-row">
                  <input type="checkbox" id="resetCheckpointCheck" />
                  <span>Lấy lại từ đầu (Reset Checkpoint)</span>
                </label>
                <label class="checkbox-row">
                  <input type="checkbox" id="autoSilverGoldCheck" checked />
                  <span>Tự động chuẩn hóa dữ liệu & cập nhật biểu đồ thống kê</span>
                </label>
                <div class="btn-group" style="margin-top:10px;">
                  <button class="btn btn-shopee" style="padding:7px 12px; font-size:0.82rem;" onclick="startShopeeLogin()">
                    🔑 Kết Nối Shopee / Nạp JSON
                  </button>
                  <button class="btn btn-outline" style="padding:7px 12px; font-size:0.82rem;" onclick="runSilverGoldOnly()">
                    ⚡ Làm Mới Báo Cáo
                  </button>
                </div>

                <!-- SHOPEE AUTH PANEL -->
                <div id="shopeeAuthBox" style="display:none; margin-top:12px; padding:12px; border-radius:10px; background:rgba(238,77,45,0.1); border:1px solid #ee4d2d;">
                  <div style="font-size:0.84rem; font-weight:700; color:#ff785a; margin-bottom:6px;">
                    🔐 Nạp Dữ Liệu JSON Shopee / Cookie (Nâng cao)
                  </div>
                  <textarea id="shopeeCookieInput" rows="3" placeholder="Dán Response JSON (search_items) hoặc Cookie Shopee..." style="width:100%; padding:8px; border-radius:8px; border:1px solid #475569; background:#0f172a; color:#f8fafc; font-size:0.8rem; margin-bottom:8px; resize:vertical;"></textarea>
                  <div style="display:flex; gap:8px;">
                    <button class="btn btn-shopee" style="flex:1; padding:7px; font-size:0.82rem;" onclick="submitShopeeAuth()">✔ Xác Nhận & Nạp</button>
                    <button class="btn btn-outline" style="padding:7px 10px; font-size:0.82rem;" onclick="document.getElementById('shopeeAuthBox').style.display='none'">Đóng</button>
                  </div>
                </div>
              </div>
            </details>
          </div>
        </div>

        <!-- RIGHT OVERVIEW & STATUS -->
        <div>
          <div class="card">
            <h2>
              <span>📊 Tổng Quan Kho Dữ Liệu Hiện Có</span>
              <button class="btn btn-outline" style="padding: 5px 12px; font-size: 0.8rem;" onclick="resetAllCheckpoints()">Khôi Phục Mặc Định</button>
            </h2>
            <div id="currentTaskText" style="margin-bottom: 12px; padding: 10px 14px; border-radius: 8px; background: rgba(59,130,246,0.1); border: 1px solid rgba(59,130,246,0.3); font-size: 0.9rem; color: #93c5fd;">
              Trạng thái: Sẵn sàng. Hãy chọn sàn, danh mục, số lượng ở cột bên trái rồi bấm <b>Bắt Đầu Lấy Dữ Liệu</b>.
            </div>
            <div class="table-scroll" style="max-height: 360px;">
              <table>
                <thead>
                  <tr>
                    <th>Sàn</th>
                    <th>Tên Danh Mục</th>
                    <th>Số Sản Phẩm Đã Lấy</th>
                    <th>Trạng Thái</th>
                  </tr>
                </thead>
                <tbody id="checkpointTableBody"></tbody>
              </table>
            </div>
          </div>

          <div class="card">
            <details>
              <summary style="cursor:pointer; font-size:0.9rem; font-weight:600; color:#94a3b8;">
                📡 Nhật Ký Hệ Thống Chi Tiết (Live Logs — Dành cho kỹ thuật)
              </summary>
              <div style="margin-top:12px;">
                <div id="logBox" class="log-box">Đang tải nhật ký hệ thống...</div>
              </div>
            </details>
          </div>
        </div>
      </div>
    </div>

    <!-- TAB 2: SILVER PRODUCTS EXPLORER -->
    <div id="tab-silver" class="tab-panel">
      <div class="card">
        <h2>
          <span>💎 Danh Sách Sản Phẩm Đã Thu Thập</span>
          <div style="display:flex; gap:10px; flex-wrap:wrap;">
            <input type="text" id="silverSearchInput" placeholder="Tìm tên sản phẩm, thương hiệu..." style="width:230px; padding:6px 10px;" oninput="renderSilverTable()" />
            <select id="silverSourceFilter" style="width:130px; padding:6px 10px;" onchange="renderSilverTable()">
              <option value="all">Tất cả sàn</option>
              <option value="tiki">Tiki</option>
              <option value="shopee">Shopee</option>
            </select>
            <button class="btn btn-success" style="padding:6px 14px; font-size:0.85rem;" onclick="downloadSilverCSV()">📥 Tải File Excel (CSV)</button>
          </div>
        </h2>
        <div id="silverKpis" class="kpi-grid"></div>
        <div class="table-scroll" style="max-height: 540px;">
          <table>
            <thead>
              <tr>
                <th>Sàn</th>
                <th>ID Sản Phẩm</th>
                <th>Thương Hiệu</th>
                <th>Tên Sản Phẩm</th>
                <th>Giá Bán (VNĐ)</th>
                <th>Giá Gốc (VNĐ)</th>
                <th>Giảm (%)</th>
                <th>Đã Bán</th>
                <th>Đánh Giá</th>
                <th>Link</th>
              </tr>
            </thead>
            <tbody id="silverTableBody"></tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- TAB 3: GOLD ANALYTICS -->
    <div id="tab-gold" class="tab-panel">
      <div class="card">
        <h2>⚖️ 1. Đối Chiếu Mặt Bằng Giá & Sản Lượng Giữa Shopee vs Tiki Theo Ngành Hàng</h2>
        <div class="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Ngành Hàng</th>
                <th>Sàn</th>
                <th>Số Sản Phẩm</th>
                <th>Số Thương Hiệu</th>
                <th>Giá Thấp Nhất</th>
                <th>Giá Trung Bình</th>
                <th>Giá Trung Vị</th>
                <th>Giá Cao Nhất</th>
                <th>Giảm Giá TB</th>
                <th>Tổng Lượng Bán</th>
                <th>Đánh Giá TB</th>
              </tr>
            </thead>
            <tbody id="goldComparisonBody"></tbody>
          </table>
        </div>
      </div>

      <div class="card">
        <h2>🏆 2. Bảng Xếp Hạng Top Sản Phẩm Bán Chạy Nhất (Best Sellers)</h2>
        <div class="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Sàn</th>
                <th>Hạng</th>
                <th>Ngành Hàng</th>
                <th>Thương Hiệu</th>
                <th>Tên Sản Phẩm</th>
                <th>Giá Bán</th>
                <th>Giảm Giá</th>
                <th>Số Lượng Đã Bán</th>
                <th>Doanh Thu Ước Tính</th>
                <th>Đánh Giá</th>
              </tr>
            </thead>
            <tbody id="goldBestSellersBody"></tbody>
          </table>
        </div>
      </div>

      <div class="card">
        <h2>📈 3. Thống Kê Giá & Giảm Giá Theo Danh Mục / Thương Hiệu</h2>
        <div class="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Sàn</th>
                <th>Danh Mục</th>
                <th>Thương Hiệu</th>
                <th>Số SP</th>
                <th>Giá TB</th>
                <th>Giá Min</th>
                <th>Giá Max</th>
                <th>Giảm Giá TB</th>
                <th>Giảm Giá Max</th>
                <th>Tổng Đã Bán</th>
              </tr>
            </thead>
            <tbody id="goldBrandStatsBody"></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <script>
    let allCategories = [];
    let silverProducts = [];

    function fmtVND(val) {
      if (val === null || val === undefined || isNaN(val)) return '-';
      return Number(val).toLocaleString('vi-VN') + ' ₫';
    }
    function fmtNum(val) {
      if (val === null || val === undefined || isNaN(val)) return '0';
      return Number(val).toLocaleString('vi-VN');
    }

    function switchTab(tabName) {
      document.querySelectorAll('.tab-btn').forEach((btn, idx) => {
        const map = ['crawl', 'silver', 'gold'];
        btn.classList.toggle('active', map[idx] === tabName);
      });
      document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
      document.getElementById('tab-' + tabName).classList.add('active');
      if (tabName === 'silver') loadSilverData();
      if (tabName === 'gold') loadGoldData();
    }

    function onSourceChange() {
      const src = document.getElementById('sourceSelect').value;
      const hint = document.getElementById('sourceHint');
      if (hint) {
        hint.textContent = src === 'tiki'
          ? 'Tiki hỗ trợ lấy dữ liệu mới trực tiếp theo thời gian thực ngay trên trình duyệt.'
          : 'Shopee lấy dữ liệu tự động khi chạy bản máy tính (python app.py); trên Web Cloud sẽ hiển thị dữ liệu Shopee đã thu thập sẵn.';
      }
      const select = document.getElementById('categorySelect');
      select.innerHTML = '';
      allCategories.filter(c => c.source === src).forEach(c => {
        const opt = document.createElement('option');
        opt.value = c.category_id;
        opt.textContent = `${c.name}`;
        select.appendChild(opt);
      });
    }

    function onModeChange() {
      const mode = document.getElementById('modeSelect').value;
      document.getElementById('categoryGroup').style.display = (mode === 'category') ? 'block' : 'none';
      document.getElementById('queryGroup').style.display = (mode === 'query') ? 'block' : 'none';
    }

    async function loadCategoriesAndCheckpoints() {
      const res = await fetch('/api/categories');
      const data = await res.json();
      allCategories = data.categories || [];
      onSourceChange();
      renderCheckpoints(data.categories || []);
    }

    function renderCheckpoints(cats) {
      const tbody = document.getElementById('checkpointTableBody');
      tbody.innerHTML = '';
      cats.forEach(c => {
        const tr = document.createElement('tr');
        const badgeClass = c.source === 'shopee' ? 'badge-shopee' : 'badge-tiki';
        const statusHtml = c.completed
          ? '<span style="color:#10b981;font-weight:600;">✔ Đã lấy xong</span>'
          : (c.total_records > 0 ? '<span style="color:#38bdf8;font-weight:600;">✔ Đã có dữ liệu</span>' : '<span style="color:#64748b;">Chưa lấy</span>');
        tr.innerHTML = `
          <td><span class="badge ${badgeClass}">${c.source}</span></td>
          <td><b>${c.name}</b></td>
          <td class="sold">${fmtNum(c.total_records)} SP</td>
          <td>${statusHtml}</td>
        `;
        tbody.appendChild(tr);
      });
    }

    async function refreshLogsAndStatus() {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();
        const badge = document.getElementById('jobStatusBadge');
        const btn = document.getElementById('startCrawlBtn');
        const taskText = document.getElementById('currentTaskText');

        if (data.job.is_running) {
          badge.className = 'badge badge-running';
          badge.textContent = '⚡ Đang chạy: ' + data.job.current_task;
          btn.disabled = true;
          taskText.textContent = 'Đang thực thi: ' + data.job.current_task + ' (Bắt đầu: ' + data.job.started_at + ')';
        } else {
          badge.className = 'badge badge-idle';
          badge.textContent = '● Sẵn sàng (Idle)';
          btn.disabled = false;
          if (data.job.last_error) {
            taskText.innerHTML = '<span style="color:#ef4444;">Lỗi gần nhất: ' + data.job.last_error + '</span>';
          } else if (data.job.last_result) {
            taskText.innerHTML = '<span style="color:#34d399;">✔ Hoàn tất gần nhất: ' + JSON.stringify(data.job.last_result) + '</span>';
          } else {
            taskText.textContent = 'Trạng thái: Sẵn sàng nhận lệnh cào.';
          }
        }

        const logBox = document.getElementById('logBox');
        logBox.textContent = data.logs || '';
        logBox.scrollTop = logBox.scrollHeight;
        if (data.categories) renderCheckpoints(data.categories);
      } catch (e) {}
    }

    async function startCrawlJob() {
      const source = document.getElementById('sourceSelect').value;
      const mode = document.getElementById('modeSelect').value;
      const category_id = document.getElementById('categorySelect').value;
      const query = document.getElementById('queryInput').value.trim();
      const limit_items = parseInt(document.getElementById('limitItemsSelect').value || '40', 10);
      const max_pages = Math.max(1, Math.ceil(limit_items / 40));
      const show_head = document.getElementById('showHeadCheck').checked;
      const reset_checkpoint = document.getElementById('resetCheckpointCheck').checked;
      const auto_silver_gold = document.getElementById('autoSilverGoldCheck').checked;

      if (mode === 'query' && !query) {
        alert('Vui lòng gõ tên sản phẩm bạn muốn tìm!');
        return;
      }

      const btn = document.getElementById('startCrawlBtn');
      const banner = document.getElementById('friendlyBanner');
      const bannerMsg = document.getElementById('friendlyBannerMsg');
      btn.disabled = true;
      btn.textContent = '⏳ Đang tự động lấy dữ liệu...';
      banner.style.display = 'none';

      try {
        const res = await fetch('/api/crawl', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            source, mode, category_id, query, limit_items, max_pages, show_head, reset_checkpoint, auto_silver_gold
          })
        });
        const out = await res.json();
        if (!res.ok) {
          alert(out.error || 'Không thể lấy dữ liệu lúc này, vui lòng thử lại.');
        } else {
          banner.style.display = 'block';
          if (out.friendly_message) {
            bannerMsg.textContent = out.friendly_message;
          } else if (out.ingested !== undefined) {
            bannerMsg.textContent = `✔ Đã lấy thành công ${fmtNum(out.ingested)} sản phẩm mới! Tổng kho hiện có ${fmtNum(out.total_silver || out.ingested)} sản phẩm sẵn sàng xem & tải Excel.`;
          } else {
            bannerMsg.textContent = '✔ Đã gửi lệnh thu thập dữ liệu tự động! Dữ liệu sẽ tự cập nhật vào Danh Sách Sản Phẩm trong giây lát.';
          }
          await loadSilverData();
        }
      } catch (e) {
        alert('Lỗi kết nối tới máy chủ: ' + e);
      } finally {
        btn.disabled = false;
        btn.textContent = '🚀 Bắt Đầu Lấy Dữ Liệu';
        refreshLogsAndStatus();
      }
    }

    async function downloadSilverCSV() {
      if (!silverProducts.length) {
        await loadSilverData();
      }
      if (!silverProducts.length) {
        alert('Chưa có sản phẩm nào để tải xuống!');
        return;
      }
      const headers = ['San', 'ID_San_Pham', 'Thuong_Hieu', 'Ten_San_Pham', 'Gia_Ban_VND', 'Gia_Goc_VND', 'Giam_Gia_Pct', 'Da_Ban', 'Danh_Gia', 'So_Luot_Danh_Gia', 'Link_San_Pham'];
      const esc = (v) => '"' + String(v ?? '').replace(/"/g, '""') + '"';
      const rows = silverProducts.map(p => [
        p.source, p.product_id, p.brand || 'Unknown', p.name, p.price, p.original_price, p.discount_rate, p.quantity_sold, p.rating_average, p.review_count, p.product_url
      ].map(esc).join(','));
      const csvContent = '\\uFEFF' + headers.join(',') + '\\n' + rows.join('\\n');
      const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'san_pham_thu_thap.csv';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
    }

    async function startShopeeLogin() {
      // Mở trực tiếp trang đăng nhập Shopee trên trình duyệt của người dùng
      window.open('https://shopee.vn/buyer/login', '_blank', 'noopener,noreferrer');
      document.getElementById('shopeeAuthBox').style.display = 'block';
      try {
        await fetch('/api/login-shopee', { method: 'POST' });
      } catch (e) {}
      refreshLogsAndStatus();
    }

    async function submitShopeeAuth() {
      const cookie_or_json = document.getElementById('shopeeCookieInput').value.trim();
      const category_id = document.getElementById('categorySelect').value;
      if (!cookie_or_json) {
        alert('Vui lòng dán Response JSON (search_items) hoặc chuỗi Cookie từ tab Shopee trước khi bấm Xác nhận!');
        return;
      }
      const res = await fetch('/api/shopee-auth', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ cookie: cookie_or_json, category_id })
      });
      const out = await res.json();
      if (res.ok) {
        document.getElementById('shopeeAuthBox').style.display = 'none';
        document.getElementById('shopeeCookieInput').value = '';
        alert(out.message || 'Đã lưu và xử lý dữ liệu Shopee thành công!');
      } else {
        alert(out.error || 'Không thể xử lý dữ liệu Shopee');
      }
      refreshLogsAndStatus();
    }

    async function runSilverGoldOnly() {
      const res = await fetch('/api/run-etl', { method: 'POST' });
      const out = await res.json();
      if (!res.ok) alert(out.error || 'Lỗi chạy ETL');
      refreshLogsAndStatus();
    }

    async function resetAllCheckpoints() {
      if (!confirm('Bạn có chắc muốn xóa toàn bộ checkpoint để cào lại từ trang 1?')) return;
      await fetch('/api/reset-checkpoint', { method: 'POST' });
      refreshLogsAndStatus();
    }

    async function loadSilverData() {
      const res = await fetch('/api/silver');
      const data = await res.json();
      silverProducts = data.products || [];
      renderSilverTable();
    }

    function renderSilverTable() {
      const q = (document.getElementById('silverSearchInput').value || '').toLowerCase();
      const src = document.getElementById('silverSourceFilter').value;
      const filtered = silverProducts.filter(p => {
        if (src !== 'all' && p.source !== src) return false;
        if (q && !(String(p.name).toLowerCase().includes(q) || String(p.brand).toLowerCase().includes(q))) return false;
        return true;
      });

      const kpiDiv = document.getElementById('silverKpis');
      const totalSold = filtered.reduce((acc, x) => acc + (Number(x.quantity_sold) || 0), 0);
      const avgPrice = filtered.length ? filtered.reduce((acc, x) => acc + (Number(x.price) || 0), 0) / filtered.length : 0;
      kpiDiv.innerHTML = `
        <div class="kpi-card"><div class="label">Tổng sản phẩm hiển thị</div><div class="value">${fmtNum(filtered.length)}</div></div>
        <div class="kpi-card"><div class="label">Giá bán trung bình</div><div class="value price">${fmtVND(Math.round(avgPrice))}</div></div>
        <div class="kpi-card"><div class="label">Tổng sản lượng đã bán</div><div class="value sold">${fmtNum(totalSold)}</div></div>
      `;

      const tbody = document.getElementById('silverTableBody');
      tbody.innerHTML = '';
      filtered.slice(0, 200).forEach(p => {
        const badgeClass = p.source === 'shopee' ? 'badge-shopee' : 'badge-tiki';
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td><span class="badge ${badgeClass}">${p.source}</span></td>
          <td><code>${p.product_id}</code></td>
          <td><b>${p.brand || 'Unknown'}</b></td>
          <td>${p.name}</td>
          <td class="price">${fmtVND(p.price)}</td>
          <td style="color:#94a3b8;text-decoration:line-through;">${fmtVND(p.original_price)}</td>
          <td class="discount">-${p.discount_rate}%</td>
          <td class="sold">${fmtNum(p.quantity_sold)}</td>
          <td>⭐ ${p.rating_average} (${fmtNum(p.review_count)})</td>
          <td><a href="${p.product_url}" target="_blank" style="color:#60a5fa;">Mở ↗</a></td>
        `;
        tbody.appendChild(tr);
      });
    }

    async function loadGoldData() {
      const res = await fetch('/api/gold');
      const data = await res.json();

      const compBody = document.getElementById('goldComparisonBody');
      compBody.innerHTML = '';
      (data.platform_price_comparison || []).forEach(r => {
        const badgeClass = r.source === 'shopee' ? 'badge-shopee' : 'badge-tiki';
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td><b>${r.category_group}</b></td>
          <td><span class="badge ${badgeClass}">${r.source}</span></td>
          <td>${fmtNum(r.product_count)}</td>
          <td>${fmtNum(r.distinct_brands)}</td>
          <td>${fmtVND(r.min_price)}</td>
          <td class="price">${fmtVND(r.avg_price)}</td>
          <td>${fmtVND(r.median_price)}</td>
          <td>${fmtVND(r.max_price)}</td>
          <td class="discount">${r.avg_discount_rate}%</td>
          <td class="sold">${fmtNum(r.total_quantity_sold)}</td>
          <td>⭐ ${r.avg_rating || '-'}</td>
        `;
        compBody.appendChild(tr);
      });

      const bestBody = document.getElementById('goldBestSellersBody');
      bestBody.innerHTML = '';
      (data.best_sellers || []).forEach(r => {
        const badgeClass = r.source === 'shopee' ? 'badge-shopee' : 'badge-tiki';
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td><span class="badge ${badgeClass}">${r.source}</span></td>
          <td><b>#${r.rank_in_source}</b></td>
          <td>${r.category_group}</td>
          <td><b>${r.brand}</b></td>
          <td><a href="${r.product_url}" target="_blank" style="color:#e2e8f0;text-decoration:none;">${r.name}</a></td>
          <td class="price">${fmtVND(r.price)}</td>
          <td class="discount">-${r.discount_rate}%</td>
          <td class="sold">${fmtNum(r.quantity_sold)}</td>
          <td class="price">${fmtVND(r.estimated_revenue)}</td>
          <td>⭐ ${r.rating_average}</td>
        `;
        bestBody.appendChild(tr);
      });

      const brandBody = document.getElementById('goldBrandStatsBody');
      brandBody.innerHTML = '';
      (data.category_brand_stats || []).forEach(r => {
        const badgeClass = r.source === 'shopee' ? 'badge-shopee' : 'badge-tiki';
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td><span class="badge ${badgeClass}">${r.source}</span></td>
          <td>${r.category_name}</td>
          <td><b>${r.brand}</b></td>
          <td>${fmtNum(r.product_count)}</td>
          <td class="price">${fmtVND(r.avg_price)}</td>
          <td>${fmtVND(r.min_price)}</td>
          <td>${fmtVND(r.max_price)}</td>
          <td class="discount">${r.avg_discount_rate}%</td>
          <td class="discount">${r.max_discount_rate}%</td>
          <td class="sold">${fmtNum(r.total_quantity_sold)}</td>
        `;
        brandBody.appendChild(tr);
      });
    }

    loadCategoriesAndCheckpoints();
    refreshLogsAndStatus();
    setInterval(refreshLogsAndStatus, 2000);
  </script>
</body>
</html>
"""


def _get_categories_with_progress() -> List[Dict[str, Any]]:
    cm = _get_category_manager()
    cp = CheckpointManager()
    items: List[Dict[str, Any]] = []
    for t in cm.list_all():
        key = str(t.category_id) if t.category_id else t.target_id
        prog = cp.get_progress(key)
        items.append({
            "target_id": t.target_id,
            "name": t.name,
            "source": t.source,
            "keyword": t.keyword,
            "category_id": t.category_id,
            "last_page": prog.get("last_page", 0),
            "total_records": prog.get("total_records", 0),
            "completed": prog.get("completed", False),
        })
    return items


def _read_tail_logs(lines_count: int = 35) -> str:
    log_file = LOGS_DIR / "crawler.log"
    if not log_file.exists():
        return "Chưa có file log."
    try:
        lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(lines[-lines_count:])
    except Exception as e:
        return f"Không thể đọc log: {e}"


def _run_crawl_background(payload: Dict[str, Any]) -> None:
    source = str(payload.get("source") or "tiki").lower()
    mode = str(payload.get("mode") or "category")
    category_id = payload.get("category_id")
    query = str(payload.get("query") or "").strip()
    max_pages = int(payload.get("max_pages") or 2)
    show_head = bool(payload.get("show_head", False))
    reset_cp = bool(payload.get("reset_checkpoint", False))
    auto_etl = bool(payload.get("auto_silver_gold", True))

    with JOB_STATE.lock:
        JOB_STATE.is_running = True
        JOB_STATE.started_at = datetime.now().strftime("%H:%M:%S")
        JOB_STATE.current_task = f"Cào {source.upper()} ({mode})"
        JOB_STATE.last_error = None

    try:
        from src.analytics.gold_builder import GoldBuilder
        from src.core.orchestrator import CrawlerOrchestrator
        from src.transform.silver_writer import SilverPipeline
        from src.web.snapshot import build_and_save_web_snapshot

        cm = CategoryManager()
        cp = CheckpointManager()
        bw = BronzeWriter()
        orchestrator = CrawlerOrchestrator(category_mgr=cm, checkpoint_mgr=cp, bronze_writer=bw)

        targets: List[CrawlTarget] = []
        if mode == "category" and category_id:
            target = cm.get_by_id(int(category_id), source=source)
            if target:
                targets.append(target)
        elif mode == "query" and query:
            targets = orchestrator.resolve_targets(query=query, source=source, allow_custom_fallback=True)
        elif mode == "all":
            targets = orchestrator.list_all_categories(source=source)

        if not targets:
            raise ValueError("Không tìm thấy mục tiêu nào hợp lệ để cào.")

        results = orchestrator.crawl_targets(
            targets=targets,
            max_pages=max_pages,
            headless=not show_head,
            force_restart=reset_cp,
        )
        total_ingested = sum(r.ingested_records for r in results)

        etl_info = {}
        if auto_etl:
            with JOB_STATE.lock:
                JOB_STATE.current_task = "Đang chuẩn hóa Silver & xây dựng Gold..."
            silver_res = SilverPipeline().run()
            gold_res = GoldBuilder(category_mgr=cm).run()
            build_and_save_web_snapshot()
            etl_info = {
                "silver_records": silver_res.get("total_silver", 0),
                "gold_status": gold_res.get("status"),
            }

        with JOB_STATE.lock:
            JOB_STATE.last_result = {
                "source": source,
                "targets_crawled": len(targets),
                "ingested_records": total_ingested,
                **etl_info,
            }
    except Exception as e:
        logger.error(f"Background crawl error: {e}\n{traceback.format_exc()}")
        with JOB_STATE.lock:
            JOB_STATE.last_error = str(e)
    finally:
        with JOB_STATE.lock:
            JOB_STATE.is_running = False
            JOB_STATE.current_task = "Idle"


def _run_etl_background() -> None:
    with JOB_STATE.lock:
        JOB_STATE.is_running = True
        JOB_STATE.started_at = datetime.now().strftime("%H:%M:%S")
        JOB_STATE.current_task = "Chạy Silver & Gold Pipeline"
        JOB_STATE.last_error = None
    try:
        from src.analytics.gold_builder import GoldBuilder
        from src.transform.silver_writer import SilverPipeline
        from src.web.snapshot import build_and_save_web_snapshot

        silver_res = SilverPipeline().run()
        gold_res = GoldBuilder().run()
        build_and_save_web_snapshot()
        with JOB_STATE.lock:
            JOB_STATE.last_result = {
                "silver_records": silver_res.get("total_silver", 0),
                "gold_tables": len(gold_res.get("tables", {})),
            }
    except Exception as e:
        with JOB_STATE.lock:
            JOB_STATE.last_error = str(e)
    finally:
        with JOB_STATE.lock:
            JOB_STATE.is_running = False
            JOB_STATE.current_task = "Idle"


def _run_shopee_login_background() -> None:
    with JOB_STATE.lock:
        JOB_STATE.is_running = True
        JOB_STATE.started_at = datetime.now().strftime("%H:%M:%S")
        JOB_STATE.current_task = "Đăng nhập Shopee trên Chrome"
        JOB_STATE.last_error = None
    try:
        from src.scraper.shopee_scraper import ShopeeScraper

        scraper = ShopeeScraper()
        scraper.login_session()
        with JOB_STATE.lock:
            JOB_STATE.last_result = {"action": "Shopee Login Saved"}
    except Exception as e:
        with JOB_STATE.lock:
            JOB_STATE.last_error = str(e)
    finally:
        with JOB_STATE.lock:
            JOB_STATE.is_running = False
            JOB_STATE.current_task = "Idle"


class DashboardRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        pass  # Suppress noisy HTTP access logs in console

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
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/" or path == "/index.html":
            self._send_html(HTML_DASHBOARD)
            return

        if path == "/api/categories":
            self._send_json({"categories": _get_categories_with_progress()})
            return

        if path == "/api/status":
            self._send_json({
                "job": JOB_STATE.to_dict(),
                "logs": _read_tail_logs(35),
                "categories": _get_categories_with_progress(),
            })
            return

        if path == "/api/silver":
            import polars as pl
            files = sorted(SILVER_DIR.rglob("products.parquet")) if SILVER_DIR.exists() else []
            if not files:
                self._send_json({"products": []})
                return
            df = pl.read_parquet(files).sort("quantity_sold", descending=True)
            self._send_json({"products": df.to_dicts()})
            return

        if path == "/api/gold":
            import polars as pl
            result: Dict[str, Any] = {}
            for name in ("platform_price_comparison", "best_sellers", "category_brand_stats"):
                fp = GOLD_DIR / f"{name}.parquet"
                if fp.exists():
                    result[name] = pl.read_parquet(fp).to_dicts()
                else:
                    result[name] = []
            self._send_json(result)
            return

        self._send_json({"error": "Not found"}, status=404)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path

        content_len = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_len).decode("utf-8") if content_len > 0 else "{}"
        try:
            payload = json.loads(raw_body) if raw_body else {}
        except Exception:
            payload = {}

        if path == "/api/crawl":
            if JOB_STATE.is_running:
                self._send_json({"error": "Đang có một tiến trình chạy, vui lòng đợi hoàn tất!"}, status=409)
                return
            t = threading.Thread(target=_run_crawl_background, args=(payload,), daemon=True)
            t.start()
            self._send_json({"status": "started"})
            return

        if path == "/api/run-etl":
            if JOB_STATE.is_running:
                self._send_json({"error": "Đang có một tiến trình chạy!"}, status=409)
                return
            t = threading.Thread(target=_run_etl_background, daemon=True)
            t.start()
            self._send_json({"status": "started"})
            return

        if path == "/api/login-shopee":
            if JOB_STATE.is_running:
                self._send_json({"error": "Đang có một tiến trình chạy!"}, status=409)
                return
            t = threading.Thread(target=_run_shopee_login_background, daemon=True)
            t.start()
            self._send_json({"status": "started"})
            return

        if path == "/api/shopee-auth":
            from config import settings
            raw_val = str(payload.get("cookie") or "").strip()
            cat_id_raw = payload.get("category_id")
            cat_id = int(cat_id_raw) if cat_id_raw and str(cat_id_raw).isdigit() else 11035954

            if not raw_val:
                self._send_json({"error": "Vui lòng dán Response JSON (search_items) hoặc Cookie Shopee."}, status=400)
                return

            # If user pasted raw Shopee search_items JSON -> write to Bronze, transform to Silver & Gold, update snapshot!
            if raw_val.startswith("{") or raw_val.startswith("["):
                try:
                    from src.analytics.gold_builder import GoldBuilder
                    from src.transform.shopee_transformer import ShopeeTransformer
                    from src.transform.silver_writer import SilverPipeline
                    from src.web.snapshot import build_and_save_web_snapshot

                    parsed_json = json.loads(raw_val)
                    items = ShopeeTransformer.extract_items_from_payload(parsed_json)
                    if not items:
                        root_keys = list(parsed_json.keys())[:8] if isinstance(parsed_json, dict) else ["list"]
                        self._send_json({
                            "error": f"Không tìm thấy sản phẩm Shopee hợp lệ nào trong JSON (keys: {root_keys})."
                        }, status=400)
                        return

                    cm = _get_category_manager()
                    target = cm.get_by_id(cat_id, source="shopee") or CrawlTarget(
                        target_id=f"shopee_{cat_id}",
                        name=f"Shopee Category {cat_id}",
                        source="shopee",
                        keyword="shopee",
                        category_id=cat_id,
                    )
                    cp = CheckpointManager()
                    prog = cp.get_progress(str(cat_id))
                    next_page = int(prog.get("last_page", 0)) + 1

                    bw = BronzeWriter()
                    bw.write_page(target=target, records=items, page_number=next_page)
                    cp.update_progress(str(cat_id), page=next_page, added_records=len(items), completed=False)

                    silver_res = SilverPipeline().run()
                    GoldBuilder(category_mgr=cm).run()
                    build_and_save_web_snapshot()

                    msg = (
                        f"✔ Đã ghi {len(items)} sản phẩm Shopee vào Tầng Bronze (trang {next_page}), "
                        f"chuẩn hóa Silver (tổng {silver_res.get('total_silver', 0)} SP) và cập nhật Tầng Gold!"
                    )
                    logger.info(msg)
                    self._send_json({"status": "ingested", "count": len(items), "message": msg})
                    return
                except Exception as e:
                    self._send_json({"error": f"Lỗi xử lý JSON Shopee: {e}"}, status=400)
                    return

            for pair in raw_val.split(";"):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    settings.SHOPEE_COOKIES[k.strip()] = v.strip()
            logger.info("Đã cấp quyền và đồng bộ Cookie phiên đăng nhập Shopee từ giao diện Web.")
            self._send_json({
                "status": "authorized",
                "message": "Đã lưu Cookie phiên đăng nhập Shopee thành công!",
            })
            return

        if path == "/api/reset-checkpoint":
            CheckpointManager().reset()
            self._send_json({"status": "reset_ok"})
            return

        self._send_json({"error": "Not found"}, status=404)


def start_web_server(host: str = "127.0.0.1", port: int = 8080, open_browser: bool = True) -> None:
    """Starts the interactive Web UI Dashboard server."""
    url = f"http://{host}:{port}"
    server = ThreadingHTTPServer((host, port), DashboardRequestHandler)
    server.daemon_threads = True
    _get_category_manager()
    logger.info(f"Khoi chay Web UI Dashboard tai: {url}")
    print("\n" + "=" * 65)
    print(f" 🌐 E-COMMERCE DATA PLATFORM WEB UI ĐANG CHẠY TẠI: {url}")
    print(" -> Điền thông số cào, theo dõi tiến độ & xem báo cáo Silver/Gold trên trình duyệt.")
    print(" -> Nhấn Ctrl+C trên Terminal để dừng Web Server.")
    print("=" * 65 + "\n")

    if open_browser:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        print("\nĐã dừng Web Server.")
    finally:
        server.server_close()
