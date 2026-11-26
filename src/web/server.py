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
      <button class="tab-btn active" onclick="switchTab('crawl')">1. Trung Tâm Cào Dữ Liệu (Playwright)</button>
      <button class="tab-btn" onclick="switchTab('silver')">2. Dữ Liệu Sản Phẩm (Tầng Silver)</button>
      <button class="tab-btn" onclick="switchTab('gold')">3. Báo Cáo Phân Tích BI (Tầng Gold)</button>
    </div>

    <!-- TAB 1: CRAWL CONTROL CENTER -->
    <div id="tab-crawl" class="tab-panel active">
      <div class="grid-2">
        <!-- LEFT FORM -->
        <div>
          <div class="card">
            <h2>⚙️ Điền Thông Tin & Thông Số Cào</h2>

            <div class="form-group">
              <label>1. Chọn Sàn Thương Mại Điện Tử (Source)</label>
              <select id="sourceSelect" onchange="onSourceChange()">
                <option value="tiki">Tiki (Playwright - Ổn định, không bị chặn Captcha)</option>
                <option value="shopee">Shopee (Playwright + Chrome Profile)</option>
              </select>
            </div>

            <div class="form-group">
              <label>2. Chế Độ Chọn Mục Tiêu Cào</label>
              <select id="modeSelect" onchange="onModeChange()">
                <option value="category">Chọn từ Danh mục có sẵn</option>
                <option value="query">Nhập từ khóa tìm kiếm bất kỳ (Theo ý muốn)</option>
                <option value="all">Cào toàn bộ danh mục của sàn</option>
              </select>
            </div>

            <div class="form-group" id="categoryGroup">
              <label>3. Chọn Danh Mục</label>
              <select id="categorySelect"></select>
              <div class="hint">Hệ thống tự động nạp danh mục tương ứng với sàn đã chọn.</div>
            </div>

            <div class="form-group" id="queryGroup" style="display:none;">
              <label>3. Nhập Từ Khóa Sản Phẩm / Danh Mục Muốn Cào</label>
              <input type="text" id="queryInput" placeholder="VD: laptop gaming, bàn phím cơ, tai nghe sony..." />
              <div class="hint">Nếu khớp danh mục có sẵn sẽ cào theo danh mục, nếu chưa có sẽ tự động tạo mục tiêu cào mới.</div>
            </div>

            <div class="form-group">
              <label>4. Số Trang Tối Đa Cần Cào (Max Pages)</label>
              <input type="number" id="maxPagesInput" value="2" min="1" max="50" />
              <div class="hint">Mỗi trang chứa khoảng 40 (Tiki) đến 60 (Shopee) sản phẩm.</div>
            </div>

            <div class="form-group" style="margin-top: 14px;">
              <label>5. Tùy Chọn Vận Hành Pipeline</label>
              <label class="checkbox-row">
                <input type="checkbox" id="showHeadCheck" />
                <span>Mở cửa sổ trình duyệt Chrome trực quan (<code>--head</code>)</span>
              </label>
              <label class="checkbox-row">
                <input type="checkbox" id="resetCheckpointCheck" />
                <span>Cào lại từ Trang 1 (Reset Checkpoint của mục tiêu này)</span>
              </label>
              <label class="checkbox-row">
                <input type="checkbox" id="autoSilverGoldCheck" checked />
                <span>Tự động chuẩn hóa <b>Tầng Silver</b> & cập nhật báo cáo <b>Tầng Gold</b> sau khi cào</span>
              </label>
            </div>

            <div class="btn-group">
              <button id="startCrawlBtn" class="btn btn-primary" onclick="startCrawlJob()">
                🚀 Bắt Đầu Cào Dữ Liệu
              </button>
            </div>

            <div class="btn-group">
              <button class="btn btn-shopee" onclick="startShopeeLogin()">
                🔑 Đăng Nhập Shopee & Cấp Quyền
              </button>
              <button class="btn btn-success" onclick="runSilverGoldOnly()">
                ⚡ Chạy Silver & Gold Ngay
              </button>
            </div>

            <!-- SHOPEE AUTH PANEL -->
            <div id="shopeeAuthBox" style="display:none; margin-top:14px; padding:14px; border-radius:10px; background:rgba(238,77,45,0.1); border:1px solid #ee4d2d;">
              <div style="font-size:0.88rem; font-weight:700; color:#ff785a; margin-bottom:8px;">
                🔐 Kết Nối Quyền Truy Cập Shopee
              </div>
              <div style="font-size:0.8rem; color:#cbd5e1; margin-bottom:10px;">
                Trang <b>shopee.vn/buyer/login</b> đã được mở ở tab mới. Sau khi đăng nhập xong trên Shopee:<br/>
                • Bạn có thể dán <b>Cookie Shopee</b> (<code>SPC_U=...; SPC_ST=...</code>) hoặc <b>JSON search_items</b> vào ô dưới để cấp quyền phiên làm việc cho Cloud:
              </div>
              <input type="text" id="shopeeCookieInput" placeholder="Dán Cookie Shopee (SPC_ST=...) hoặc để trống nếu chỉ xác nhận phiên..." style="margin-bottom:8px;" />
              <div style="display:flex; gap:8px;">
                <button class="btn btn-shopee" style="flex:1; padding:8px;" onclick="submitShopeeAuth()">✔ Xác Nhận Cấp Quyền</button>
                <button class="btn btn-outline" style="padding:8px 12px;" onclick="document.getElementById('shopeeAuthBox').style.display='none'">Đóng</button>
              </div>
            </div>
          </div>
        </div>

        <!-- RIGHT MONITOR -->
        <div>
          <div class="card">
            <h2>
              <span>📡 Trạng Thái Thực Thi & Live Logs</span>
              <button class="btn btn-outline" style="padding: 4px 10px; font-size: 0.78rem;" onclick="refreshLogsAndStatus()">Làm mới</button>
            </h2>
            <div id="currentTaskText" style="margin-bottom: 10px; font-size: 0.9rem; color: #93c5fd;">
              Trạng thái: Đang chờ lệnh...
            </div>
            <div id="logBox" class="log-box">Đang tải log hệ thống...</div>
          </div>

          <div class="card">
            <h2>
              <span>📊 Tiến Độ Cào Các Danh Mục (Checkpoints)</span>
              <button class="btn btn-outline" style="padding: 4px 10px; font-size: 0.78rem;" onclick="resetAllCheckpoints()">Reset Tất Cả</button>
            </h2>
            <div class="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Sàn</th>
                    <th>ID</th>
                    <th>Tên Danh Mục</th>
                    <th>Từ Khóa</th>
                    <th>Trang Cuối</th>
                    <th>Đã Cào</th>
                    <th>Trạng Thái</th>
                  </tr>
                </thead>
                <tbody id="checkpointTableBody"></tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- TAB 2: SILVER PRODUCTS EXPLORER -->
    <div id="tab-silver" class="tab-panel">
      <div class="card">
        <h2>
          <span>💎 Kho Dữ Liệu Sản Phẩm Đã Làm Sạch (Silver Layer - Parquet)</span>
          <div style="display:flex; gap:10px;">
            <input type="text" id="silverSearchInput" placeholder="Tìm tên SP, thương hiệu..." style="width:240px; padding:6px 10px;" oninput="renderSilverTable()" />
            <select id="silverSourceFilter" style="width:130px; padding:6px 10px;" onchange="renderSilverTable()">
              <option value="all">Tất cả sàn</option>
              <option value="shopee">Shopee</option>
              <option value="tiki">Tiki</option>
            </select>
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
      const select = document.getElementById('categorySelect');
      select.innerHTML = '';
      allCategories.filter(c => c.source === src).forEach(c => {
        const opt = document.createElement('option');
        opt.value = c.category_id;
        opt.textContent = `${c.name} (ID: ${c.category_id} | từ khóa: '${c.keyword}')`;
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
          ? '<span style="color:#10b981;font-weight:600;">✔ Hoàn thành</span>'
          : (c.last_page > 0 ? '<span style="color:#fbbf24;">⏳ Đang dở</span>' : '<span style="color:#64748b;">Chưa cào</span>');
        tr.innerHTML = `
          <td><span class="badge ${badgeClass}">${c.source}</span></td>
          <td><code>${c.category_id}</code></td>
          <td><b>${c.name}</b></td>
          <td>${c.keyword}</td>
          <td>${c.last_page}</td>
          <td class="sold">${fmtNum(c.total_records)}</td>
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
      const max_pages = parseInt(document.getElementById('maxPagesInput').value || '2', 10);
      const show_head = document.getElementById('showHeadCheck').checked;
      const reset_checkpoint = document.getElementById('resetCheckpointCheck').checked;
      const auto_silver_gold = document.getElementById('autoSilverGoldCheck').checked;

      if (mode === 'query' && !query) {
        alert('Vui lòng nhập từ khóa muốn cào!');
        return;
      }

      const res = await fetch('/api/crawl', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          source, mode, category_id, query, max_pages, show_head, reset_checkpoint, auto_silver_gold
        })
      });
      const out = await res.json();
      if (!res.ok) alert(out.error || 'Không thể khởi chạy job');
      refreshLogsAndStatus();
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
      const res = await fetch('/api/shopee-auth', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ cookie: cookie_or_json })
      });
      const out = await res.json();
      if (res.ok) {
        document.getElementById('shopeeAuthBox').style.display = 'none';
        alert(out.message || 'Đã lưu quyền truy cập phiên Shopee thành công!');
      } else {
        alert(out.error || 'Không thể lưu quyền truy cập Shopee');
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
    cm = CategoryManager()
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
            raw_cookie = str(payload.get("cookie") or "").strip()
            if raw_cookie:
                for pair in raw_cookie.split(";"):
                    if "=" in pair:
                        k, v = pair.split("=", 1)
                        settings.SHOPEE_COOKIES[k.strip()] = v.strip()
            logger.info("Đã cấp quyền và đồng bộ phiên đăng nhập Shopee từ giao diện Web.")
            self._send_json({
                "status": "authorized",
                "message": "Đã kết nối và cấp quyền phiên đăng nhập Shopee thành công!",
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
    logger.info(f"Khoi chay Web UI Dashboard tai: {url}")
    print("\n" + "=" * 65)
    print(f" 🌐 E-COMMERCE DATA PLATFORM WEB UI ĐANG CHẠY TẠI: {url}")
    print(" -> Điền thông số cào, theo dõi tiến độ & xem báo cáo Silver/Gold trên trình duyệt.")
    print(" -> Nhấn Ctrl+C trên Terminal để dừng Web Server.")
    print("=" * 65 + "\n")

    if open_browser:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nĐã dừng Web Server.")
    finally:
        server.server_close()
