#!/usr/bin/env python3
"""
OpenClaw Hive - Task Monitor Dashboard
A lightweight web dashboard to monitor task progress for all config_tasks.

Uses Python standard library + omegaconf (already installed for hive).
No Flask or other web framework required.

Usage:
    python web_dashboard.py [--port 5000] [--host 0.0.0.0]

Then open http://<host>:<port> in your browser.
"""

import os
import sys
import re
import json
import argparse
from collections import Counter
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

from omegaconf import OmegaConf

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_TASKS_DIR = os.path.join(SCRIPT_DIR, "config_tasks")
OUTPUTS_DIR = os.path.join(SCRIPT_DIR, "outputs")

# ---------------------------------------------------------------------------
# Data collection helpers (mirrors hive.sh --stats logic)
# ---------------------------------------------------------------------------

def list_config_files():
    """List all config YAML files in config_tasks/ directory."""
    configs = []
    if not os.path.isdir(CONFIG_TASKS_DIR):
        return configs
    for f in sorted(os.listdir(CONFIG_TASKS_DIR)):
        if f.endswith((".yaml", ".yml")):
            configs.append(f)
    return configs


def load_config(config_filename):
    """Load a config YAML file and return OmegaConf object."""
    path = os.path.join(CONFIG_TASKS_DIR, config_filename)
    try:
        return OmegaConf.load(path)
    except Exception:
        return None


def get_output_dir(config_filename):
    """Get the output directory for a config file (mirrors hive.sh logic)."""
    basename = os.path.splitext(config_filename)[0]
    return os.path.join(OUTPUTS_DIR, basename)


def read_jsonl(filepath):
    """Read a JSONL file and return set of non-empty lines."""
    items = set()
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        items.add(line)
        except Exception:
            pass
    return items


def check_process_alive(pid_file):
    """Check if the process in PID file is still alive."""
    if not pid_file or not os.path.exists(pid_file):
        return False, None
    try:
        with open(pid_file, "r") as f:
            pid = f.read().strip()
        if not pid:
            return False, None
        os.kill(int(pid), 0)
        return True, int(pid)
    except (OSError, ValueError, ProcessLookupError):
        return False, pid


def parse_log_errors(log_file):
    """Parse log file and categorize errors (mirrors hive.sh --stats logic)."""
    error_categories = Counter()
    if not log_file or not os.path.exists(log_file):
        return error_categories

    error_keywords = [
        ("Gateway startup timeout", "Gateway startup timeout"),
        ("Gateway start failed", "Gateway start failed"),
        ("Gateway start unexpected", "Gateway unexpected output"),
        ("Failed to update port", "Port update failed"),
        ("Script execution failed", "Script execution failed"),
        ("Skill download failed", "Skill download failed"),
        ("User profile download failed", "User profile download failed"),
        ("Agents download failed", "Agents download failed"),
        ("Upload failed", "OBS upload failed"),
        (r"upload .* failed", "File upload to sandbox failed"),
        ("extract code failed", "Code extraction failed"),
        ("LLM error api_error", "LLM API error"),
    ]

    try:
        with open(log_file, "r", encoding="utf-8", errors="replace") as f:
            log_content = f.read()
    except Exception:
        return error_categories

    error_pattern = re.compile(r"Task \d+ failed:.*?RuntimeError: (.+?)(?:\n|$)")
    traceback_pattern = re.compile(r"Traceback \(most recent call last\)")

    for match in error_pattern.finditer(log_content):
        msg = match.group(1)
        classified = False
        for keyword, category in error_keywords:
            if re.search(keyword, msg, re.IGNORECASE):
                error_categories[category] += 1
                classified = True
                break
        if not classified:
            error_categories["Other RuntimeError"] += 1

    unmatched_tracebacks = len(traceback_pattern.findall(log_content)) - sum(error_categories.values())
    if unmatched_tracebacks > 0:
        error_categories["Uncategorized Traceback"] += unmatched_tracebacks

    return error_categories


def get_log_tail(log_file, num_lines=50):
    """Get the last N lines of a log file."""
    if not log_file or not os.path.exists(log_file):
        return []
    try:
        with open(log_file, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        return [line.rstrip() for line in lines[-num_lines:]]
    except Exception:
        return []


def get_task_status(config_filename):
    """Collect full status for one config task."""
    cfg = load_config(config_filename)
    if cfg is None:
        return {
            "config_file": config_filename,
            "error": "Failed to load config",
            "status": "ERROR",
        }

    run_cfg = cfg.run_config
    output_dir = get_output_dir(config_filename)

    # Task counts
    input_path = run_cfg.task.task_input_path
    complete_record = run_cfg.task.get("task_complete_record", "complete.jsonl")
    failed_record = run_cfg.task.get("task_failed_record", "failed.jsonl")
    complete_file = os.path.join(output_dir, complete_record)
    failed_file = os.path.join(output_dir, failed_record)

    total = 0
    if run_cfg.total_num != 0:
        total = run_cfg.total_num
    else:
        if os.path.isdir(input_path):
            total = len([f for f in os.listdir(input_path)
                         if os.path.isfile(os.path.join(input_path, f))])
    total = max(0, total - run_cfg.start_index)

    complete_set = read_jsonl(complete_file)
    failed_set = read_jsonl(failed_file)

    done = len(complete_set)
    fail = len(failed_set)
    finished = done + fail
    pending = max(0, total - finished)
    rate = (done / finished * 100) if finished > 0 else 0

    # Process status
    pid_file = os.path.join(output_dir, "hive.pid")
    is_alive, pid = check_process_alive(pid_file)
    proc_status = "RUNNING" if is_alive else "STOPPED"

    # Log files
    log_file = os.path.join(output_dir, "nohup.log")
    clean_log_file = os.path.join(output_dir, "nohup_clean.log")

    # Error analysis
    error_categories = parse_log_errors(log_file)

    # Recent log
    recent_log = get_log_tail(clean_log_file if os.path.exists(clean_log_file) else log_file, 30)

    # Timestamps
    pid_mtime = None
    if os.path.exists(pid_file):
        pid_mtime = datetime.fromtimestamp(os.path.getmtime(pid_file)).strftime("%Y-%m-%d %H:%M:%S")

    # End time: log file's last modification time, only when process is stopped
    end_time = None
    if not is_alive:
        log_for_time = log_file if os.path.exists(log_file) else (clean_log_file if os.path.exists(clean_log_file) else None)
        if log_for_time:
            end_time = datetime.fromtimestamp(os.path.getmtime(log_for_time)).strftime("%Y-%m-%d %H:%M:%S")

    # API key short display
    api_key = run_cfg.sandbox.get("openclaw_api_key", "")
    if api_key and len(api_key) >= 4:
        api_key_short = "key-" + api_key[-4:]
    else:
        api_key_short = "nokey"

    return {
        "config_file": config_filename,
        "output_dir": output_dir,
        "status": proc_status,
        "pid": pid if is_alive else None,
        "pid_mtime": pid_mtime,
        "end_time": end_time,
        "total": total,
        "completed": done,
        "failed": fail,
        "pending": pending,
        "success_rate": round(rate, 1),
        "concurrent_num": run_cfg.concurrent_num,
        "start_index": run_cfg.start_index,
        "api_key_short": api_key_short,
        "error_categories": dict(error_categories.most_common()),
        "recent_log": recent_log,
    }


def get_all_status():
    """Get status for all config tasks, sorted by date in filename (newest first).
    Configs without a date pattern in filename are excluded entirely.
    Assign sequence numbers such that the newest config gets the largest seq number.
    """
    configs = list_config_files()

    # Extract date from filename (e.g. config_0609_demo.yaml -> 0609)
    date_pattern = re.compile(r"(\d{4})")

    dated = []   # (date_str, config_file)
    for cfg_file in configs:
        m = date_pattern.search(cfg_file)
        if m:
            dated.append((m.group(1), cfg_file))

    # Sort dated configs by date descending (newest first)
    dated.sort(key=lambda x: x[0], reverse=True)

    total = len(dated)
    results = []
    # Assign seq numbers: newest (idx=0) gets total, oldest gets 1
    for idx, (_, cfg_file) in enumerate(dated, start=1):
        status = get_task_status(cfg_file)
        status["seq"] = total - idx + 1   # Reverse order: newest gets highest number
        results.append(status)

    return results


# ---------------------------------------------------------------------------
# 精美浅色系 HTML 模板（支持标签化元信息 + 倒序序号）
# ---------------------------------------------------------------------------

HTML_PAGE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>OpenClaw Hive - Task Monitor</title>
<style>
:root {
    --bg-primary: #f6f8fa;
    --bg-secondary: #ffffff;
    --bg-card: #ffffff;
    --bg-card-hover: #fafbfc;
    --text-primary: #24292f;
    --text-secondary: #57606a;
    --text-muted: #8c95a0;
    --accent-blue: #0969da;
    --accent-blue-light: #ddf4ff;
    --accent-green: #1a7f37;
    --accent-green-light: #dafbe1;
    --accent-red: #cf222e;
    --accent-red-light: #ffebe9;
    --accent-yellow: #9a6700;
    --accent-yellow-light: #fff8c5;
    --border: #d0d7de;
    --border-light: #e1e4e8;
    --shadow-sm: 0 1px 2px rgba(31,35,40,0.04);
    --shadow-md: 0 3px 6px rgba(140,149,160,0.1);
    --radius-lg: 10px;
    --radius-md: 6px;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    background: var(--bg-primary); color: var(--text-primary); min-height: 100vh; line-height: 1.5;
}
.header {
    background: var(--bg-secondary); border-bottom: 1px solid var(--border);
    padding: 14px 32px; display: flex; align-items: center;
    justify-content: space-between; position: sticky; top: 0; z-index: 100;
    box-shadow: var(--shadow-sm);
}
.header-left { display: flex; align-items: center; gap: 12px; }
.header h1 {
    font-size: 18px; font-weight: 600; color: var(--text-primary);
}
.header .logo {
    width: 30px; height: 30px; background: var(--accent-blue-light);
    border-radius: var(--radius-md); display: flex; align-items: center; justify-content: center; font-size: 16px;
}
.header-right {
    display: flex; align-items: center; gap: 20px; font-size: 13px; color: var(--text-secondary);
}
.refresh-btn {
    background: var(--accent-blue); color: white; border: 1px solid rgba(27,31,36,0.15);
    padding: 6px 14px; border-radius: var(--radius-md); cursor: pointer;
    font-size: 13px; font-weight: 500; transition: all 0.15s ease;
    display: flex; align-items: center; gap: 6px; box-shadow: var(--shadow-sm);
}
.refresh-btn:hover { background: #0c63ce; }
.refresh-btn:active { transform: translateY(1px); }
.refresh-btn.spinning .refresh-icon { animation: spin 1s linear infinite; }
@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }

.auto-refresh { display: flex; align-items: center; gap: 8px; }
.auto-refresh select {
    background: var(--bg-secondary); color: var(--text-primary);
    border: 1px solid var(--border); padding: 4px 8px;
    border-radius: var(--radius-md); font-size: 13px; cursor: pointer; outline: none;
}
.auto-refresh select:focus { border-color: var(--accent-blue); }

.container { max-width: 1280px; margin: 0 auto; padding: 32px 24px; }

/* 概览卡片样式 - 增加填充和视觉效果 */
.summary {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 20px; margin-bottom: 32px;
}
.summary-card {
    background: var(--bg-card); border: 1px solid var(--border);
    border-radius: var(--radius-lg); padding: 20px 24px; /* 增加填充 */
    display: flex; flex-direction: column; justify-content: space-between;
    box-shadow: var(--shadow-sm); transition: transform 0.2s, box-shadow 0.2s;
}
.summary-card:hover { transform: translateY(-2px); box-shadow: var(--shadow-md); }
.summary-card .label {
    font-size: 13px; font-weight: 600; color: var(--text-secondary); margin-bottom: 8px;
    letter-spacing: 0.3px;
}
.summary-card .value { font-size: 32px; font-weight: 700; font-family: monospace; }
.summary-card.total { border-left: 4px solid var(--accent-blue); }
.summary-card.total .value { color: var(--accent-blue); }
.summary-card.completed { border-left: 4px solid var(--accent-green); }
.summary-card.completed .value { color: var(--accent-green); }
.summary-card.failed { border-left: 4px solid var(--accent-red); }
.summary-card.failed .value { color: var(--accent-red); }
.summary-card.running { border-left: 4px solid var(--accent-yellow); }
.summary-card.running .value { color: var(--accent-yellow); }

/* 任务卡片样式 */
.task-card {
    background: var(--bg-card); border: 1px solid var(--border);
    border-radius: var(--radius-lg); margin-bottom: 16px; overflow: hidden; 
    box-shadow: var(--shadow-sm); transition: box-shadow 0.2s;
}
.task-card:hover { box-shadow: var(--shadow-md); }
.task-card-header {
    padding: 16px 24px; display: flex; align-items: center;
    justify-content: space-between; cursor: pointer; user-select: none;
}
.task-card-header:hover { background: var(--bg-card-hover); }
.task-name { display: flex; align-items: center; gap: 14px; flex-wrap: wrap; }
.task-name .seq-num {
    font-size: 13px; font-weight: 600; color: var(--text-secondary);
    background: var(--bg-primary); width: 28px; height: 28px;
    border-radius: 50%; display: inline-flex; align-items: center; justify-content: center;
    border: 1px solid var(--border-light);
}
.task-name .config-icon { font-size: 16px; display: flex; align-items: center; }
.task-name h3 { font-size: 15px; font-weight: 600; color: var(--text-primary); }

/* 标签样式 - 用于 API Key、启动时间、结束时间 */
.tag {
    display: inline-block;
    background: var(--bg-primary);
    border: 1px solid var(--border-light);
    border-radius: 20px;
    padding: 2px 10px;
    font-size: 11px;
    font-weight: 500;
    color: var(--text-secondary);
    margin-right: 8px;
    margin-top: 4px;
    white-space: nowrap;
}
.tag.key { background: var(--accent-blue-light); border-color: var(--accent-blue); color: var(--accent-blue); }
.tag.start { background: var(--accent-green-light); border-color: var(--accent-green); color: var(--accent-green); }
.tag.end { background: #f2f2f2; border-color: var(--border); color: var(--text-muted); }
.tag i { margin-right: 4px; font-size: 10px; }

.config-meta {
    display: flex;
    flex-wrap: wrap;
    margin-top: 6px;
    gap: 6px;
}

.status-badge {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 4px 10px; border-radius: 20px; font-size: 12px; font-weight: 500;
}
.status-badge.running { background: var(--accent-green-light); color: var(--accent-green); }
.status-badge.stopped { background: var(--bg-primary); color: var(--text-secondary); border: 1px solid var(--border); }
.status-badge.error { background: var(--accent-red-light); color: var(--accent-red); }

.status-dot { width: 6px; height: 6px; border-radius: 50%; display: inline-block; }
.status-badge.running .status-dot { background: var(--accent-green); animation: pulse 2s infinite; }
.status-badge.stopped .status-dot { background: var(--text-muted); }
.status-badge.error .status-dot { background: var(--accent-red); }
@keyframes pulse { 0%,100% { opacity:1; transform: scale(1); } 50% { opacity:0.4; transform: scale(1.1); } }

.task-card-body { padding: 0 24px 24px; display: none; border-top: 1px solid var(--border-light); background: #fafbfc; }
.task-card.expanded .task-card-body { display: block; }
.expand-icon { transition: transform 0.2s; color: var(--text-muted); font-size: 12px; }
.task-card.expanded .expand-icon { transform: rotate(180deg); }

.progress-section { margin-top: 20px; margin-bottom: 20px; }
.progress-bar-container {
    background: #eaeef2; border-radius: 6px; height: 16px;
    overflow: hidden; display: flex; margin-bottom: 8px;
}
.progress-completed {
    background: #2da44e; height: 100%; transition: width 0.4s ease; 
    display: flex; align-items: center; justify-content: center; font-size: 10px; font-weight: 600; color: white;
}
.progress-failed {
    background: #cf222e; height: 100%; transition: width 0.4s ease;
    display: flex; align-items: center; justify-content: center; font-size: 10px; font-weight: 600; color: white;
}
.progress-pending { background: transparent; height: 100%; flex: 1; }
.progress-labels {
    display: flex; justify-content: space-between; font-size: 12px; color: var(--text-secondary);
}
.progress-labels .success-rate { font-weight: 600; color: var(--accent-green); }

.stats-grid {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
    gap: 12px; margin-bottom: 20px;
}
.stat-item { background: var(--bg-secondary); border: 1px solid var(--border-light); border-radius: var(--radius-md); padding: 10px; text-align: center; }
.stat-item .stat-label { font-size: 11px; color: var(--text-muted); font-weight: 500; margin-bottom: 2px; }
.stat-item .stat-value { font-size: 16px; font-weight: 600; font-family: monospace; }

.error-section { margin-bottom: 20px; }
.error-section h4 { font-size: 12px; color: var(--text-secondary); margin-bottom: 8px; font-weight: 600; }
.error-item {
    display: flex; align-items: center; justify-content: space-between;
    padding: 6px 12px; background: var(--accent-red-light); border-radius: var(--radius-md);
    margin-bottom: 4px; font-size: 12px; border: 1px solid rgba(207,34,46,0.05);
}
.error-item .error-name { color: #a40e1b; font-weight: 500; }
.error-item .error-count {
    background: rgba(207,34,46,0.1); color: var(--accent-red);
    padding: 1px 6px; border-radius: 10px; font-weight: 600; font-size: 11px;
}

.log-section h4 { font-size: 12px; color: var(--text-secondary); margin-bottom: 8px; font-weight: 600; }
.log-viewer {
    background: #24292f; border: 1px solid #1b1f23; border-radius: var(--radius-md);
    padding: 14px; max-height: 260px; overflow-y: auto;
    font-family: ui-monospace, SFMono-Regular, SF Mono, Menlo, Consolas, Liberation Mono, monospace;
    font-size: 12px; line-height: 1.5; box-shadow: inset 0 1px 3px rgba(0,0,0,0.15);
}
.log-viewer::-webkit-scrollbar { width: 6px; }
.log-viewer::-webkit-scrollbar-track { background: transparent; }
.log-viewer::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.2); border-radius: 3px; }
.log-line { color: #c9d1d9; white-space: pre-wrap; word-break: break-all; margin-bottom: 2px; }
.log-line.error { color: #ff7b72; font-weight: 500; }
.log-line.warning { color: #d2a8ff; }
.log-line.success { color: #7ee787; }

.empty-state { text-align: center; padding: 60px 20px; color: var(--text-muted); }
.empty-state .icon { font-size: 40px; margin-bottom: 12px; }
.empty-state h3 { font-size: 16px; margin-bottom: 4px; color: var(--text-secondary); }

.footer {
    text-align: center; padding: 28px; color: var(--text-muted);
    font-size: 12px; border-top: 1px solid var(--border); margin-top: 32px; background: var(--bg-secondary);
}

@media (max-width: 768px) {
    .header { padding: 12px 16px; flex-wrap: wrap; gap: 12px; }
    .container { padding: 16px; }
    .summary { grid-template-columns: repeat(2, 1fr); }
    .stats-grid { grid-template-columns: repeat(2, 1fr); }
    .task-card-header { padding: 14px 16px; }
    .task-card-body { padding: 0 16px 16px; }
}
</style>
</head>
<body>

<div class="header">
    <div class="header-left">
        <div class="logo">🐝</div>
        <h1>OpenClaw Hive Monitor</h1>
    </div>
    <div class="header-right">
        <div class="auto-refresh">
            <span>自动刷新:</span>
            <select id="refreshInterval" onchange="setAutoRefresh(this.value)">
                <option value="0">关闭</option>
                <option value="120" selected>2分钟</option>
                <option value="300">5分钟</option>
                <option value="600">10分钟</option>
            </select>
        </div>
        <button class="refresh-btn" onclick="refreshData()">
            <span class="refresh-icon">↻</span> 刷新数据
        </button>
        <span id="lastUpdate"></span>
    </div>
</div>

<div class="container">
    <div class="summary" id="summary"></div>
    <div id="tasks"></div>
</div>

<div class="footer">
    OpenClaw Hive Task Monitor · 数据自动读取自本地 config_tasks 与 outputs 目录
</div>

<script>
var autoRefreshTimer = null;
var expandedCards = {};

function setAutoRefresh(seconds) {
    if (autoRefreshTimer) clearInterval(autoRefreshTimer);
    if (seconds > 0) {
        autoRefreshTimer = setInterval(refreshData, seconds * 1000);
    }
}

function formatTime() {
    return new Date().toLocaleTimeString('zh-CN', { hour12: false });
}

function refreshData() {
    var btn = document.querySelector('.refresh-btn');
    btn.classList.add('spinning');
    fetch('/api/status')
        .then(function(r) { return r.json(); })
        .then(function(data) {
            renderSummary(data.summary);
            renderTasks(data.tasks);
            document.getElementById('lastUpdate').textContent = '上次同步: ' + formatTime();
        })
        .catch(function(err) {
            console.error('Failed to fetch status:', err);
            document.getElementById('lastUpdate').textContent = '错误: ' + err.message;
        })
        .finally(function() { btn.classList.remove('spinning'); });
}

function renderSummary(s) {
    document.getElementById('summary').innerHTML =
        '<div class="summary-card total"><div class="label">📋 配置模板数</div><div class="value">' + s.config_count + '</div></div>' +
        '<div class="summary-card total"><div class="label">📊 总任务量</div><div class="value">' + s.total + '</div></div>' +
        '<div class="summary-card completed"><div class="label">✅ 已成功</div><div class="value">' + s.completed + '</div></div>' +
        '<div class="summary-card failed"><div class="label">❌ 已失败</div><div class="value">' + s.failed + '</div></div>' +
        '<div class="summary-card running"><div class="label">⚙️ 运行中进程</div><div class="value">' + s.running_count + '</div></div>';
}

function classifyLogLine(line) {
    var l = line.toLowerCase();
    if (l.indexOf('error') >= 0 || l.indexOf('failed') >= 0 || l.indexOf('traceback') >= 0) return 'error';
    if (l.indexOf('warning') >= 0 || l.indexOf('warn') >= 0) return 'warning';
    if (l.indexOf('success') >= 0 || l.indexOf('completed') >= 0 || l.indexOf('done') >= 0) return 'success';
    return '';
}

function esc(s) {
    if (!s) return '';
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function renderTasks(tasks) {
    var el = document.getElementById('tasks');
    if (!tasks || tasks.length === 0) {
        el.innerHTML = '<div class="empty-state"><div class="icon">📂</div><h3>未检测到有效的配置任务</h3><p>请将 YAML 配置文件放置在 <code>config_tasks/</code> 目录下</p></div>';
        return;
    }
    var html = '';
    for (var i = 0; i < tasks.length; i++) {
        var t = tasks[i];
        if (t.error) {
            html += '<div class="task-card"><div class="task-card-header"><div class="task-name"><span class="config-icon">⚠️</span><div><h3>' + esc(t.config_file) + '</h3><div class="config-file">' + esc(t.error) + '</div></div></div><span class="status-badge error"><span class="status-dot"></span> 配置错误</span></div></div>';
            continue;
        }
        html += renderTask(t);
    }
    el.innerHTML = html;
}

function renderTask(t) {
    var isExp = !!expandedCards[t.config_file];
    var total = t.total || 0, completed = t.completed || 0, failed = t.failed || 0, pending = t.pending || 0;
    var completedPct = total > 0 ? (completed / total * 100) : 0;
    var failedPct = total > 0 ? (failed / total * 100) : 0;
    var statusClass = t.status === 'RUNNING' ? 'running' : 'stopped';
    var statusLabel = t.status === 'RUNNING' ? '运行中 (PID: ' + t.pid + ')' : '已停止';

    var errorHtml = '';
    var ek = t.error_categories ? Object.keys(t.error_categories) : [];
    if (ek.length > 0) {
        errorHtml = '<div class="error-section"><h4>异常统计分类</h4>';
        for (var j = 0; j < ek.length; j++) {
            errorHtml += '<div class="error-item"><span class="error-name">' + esc(ek[j]) + '</span><span class="error-count">' + t.error_categories[ek[j]] + '</span></div>';
        }
        errorHtml += '</div>';
    }

    var logHtml = '';
    if (t.recent_log && t.recent_log.length > 0) {
        logHtml = '<div class="log-section"><h4>实时日志观察 (最后 ' + t.recent_log.length + ' 行)</h4><div class="log-viewer">';
        for (var k = 0; k < t.recent_log.length; k++) {
            var cls = classifyLogLine(t.recent_log[k]);
            logHtml += '<div class="log-line ' + cls + '">' + esc(t.recent_log[k]) + '</div>';
        }
        logHtml += '</div></div>';
    }

    var icon = t.status === 'RUNNING' ? '📄' : '📁';
    
    // 构建标签式元信息
    var metaHtml = '<div class="config-meta">';
    // API Key 标签
    metaHtml += '<span class="tag key"><i>🔑</i> ' + esc(t.api_key_short) + '</span>';
    // 启动时间标签
    if (t.pid_mtime) {
        metaHtml += '<span class="tag start"><i>▶️</i> 启动: ' + t.pid_mtime + '</span>';
    }
    // 结束时间标签
    if (t.end_time) {
        metaHtml += '<span class="tag end"><i>⏹️</i> 结束: ' + t.end_time + '</span>';
    }
    metaHtml += '</div>';

    var safeId = t.config_file.replace(/[^a-zA-Z0-9_-]/g, '_');

    return '<div class="task-card ' + (isExp ? 'expanded' : '') + '" id="card_' + safeId + '">' +
        '<div class="task-card-header" onclick="toggleCard(\'' + safeId + '\')">' +
        '<div class="task-name">' +
            '<span class="seq-num">' + t.seq + '</span>' +
            '<span class="config-icon">' + icon + '</span>' +
            '<div><h3>' + esc(t.config_file) + '</h3>' + metaHtml + '</div>' +
        '</div>' +
        '<div style="display:flex;align-items:center;gap:16px;">' +
            '<span class="status-badge ' + statusClass + '"><span class="status-dot"></span> ' + statusLabel + '</span>' +
            '<span class="expand-icon">▼</span>' +
        '</div>' +
        '</div>' +
        '<div class="task-card-body">' +
        '<div class="progress-section"><div class="progress-bar-container">' +
        '<div class="progress-completed" style="width:' + completedPct + '%">' + (completedPct >= 5 ? completed : '') + '</div>' +
        '<div class="progress-failed" style="width:' + failedPct + '%">' + (failedPct >= 5 ? failed : '') + '</div>' +
        '<div class="progress-pending"></div>' +
        '</div><div class="progress-labels"><span>🟢 ' + completed + ' 成功 &nbsp; 🔴 ' + failed + ' 失败 &nbsp; 🟡 ' + pending + ' 等待中</span><span class="success-rate">成功率: ' + t.success_rate + '%</span></div></div>' +
        '<div class="stats-grid">' +
        '<div class="stat-item"><div class="stat-label">总任务数</div><div class="stat-value" style="color:var(--accent-blue)">' + total + '</div></div>' +
        '<div class="stat-item"><div class="stat-label">成功量</div><div class="stat-value" style="color:var(--accent-green)">' + completed + '</div></div>' +
        '<div class="stat-item"><div class="stat-label">失败量</div><div class="stat-value" style="color:var(--accent-red)">' + failed + '</div></div>' +
        '<div class="stat-item"><div class="stat-label">待处理</div><div class="stat-value" style="color:var(--accent-yellow)">' + pending + '</div></div>' +
        '<div class="stat-item"><div class="stat-label">并发控制数</div><div class="stat-value">' + t.concurrent_num + '</div></div>' +
        '</div>' +
        errorHtml + logHtml +
        '</div></div>';
}

function toggleCard(safeId) {
    expandedCards[safeId] = !expandedCards[safeId];
    var card = document.getElementById('card_' + safeId);
    if (card) card.classList.toggle('expanded');
}

refreshData();
setAutoRefresh(120);
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# HTTP Handler
# ---------------------------------------------------------------------------

class DashboardHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler serving the dashboard page and API."""

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/" or path == "/index.html":
            self._send_html(HTML_PAGE)
        elif path == "/api/status":
            self._send_json()
        else:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Not Found")

    def _send_html(self, html):
        data = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_json(self):
        tasks = get_all_status()
        summary = {
            "config_count": len(tasks),
            "total": sum(t.get("total", 0) for t in tasks if "error" not in t),
            "completed": sum(t.get("completed", 0) for t in tasks if "error" not in t),
            "failed": sum(t.get("failed", 0) for t in tasks if "error" not in t),
            "running_count": sum(1 for t in tasks if t.get("status") == "RUNNING"),
        }
        summary["pending"] = max(0, summary["total"] - summary["completed"] - summary["failed"])

        payload = json.dumps({
            "summary": summary,
            "tasks": tasks,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }, ensure_ascii=False)
        data = payload.encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache, no-store")
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format, *args):
        """Suppress default request logging to keep console clean."""
        pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="OpenClaw Hive Task Monitor Dashboard")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=5000, help="Port to bind (default: 5000)")
    args = parser.parse_args()

    configs = list_config_files()
    print("=" * 50)
    print("   🐝 OpenClaw Hive 任务监控台")
    print(f"  🌐 仪表盘地址: http://{args.host}:{args.port}")
    print(f"  📁 配置目录:  {CONFIG_TASKS_DIR}")
    print(f"  📂 输出目录: {OUTPUTS_DIR}")
    print(f"  📄 发现配置数: {len(configs)}")
    # for c in configs:
    #     print(f"    - {c}")
    # print("=" * 50)

    server = HTTPServer((args.host, args.port), DashboardHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n✨ 正在关闭监控面板...")
        server.server_close()


if __name__ == "__main__":
    main()