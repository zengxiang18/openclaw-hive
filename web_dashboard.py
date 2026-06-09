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
    Configs without a date pattern in filename are excluded from stats display.
    """
    configs = list_config_files()

    # Extract date from filename (e.g. config_0609_demo.yaml -> 0609)
    date_pattern = re.compile(r"(\d{4})")

    dated = []   # (date_str, config_file)
    undated = [] # config_file without date
    for cfg_file in configs:
        m = date_pattern.search(cfg_file)
        if m:
            dated.append((m.group(1), cfg_file))
        else:
            undated.append(cfg_file)

    # Sort dated configs by date descending (newest first)
    dated.sort(key=lambda x: x[0], reverse=True)

    # Build results with sequence numbers
    results = []
    for idx, (_, cfg_file) in enumerate(dated, start=1):
        status = get_task_status(cfg_file)
        status["seq"] = idx
        results.append(status)

    # Undated configs get no seq (excluded from main display)
    for cfg_file in undated:
        status = get_task_status(cfg_file)
        status["seq"] = 0
        results.append(status)

    return results


# ---------------------------------------------------------------------------
# HTML Template
# ---------------------------------------------------------------------------

HTML_PAGE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>OpenClaw Hive - Task Monitor</title>
<style>
:root {
    --bg-primary: #0f172a;
    --bg-secondary: #1e293b;
    --bg-card: #1e293b;
    --bg-card-hover: #334155;
    --text-primary: #f1f5f9;
    --text-secondary: #94a3b8;
    --text-muted: #64748b;
    --accent-blue: #3b82f6;
    --accent-green: #22c55e;
    --accent-red: #ef4444;
    --accent-yellow: #eab308;
    --accent-purple: #a855f7;
    --border: #334155;
    --shadow: 0 4px 6px -1px rgba(0,0,0,0.3);
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif;
    background: var(--bg-primary); color: var(--text-primary); min-height: 100vh;
}
.header {
    background: var(--bg-secondary); border-bottom: 1px solid var(--border);
    padding: 16px 32px; display: flex; align-items: center;
    justify-content: space-between; position: sticky; top: 0; z-index: 100;
}
.header-left { display: flex; align-items: center; gap: 12px; }
.header h1 {
    font-size: 20px; font-weight: 700;
    background: linear-gradient(135deg, var(--accent-blue), var(--accent-purple));
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
}
.header .logo {
    width: 32px; height: 32px;
    background: linear-gradient(135deg, var(--accent-blue), var(--accent-purple));
    border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 18px;
}
.header-right {
    display: flex; align-items: center; gap: 16px; font-size: 13px; color: var(--text-secondary);
}
.refresh-btn {
    background: var(--accent-blue); color: white; border: none;
    padding: 8px 16px; border-radius: 8px; cursor: pointer;
    font-size: 13px; font-weight: 500; transition: all 0.2s;
    display: flex; align-items: center; gap: 6px;
}
.refresh-btn:hover { background: #2563eb; transform: translateY(-1px); }
.refresh-btn:active { transform: translateY(0); }
.refresh-btn.spinning .refresh-icon { animation: spin 1s linear infinite; }
@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
.auto-refresh { display: flex; align-items: center; gap: 8px; }
.auto-refresh select {
    background: var(--bg-primary); color: var(--text-primary);
    border: 1px solid var(--border); padding: 6px 10px;
    border-radius: 6px; font-size: 13px; cursor: pointer;
}
.container { max-width: 1400px; margin: 0 auto; padding: 24px; }
.summary {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px; margin-bottom: 24px;
}
.summary-card {
    background: var(--bg-card); border: 1px solid var(--border);
    border-radius: 12px; padding: 20px; text-align: center; transition: transform 0.2s;
}
.summary-card:hover { transform: translateY(-2px); box-shadow: var(--shadow); }
.summary-card .label {
    font-size: 12px; text-transform: uppercase; letter-spacing: 1px;
    color: var(--text-muted); margin-bottom: 8px;
}
.summary-card .value { font-size: 32px; font-weight: 700; }
.summary-card.total .value { color: var(--accent-blue); }
.summary-card.completed .value { color: var(--accent-green); }
.summary-card.failed .value { color: var(--accent-red); }
.summary-card.running .value { color: var(--accent-yellow); }

.task-card {
    background: var(--bg-card); border: 1px solid var(--border);
    border-radius: 12px; margin-bottom: 16px; overflow: hidden; transition: box-shadow 0.2s;
}
.task-card:hover { box-shadow: var(--shadow); }
.task-card-header {
    padding: 20px 24px; display: flex; align-items: center;
    justify-content: space-between; cursor: pointer; user-select: none;
}
.task-card-header:hover { background: var(--bg-card-hover); }
.task-name { display: flex; align-items: center; gap: 12px; }
.task-name .config-icon { font-size: 20px; }
.task-name h3 { font-size: 16px; font-weight: 600; }
.task-name .config-file { font-size: 12px; color: var(--text-muted); margin-top: 2px; }
.status-badge {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 4px 12px; border-radius: 20px; font-size: 12px;
    font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;
}
.status-badge.running {
    background: rgba(234,179,8,0.15); color: var(--accent-yellow);
    border: 1px solid rgba(234,179,8,0.3);
}
.status-badge.stopped {
    background: rgba(100,116,139,0.15); color: var(--text-muted);
    border: 1px solid rgba(100,116,139,0.3);
}
.status-badge.error {
    background: rgba(239,68,68,0.15); color: var(--accent-red);
    border: 1px solid rgba(239,68,68,0.3);
}
.status-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
.status-badge.running .status-dot {
    background: var(--accent-yellow); animation: pulse 2s infinite;
}
.status-badge.stopped .status-dot { background: var(--text-muted); }
.status-badge.error .status-dot { background: var(--accent-red); }
@keyframes pulse { 0%,100% { opacity:1; } 50% { opacity:0.4; } }

.task-card-body { padding: 0 24px 20px; display: none; }
.task-card.expanded .task-card-body { display: block; }
.expand-icon { transition: transform 0.2s; color: var(--text-muted); font-size: 14px; }
.task-card.expanded .expand-icon { transform: rotate(180deg); }

.progress-section { margin-bottom: 20px; }
.progress-bar-container {
    background: var(--bg-primary); border-radius: 8px; height: 28px;
    overflow: hidden; display: flex; margin-bottom: 8px;
}
.progress-completed {
    background: linear-gradient(90deg, #16a34a, #22c55e); height: 100%;
    transition: width 0.5s ease; display: flex; align-items: center;
    justify-content: center; font-size: 11px; font-weight: 600; color: white; min-width: 0;
}
.progress-failed {
    background: linear-gradient(90deg, #dc2626, #ef4444); height: 100%;
    transition: width 0.5s ease; display: flex; align-items: center;
    justify-content: center; font-size: 11px; font-weight: 600; color: white; min-width: 0;
}
.progress-pending { background: var(--bg-primary); height: 100%; flex: 1; }
.progress-labels {
    display: flex; justify-content: space-between; font-size: 12px; color: var(--text-secondary);
}
.progress-labels .success-rate { font-weight: 600; color: var(--accent-green); }

.stats-grid {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: 12px; margin-bottom: 20px;
}
.stat-item { background: var(--bg-primary); border-radius: 8px; padding: 12px; text-align: center; }
.stat-item .stat-label {
    font-size: 11px; color: var(--text-muted); text-transform: uppercase;
    letter-spacing: 0.5px; margin-bottom: 4px;
}
.stat-item .stat-value { font-size: 18px; font-weight: 700; }

.error-section { margin-bottom: 16px; }
.error-section h4 {
    font-size: 13px; color: var(--text-secondary); margin-bottom: 8px;
    text-transform: uppercase; letter-spacing: 0.5px;
}
.error-item {
    display: flex; align-items: center; justify-content: space-between;
    padding: 8px 12px; background: var(--bg-primary); border-radius: 6px;
    margin-bottom: 4px; font-size: 13px;
}
.error-item .error-name { color: var(--text-secondary); }
.error-item .error-count {
    background: rgba(239,68,68,0.15); color: var(--accent-red);
    padding: 2px 8px; border-radius: 10px; font-weight: 600; font-size: 12px;
}

.log-section h4 {
    font-size: 13px; color: var(--text-secondary); margin-bottom: 8px;
    text-transform: uppercase; letter-spacing: 0.5px;
}
.log-viewer {
    background: #0c0e14; border: 1px solid var(--border); border-radius: 8px;
    padding: 12px; max-height: 300px; overflow-y: auto;
    font-family: 'Cascadia Code','Fira Code','Consolas',monospace;
    font-size: 12px; line-height: 1.6;
}
.log-viewer::-webkit-scrollbar { width: 6px; }
.log-viewer::-webkit-scrollbar-track { background: transparent; }
.log-viewer::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
.log-line { color: var(--text-secondary); white-space: pre-wrap; word-break: break-all; }
.log-line.error { color: var(--accent-red); }
.log-line.warning { color: var(--accent-yellow); }
.log-line.success { color: var(--accent-green); }

.empty-state { text-align: center; padding: 60px 20px; color: var(--text-muted); }
.empty-state .icon { font-size: 48px; margin-bottom: 16px; }
.empty-state h3 { font-size: 18px; margin-bottom: 8px; color: var(--text-secondary); }
.empty-state p { font-size: 14px; }

.footer {
    text-align: center; padding: 24px; color: var(--text-muted);
    font-size: 12px; border-top: 1px solid var(--border); margin-top: 24px;
}

@media (max-width: 768px) {
    .header { padding: 12px 16px; flex-wrap: wrap; gap: 12px; }
    .container { padding: 16px; }
    .summary { grid-template-columns: repeat(2, 1fr); }
    .stats-grid { grid-template-columns: repeat(2, 1fr); }
    .task-card-header { padding: 16px; }
    .task-card-body { padding: 0 16px 16px; }
}
</style>
</head>
<body>

<div class="header">
    <div class="header-left">
        <div class="logo">&#x1f41d;</div>
        <h1>OpenClaw Hive Monitor</h1>
    </div>
    <div class="header-right">
        <div class="auto-refresh">
            <span>Auto-refresh:</span>
            <select id="refreshInterval" onchange="setAutoRefresh(this.value)">
                <option value="0">Off</option>
                <option value="120" selected>2min</option>
                <option value="300">5min</option>
                <option value="600">10min</option>
            </select>
        </div>
        <button class="refresh-btn" onclick="refreshData()">
            <span class="refresh-icon">&#x21bb;</span> Refresh
        </button>
        <span id="lastUpdate"></span>
    </div>
</div>

<div class="container">
    <div class="summary" id="summary"></div>
    <div id="tasks"></div>
</div>

<div class="footer">
    OpenClaw Hive Task Monitor &middot; Data refreshes from local config_tasks &amp; outputs
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
            document.getElementById('lastUpdate').textContent = 'Updated: ' + formatTime();
        })
        .catch(function(err) {
            console.error('Failed to fetch status:', err);
            document.getElementById('lastUpdate').textContent = 'Error: ' + err.message;
        })
        .finally(function() { btn.classList.remove('spinning'); });
}

function renderSummary(s) {
    document.getElementById('summary').innerHTML =
        '<div class="summary-card total"><div class="label">Configs</div><div class="value">' + s.config_count + '</div></div>' +
        '<div class="summary-card completed"><div class="label">Completed</div><div class="value">' + s.completed + '</div></div>' +
        '<div class="summary-card failed"><div class="label">Failed</div><div class="value">' + s.failed + '</div></div>' +
        '<div class="summary-card running"><div class="label">Running Tasks</div><div class="value">' + s.running_count + '</div></div>';
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
        el.innerHTML = '<div class="empty-state"><div class="icon">&#x1f4c2;</div><h3>No Config Tasks Found</h3><p>Place YAML config files in <code>config_tasks/</code> directory</p></div>';
        return;
    }
    var html = '';
    for (var i = 0; i < tasks.length; i++) {
        var t = tasks[i];
        // Skip configs without date (seq=0)
        if (!t.seq) continue;
        if (t.error) {
            html += '<div class="task-card"><div class="task-card-header"><div class="task-name"><span class="config-icon">&#x26a0;&#xfe0f;</span><div><h3>' + esc(t.config_file) + '</h3><div class="config-file">' + esc(t.error) + '</div></div></div><span class="status-badge error"><span class="status-dot"></span> Error</span></div></div>';
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
    var statusLabel = t.status === 'RUNNING' ? 'Running (PID: ' + t.pid + ')' : 'Stopped';

    var errorHtml = '';
    var ek = t.error_categories ? Object.keys(t.error_categories) : [];
    if (ek.length > 0) {
        errorHtml = '<div class="error-section"><h4>Error Breakdown</h4>';
        for (var j = 0; j < ek.length; j++) {
            errorHtml += '<div class="error-item"><span class="error-name">' + esc(ek[j]) + '</span><span class="error-count">' + t.error_categories[ek[j]] + '</span></div>';
        }
        errorHtml += '</div>';
    }

    var logHtml = '';
    if (t.recent_log && t.recent_log.length > 0) {
        logHtml = '<div class="log-section"><h4>Recent Log (last ' + t.recent_log.length + ' lines)</h4><div class="log-viewer">';
        for (var k = 0; k < t.recent_log.length; k++) {
            var cls = classifyLogLine(t.recent_log[k]);
            logHtml += '<div class="log-line ' + cls + '">' + esc(t.recent_log[k]) + '</div>';
        }
        logHtml += '</div></div>';
    }

    var icon = t.status === 'RUNNING' ? '&#x1f7e2;' : '&#x26aa;';
    var meta = esc(t.api_key_short) + ' &middot; concurrent: ' + t.concurrent_num;
    if (t.pid_mtime) meta += ' &middot; started: ' + t.pid_mtime;
    if (t.end_time) meta += ' &middot; ended: ' + t.end_time;

    var safeId = t.config_file.replace(/[^a-zA-Z0-9_-]/g, '_');
    var seqLabel = t.seq ? '#' + t.seq + ' ' : '';

    return '<div class="task-card ' + (isExp ? 'expanded' : '') + '" id="card_' + safeId + '">' +
        '<div class="task-card-header" onclick="toggleCard(\'' + safeId + '\')">' +
        '<div class="task-name"><span class="config-icon">' + icon + '</span><div><h3>' + seqLabel + esc(t.config_file) + '</h3><div class="config-file">' + meta + '</div></div></div>' +
        '<div style="display:flex;align-items:center;gap:12px;"><span class="status-badge ' + statusClass + '"><span class="status-dot"></span> ' + statusLabel + '</span><span class="expand-icon">&#x25bc;</span></div>' +
        '</div>' +
        '<div class="task-card-body">' +
        '<div class="progress-section"><div class="progress-bar-container">' +
        '<div class="progress-completed" style="width:' + completedPct + '%">' + (completedPct >= 8 ? completed : '') + '</div>' +
        '<div class="progress-failed" style="width:' + failedPct + '%">' + (failedPct >= 8 ? failed : '') + '</div>' +
        '<div class="progress-pending"></div>' +
        '</div><div class="progress-labels"><span>&#x2705; ' + completed + ' completed &middot; &#x274c; ' + failed + ' failed &middot; &#x23f3; ' + pending + ' pending</span><span class="success-rate">' + t.success_rate + '% success</span></div></div>' +
        '<div class="stats-grid">' +
        '<div class="stat-item"><div class="stat-label">Total</div><div class="stat-value" style="color:var(--accent-blue)">' + total + '</div></div>' +
        '<div class="stat-item"><div class="stat-label">Completed</div><div class="stat-value" style="color:var(--accent-green)">' + completed + '</div></div>' +
        '<div class="stat-item"><div class="stat-label">Failed</div><div class="stat-value" style="color:var(--accent-red)">' + failed + '</div></div>' +
        '<div class="stat-item"><div class="stat-label">Pending</div><div class="stat-value" style="color:var(--accent-yellow)">' + pending + '</div></div>' +
        '<div class="stat-item"><div class="stat-label">Concurrent</div><div class="stat-value">' + t.concurrent_num + '</div></div>' +
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
    print("  \U0001f41d OpenClaw Hive Task Monitor")
    print(f"  Dashboard: http://{args.host}:{args.port}")
    print(f"  Config dir: {CONFIG_TASKS_DIR}")
    print(f"  Outputs dir: {OUTPUTS_DIR}")
    print(f"  Configs found: {len(configs)}")
    for c in configs:
        print(f"    - {c}")
    print("=" * 50)

    server = HTTPServer((args.host, args.port), DashboardHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down dashboard...")
        server.server_close()


if __name__ == "__main__":
    main()