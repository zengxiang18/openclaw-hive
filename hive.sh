#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 默认配置文件
CONFIG_FILE="${SCRIPT_DIR}/config.yaml"

# 解析参数
ACTION=""
FAILED_MODE=false   # 仅执行失败标志

while [[ $# -gt 0 ]]; do
    case "$1" in
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --failed)
            FAILED_MODE=true
            shift
            ;;
        --start)
            ACTION="start"
            shift
            ;;
        --stop)
            ACTION="stop"
            shift
            ;;
        --restart)
            ACTION="restart"
            shift
            ;;
        --status)
            ACTION="status"
            shift
            ;;
        --clear)
            ACTION="clear"
            shift
            ;;
        --stats)
            ACTION="stats"
            shift
            ;;
        *)
            echo "Usage: $0 {--start|--stop|--restart|--status|--clear|--stats} [--config <path>] [--failed]"
            echo "  --failed : only effective when used with --start, passes --failed to hive.py"
            exit 1
            ;;
    esac
done

if [ -z "$ACTION" ]; then
    echo "Usage: $0 {--start|--stop|--restart|--status|--clear|--stats} [--config <path>] [--failed]"
    exit 1
fi

# PID/LOG 放到 output 目录下，按 config 文件名隔离
CONFIG_BASENAME="$(basename "${CONFIG_FILE}" .yaml)"
OUTPUT_DIR="${SCRIPT_DIR}/outputs/${CONFIG_BASENAME}"
mkdir -p "$OUTPUT_DIR"
PID_FILE="${OUTPUT_DIR}/hive.pid"
LOG_FILE="${OUTPUT_DIR}/nohup.log"
CLEAN_LOG_FILE="${OUTPUT_DIR}/nohup_clean.log"

get_pid() {
    if [ -f "$PID_FILE" ]; then
        cat "$PID_FILE"
    else
        echo ""
    fi
}

is_running() {
    pid=$(get_pid)
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        return 0
    fi
    return 1
}

do_start() {
    if is_running; then
        pid=$(get_pid)
        echo "Service is already running (PID: $pid, config: $CONFIG_FILE)"
        return 1
    fi
    echo "Starting service (config: $CONFIG_FILE)..."
    # 构造命令
    local cmd_args=("python" "hive.py" "--config" "$CONFIG_FILE")
    if [ "$FAILED_MODE" = true ]; then
        cmd_args+=("--failed")
    fi
    RLXF_CLEAN_LOG_PATH="$CLEAN_LOG_FILE" nohup "${cmd_args[@]}" > "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "Service started (PID: $(get_pid), log: $LOG_FILE, clean_log: $CLEAN_LOG_FILE)"
}

do_stop() {
    if ! is_running; then
        echo "Service is not running (config: $CONFIG_FILE)"
    else
        pid=$(get_pid)
        echo "Stopping service (PID: $pid)..."
        kill "$pid" 2>/dev/null
        rm -f "$PID_FILE"
        echo "Service stopped"
    fi
    # 清理 pods
    python run_clear.py --config "$CONFIG_FILE"
}

do_restart() {
    do_stop
    sleep 60
    do_start
}

do_status() {
    if is_running; then
        echo "$(get_pid)"
    else
        echo "0"
    fi
}

do_clear() {
    echo "Clearing pods..."
    python run_clear.py --config "$CONFIG_FILE" --del_all
    # 删除环境信息数据库，避免残留
    rm -f "${SCRIPT_DIR}/env_info.db"
    echo "成功删除 env_info.db !!!"
}

do_stats() {
    python3 << 'PYEOF'
import os, sys, re
from collections import Counter

try:
    from omegaconf import OmegaConf
except ImportError:
    print("Error: omegaconf not installed (pip install omegaconf)")
    sys.exit(1)

config_file = os.environ.get("_STATS_CONFIG")
log_file = os.environ.get("_STATS_LOG")
pid_file = os.environ.get("_STATS_PID")
output_dir = os.environ.get("_STATS_OUTPUT")

cfg = OmegaConf.load(config_file)
run_cfg = cfg.run_config

input_path = run_cfg.task.task_input_path
complete_record = run_cfg.task.get("task_complete_record", "complete.jsonl")
failed_record = run_cfg.task.get("task_failed_record", "failed.jsonl")
complete_file = os.path.join(output_dir, complete_record)
failed_file = os.path.join(output_dir, failed_record)

# --- 基础统计 ---
total = 0
if run_cfg.total_num != 0:
    total = run_cfg.total_num
else:
    if os.path.isdir(input_path):
        total = len([f for f in os.listdir(input_path) if os.path.isfile(os.path.join(input_path, f))])
total = max(0, total - run_cfg.start_index)

complete_set = set()
if os.path.exists(complete_file):
    with open(complete_file, "r") as f:
        complete_set = set(line.strip() for line in f if line.strip())

failed_set = set()
if os.path.exists(failed_file):
    with open(failed_file, "r") as f:
        failed_set = set(line.strip() for line in f if line.strip())

done = len(complete_set)
fail = len(failed_set)
finished = done + fail
pending = total - finished
rate = (done / finished * 100) if finished > 0 else 0

# --- 进程状态 ---
proc_status = "NOT RUNNING"
if pid_file and os.path.exists(pid_file):
    with open(pid_file) as f:
        pid = f.read().strip()
    if pid:
        try:
            os.kill(int(pid), 0)
            proc_status = f"RUNNING (PID: {pid})"
        except (OSError, ValueError):
            proc_status = "NOT RUNNING (stale PID)"

# --- 错误分类（从日志分析）---
error_categories = Counter()
error_pattern = re.compile(r"Task \d+ failed:.*?RuntimeError: (.+?)(?:\\n|$)")
traceback_pattern = re.compile(r"Traceback \(most recent call last\)")

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
    ("upload .* failed", "File upload to sandbox failed"),
    ("extract code failed", "Code extraction failed"),
    ('LLM error api_error')
]

if log_file and os.path.exists(log_file):
    with open(log_file, "r", errors="replace") as f:
        log_content = f.read()

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

# --- 输出 ---
print("=" * 50)
print(f"  Config:          {config_file}")
print(f"  Output:          {output_dir}")
print(f"  APIKey:          {run_cfg.sandbox.openclaw_api_key}")
print(f"  Process:         {proc_status}")
print(f"  Concurrent:      {run_cfg.concurrent_num}")
print(f"  Total tasks:     {total}")
print(f"  Completed:       {done}")
print(f"  Failed:          {fail}")
print(f"  Pending/Running: {pending}")
print(f"  Success rate:    {rate:.1f}% ({done}/{finished} finished)")
print("=" * 50)

if error_categories:
    print("\nError Breakdown (from log):")
    for cat, cnt in error_categories.most_common():
        print(f"  {cat:.<36s} {cnt}")

if not error_categories and fail == 0 and done > 0:
    print("\nAll finished tasks completed successfully.")
PYEOF
}

case "$ACTION" in
    start)
        do_start
        ;;
    stop)
        do_stop
        ;;
    restart)
        do_restart
        ;;
    status)
        do_status
        ;;
    clear)
        do_clear
        ;;
    stats)
        _STATS_CONFIG="$CONFIG_FILE" _STATS_LOG="$LOG_FILE" _STATS_PID="$PID_FILE" _STATS_OUTPUT="$OUTPUT_DIR" do_stats
        ;;
esac

exit 0
