# OpenClaw Hive

OpenClaw 蒸馏任务批量编排系统。在 K8s 集群上并发创建沙箱 Pod，自动部署代码和数据，执行 AI Agent 多轮对话任务，并将执行轨迹上传至 OBS。

## 系统架构

```
hive.sh (服务管理)
  └── hive.py (本地编排层)
        ├── 创建 K8s 沙箱 Pod
        ├── 上传代码 + 数据到 Pod
        ├── 在 Pod 内启动 OpenClaw Gateway
        ├── 在 Pod 内执行 openclaw_automation.py
        └── 回收结果至 OBS

每个 Pod 内部:
  openclaw_automation.py (沙箱执行层)
    ├── 连接 OpenClaw Gateway (WebSocket)
    ├── 注册 Agent、安装技能
    ├── 执行多轮对话 (支持 User Simulator)
    └── 生成执行日志和交付件
```

## 目录结构

```
openclaw-hive/
├── hive.py                    # 核心编排脚本
├── hive.sh                    # 服务管理脚本 (start/stop/stats)
├── config.yaml                # 主配置文件
├── run_clear.py               # Pod 清理工具
├── requirements.txt           # Python 依赖
├── execution_client/          # 沙箱交互客户端库
│   ├── client/client.py       #   ExecutionClient 实现
│   ├── core/                  #   日志、HTTP、工具函数
│   └── models/                #   请求/响应数据模型
├── openclaw-task/             # 沙箱内执行的代码
│   ├── openclaw_automation.py #   Agent 任务自动化主脚本
│   └── user_simulator.py      #   用户模拟器
├── uploads/                   # 上传到沙箱的文件
│   ├── openclaw-task.tar      #   代码打包
│   ├── openclaw.json          #   OpenClaw 配置
│   ├── user_proxy_model.json  #   代理模型配置
│   └── configs/               #   任务数据配置 (每个 JSON 一个任务)
├── outputs/                   # 执行结果
│   ├── complete.jsonl         #   已完成任务记录
│   └── failed.jsonl           #   失败任务记录
└── env_info.db                # 沙箱 Pod 注册表 (SQLite)
```

## 快速开始

### 1. 安装依赖

```bash
conda activate openclaw
pip install -r requirements.txt
```

### 2. 准备配置

编辑 `config.yaml`，核心配置项：

| 配置项 | 说明 | 示例 |
|--------|------|------|
| `env_make.image_name` | 沙箱 Docker 镜像 | `swr.../openclaw:0.0.8` |
| `env_make.runtime_type` | 运行时类型 | `k8s` / `docker` / `local` |
| `remote_server.user_id` | 用户标识，用于 Pod 命名 | `zx0522claude47` |
| `run_config.concurrent_num` | 并发数 | Claude 推荐 `100~150` |
| `run_config.sandbox.openclaw_start_timeout` | Gateway 启动超时 | 镜像 >0.0.6 设 `80`，旧版设 `10` |
| `run_config.task.task_input_path` | 任务配置目录 | `uploads/configs` |
| `run_config.task.main_code_dir` | 源码目录（自动打包） | `openclaw-task` |
| `run_config.obs.traj_save_path` | OBS 轨迹保存路径 | `openclaw_trajs/traj_0522` |

### 3. 准备任务数据

将任务配置 JSON 文件放入 `uploads/configs/` 目录，每个 JSON 文件定义一个独立任务。

### 4. 启动任务

```bash
bash hive.sh --start
```

### 5. 查看运行状态

```bash
bash hive.sh --stats
```

输出示例：
```
==================================================
  Config:          config.yaml
  Process:         RUNNING (PID: 12345)
  Total tasks:     500
  Completed:       342
  Failed:          28
  Pending/Running: 130
  Success rate:    92.4% (342/370 finished)
==================================================

Error Breakdown (from log):
  Gateway timeout:.................. 12
  Script execution failed:.......... 8
  OBS upload failed:................ 5
```

### 6. 停止任务并清理 Pod

```bash
bash hive.sh --stop
```

## 服务管理命令

```bash
bash hive.sh --start                    # 启动任务
bash hive.sh --stop                     # 停止任务 + 清理 Pod
bash hive.sh --restart                  # 重启
bash hive.sh --status                   # 查看进程 PID (0=未运行)
bash hive.sh --stats                    # 查看任务统计和错误分析
bash hive.sh --clear                    # 仅清理 Pod

# 指定配置文件（支持多实例）
bash hive.sh --start --config config-glm.yaml
bash hive.sh --stats --config config-glm.yaml
```

不同的 `--config` 会生成独立的 PID 文件和日志文件（如 `nohup_config-glm.pid`），互不冲突。

## K8s 沙箱操作

### 查看沙箱 Pod

```bash
# 按 user_id 过滤 Pod
kubectl get pods | grep "zx0522claude47"
```

### 进入沙箱调试

```bash
kubectl exec -it <pod-name> -- /bin/bash
```

### 沙箱内关键路径

```
~/workspace/workdir/run.log         # 任务运行日志
~/workspace/workdir/gateway.log     # Gateway 日志
~/workspace/openclaw-task/          # 代码目录
~/workspace/config/                 # 任务数据配置
~/.openclaw/openclaw.json           # OpenClaw 配置
~/.openclaw/agents/                 # Agent 会话历史
~/.openclaw/skills/                 # 已安装技能
~/.openclaw/workspace/              # Agent 交付件
```

### 手动清理残留 Pod

```bash
# 查看未释放的 Pod 数量
python run_clear.py --config config.yaml

# 删除所有残留 Pod
python run_clear.py --config config.yaml --delete
```

## 配置文件详解

### config.yaml 完整结构

```yaml
# ---- 远程服务器 ----
remote_server:
  schema: http
  host: 127.0.0.1
  port: 8080
  project_id: openclaw
  user_id: zx0522claude47           # Pod 命名前缀
  url_path: /v1/env/gem
  http_client:
    max_retry_count: 1
    connect_timeout: 10.0
    read_timeout: 180.0
    write_timeout: 60.0

# ---- OBS 存储 ----
s3:
  s3_uploader_script: /home/ma-user/obsutil/obsutil
  s3_download_script: /home/ma-user/obsutil/obsutil
  bucket_name: "obs://rl-agentdata"

# ---- 沙箱环境 ----
env_make:
  image_name: swr.../openclaw:0.0.8   # Docker 镜像
  wait_for_ready: true
  wait_timeout: 300
  runtime_type: k8s                    # k8s | docker | local
  rabbitmq:
    is_use: false
  args:
    resources:
      cpu_request: "1"
      cpu_limit: "2"
      memory_request: "5Gi"
      memory_limit: "8Gi"
    env:                               # 注入到 Pod 的环境变量
      - name: SERPER_API_KEY
        value: "..."

# ---- 运行配置 ----
run_config:
  start_index: 0                       # 起始任务索引
  total_num: 0                         # 任务总数 (0=不限制)
  concurrent_num: 100                  # 并发 Pod 数量

  sandbox:
    result_log: "run.log"
    openclaw_local_config_file: "uploads/openclaw.json"
    user_proxy_model_local_file: "uploads/user_proxy_model.json"
    openclaw_start_timeout: 80         # 镜像 >0.0.6 设大于60

  task:
    task_input_path: "uploads/configs"
    task_output_path: "outputs"
    task_complete_record: "complete.jsonl"
    task_failed_record: "failed.jsonl"
    main_code_tar: "uploads/openclaw-task.tar"
    main_code_dir: ""                  # 非空时自动打包，如 "openclaw-task"
    main_python_file: "openclaw_automation.py"

  obs:
    download_timeout: 1200
    upload_timeout: 900
    traj_save_path: "openclaw_trajs/traj_0522"       # 轨迹上传路径（勿尾加 /）
    workspace_save_path: "openclaw_trajs/traj_0522_workspace"
    skill_download_path: "skills/260325/skill_localize/skills_library"
    user_config_download_path: "task_data/260520/web_configs"
    user_profile_download_path:
    agents_download_path: "task_data/260413_noenv/noenv_configs/agents"
```

### 关键配置说明

**concurrent_num**：并发 Pod 数量。Claude 模型推荐 100~150，GLM 等较轻模型可适当增大。默认先设 `1` 测试通过后再调大。

**openclaw_start_timeout**：OpenClaw Gateway 在 Pod 内的启动等待时间。镜像版本 > 0.0.6 启动较慢，需设置 > 60 秒；旧版本 0.0.6 设 10 秒即可。

**main_code_dir**：设置后会自动将该目录打包为 tar 上传到沙箱，无需手动执行 `tar -cf`。留空则使用 `main_code_tar` 指定的预打包文件。

**traj_save_path**：OBS 上传路径，末尾不要加 `/`，否则 OBS 路径会出现 `//`。

## 单任务执行流程

每个任务在独立 Pod 中按以下顺序执行：

```
1. 创建 K8s Pod（分配随机端口和 env_id）
2. 上传并解压代码包 (openclaw-task.tar)
3. 从 OBS 下载 skills、user_profile、agents 配置
4. 上传任务数据配置 JSON
5. 上传 user_proxy_model 配置
6. 启动 OpenClaw Gateway（端口冲突自动重试）
7. 执行 python openclaw_automation.py
8. 上传执行轨迹到 OBS（api_use.log、run.log、gateway.log、agents 会话）
9. 上传 workspace 交付件到 OBS
10. 记录结果到 complete.jsonl 或 failed.jsonl
```

## 失败任务重跑

```bash
# 仅重跑 failed.jsonl 中记录的失败任务
bash hive.sh --start
# 等待完成后
python hive.py --config config.yaml --failed
```

程序启动时会自动跳过 `complete.jsonl` 中已完成的任务。

## 多数据集运行

不同数据集使用不同的配置文件，各自独立运行：

```bash
# 数据集 A
cp config.yaml config-dataset-a.yaml
# 编辑 config-dataset-a.yaml：修改 task_input_path、task_output_path、traj_save_path
bash hive.sh --start --config config-dataset-a.yaml

# 数据集 B
cp config.yaml config-dataset-b.yaml
bash hive.sh --start --config config-dataset-b.yaml

# 分别查看状态
bash hive.sh --stats --config config-dataset-a.yaml
bash hive.sh --stats --config config-dataset-b.yaml

# 分别停止
bash hive.sh --stop --config config-dataset-a.yaml
bash hive.sh --stop --config config-dataset-b.yaml
```

每个配置文件对应独立的 PID、日志、输出目录，不会相互冲突。

## 日志说明

| 日志文件 | 位置 | 内容 |
|----------|------|------|
| `nohup_<config>.log` | 本地 | hive.py 主进程日志，包含所有任务的调度和错误信息 |
| `env_info.db` | 本地 | SQLite Pod 注册表，记录已创建/已销毁的 Pod |
| `outputs/complete.jsonl` | 本地 | 已完成任务列表（每行一个配置文件名） |
| `outputs/failed.jsonl` | 本地 | 失败任务列表 |
| `~/workspace/workdir/run.log` | 沙箱 | 单任务执行日志 |
| `~/workspace/workdir/gateway.log` | 沙箱 | OpenClaw Gateway 日志 |

## 故障排查

### 任务全部失败

1. 先将 `concurrent_num` 设为 `1`，排除并发问题
2. 查看 `nohup_config.log` 中的错误信息
3. 用 `kubectl exec` 进入 Pod 检查 `run.log`

### Gateway 启动超时

- 检查 `openclaw_start_timeout` 是否足够大（镜像 >0.0.6 需 >60）
- 检查镜像版本是否正确

### Pod 残留未清理

```bash
# 查看残留数量
python run_clear.py --config config.yaml
# 强制清理
python run_clear.py --config config.yaml --delete
```

### OBS 上传失败

- 检查沙箱内 obsutil 是否可用
- 检查 `s3.bucket_name` 配置
- 上传操作已内置 3 次重试

## 镜像版本说明

| 镜像版本 | 对应 OpenClaw 版本 | 备注 |
|---------|-------------------|------|
| 0.0.6   | 2026.3.2          | 旧版本 |
| 0.0.8   | 2026.4.26         | Gateway 启动较慢，`openclaw_start_timeout` 建议 ≥60 |

---

## 环境要求

- Python 3.10+
- K8s 集群（或 Docker）
- OBS 存储 + obsutil
- OpenClaw 镜像
