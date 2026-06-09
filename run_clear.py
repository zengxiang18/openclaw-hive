import argparse
import subprocess
from omegaconf import OmegaConf

from execution_client.client.client import delete_env, get_all_use_env_id

"""
根据配置文件清理pods, user_id 用来过滤相关pods
清理pods, 手动 kill 脚本后，pods不会立马销毁，执行该脚本立即清理pods,释放k8s资源
python run_clear.py --del_all 删除所有pods
python run_clear.py --config config.yaml 删除指定沙箱
"""

def get_env_ids_from_k8s(user_id):
    """
    通过 kubectl 获取属于指定 user_id 的所有 Pod，然后提取env_id 列表
    """
    prefix = f"sandbox-k8s-pod{user_id}"
    try:
        result = subprocess.run(
            ["kubectl", "get", "pods", "--no-headers"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            print(f"kubectl get pods 执行失败：{result.stderr.strip()}")
        env_ids = []
        for line in result.stdout.strip().splitlines():
            pod_name = line.split()[0]
            if pod_name.startswith(prefix):
                env_id = pod_name.removeprefix(prefix)
                env_ids.append(env_id)
        return env_ids
    except subprocess.TimeoutExpired:
        print("错误： kubectl 执行超时")
        return []


def main() -> None:
    parser = argparse.ArgumentParser(description="Openclaw distillation Clear Pods")
    parser.add_argument('--config', help="配置文件路径 (JSON/YAML)")
    parser.add_argument('--del_all', action='store_true', help='删除全部pods')
    args = parser.parse_args()

    if not args.config:
        raise ValueError("not found --config")
    config_dict = OmegaConf.load(args.config)

    if args.del_all:
        env_ids = get_all_use_env_id()
    else:
        user_id = config_dict.remote_server.user_id
        env_ids = get_env_ids_from_k8s(user_id)
        if not env_ids:
            print(f"user_id={user_id} 下没有找到任何运行中的 Pod")
        else:
            print(f"user_id={user_id} 找到 {len(env_ids)}个 Pod")
    delete_env(config_dict, env_ids=env_ids)

if __name__ == "__main__":
    main()


