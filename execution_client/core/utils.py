import os
import random
import zipfile
from omegaconf import DictConfig


def zip_local_folder(folder_path: str, zip_file_path: str):
    """
    压缩本地文件夹为 ZIP 文件
    :param folder_path: 本地文件夹路径
    :param zip_file_path: 生成的 ZIP 文件路径
    """
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, os.path.dirname(folder_path))
                zipf.write(file_path, arcname)

def unzip_local_file(zip_file_path: str, extract_to: str):
    """
    解压本地 ZIP 文件
    :param zip_file_path: ZIP 文件路径
    :param extract_to: 解压目标目录
    """
    os.makedirs(extract_to, exist_ok=True)
    with zipfile.ZipFile(zip_file_path, 'r') as zipf:
        zipf.extractall(extract_to)


def get_base_url(config: DictConfig):
    remote_server = config.remote_server
    schema = remote_server.schema
    host = remote_server.host
    port = remote_server.port
    project_id = remote_server.project_id
    user_id = remote_server.user_id
    url_path = config.remote_server.url_path
    base_url = f"{schema}://{host}:{port}/{project_id}/{user_id}{url_path}"
    return base_url


def generate_random_port(min_port=10000, max_port=50000):
    """
    生成 10000 到 50000 之间（包含边界值）的随机端口号
    """
    # random.randint(a, b) 会生成 [a, b] 闭区间内的随机整数
    port = random.randint(min_port, max_port)
    return port


def get_s3_downloader_command(s3_config, objects_storage_path, bucket_path, is_single_file=False):
    command = (f"python {s3_config.s3_download_script} --objects_storage_path='{objects_storage_path}' "
    f"--app_token={s3_config.app_token} --region={s3_config.region} "
    f"--bucket_name={s3_config.bucket_name} --bucket_path='{bucket_path}' --show_speed")
    if is_single_file is True:
        command += " --single_file"
    return command


def get_s3_uploader_command(s3_config, local_folder_absolute_path, bucket_path):
    command = (f"python {s3_config.s3_uploader_script} --local_folder_absolute_path='{local_folder_absolute_path}' "
    f"--app_token={s3_config.app_token} --region={s3_config.region} "
    f"--bucket_name={s3_config.bucket_name} --bucket_path='{bucket_path}' --show_speed")
    return command


def get_obsutil_downloader_command(s3_config, objects_storage_path, bucket_path):
    command = f"{s3_config.s3_download_script} cp {s3_config.bucket_name}/{bucket_path} {objects_storage_path} -r -f"
    return command


def get_obsutil_uploader_command(s3_config, local_folder_absolute_path, bucket_path):
    command = f"{s3_config.s3_download_script} cp {local_folder_absolute_path} {s3_config.bucket_name}/{bucket_path} -r -f"
    return command