from typing import Optional, Any, List, Dict, Union
from dataclasses import dataclass, field
from pydantic import BaseModel

class ExtendUploadFile(BaseModel):
    upload_path: str
    remote_path: str

    def to_dict(self):
        return {
            "cmd_name": "upload_file",
            "args": {
                "upload_path": self.upload_path,
                "remote_path": self.remote_path,
            },
        }

class ExtendDownloadFile(BaseModel):
    download_path: str
    remote_path: str

    def to_dict(self):
        return {
            "cmd_name": "download_file",
            "args": {
                "download_path": self.download_path,
                "remote_path": self.remote_path,
            },
        }

class ExtendExecCommand(BaseModel):
    command: Union[str, List[str]]
    timeout: int
    mode: str = "standard"

    def to_dict(self):
        return {
            "cmd_name": "exec_command",
            "args": {
                "command": self.command,
                "timeout": self.timeout,
                "mode": self.mode,
            },
        }


@dataclass
class Resources:
    cpu_request: str = "1"
    cpu_limit: str = "1.5"
    memory_request: str = "4Gi"
    memory_limit: str = "4Gi"
    npu_request: str = "0.0"
    npu_limit: str = "2.0"
    gpu_request: str = "0.0"
    gpu_limit: str = "2.0"


@dataclass
class MakeArgs:
    resources: Resources
    priority: int
    env: List = field(default_factory=list)



@dataclass
class EnvMakeRequest:
    image_name: str
    wait_for_ready: bool
    wait_timeout: int
    args: MakeArgs  # resource信息，用户自定义信息等。
    env_id: str = None
    runtime_type: str = "k8s"  # 运行时类型: k8s | docker | local


class EnvResetRequest(BaseModel):
    env_id: str
    seed: Optional[int] = None


class EnvStepRequest(BaseModel):
    env_id: str
    action: str


class EnvCloseRequest(BaseModel):
    env_id: str


class EnvExtendRequest(BaseModel):
    env_id: str
    cmd_name: str
    args: dict[str, Any]  # 直接操作沙箱的一些命令，类似 upload_file/bash_cmd
