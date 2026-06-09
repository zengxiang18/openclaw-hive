import asyncio
import json
import os
import re
import uuid
import traceback
import tempfile
from dataclasses import asdict

import httpx
from httpx_sse import aconnect_sse
from omegaconf import DictConfig
from typing import Union, Dict
from execution_client.models.request import EnvMakeRequest, EnvStepRequest
from execution_client.models.response import Result, EnvStepResponse
from execution_client.core.error_code import ErrorCode
from execution_client.client.base_client import BaseClient
from execution_client.core.async_http_client import AsyncHttpClientConfig, AsyncHttpClientBatch
from execution_client.core.utils import zip_local_folder, unzip_local_file, get_base_url
from execution_client.core.logging import ManageLogger
from execution_client.core.rmq_client import RmqClient
from execution_client.core.db import init_db, add_env_info, soft_delete_env_info, get_all_use_env_id
init_db()


class ExecutionClient(BaseClient):
    def __init__(self, env_id: str, config: DictConfig, http_client: AsyncHttpClientBatch, **kwargs):
        super().__init__(env_id, **kwargs)
        self.config = config
        self.base_url = get_base_url(config)
        self.http_client = http_client
        self.logger = ManageLogger(self.__class__.__name__).get_logger()

    async def close(self) -> Result:
        if not hasattr(self, "_env_id"):
            return Result(code=ErrorCode.PARAMS_MISS[0], msg="The self object does not have the _env_id attribute.")
        result = Result()
        try:
            request_id = uuid.uuid4().hex
            self.logger.info(f"[close]request_id: {request_id}, env_id: {self._env_id}")
            response = (await self.http_client.request_with_retry([{
                "method": "POST", 
                "url": f"{self.base_url}/close", 
                "headers": {"Content-Type": "application/json"},
                "json": {"env_id": self._env_id}
            }]))[0]
            response_dict = response.json()
            if response_dict.get("status_code") == ErrorCode.SUCCESS[0]:
                env_id = response_dict["env_id"]
                msg = response_dict["msg"]
                soft_delete_env_info(env_id)
                result = Result(code=ErrorCode.SUCCESS[0], msg="", data={"env_id": env_id, "msg": msg})
            self.logger.info(f"[close]request_id: {request_id}, status_code: {response.status_code}, res: {response_dict}")
        except Exception as e:
            error = traceback.format_exc()
            self.logger.error(f"[close]error: {error}, env_id: {self._env_id}")
            result = Result(code=ErrorCode.UNKNOWN_ERROR[0], msg=str(e))
        finally:
            await self.http_client.close()
        return result

    async def async_stream_client(self, url: str, post_data, timeout):
        """异步SSE客户端"""
        headers = {
            "Accept": "text/event-stream",
            "Content-Type": "application/json",
            "User-Agent": "SSE-Async-Client/1.0"
        }

        self.logger.info(f"sse connect to: {url}")

        def dump_data(data, count):
            env_id = data.get("env_id")
            response = data.get("response")
            if response:
                command = response.get("command")
                exit_code = response.get("exit_code", None)
                stdout = response.get("stdout")
                stderr = response.get("stderr")
                tailing_stdout = stdout[-100:] if stdout else None
                tailing_stderr = stderr[-100:] if stderr else None
                return f"[#{count}][{env_id}:[{command}]: exit_code: [{exit_code}], stdout: [{tailing_stdout}], stderr: [{tailing_stderr}]"
            else:
                return f"[#{count}][{env_id}]: no response"

        async def sse_stream():
            async with httpx.AsyncClient(timeout=httpx.Timeout(None), trust_env=False) as client:  # 使用 None 禁用 httpx 的超时
                try:
                    async with aconnect_sse(
                            client,
                            "POST",  # 指定使用 POST 方法
                            url,
                            headers=headers,
                            json=post_data  # 传递 JSON 数据
                    ) as event_source:
                        event_count = 0
                        async for sse_event in event_source.aiter_sse():
                            event_count += 1
                            # self.logger.info(f"\n事件 #{event_count}:")
                            # self.logger.info(f"  类型: {sse_event.event}")

                            # 解析数据
                            if sse_event.data:
                                try:
                                    data = json.loads(sse_event.data)
                                    self.logger.info(f"sse data: {dump_data(data, event_count)}")
                                    result = Result(code=200, msg="", data=data)
                                except json.JSONDecodeError:
                                    self.logger.error(f"  raw response: {sse_event.data}")
                            if sse_event.event == "done":
                                return result

                except asyncio.CancelledError:
                    return Result(code=ErrorCode.UNKNOWN_ERROR[0], msg="task cancelled")
                except httpx.TimeoutException:
                    return Result(code=ErrorCode.UNKNOWN_ERROR[0], msg="timeout")
                except Exception as e:
                    return Result(code=ErrorCode.UNKNOWN_ERROR[0], msg=f"{e}")

        try:
            return await asyncio.wait_for(sse_stream(), timeout=timeout)
        except asyncio.TimeoutError:
            return Result(code=ErrorCode.UNKNOWN_ERROR[0], msg=f"timeout after {timeout}s")


    async def extend(self, args: Dict) -> Result:
        try:
            request_id = uuid.uuid4().hex
            self.logger.info(f"[extend]request_id: {request_id}, env_id: {self._env_id}, args: {args}")
            cmd_name = args.get("cmd_name")
            if cmd_name == "upload_file":
                upload_path = args["args"]["upload_path"]
                remote_path = args["args"]["remote_path"]
                if os.path.exists(upload_path):
                    if os.path.isfile(upload_path):  # 上传文件
                        with open(upload_path, "rb") as f:
                            request = {
                                "method": "POST", 
                                "url": f"{self.base_url}/upload_file",
                                "files": {"file": (f.name, f)},
                                "data": {"env_id": self._env_id, "remote_path": remote_path},
                            }
                            response = (await self.http_client.request_with_retry([request]))[0]
                    elif os.path.isdir(upload_path):  # 文件夹压缩后上传
                        # with tempfile.NamedTemporaryFile(delete=True, suffix='.zip') as temp_zip:
                            temp_zip = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
                            temp_zip.close()
                            zip_local_folder(upload_path, temp_zip.name)
                            with open(temp_zip.name, "rb") as f:
                                request = {
                                    "method": "POST", 
                                    "url": f"{self.base_url}/upload_file",
                                    "files": {"file": (f.name, f)},
                                    # "params": {"env_id": self._env_id, "remote_path": remote_path},
                                    "data": {"env_id": self._env_id, "remote_path": remote_path},
                                }
                                response = (await self.http_client.request_with_retry([request]))[0]
                            os.unlink(temp_zip.name)  # 删除临时文件
                    else:
                        return Result(code=ErrorCode.UNKNOWN_ERROR[0], msg=f"The upload_path[{upload_path}] is an unknown file type.")
                else:
                    return Result(code=ErrorCode.PARAMS_ERROR[0], msg=f"The upload_path[{upload_path}] does not exist.")
            elif cmd_name == "download_file":
                download_path = args["args"]["download_path"]
                remote_path = args["args"]["remote_path"]
                response = (await self.http_client.request_with_retry([{
                    "method": "POST", 
                    "url": f"{self.base_url}/download_file",
                    "params": {"env_id": self._env_id, "remote_path": remote_path},
                }]))[0]
                save_dir = os.path.dirname(download_path)
                if save_dir:
                    os.makedirs(save_dir, exist_ok=True)
                with open(download_path, "wb") as f:
                    for chunk in response.iter_bytes(chunk_size=1024):
                        f.write(chunk)
                return Result(code=ErrorCode.SUCCESS[0], msg="", data={"download_path": download_path})
            elif cmd_name == "exec_command":
                mode = args["args"].get("mode", "standard")
                if mode == "stream":
                    timeout = args["args"].get("timeout", 3600)
                    return await self.async_stream_client(f"{self.base_url}/stream",
                                                       post_data={"env_id": self._env_id} | args, timeout=timeout)
                else:
                    response = (await self.http_client.request_with_retry([{
                        "method": "POST",
                        "url": f"{self.base_url}/extend",
                        "headers": {"Content-Type": "application/json"},
                        "json": {"env_id": self._env_id} | args
                    }]))[0]
            else:
                return Result(code=ErrorCode.PARAMS_ERROR[0], msg=f"Unsupported cmd_name[{cmd_name}].")
            result = response.json()
            status_code = response.status_code
            self.logger.info(f"[extend]request_id: {request_id}, status_code: {status_code}, result: {result}")
            data = result.get("response")
            code=result.get("status_code")
            if isinstance(data, dict) and data.get("success") == False:
                code = ErrorCode.UNKNOWN_ERROR[0]
            return Result(code=code, msg=result.get("msg"), data=data)
        except Exception as e:
            error = traceback.format_exc()
            self.logger.error(f"[extend]error: {error}, env_id: {self._env_id}")
            return Result(code=ErrorCode.UNKNOWN_ERROR[0], msg=str(e))

    async def step(self, adaptor_url, action, timeout=10) -> Result:
        try:
            request_id = uuid.uuid4().hex
            self.logger.info(f"[step]request_id: {request_id}, env_id: {self._env_id}")
            response = (await self.http_client.request_with_retry([{
                "method": "POST", 
                "url": f"{self.base_url}/step", 
                "headers": {"Content-Type": "application/json"},
                "json": {"env_id": self._env_id, "action": action, "adaptor_url": adaptor_url, "timeout": timeout}
            }]))[0]
            response_dict = response.json()
            result = Result(**response_dict)
            self.logger.info(f"[step]request_id: {request_id}, status_code: {response.status_code}, res: {response_dict}")
        except Exception as e:
            error = traceback.format_exc()
            self.logger.error(f"[step]error: {error}, env_id: {self._env_id}")
            result = Result(code=ErrorCode.UNKNOWN_ERROR[0], msg=str(e))
        return result


# 静态方法
async def make(request: EnvMakeRequest, config: DictConfig, rmq_client: RmqClient=None) -> Union[ExecutionClient, Result]:
    logger = ManageLogger(__name__).get_logger()
    result = Result()
    try:
        base_url = get_base_url(config)
        request_id = uuid.uuid4().hex
        logger.info(f"[make]request_id: {request_id}, request: {asdict(request)}")
        client_config = config.remote_server.http_client
        http_config = AsyncHttpClientConfig(max_retry_count=client_config.max_retry_count, 
                                            connect_timeout=client_config.connect_timeout,
                                            read_timeout=client_config.read_timeout,
                                            write_timeout=client_config.write_timeout)
        http_client = AsyncHttpClientBatch(http_config)
        if rmq_client is None:
            response = (await http_client.request_with_retry([{
                    "method": "POST", 
                    "url": f"{base_url}/make", 
                    "headers": {"Content-Type": "application/json"},
                    "json": asdict(request)
                }]))[0]
            logger.info(f"[make]request: {request}, response: {response.text}")
            response_dict = response.json()
        else:  # RabbitMQ make env
            response_dict = await rmq_client.call_rpc(
                routing_key=rmq_client.rmq_config.queue,
                payload=asdict(request),
                priority=rmq_client.rmq_config.priority,
                timeout=config.env_make.wait_timeout,
            )
        if response_dict.get("status_code") == ErrorCode.SUCCESS[0]:
            env_id = response_dict["env_id"]
            add_env_info(env_id)
            return ExecutionClient(env_id=env_id, config=config, http_client=http_client)
        elif "创建失败, Pod" in response_dict["msg"]:
            env_id = re.search(r"Pod '([^']*)'", response_dict["msg"]).group(1).removeprefix(f"{config.remote_server.user_id}-")
            client = ExecutionClient(env_id=env_id, config=config, http_client=http_client)
            await client.close()
        else:
            await http_client.close()
        logger.error(f"[make]request_id: {request_id}, status_code: {response.status_code}, res: {response_dict}")
        result = Result(code=response_dict.get("status_code"), msg=response_dict.get("msg"), data=response_dict.get("data"))
    except Exception as e:
        error = traceback.format_exc()
        logger.error(f"[make]error: {error}")
        result = Result(code=ErrorCode.UNKNOWN_ERROR[0], msg=str(e))
    return result


def delete_env(config: DictConfig, env_ids: list = None):
    client_config = config.remote_server.http_client
    http_config = AsyncHttpClientConfig(max_retry_count=client_config.max_retry_count, 
                                        connect_timeout=client_config.connect_timeout,
                                        read_timeout=client_config.read_timeout,
                                        write_timeout=client_config.write_timeout)
    async def close_client(env_id):
        http_client = AsyncHttpClientBatch(http_config)
        client = ExecutionClient(env_id=env_id, config=config, http_client=http_client)
        try:
            await client.close()
        except Exception:
            soft_delete_env_info(env_id)

    async def close_all_clients():
        tasks = [close_client(env_id) for env_id in env_ids]
        await asyncio.gather(*tasks)
    asyncio.run(close_all_clients())
