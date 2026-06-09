import asyncio
import json
import os
import traceback
import uuid
from dataclasses import dataclass
from typing import Dict, List, Any

import httpx
from httpx import Timeout

from .async_thread_manager import AsyncThreadManager
from execution_client.core.logging import ManageLogger


@dataclass
class AsyncHttpClientConfig:
    base_url: str = ""
    request_with_job_id: bool = True
    request_with_node_id: bool = True
    max_retry_count: int = 3
    backoff_factor: float = 5.0
    connect_timeout: float = 10.0
    read_timeout: float = 60.0
    write_timeout: float = 60.0
    pool_timeout: float = 120.0  # How long to wait for a connection pool to become available
    connection_pool_size: int = 10

    def build_httpx_config(self):
        limits = httpx.Limits(
            max_connections=self.connection_pool_size,
            max_keepalive_connections=self.connection_pool_size
        )
        timeout_config = Timeout(
            connect=self.connect_timeout,
            read=self.read_timeout,
            write=self.write_timeout,
            pool=self.pool_timeout,
        )
        return limits, timeout_config


class AsyncHttpClient:
    def __init__(self, config: AsyncHttpClientConfig,
                 base_headers: Dict[str, str] = None,
                 retry_on_status: List[int] = None,
                 http_client=None):
        self.logger = ManageLogger("AsyncHttpClient").get_logger()
        self.config = config
        self.base_headers = base_headers or {}
        self.retry_on_status = retry_on_status or [429, 500, 502, 503, 504]
        if http_client:
            self.http_client = http_client
        else:
            limits, timeout_config = config.build_httpx_config()
            self.http_client = httpx.AsyncClient(
                limits=limits,
                timeout=timeout_config,
                trust_env=False
            )

    def _should_retry(self, exc: Exception) -> bool:
        """默认重试异常判断逻辑"""
        if isinstance(exc, (
            httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.WriteTimeout,
            httpx.PoolTimeout,
            httpx.NetworkError,
            httpx.HTTPStatusError,
            httpx.TransportError,
        )):
            return True
        if "the handler is closed" in str(exc):
            return True
        return False

    def _merge_headers(self, additional: Dict[str, str] = None ) -> Dict[str, str]:
        if additional is None:
            return self.base_headers.copy()
        return {**self.base_headers, **additional}

    def _extract_exception_details(self, exc: Exception) -> dict:
        """提取异常的详细信息"""
        return {
            "exception_module": type(exc).__module__,
            "exception_args": exc.args,
            "stack_trace": traceback.format_exception(
                type(exc), exc, exc.__traceback__
            ) if hasattr(exc, '__traceback__') else [],
        }

    def _create_error_response_from_exception(self, exc: Exception) -> httpx.Response:
        # 尝试获取请求信息
        request = getattr(exc, 'request', None)
        response = getattr(exc, 'response', None)
        status_code = response.status_code if response else 599

        # 创建错误内容
        error_content = json.dumps({
            "error": self._extract_exception_details(exc),
            "exception_type": type(exc).__name__,
            "retry_attempts": self.config.max_retry_count
        }).encode()

        return httpx.Response(
            status_code=status_code,
            content=error_content,
            request=request,
            headers={
                "Content-Type": "application/json",
                "X-Error-Type": "ClientException"
            }
        )

    # stateless api so as for multiple requests at the same time
    async def request_with_retry(self, method: str, url: str, headers: Dict[str, str] = None, **kwargs):
        merged_headers = self._merge_headers(headers)
        merged_headers = {key.lower(): value for key, value in merged_headers.items()}

        retries = 0
        while retries <= self.config.max_retry_count:
            try:
                response = await self.http_client.request(
                    method,
                    url,
                    headers=merged_headers,  # 使用合并后的headers
                    **kwargs
                )

                if response.status_code in self.retry_on_status:
                    raise httpx.HTTPStatusError(
                        f"Retryable status code: {response.status_code}",
                        request=response.request,
                        response=response
                    )

                return response

            except Exception as e:
                if not self._should_retry(e) or retries >= self.config.max_retry_count:
                    self.logger.error(f"Request failed after {retries} retries: {str(e)}")
                    return self._create_error_response_from_exception(e)

                # 计算指数退避时间
                wait_time = min(self.config.backoff_factor * (2 ** retries), 30)
                self.logger.warning(
                    f"Request failed (attempt {retries + 1}/{self.config.max_retry_count}), "
                    f"retrying in {wait_time:.2f}s: {str(e)}"
                )

                await asyncio.sleep(wait_time)
                retries += 1

    async def close(self):
        await self.http_client.aclose()


class AsyncHttpClientBatch:
    def __init__(self, config: AsyncHttpClientConfig,
                 base_headers: Dict[str, str] = None,
                 retry_on_status: List[int] = None):
        self.logger = ManageLogger("AsyncHttpClient").get_logger()
        self.config = config
        self.base_headers = base_headers or {}
        self.retry_on_status = retry_on_status
        self.client: AsyncHttpClient = AsyncHttpClient(self.config, self.base_headers, self.retry_on_status)
        self.aio_manager = AsyncThreadManager()

    # prefer this api to avoid connection leakage
    async def request_with_retry(self, requests: List[Dict[str, Any]]):
        tasks = [self.aio_manager.async_run(self.client.request_with_retry(**req))
                 for req in requests]
        return await asyncio.gather(*tasks)

    def sync_request_with_retry(self, requests: List[Dict[str, Any]]):
        async def _wrapper():
            coros = [self.client.request_with_retry(**req) for req in requests]
            return await asyncio.gather(*coros)

        return self.aio_manager.sync_run(_wrapper())

    async def close(self):
        await self.client.close()
        self.aio_manager.stop()

