import asyncio
import json
import uuid
import logging
from typing import Callable, Dict, Any
import aio_pika

logger = logging.getLogger(__name__)

class RmqClient:
    def __init__(self, amqp_url: str, rmq_config):
        self.amqp_url = amqp_url
        self.rmq_config = rmq_config
        self.connection: aio_pika.RobustConnection = None
        self.channel: aio_pika.Channel = None
        self.rpc_futures: Dict[str, asyncio.Future] = {}

    async def connect(self):
        """
        作用: 建立到 RabbitMQ 的可靠连接并初始化 Channel。
        输入: 无
        输出: 无
        """
        self.connection = await aio_pika.connect_robust(self.amqp_url)
        self.channel = await self.connection.channel()
        logger.info("RabbitMQ connected.")

    async def start_rpc_listener(self):
        """
        作用: 客户端专用。启动对 Direct-Reply 伪队列的监听，接收所有 RPC 回调。
        输入: 无
        输出: 无
        """
        # 获取伪队列，Direct-Reply 必须开启 no_ack=True
        reply_queue = await self.channel.get_queue("amq.rabbitmq.reply-to")
        await reply_queue.consume(self._on_rpc_reply, no_ack=True)
        logger.info("Listening on amq.rabbitmq.reply-to")

    async def _on_rpc_reply(self, message: aio_pika.IncomingMessage):
        """内部方法：处理收到的 RPC 响应"""
        corr_id = message.correlation_id
        if corr_id in self.rpc_futures:
            future = self.rpc_futures.pop(corr_id)
            if not future.done():
                body = json.loads(message.body.decode())
                future.set_result(body)

    async def call_rpc(self, routing_key: str, payload: dict, priority: int = 0, timeout: int = 30) -> dict:
        """
        作用: 发送 RPC 请求并同步等待响应（协程阻塞）。
        输入:
            - routing_key: 目标队列名称
            - payload: 请求数据字典
            - priority: 优先级 (0-10)
            - timeout: 本地等待超时时间(秒)
        输出: 服务端返回的结果字典
        """
        corr_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.rpc_futures[corr_id] = future

        message = aio_pika.Message(
            body=json.dumps(payload).encode(),
            correlation_id=corr_id,
            reply_to="amq.rabbitmq.reply-to",
            priority=priority,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )

        try:
            await self.channel.default_exchange.publish(
                message, 
                routing_key=routing_key
            )
            # 等待 Future 完成或超时
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            self.rpc_futures.pop(corr_id, None)
            logger.error(f"RPC call timeout for correlation_id: {corr_id}")
            raise TimeoutError("RPC timeout")
        except Exception as e:
            self.rpc_futures.pop(corr_id, None)
            logger.error(f"RPC publish error: {e}")
            raise

    async def start_server_consumer(self, queue_name: str, handler: Callable, prefetch: int = 50):
        """
        作用: 服务端专用。监听指定队列，将消息体交给 handler 处理，并返回结果。
        输入:
            - queue_name: 监听的队列名称
            - handler: 异步处理函数，接收 dict，返回 dict
            - prefetch: 预取数量，控制并发
        输出: 无
        """
        await self.channel.set_qos(prefetch_count=prefetch)
        # passive=True 假设队列已由运维创建，此处仅获取
        queue = await self.channel.declare_queue(queue_name, passive=True)
        
        async def _process_message(message: aio_pika.IncomingMessage):
            async with message.process(): # 上下文管理器自动处理手动 ACK 和 reject
                req_data = json.loads(message.body.decode())
                
                # 调用业务逻辑
                result_data = await handler(req_data)
                
                # 返回响应
                if message.reply_to:
                    reply_msg = aio_pika.Message(
                        body=json.dumps(result_data).encode(),
                        correlation_id=message.correlation_id,
                        content_type="application/json"
                    )
                    await self.channel.default_exchange.publish(
                        reply_msg,
                        routing_key=message.reply_to
                    )

        await queue.consume(_process_message)
        logger.info(f"Server consuming from {queue_name}")

    async def close(self):
        """
        作用: 关闭 RabbitMQ 连接。
        输入: 无
        输出: 无
        """
        if self.connection:
            await self.connection.close()
            logger.info("RabbitMQ connection closed.")


def get_rmq_client(config):
    if config.env_make.rabbitmq.is_use is True:
        rmq_url = config.env_make.rabbitmq.url
        rmq_client = RmqClient(rmq_url, config.env_make.rabbitmq)
        return rmq_client
    return None

