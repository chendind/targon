import asyncio
from taocd.utils.logger import TaocdLogger

import aio_pika

logger = TaocdLogger(log_file_prefix='rabbitmq').get_logger()

# RabbitMQ 配置信息
RABBITMQ_HOST = "162.244.82.55"
RABBITMQ_PORT = 9021
RABBITMQ_USER = "admin"
RABBITMQ_PASS = "gmRU2aDcfaV6aoF"
RABBITMQ_VHOST = "tao"

# 可以在这里指定默认队列名称，如果项目中需要用多个队列，也可以在发送时动态传参
DEFAULT_QUEUE_NAME = "sn04_log_text_task"


class RabbitMQClient:
    """
    基于 aio-pika 的 RabbitMQ 客户端，支持自动重连和异步发送消息。
    """
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        virtual_host: str,
        queue_name: str = DEFAULT_QUEUE_NAME,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.queue_name = queue_name

        self.connection: aio_pika.RobustConnection | None = None
        self.channel: aio_pika.RobustChannel | None = None
        self.queue: aio_pika.Queue | None = None

    async def connect(self) -> None:
        """
        建立到 RabbitMQ 的异步连接，并初始化 channel 和 queue。
        使用 connect_robust 可以在连接断开时自动重连。
        """
        logger.info("正在连接到 RabbitMQ...")
        # connect_robust 会自动处理断线重连
        self.connection = await aio_pika.connect_robust(
            host=self.host,
            port=self.port,
            login=self.username,
            password=self.password,
            virtualhost=self.virtual_host
        )
        # 获取异步 channel
        self.channel = await self.connection.channel()
        # 设置 QoS，预抓取数可根据业务实际需要进行调整
        await self.channel.set_qos(prefetch_count=1)

        # 声明队列（不存在时自动创建），保证队列持久化
        self.queue = await self.channel.declare_queue(self.queue_name, durable=True)

        logger.info("RabbitMQ 连接成功，已声明队列: %s", self.queue_name)

    async def send_message(self, message: str, queue_name: str = None) -> None:
        """
        异步发送消息到指定（或默认）队列。若连接中断，会自动重连。
        """
        if queue_name is None:
            queue_name = self.queue_name

        # 如果连接失效，重新连接
        if not self.connection or self.connection.is_closed:
            logger.info("检测到连接已关闭，正在重新连接...")
            await self.connect()

        # 发送消息到默认交换机，routing_key 指定队列名
        await self.channel.default_exchange.publish(
            aio_pika.Message(body=message.encode()),
            routing_key=queue_name,
        )
        logger.info("已发送消息到队列 %s", queue_name)

    async def close(self) -> None:
        """
        关闭连接。
        """
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
        logger.info("RabbitMQ 连接已关闭")


# 用于缓存全局单例（或多例）客户端实例
_rabbitmq_client: RabbitMQClient | None = None


async def get_rabbitmq_client() -> RabbitMQClient:
    """
    返回一个全局单例 RabbitMQClient 实例。
    在使用前会自动检查并完成 connect。
    其他模块只需要 `from rabbitmq_client import get_rabbitmq_client` 然后 `await get_rabbitmq_client()` 即可。
    """
    global _rabbitmq_client

    if _rabbitmq_client is None:
        _rabbitmq_client = RabbitMQClient(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            username=RABBITMQ_USER,
            password=RABBITMQ_PASS,
            virtual_host=RABBITMQ_VHOST,
            queue_name=DEFAULT_QUEUE_NAME,
        )
        await _rabbitmq_client.connect()
    else:
        # 如果已经创建，但连接被关闭了，也要重新连接
        if not _rabbitmq_client.connection or _rabbitmq_client.connection.is_closed:
            logger.info("RabbitMQ 连接被关闭，将重新连接")
            await _rabbitmq_client.connect()

    return _rabbitmq_client
