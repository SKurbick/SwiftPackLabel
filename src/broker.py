from enum import Enum
from dataclasses import dataclass
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue, ExchangeType
from typing import Any, Callable, Coroutine

from src.settings import get_settings


class ExchangeName(str, Enum):
    ORDERS = "orders"


class QueueName(str, Enum):
    DELIVERED_ORDERS = "orders.delivered.fbs.first.non-aggregated"
    MOCKED_ONEC_ORDERS = "orders.onec.mocked"
    ONEC_DEAD_LETTER = "orders.onec.dlq"


class RoutingKey(str, Enum):
    DELIVERED_ORDERS = "orders.delivered.fbs.first.non-aggregated"
    MOCKED_ONEC_ORDERS = "orders.onec.mocked"


@dataclass
class ExchangeConfig:
    name: str
    type: ExchangeType = ExchangeType.DIRECT
    durable: bool = True


@dataclass
class QueueConfig:
    name: str
    routing_key: str
    durable: bool = True
    declare: bool = True


EXCHANGE_CONFIGS: dict[ExchangeName, ExchangeConfig] = {
    ExchangeName.ORDERS: ExchangeConfig(name=ExchangeName.ORDERS.value)
}


QUEUE_CONFIGS: dict[QueueName, QueueConfig] = {
    QueueName.DELIVERED_ORDERS: QueueConfig(name=QueueName.DELIVERED_ORDERS.value, routing_key=RoutingKey.DELIVERED_ORDERS.value),
    QueueName.MOCKED_ONEC_ORDERS: QueueConfig(
        name=QueueName.MOCKED_ONEC_ORDERS.value,
        routing_key=RoutingKey.MOCKED_ONEC_ORDERS.value,
        declare=False,
    ),
    QueueName.ONEC_DEAD_LETTER: QueueConfig(
        name=QueueName.ONEC_DEAD_LETTER.value,
        routing_key=QueueName.ONEC_DEAD_LETTER.value,
    ),
}


class BrokerManager:
    __instance = None

    def __init__(self, host: str, port: int, user: str, password: str, vhost: str):
        if getattr(self, "_initialized", False):
            return
        self._url: str = f"amqp://{user}:{password}@{host}:{port}/{vhost}"
        self._broker = RabbitBroker(url=self._url)
        self.exchanges: dict[ExchangeName, RabbitExchange] = {
            exchange: RabbitExchange(name=config.name, type=config.type, durable=config.durable)
            for exchange, config in EXCHANGE_CONFIGS.items()
        }
        self.queues: dict[QueueName, RabbitQueue] = {
            queue: RabbitQueue(
                name=config.name,
                routing_key=config.routing_key,
                durable=config.durable,
                declare=config.declare,
            )
            for queue, config in QUEUE_CONFIGS.items()
        }
        self._initialized = True

    @classmethod
    def get_manager(cls, host: str, port: int, user: str, password: str, vhost: str):
        if cls.__instance is None:
            cls.__instance = cls(host, port, user, password, vhost)
        return cls(host, port, user, password, vhost)

    def get_broker(self):
        return self._broker

    def _get_exchange(self, name: ExchangeName) -> RabbitExchange:
        return self.exchanges[name]

    def _get_queue(self, name: QueueName) -> RabbitQueue:
        return self.queues[name]

    def subscriber(self, queue: QueueName, exchange: ExchangeName, *args: Any, **kwargs: Any) -> Callable:
        rabbit_queue = self._get_queue(queue)
        rabbit_exchange = self._get_exchange(exchange)
        return self._broker.subscriber(queue=rabbit_queue, exchange=rabbit_exchange, *args, **kwargs)

    def publish(self, message: Any, routing_key: str, exchange: ExchangeName, *args: Any, **kwargs: Any) -> Coroutine:
        rabbit_exchange = self._get_exchange(exchange)
        return self._broker.publish(message=message, routing_key=routing_key, exchange=rabbit_exchange, *args, **kwargs)

    async def declare(self, queue: QueueName) -> None:
        """Объявляет очередь. Идемпотентно, нужно перед публикацией в неё."""
        await self._broker.declare_queue(self._get_queue(queue))

    async def publish_to_queue(self, message: Any, queue: QueueName, **kwargs: Any) -> None:
        """Кладёт сообщение в очередь."""
        await self._broker.publish(message=message, queue=self._get_queue(queue), **kwargs)


broker_manager = BrokerManager.get_manager(
    host=get_settings().RABBITMQ_HOST,
    port=get_settings().RABBITMQ_PORT,
    user=get_settings().RABBITMQ_USER,
    password=get_settings().RABBITMQ_PASSWORD,
    vhost=get_settings().RABBITMQ_VHOST
)
