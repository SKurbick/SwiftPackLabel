from enum import Enum
from dataclasses import dataclass
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue, ExchangeType
from typing import Any, Callable, Coroutine

from src.settings import get_settings


class ExchangeName(str, Enum):
    ORDERS = "orders"


class QueueName(str, Enum):
    DELIVERED_ORDERS = "ORDERS.DELIVERED.FBS.FIRST"


class RoutingKey(str, Enum):
    DELIVERED_ORDERS = "ORDERS.DELIVERED.FBS.FIRST"


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


EXCHANGE_CONFIGS: dict[ExchangeName, ExchangeConfig] = {
    ExchangeName.ORDERS: ExchangeConfig(name=ExchangeName.ORDERS.value)
}


QUEUE_CONFIGS: dict[QueueName, QueueConfig] = {
    QueueName.DELIVERED_ORDERS: QueueConfig(name=QueueName.DELIVERED_ORDERS.value, routing_key=RoutingKey.DELIVERED_ORDERS.value)
}


class BrokerManager:
    __instance = None

    def __init__(self, host: str, port: int, user: str, password: str, vhost: str) -> None:
        self.__url: str = f"amqp://{user}:{password}@{host}:{port}/{vhost}"
        self.__broker: RabbitBroker = RabbitBroker(url=self.__url)
        self.exchanges: dict[ExchangeName, RabbitExchange] = {
            exchange: RabbitExchange(name=config.name, type=config.type, durable=config.durable)
            for exchange, config in EXCHANGE_CONFIGS.items()
        }
        self.queues: dict[QueueName, RabbitQueue] = {
            queue: RabbitQueue(name=config.name, routing_key=config.routing_key, durable=config.durable)
            for queue, config in QUEUE_CONFIGS.items()
        }

    @property
    def broker(self) -> RabbitBroker:
        return self.__broker

    def _get_exchange(self, name: ExchangeName) -> RabbitExchange:
        return self.exchanges[name]

    def _get_queue(self, name: QueueName) -> RabbitQueue:
        return self.queues[name]

    def subscriber(self, queue: QueueName, exchange: ExchangeName, *args: Any, **kwargs: Any) -> Callable:
        rabbit_queue = self._get_queue(queue)
        rabbit_exchange = self._get_exchange(exchange)
        return self.__broker.subscriber(queue=rabbit_queue, exchange=rabbit_exchange, *args, **kwargs)

    def publish(self, message: Any, routing_key: str, exchange: ExchangeName, *args: Any, **kwargs: Any) -> Coroutine:
        rabbit_exchange = self._get_exchange(exchange)
        return  self.__broker.publish(message=message, routing_key=routing_key, exchange=rabbit_exchange, *args, **kwargs)

    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = super(BrokerManager, cls).__new__(cls, *args, **kwargs)
            cls.__instance.__init__(
                host=get_settings().RABBITMQ_HOST,
                port=get_settings().RABBITMQ_PORT,
                user=get_settings().RABBITMQ_USER,
                password=get_settings().RABBITMQ_PASSWORD,
                vhost=get_settings().RABBITMQ_VHOST
            )
        return cls.__instance

    @staticmethod
    def get_broker_connection():
        if not BrokerManager.__instance:
            BrokerManager.__new__(BrokerManager)
        return BrokerManager.__instance

broker = BrokerManager.get_broker_connection()
