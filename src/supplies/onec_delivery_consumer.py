import asyncio
from datetime import datetime
from typing import Any, Dict, List

from faststream import AckPolicy
from faststream.rabbit import Channel
from faststream.rabbit.annotations import RabbitMessage

from src.broker import broker_manager, ExchangeName, QueueName, RoutingKey
from src.logger import app_logger as logger
from src.settings import settings
from src.supplies.integration_1c import OneCIntegration

ATTEMPTS_HEADER = "x-delivery-attempts"
LOGGED_SUPPLIES_LIMIT = 10


def _supply_ids(payload: Dict[str, Any]) -> List[str]:
    """Достаёт номера поставок из сообщения"""
    return [
        supply.get("supply_id")
        for account in payload.get("accounts", [])
        for supply in account.get("data", [])
    ]


def _attempt_number(message: RabbitMessage) -> int:
    """попытка доставки"""
    try:
        return int((message.headers or {}).get(ATTEMPTS_HEADER, 1))
    except (TypeError, ValueError):
        return 1


async def _retry_later(payload: Dict[str, Any], attempt: int) -> None:
    """Перекладывает отгрузку в конец очереди"""
    await broker_manager.publish(
        message=payload,
        routing_key=RoutingKey.MOCKED_ONEC_ORDERS.value,
        exchange=ExchangeName.ORDERS.value,
        headers={ATTEMPTS_HEADER: str(attempt + 1)},
    )


async def _move_to_dead_letter(payload: Dict[str, Any], supplies: List[str],
                               attempt: int, error: str) -> None:
    """Убирает отгрузку в dlq"""
    await broker_manager.declare(QueueName.ONEC_DEAD_LETTER)
    await broker_manager.publish_to_queue(
        message=payload,
        queue=QueueName.ONEC_DEAD_LETTER,
        headers={
            ATTEMPTS_HEADER: str(attempt),
            "x-failure-reason": error[:200],
            "x-failed-at": datetime.now().isoformat(timespec="seconds"),
            "x-supply-ids": ",".join(filter(None, supplies))[:200],
        },
    )
    logger.error(
        f"Отгрузка убрана в {QueueName.ONEC_DEAD_LETTER.value} после {attempt} попыток. "
        f"Поставок {len(supplies)}: {supplies[:LOGGED_SUPPLIES_LIMIT]}. Причина: {error}"
    )


@broker_manager.subscriber(
    QueueName.MOCKED_ONEC_ORDERS,
    ExchangeName.ORDERS,
    channel=Channel(prefetch_count=1),
    ack_policy=AckPolicy.NACK_ON_ERROR,
)
async def deliver_shipment_to_onec(payload: Dict[str, Any], message: RabbitMessage) -> None:
    """Доставляет одну отгрузку в 1C, при неудаче - dlq"""
    if not isinstance(payload, dict) or not payload.get("accounts"):
        logger.error(f"Пропущено сообщение неизвестного формата: {str(payload)[:200]}")
        return

    supplies = _supply_ids(payload)
    result = await OneCIntegration().send_to_1c(payload)

    if result["success"]:
        logger.info(
            f"В 1C доставлено поставок {len(supplies)}: {supplies[:LOGGED_SUPPLIES_LIMIT]}"
        )
        return

    attempt = _attempt_number(message)
    error = result["error"]

    if attempt >= settings.ONEC_MAX_DELIVERY_ATTEMPTS:
        await _move_to_dead_letter(payload, supplies, attempt, error)
        return

    logger.warning(
        f"1C не принял поставок {len(supplies)} ({supplies[:LOGGED_SUPPLIES_LIMIT]}), "
        f"попытка {attempt} из {settings.ONEC_MAX_DELIVERY_ATTEMPTS}: {error}"
    )
    await asyncio.sleep(settings.ONEC_RETRY_BACKOFF_BASE_SEC)
    await _retry_later(payload, attempt)
