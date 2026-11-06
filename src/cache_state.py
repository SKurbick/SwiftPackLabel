import pickle
import time
from typing import Any, Iterable, Mapping

from redis.asyncio import Redis
from redis.exceptions import WatchError

from src.concurrency import run_blocking
from src.orders.constants_to_block import is_fbs2_supply


TAKEN_ORDERS = "cache-state:taken-orders"
DELIVERED_SUPPLIES = "cache-state:delivered-supplies"
UPDATED_AT = "cache-state:updated-at"


def is_snapshot(key: str) -> bool:
    """Проверяет, относится ли ключ к спискам СЗ или поставок."""
    return key.startswith(("cache:orders_all:", "cache:supplies_all:"))


def hide_empty_assembly_supplies(key: str, value: Any) -> Any:
    """Скрывает пустые поставки в ответе сборки, сохраняя их для очистки."""
    if key.startswith("cache:supplies_all:") and "is_delivery:False" in key:
        supplies = [supply for supply in value.supplies if supply.orders or supply.count > 0]
        return value.model_copy(update={"supplies": supplies})
    return value


def exclude_completed(key: str, value: Any, taken: set[str], delivered: set[str]) -> Any:
    """Исключает взятые СЗ и отправленные поставки из нужных списков."""
    if key.startswith("cache:orders_all:"):
        result = {}
        for wild, info in value.items():
            orders = [order for order in info.orders if str(order.id) not in taken]
            if orders:
                result[wild] = info.model_copy(update={"orders": orders, "order_count": len(orders)})
        return result
    if key.startswith("cache:supplies_all:") and "is_delivery:False" in key:
        supplies = [
            s for s in value.supplies
            if f"{s.account}:{s.supply_id}" not in delivered and not is_fbs2_supply(s.name)
        ]
        return value.model_copy(update={"supplies": supplies})
    return value


async def read_snapshot(client: Redis, key: str, data: Any) -> Any:
    """Применяет к снимку последние отметки об обработанных данных."""
    if key.startswith("cache:orders_all:"):
        candidates = list({str(order.id) for info in data.values() for order in info.orders})
        if not candidates:
            return data
        flags = await client.smismember(TAKEN_ORDERS, candidates)
        taken = {order_id for order_id, excluded in zip(candidates, flags) if excluded}
        return exclude_completed(key, data, taken, set())
    if key.startswith("cache:supplies_all:") and "is_delivery:False" in key:
        candidates = [f"{s.account}:{s.supply_id}" for s in data.supplies]
        if not candidates:
            return data
        flags = await client.smismember(DELIVERED_SUPPLIES, candidates)
        delivered = {supply_id for supply_id, excluded in zip(candidates, flags) if excluded}
        return exclude_completed(key, data, set(), delivered)
    return data


async def publish_snapshots(
    client: Redis, values: Mapping[str, Any], *, update_freshness: bool = True,
) -> None:
    """Публикует снимки вместе, учитывая операции во время их подготовки."""
    for _ in range(20):
        async with client.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(TAKEN_ORDERS, DELIVERED_SUPPLIES)
                payloads = {}
                for key, value in values.items():
                    filtered = await read_snapshot(pipe, key, value)
                    payloads[key] = await run_blocking(pickle.dumps, filtered)
                pipe.multi()
                for key, payload in payloads.items():
                    pipe.set(key, payload)  # Снимок храним до замены, время обновления — отдельно.
                if update_freshness:
                    pipe.hset(UPDATED_AT, mapping={key: str(time.time()) for key in payloads})
                await pipe.execute()
                return
            except WatchError:
                continue
    raise RuntimeError("Кэш не опубликован: состояние заказов непрерывно меняется")


async def prune_snapshot(client: Redis, key: str) -> None:
    """Убирает обработанные записи, сохраняя параллельные обновления."""
    for _ in range(20):
        async with client.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(key, TAKEN_ORDERS, DELIVERED_SUPPLIES)
                raw = await pipe.get(key)
                if raw is None:
                    return
                value = await run_blocking(pickle.loads, raw)
                value = await read_snapshot(pipe, key, value)
                payload = await run_blocking(pickle.dumps, value)
                pipe.multi()
                pipe.set(key, payload)
                await pipe.execute()
                return
            except WatchError:
                continue
    raise RuntimeError("Не удалось обновить изменяющийся снимок")


class OperationCache:
    """Хранит отметки о взятых СЗ и отправленных поставках."""

    def __init__(self, client: Redis | None = None) -> None:
        self.client = client

    @staticmethod
    def _supply_content_keys(account: str, supply_id: str) -> tuple[str, str]:
        """Возвращает ключи отметки пустоты и версии состава."""
        return (
            f"cache-state:empty-supply:{account}:{supply_id}",
            f"cache-state:supply-version:{account}:{supply_id}",
        )

    async def empty_supply_state(self, account: str, supply_id: str) -> tuple[bool, bytes | None]:
        """Проверяет, подтверждалась ли пустота поставки недавно."""
        if self.client is None:
            return False, None
        empty_key, version_key = self._supply_content_keys(account, supply_id)
        empty, version = await self.client.mget(empty_key, version_key)
        return empty is not None, version

    async def remember_empty_supply(
        self, account: str, supply_id: str, version: bytes | None, ttl: int,
    ) -> None:
        """Запоминает пустоту, если состав не меняли во время запроса WB."""
        if self.client is None or ttl <= 0:
            return
        empty_key, version_key = self._supply_content_keys(account, supply_id)
        async with self.client.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(version_key)
                if await pipe.get(version_key) != version:
                    return
                pipe.multi()
                pipe.set(empty_key, b"1", ex=ttl)
                await pipe.execute()
            except WatchError:
                return

    async def invalidate_supply_contents(self, account: str, supply_id: str) -> None:
        """Сбрасывает пустоту и отклоняет запоздавшие ответы о старом составе."""
        if self.client is None:
            return
        empty_key, version_key = self._supply_content_keys(account, supply_id)
        async with self.client.pipeline(transaction=True) as pipe:
            pipe.incr(version_key)
            pipe.delete(empty_key)
            await pipe.execute()

    async def record_taken_orders(self, order_ids: Iterable[int]) -> None:
        """Запоминает взятые СЗ и убирает их из кеша."""
        taken = {str(order_id) for order_id in order_ids}
        if not taken or self.client is None:
            return
        await self.client.sadd(TAKEN_ORDERS, *taken)
        async for raw_key in self.client.scan_iter(match="cache:orders_all:*"):
            key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
            await prune_snapshot(self.client, key)

    async def mark_supply_delivered(self, account: str, supply_id: str) -> None:
        """Убирает отправленную поставку из списков «На сборке»."""
        if self.client is None:
            return
        await self.client.sadd(DELIVERED_SUPPLIES, f"{account}:{supply_id}")
        async for raw_key in self.client.scan_iter(match="cache:supplies_all:*is_delivery:False"):
            key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
            await prune_snapshot(self.client, key)

    async def available_order_ids(self, order_ids: list[int]) -> list[int]:
        """Возвращает СЗ, ещё не взятые в круг."""
        if not order_ids or self.client is None:
            return order_ids
        taken = await self.client.smismember(TAKEN_ORDERS, [str(order_id) for order_id in order_ids])
        return [order_id for order_id, was_taken in zip(order_ids, taken) if not was_taken]


operation_cache = OperationCache()
