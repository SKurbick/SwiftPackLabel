import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
from typing import Awaitable, Callable, Iterable, TypeVar
from collections import deque
from typing import Hashable

from src.logger import app_logger as logger
from src.settings import settings

T = TypeVar("T")
R = TypeVar("R")

_executor = ThreadPoolExecutor(
    max_workers=max(1, settings.BLOCKING_POOL_MAX_WORKERS),
    thread_name_prefix="blocking",
)


async def run_blocking(func: Callable[..., T], /, *args, **kwargs) -> T:
    """выполняет синхронную функцию в пуле потоков"""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, functools.partial(func, *args, **kwargs))


async def map_limited(
    func: Callable[[T], Awaitable[R]], items: Iterable[T], limit: int,
    group: Callable[[T], Hashable] | None = None, per_group_limit: int = 1,
) -> list[R]:
    """Обрабатывает элементы с общим лимитом и отдельным лимитом на группу."""
    values = list(items)
    results = {}
    pending = iter(enumerate(values))
    queues = {}
    if group is not None:
        for index, value in enumerate(values):
            queues.setdefault(group(value), deque()).append((index, value))
    groups = deque(queues)
    active = {}
    condition = asyncio.Condition()

    async def grouped_worker() -> None:
        while True:
            async with condition:
                while True:
                    if not groups:
                        return
                    selected = None
                    found = False
                    for _ in range(len(groups)):
                        key = groups[0]
                        if active.get(key, 0) < max(1, per_group_limit):
                            selected = key
                            found = True
                            break
                        groups.rotate(-1)
                    if found:
                        index, value = queues[selected].popleft()
                        active[selected] = active.get(selected, 0) + 1
                        if queues[selected]:
                            groups.rotate(-1)
                        else:
                            groups.popleft()
                        break
                    await condition.wait()
            try:
                results[index] = await func(value)
            finally:
                async with condition:
                    active[selected] -= 1
                    condition.notify_all()

    async def worker() -> None:
        for index, value in pending:
            results[index] = await func(value)

    run_worker = grouped_worker if group is not None else worker
    tasks = [asyncio.create_task(run_worker()) for _ in range(min(max(1, limit), len(values)))]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return [results[index] for index in range(len(values))]


def shutdown_blocking_pool(wait: bool = False) -> None:
    """Останавливает пул потоков при остановки приложухи"""
    _executor.shutdown(wait=wait, cancel_futures=not wait)
    logger.info("Пул блокирующих задач остановлен")
