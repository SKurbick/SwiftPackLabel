import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
from typing import Awaitable, Callable, Iterable, TypeVar

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
) -> list[R]:
    """Обрабатывает ограниченное число элементов одновременно, сохраняя порядок."""
    values = list(items)
    results = {}
    pending = iter(enumerate(values))

    async def worker() -> None:
        for index, value in pending:
            results[index] = await func(value)

    tasks = [asyncio.create_task(worker()) for _ in range(min(max(1, limit), len(values)))]
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
