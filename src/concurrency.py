import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, TypeVar

from src.logger import app_logger as logger
from src.settings import settings

T = TypeVar("T")

_executor = ThreadPoolExecutor(
    max_workers=max(1, settings.BLOCKING_POOL_MAX_WORKERS),
    thread_name_prefix="blocking",
)


async def run_blocking(func: Callable[..., T], /, *args, **kwargs) -> T:
    """выполняет синхронную функцию в пуле потоков"""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, functools.partial(func, *args, **kwargs))


def shutdown_blocking_pool(wait: bool = False) -> None:
    """Останавливает пул потоков при остановки приложухи"""
    _executor.shutdown(wait=wait, cancel_futures=not wait)
    logger.info("Пул блокирующих задач остановлен")
