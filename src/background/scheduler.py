import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Awaitable, Callable
from uuid import uuid4
from zoneinfo import ZoneInfo

from src.diagnostics import safe_error
from src.logger import app_logger as logger


RENEW = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return 0
"""
RELEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""
SAVE_STATE = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[2], ARGV[2])
redis.call('HSET', KEYS[3], 'status', ARGV[3], 'updated_at', ARGV[4])
return 1
"""


class LeaseLostError(RuntimeError):
    """Задача потеряла право на выполнение."""


@dataclass(frozen=True)
class Job:
    name: str
    run: Callable[[], Awaitable[object]]
    timeout: float = 600
    interval: float | None = None
    daily_at: tuple[int, int] | None = None
    timezone: str = "UTC"

    def __post_init__(self):
        if (self.interval is None) == (self.daily_at is None):
            raise ValueError("Нужно задать интервал или время запуска")
        if self.timeout <= 0 or (self.interval is not None and self.interval <= 0):
            raise ValueError("Интервалы задачи должны быть положительными")
        if self.daily_at is not None:
            hour, minute = self.daily_at
            datetime(2000, 1, 1, hour, minute, tzinfo=ZoneInfo(self.timezone))

    def next_run(self, now: float) -> float:
        """Возвращает следующий запуск по прежнему расписанию."""
        if self.interval is not None:
            return now + self.interval
        hour, minute = self.daily_at
        current = datetime.fromtimestamp(now, ZoneInfo(self.timezone))
        target = current.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= current:
            target += timedelta(days=1)
        return target.timestamp()


class BackgroundScheduler:
    """Не даёт нескольким процессам Swift одновременно выполнять одну задачу."""

    def __init__(self, redis, jobs, *, poll_interval=5, lease_seconds=30,
                 retry_interval=60, clock=time.time, close_redis=False):
        self.redis = redis
        self.jobs = tuple(jobs)
        self.poll_interval = poll_interval
        self.lease_seconds = lease_seconds
        self.retry_interval = retry_interval
        self.clock = clock
        self.close_redis = close_redis
        self.tasks = []
        if len({job.name for job in self.jobs}) != len(self.jobs):
            raise ValueError("Названия фоновых задач должны отличаться")

    @staticmethod
    def keys(name):
        prefix = f"background:{name}"
        return f"{prefix}:lock", f"{prefix}:next_run", f"{prefix}:state"

    def start(self):
        """Запускает задачи без ожидания их завершения."""
        if not self.tasks:
            self.tasks = [asyncio.create_task(self._loop(job), name=f"background:{job.name}")
                          for job in self.jobs]

    async def stop(self):
        """Останавливает задачи до закрытия БД и Redis."""
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()
        if self.close_redis:
            await self.redis.aclose()
            await self.redis.connection_pool.disconnect()

    async def _loop(self, job):
        while True:
            try:
                await self.run_due(job)
            except Exception as exc:
                logger.error(f"Не удалось запустить фоновую задачу {job.name}: {safe_error(exc)}")
            await asyncio.sleep(self.poll_interval)

    async def _heartbeat(self, key, token):
        while True:
            await asyncio.sleep(self.lease_seconds / 3)
            if not await self.redis.eval(RENEW, 1, key, token, self.lease_seconds):
                raise LeaseLostError("Блокировка фоновой задачи потеряна")

    async def run_due(self, job):
        """Выполняет просроченную задачу под продлеваемой блокировкой."""
        lock_key, next_key, _ = self.keys(job.name)
        await self.redis.set(next_key, job.next_run(self.clock()), nx=True)
        if float(await self.redis.get(next_key)) > self.clock():
            return False
        token = uuid4().hex
        if not await self.redis.set(lock_key, token, nx=True, ex=self.lease_seconds):
            return False
        work = None
        heartbeat = None
        try:
            # Другой процесс мог завершить задачу до получения блокировки.
            if float(await self.redis.get(next_key)) > self.clock():
                return False
            heartbeat = asyncio.create_task(self._heartbeat(lock_key, token))
            work = asyncio.create_task(self._execute(job, token))
            done, _ = await asyncio.wait((work, heartbeat), return_when=asyncio.FIRST_COMPLETED)
            if heartbeat in done:
                await heartbeat
            return await work
        finally:
            for task in (work, heartbeat):
                if task is not None:
                    task.cancel()
            await asyncio.gather(*(task for task in (work, heartbeat) if task is not None),
                                 return_exceptions=True)
            try:
                await self.redis.eval(RELEASE, 1, lock_key, token)
            except Exception:
                logger.warning(f"Не удалось снять блокировку задачи {job.name}; она истечёт сама")

    async def _save(self, job, token, due, status):
        saved = await self.redis.eval(SAVE_STATE, 3, *self.keys(job.name), token,
                                      due, status, self.clock())
        if not saved:
            raise LeaseLostError("Состояние задачи не сохранено: блокировка потеряна")

    async def _execute(self, job, token):
        now = self.clock()
        await self._save(job, token, now, "running")
        logger.info(f"Запуск фоновой задачи {job.name}")
        try:
            async with asyncio.timeout(job.timeout):
                await job.run()
        except Exception as exc:
            logger.error(f"Ошибка фоновой задачи {job.name}; будет повторная попытка: {safe_error(exc)}")
            await self._save(job, token, self.clock() + self.retry_interval, "error")
            return False
        await self._save(job, token, job.next_run(now), "success")
        logger.info(f"Фоновая задача {job.name} завершена")
        return True

    async def snapshot(self):
        """Показывает общее состояние задач без данных заказов и токенов."""
        result = {}
        for job in self.jobs:
            lock_key, next_key, state_key = self.keys(job.name)
            due = await self.redis.get(next_key)
            state = await self.redis.hgetall(state_key)
            result[job.name] = {
                "next_run": float(due) if due is not None else None,
                "active": bool(await self.redis.exists(lock_key)),
                **{(k.decode() if isinstance(k, bytes) else k):
                   (v.decode() if isinstance(v, bytes) else v) for k, v in state.items()},
            }
        status = "ok" if self.tasks else "stopped"
        if any(job.get("status") == "error" for job in result.values()):
            status = "degraded"
        return {"status": status, "jobs": result}
