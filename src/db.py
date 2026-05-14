import asyncpg
import time
from typing import AsyncGenerator
from contextlib import asynccontextmanager
from src.diagnostics import diagnostics, get_caller_name, get_request_id, short_query
from src.settings import settings


class DatabaseManager:
    def __init__(
            self,
            host: str = settings.db_app_host,
            port: int = settings.db_app_port,
            user: str = settings.db_app_user,
            password: str = settings.db_app_password,
            database: str = settings.dp_app_name
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.pool = None
        self.max_size = settings.async_pg_pool_size + 10

    async def create_pool(
            self,
            min_size: int = settings.async_pg_pool_size,
            max_size: int = settings.async_pg_pool_size + 10
    ):
        """Создание пула соединений"""
        self.max_size = max_size
        self.pool = await asyncpg.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            min_size=min_size,
            max_size=max_size
        )
        return self.pool

    def get_pool_stats(self) -> dict:
        """Получение безопасной статистики пула соединений."""
        try:
            if not self.pool:
                return {"size": 0, "idle": 0, "used": 0, "max": self.max_size}
            size = self.pool.get_size()
            idle = self.pool.get_idle_size()
            return {"size": size, "idle": idle, "used": size - idle, "max": self.max_size}
        except Exception:
            return {"size": None, "idle": None, "used": None, "max": self.max_size}

    @asynccontextmanager
    async def connection(self):
        """Получение соединения из пула"""
        if not self.pool:
            await self.create_pool()

        acquire_started = time.monotonic()
        connection = await self.pool.acquire()
        acquire_wait_ms = (time.monotonic() - acquire_started) * 1000
        held_started = time.monotonic()
        if acquire_wait_ms >= settings.DIAGNOSTICS_DB_ACQUIRE_SLOW_MS:
            diagnostics.record_event(
                "DB_ACQUIRE_SLOW",
                level="warning",
                request_id=get_request_id(),
                duration_ms=round(acquire_wait_ms, 3),
                pool=self.get_pool_stats(),
                caller=get_caller_name(),
            )
        try:
            yield connection
        finally:
            held_ms = (time.monotonic() - held_started) * 1000
            try:
                await self.pool.release(connection)
            finally:
                if held_ms >= settings.DIAGNOSTICS_DB_HOLD_LONG_MS:
                    diagnostics.record_event(
                        "DB_CONNECTION_HELD_LONG",
                        level="warning",
                        request_id=get_request_id(),
                        held_ms=round(held_ms, 3),
                        pool=self.get_pool_stats(),
                        caller=get_caller_name(),
                    )

    async def fetch(self, query, *args):
        """Выполнение запроса с возвратом множества записей"""
        async with self.connection() as conn:
            started = time.monotonic()
            try:
                return await conn.fetch(query, *args)
            finally:
                self._record_slow_query(query, started)

    async def fetchrow(self, query, *args):
        """Выполнение запроса с возвратом одной записи"""
        async with self.connection() as conn:
            started = time.monotonic()
            try:
                return await conn.fetchrow(query, *args)
            finally:
                self._record_slow_query(query, started)

    async def fetchval(self, query, *args):
        """Выполнение запроса с возвратом одного значения"""
        async with self.connection() as conn:
            started = time.monotonic()
            try:
                return await conn.fetchval(query, *args)
            finally:
                self._record_slow_query(query, started)

    async def execute(self, query, *args):
        """Выполнение запроса без возврата данных"""
        async with self.connection() as conn:
            started = time.monotonic()
            try:
                return await conn.execute(query, *args)
            finally:
                self._record_slow_query(query, started)

    def _record_slow_query(self, query, started: float) -> None:
        duration_ms = (time.monotonic() - started) * 1000
        if duration_ms >= settings.DIAGNOSTICS_DB_QUERY_SLOW_MS:
            diagnostics.record_event(
                "DB_QUERY_SLOW",
                level="warning",
                request_id=get_request_id(),
                duration_ms=round(duration_ms, 3),
                query=short_query(query),
                pool=self.get_pool_stats(),
                caller=get_caller_name(),
            )


# Основной пул для FastAPI приложения
db = DatabaseManager()

# Отдельные пулы для Celery задач
celery_orders_db = DatabaseManager()
celery_hanging_supplies_db = DatabaseManager()


async def get_db_connection() -> AsyncGenerator:
    """Генератор соединения с базой данных"""
    async with db.connection() as connection:
        yield connection


async def get_celery_orders_db():
    """Получение БД соединения для Celery задач синхронизации заказов"""
    return celery_orders_db


async def get_celery_hanging_supplies_db():
    """Получение БД соединения для Celery задач синхронизации висячих поставок"""
    return celery_hanging_supplies_db


async def check_db_connected() -> None:
    try:
        # Создаем пулы для основного приложения
        if not db.pool:
            await db.create_pool()
        await db.execute("SELECT 1")
        
        # Создаем пулы для Celery задач
        if not celery_orders_db.pool:
            await celery_orders_db.create_pool()
            
        if not celery_hanging_supplies_db.pool:
            await celery_hanging_supplies_db.create_pool()
            
    except Exception as e:
        raise e


async def check_db_disconnected() -> None:
    try:
        # Закрываем основной пул
        if db.pool:
            await db.pool.close()
            db.pool = None
            
        # Закрываем Celery пулы
        if celery_orders_db.pool:
            await celery_orders_db.pool.close()
            celery_orders_db.pool = None
            
        if celery_hanging_supplies_db.pool:
            await celery_hanging_supplies_db.pool.close()
            celery_hanging_supplies_db.pool = None
            
    except Exception as e:
        raise e
