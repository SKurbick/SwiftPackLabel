import asyncpg
from typing import AsyncGenerator
from contextlib import asynccontextmanager
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

    async def create_pool(
            self,
            min_size: int = settings.async_pg_pool_size,
            max_size: int = settings.async_pg_pool_size + 10
    ):
        """Создание пула соединений"""
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

    @asynccontextmanager
    async def connection(self):
        """Получение соединения из пула"""
        if not self.pool:
            await self.create_pool()

        async with self.pool.acquire() as connection:
            yield connection

    async def fetch(self, query, *args):
        """Выполнение запроса с возвратом множества записей"""
        async with self.connection() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query, *args):
        """Выполнение запроса с возвратом одной записи"""
        async with self.connection() as conn:
            return await conn.fetchrow(query, *args)

    async def execute(self, query, *args):
        """Выполнение запроса без возврата данных"""
        async with self.connection() as conn:
            return await conn.execute(query, *args)


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
