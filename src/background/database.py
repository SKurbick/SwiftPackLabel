import asyncpg

from src.db import DatabaseManager
from src.settings import settings


background_db = DatabaseManager()


async def connect_background_db() -> None:
    """мини пул отдельный"""
    if background_db.pool is not None:
        return
    background_db.pool = await asyncpg.create_pool(
        host=background_db.host,
        port=background_db.port,
        user=background_db.user,
        password=background_db.password,
        database=background_db.database,
        min_size=0,
        max_size=settings.BACKGROUND_DB_POOL_MAX_SIZE,
        timeout=settings.connection_timeout,
        command_timeout=60,
        max_inactive_connection_lifetime=settings.max_connection_idle_time,
    )
    background_db.max_size = settings.BACKGROUND_DB_POOL_MAX_SIZE


async def close_background_db() -> None:
    """зыкрытие пула"""
    if background_db.pool is not None:
        await background_db.pool.close()
        background_db.pool = None
