import asyncpg
import asyncio

from src.available_quantity.service import AvailableQuantityService
from src.celery_app import celery_app
from src.logger import get_logger
from src.available_quantity.repository import AvailableQuantityRepository
from src.settings import settings

logger = get_logger()

@celery_app.task(name="sync_update_available_quantity")
def sync_update_available_quantity():
    try:
        logger.info("Выполнение периодической задачи подсчета свободных остатков")
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError("Event loop is closed")
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        result = loop.run_until_complete(update())

        logger.info(f"Периодическая задача подсчета свободных остатков успешно завершена!")
        return result

    except Exception as error:
        logger.error(f"Ошибка в периодическом обновлении свободных остатков: {error}")
        raise

async def update():
    pool = None
    try:
        pool = await asyncpg.create_pool(
            host=settings.db_app_host,
            port=settings.db_app_port,
            user=settings.db_app_user,
            password=settings.db_app_password,
            database=settings.dp_app_name,
            min_size=1,
            max_size=5,
            command_timeout=60
        )


        async with pool.acquire() as connection:
            repository = AvailableQuantityRepository(connection)
            service = AvailableQuantityService(repository)
            await service.update_available_quantity()

    except Exception as error:
        logger.error(f"Ошибка в выполнении автоматического пересчёта свободных остатков: {error}")

    finally:
        await pool.close()