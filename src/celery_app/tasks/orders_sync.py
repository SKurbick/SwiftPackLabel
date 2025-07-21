"""
Периодические задачи для синхронизации заказов Wildberries
"""
import asyncio
from src.celery_app.celery import celery_app
from src.wildberries_api.orders import Orders
from src.utils import get_wb_tokens
from src.logger import get_logger

logger = get_logger()


@celery_app.task(name='sync_orders_periodic')
def sync_orders_periodic():
    """
    Периодическая задача для синхронизации заказов каждые 10 минут.
    """
    try:
        logger.info("Запуск периодической синхронизации заказов")
        
        # Получаем или создаем event loop для Celery задачи
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError("Event loop is closed")
        except RuntimeError:
            # Создаем новый event loop если текущий закрыт или отсутствует
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # Выполняем асинхронную функцию в текущем loop
        result = loop.run_until_complete(_sync_orders_async())
        
        logger.info(f"Периодическая синхронизация завершена: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Ошибка в периодической синхронизации заказов: {e}")
        raise


async def _sync_orders_async():
    """
    Асинхронная синхронизация заказов для всех аккаунтов с bulk update.
    """
    results = {}
    all_orders = []
    
    # Получаем все токены
    tokens_data = get_wb_tokens()
    
    # Этап 1: Получаем заказы от всех аккаунтов БЕЗ сохранения в БД
    for account_name, tokens in tokens_data.items():
        try:
            logger.info(f"Получение заказов для аккаунта: {account_name}")
            
            # Создаем API клиент
            orders_api = Orders(account_name, tokens)
            
            # Получаем заказы только из API
            orders = await orders_api.get_orders()
            
            # Добавляем все заказы в общий список
            all_orders.extend(orders)
            
            results[account_name] = {
                'orders_count': len(orders),
                'status': 'success'
            }
            
            logger.info(f"Получено {len(orders)} заказов для {account_name}")
            
        except Exception as e:
            logger.error(f"Ошибка получения заказов для {account_name}: {e}")
            results[account_name] = {
                'orders_count': 0,
                'status': 'error',
                'error': str(e)
            }
    
    # Этап 2: Batch update всех заказов
    if all_orders:
        try:
            logger.info(f"Начинаем batch update {len(all_orders)} заказов в БД")
            from src.models.orders_wb import OrdersDB
            from src.settings import settings
            import asyncpg
            
            # Создаем собственный пул для этой задачи
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
            
            # Разделяем на batch по 1000 записей
            batch_size = 1000
            total_batches = (len(all_orders) + batch_size - 1) // batch_size
            processed_orders = 0
            
            try:
                # Используем наш пул соединений
                async with pool.acquire() as connection:
                    for i in range(0, len(all_orders), batch_size):
                        batch = all_orders[i:i + batch_size]
                        current_batch = (i // batch_size) + 1
                        
                        logger.info(f"Обрабатываем batch {current_batch}/{total_batches}: {len(batch)} заказов")
                        
                        # Вызываем метод напрямую с соединением
                        await OrdersDB._update_orders_with_connection(connection, batch)
                        processed_orders += len(batch)
                        
                        # Добавляем задержку между batch операциями
                        if current_batch < total_batches:  # Не ждем после последнего batch
                            await asyncio.sleep(0.1)
                        
                logger.info(f"Успешно обновлено {processed_orders} заказов в базе данных ({total_batches} batch)")
            finally:
                # Обязательно закрываем пул
                await pool.close()
            
        except Exception as e:
            logger.error(f"Ошибка batch update заказов в БД: {e}")
            # Помечаем все аккаунты как ошибочные, если БД недоступна
            for account_name in results:
                if results[account_name]['status'] == 'success':
                    results[account_name]['status'] = 'db_error'
                    results[account_name]['error'] = str(e)
    else:
        logger.info("Нет заказов для обновления в БД")
    
    return results