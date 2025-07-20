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
        result = asyncio.run(_sync_orders_async())
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
    
    # Этап 2: Один bulk update всех заказов
    if all_orders:
        try:
            logger.info(f"Начинаем bulk update {len(all_orders)} заказов в БД")
            from src.models.orders_wb import OrdersDB
            await OrdersDB.update_orders(all_orders)
            logger.info(f"Успешно обновлено {len(all_orders)} заказов в базе данных")
        except Exception as e:
            logger.error(f"Ошибка bulk update заказов в БД: {e}")
            # Помечаем все аккаунты как ошибочные, если БД недоступна
            for account_name in results:
                if results[account_name]['status'] == 'success':
                    results[account_name]['status'] = 'db_error'
                    results[account_name]['error'] = str(e)
    else:
        logger.info("Нет заказов для обновления в БД")
    
    return results