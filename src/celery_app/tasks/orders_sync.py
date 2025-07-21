"""
Периодические задачи для синхронизации заказов Wildberries
"""
import asyncio
from src.celery_app.celery import celery_app
from src.wildberries_api.orders import Orders
from src.utils import get_wb_tokens
from src.logger import get_logger

logger = get_logger()


@celery_app.task(name='sync_orders_periodic', soft_time_limit=600, time_limit=600)
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
    Асинхронная синхронизация заказов для всех аккаунтов.
    """
    results = {}
    
    # Получаем все токены
    tokens_data = get_wb_tokens()
    for account_name, tokens in tokens_data.items():
        try:
            logger.info(f"Синхронизация заказов для аккаунта: {account_name}")
            
            # Создаем API клиент
            orders_api = Orders(account_name, tokens)
            
            # Получаем и обновляем заказы (get_orders автоматически сохраняет в БД)
            orders = await orders_api.get_orders()
            
            results[account_name] = {
                'orders_count': len(orders),
                'status': 'success'
            }
            
            logger.info(f"Обновлено {len(orders)} заказов для {account_name}")
            
        except Exception as e:
            logger.error(f"Ошибка синхронизации для {account_name}: {e}")
            results[account_name] = {
                'orders_count': 0,
                'status': 'error',
                'error': str(e)
            }
    
    return results