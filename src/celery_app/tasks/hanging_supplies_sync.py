"""
Celery задачи для синхронизации висячих поставок.
"""
import asyncio
import json
from datetime import datetime
from typing import Dict, Any, List, Set

from src.celery_app.celery import celery_app
from src.models.hanging_supplies import HangingSupplies
from src.logger import get_logger

logger = get_logger()


class HangingSuppliesService:
    """Сервис для синхронизации висячих поставок с актуальными данными API."""
    
    def __init__(self, db):
        self.db = db
        self.hanging_supplies_model = HangingSupplies(db)
    
    async def get_all_hanging_supplies(self) -> List[Dict[str, Any]]:
        """
        Получает все висячие поставки из БД.
        
        Returns:
            List[Dict[str, Any]]: Список всех висячих поставок
        """
        return await self.hanging_supplies_model.get_hanging_supplies()
    
    def _validate_supplies_data(self, supplies_data: Dict[str, Dict[str, Any]]) -> None:
        """
        Валидация входных данных поставок.
        
        Args:
            supplies_data: Данные поставок для валидации
            
        Raises:
            ValueError: При некорректных данных
        """
        if not isinstance(supplies_data, dict):
            raise ValueError("supplies_data must be a dictionary")
        
        for account, account_data in supplies_data.items():
            if not isinstance(account_data, dict):
                raise ValueError(f"Data for account {account} must be a dictionary")
            
            for supply_id, supply_data in account_data.items():
                if not isinstance(supply_data, dict):
                    raise ValueError(f"Supply {supply_id} data must be a dictionary")
                if 'orders' not in supply_data:
                    raise ValueError(f"Supply {supply_id} must have 'orders' key")
                if not isinstance(supply_data['orders'], list):
                    raise ValueError(f"Supply {supply_id} orders must be a list")

    async def sync_hanging_supplies_with_current_data(self, current_supplies_data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Синхронизирует висячие поставки с актуальными данными из API.
        
        Args:
            current_supplies_data: Данные поставок из API (результат group_result)
            
        Returns:
            Dict[str, Any]: Статистика синхронизации
        """
        # Валидация входных данных
        self._validate_supplies_data(current_supplies_data)
        
        hanging_supplies = await self.get_all_hanging_supplies()
        sync_session = f"sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        processed_count = 0
        changes_count = 0
        
        logger.info(f"Начинаем синхронизацию {len(hanging_supplies)} висячих поставок")
        
        for hanging_supply in hanging_supplies:
            supply_id = hanging_supply['supply_id']
            account = hanging_supply['account']
            
            try:
                # Получаем исходные заказы из БД
                order_data = hanging_supply['order_data']
                if isinstance(order_data, str):
                    try:
                        order_data = json.loads(order_data)
                    except json.JSONDecodeError as e:
                        logger.error(f"Некорректный JSON в order_data для поставки {supply_id} ({account}): {str(e)}")
                        continue
                
                if not isinstance(order_data, dict) or 'orders' not in order_data:
                    logger.error(f"Некорректная структура order_data для поставки {supply_id} ({account})")
                    continue
                
                original_orders = order_data['orders']
                if not isinstance(original_orders, list):
                    logger.error(f"orders должен быть списком для поставки {supply_id} ({account})")
                    continue
                
                original_ids = {order['id'] for order in original_orders if isinstance(order, dict) and 'id' in order}
                
                # Получаем текущие заказы из переданных данных API
                current_orders = self._get_orders_from_supplies_data(
                    supply_id, account, current_supplies_data
                )
                current_ids = {order['id'] for order in current_orders if isinstance(order, dict) and 'id' in order}
                
                # Определяем изменения
                removed_ids = original_ids - current_ids  # Ушли из поставки
                added_ids = current_ids - original_ids    # Добавились в поставку
                
                # Обновляем order_data и логируем изменения, если они есть
                if removed_ids or added_ids:
                    # Обновляем order_data с актуальными данными из API
                    await self._update_order_data_and_log_changes(
                        supply_id, account, removed_ids, added_ids,
                        original_orders, current_orders, sync_session
                    )
                    processed_count += 1
                    changes_count += len(removed_ids) + len(added_ids)
                    
                    logger.info(f"Поставка {supply_id} ({account}): +{len(added_ids)}, -{len(removed_ids)} заказов")
                    
            except Exception as e:
                logger.error(f"Ошибка при синхронизации поставки {supply_id} ({account}): {str(e)}")
                continue
        
        result = {
            "sync_session": sync_session,
            "total_hanging_supplies": len(hanging_supplies),
            "processed_supplies": processed_count,
            "total_changes": changes_count,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        logger.info(f"Синхронизация завершена: {result}")
        return result
    
    def _get_orders_from_supplies_data(self, supply_id: str, account: str, supplies_data: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Извлекает заказы конкретной поставки из данных API.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            supplies_data: Данные поставок из API
            
        Returns:
            List[Dict[str, Any]]: Список заказов в поставке
        """
        if account not in supplies_data:
            logger.debug(f"Аккаунт {account} не найден в данных API")
            return []
        
        if supply_id not in supplies_data[account]:
            logger.debug(f"Поставка {supply_id} не найдена для аккаунта {account}")
            return []
        
        orders = supplies_data[account][supply_id].get('orders', [])
        logger.debug(f"Найдено {len(orders)} заказов для поставки {supply_id} ({account})")
        return orders
    
    async def _update_order_data_and_log_changes(self, supply_id: str, account: str, 
                                               removed_ids: Set[int], added_ids: Set[int],
                                               original_orders: List[Dict[str, Any]], 
                                               current_orders: List[Dict[str, Any]], 
                                               sync_session: str) -> None:
        """
        Обновляет order_data с актуальными заказами и логирует изменения в висячей поставке.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            removed_ids: Множество ID удаленных заказов
            added_ids: Множество ID добавленных заказов
            original_orders: Исходные заказы из БД
            current_orders: Текущие заказы из API
            sync_session: Идентификатор сессии синхронизации
        """
        changes_log_entries = []
        timestamp = datetime.utcnow().isoformat()
        
        # Логируем удаленные заказы
        for order_id in removed_ids:
            order_data = next((o for o in original_orders if o['id'] == order_id), None)
            if order_data:
                changes_log_entries.append({
                    "timestamp": timestamp,
                    "change_type": "removed",
                    "order_id": order_id,
                    "order_data": {
                        "id": order_data['id'],
                        "wild": order_data.get('article', ''),
                        "nm_id": order_data.get('nmId', 0),
                        "price": order_data.get('price', 0),
                        "subject_name": order_data.get('subject_name', ''),
                        "created_at": order_data.get('createdAt', '')
                    },
                    "sync_session": sync_session
                })
                logger.debug(f"Заказ {order_id} удален из поставки {supply_id}")
        
        # Логируем добавленные заказы
        for order_id in added_ids:
            order_data = next((o for o in current_orders if o['id'] == order_id), None)
            if order_data:
                changes_log_entries.append({
                    "timestamp": timestamp,
                    "change_type": "added",
                    "order_id": order_id,
                    "order_data": {
                        "id": order_data['id'],
                        "wild": order_data.get('article', ''),
                        "nm_id": order_data.get('nmId', 0),
                        "price": order_data.get('price', 0),
                        "subject_name": order_data.get('subject_name', ''),
                        "created_at": order_data.get('createdAt', '')
                    },
                    "sync_session": sync_session
                })
                logger.debug(f"Заказ {order_id} добавлен в поставку {supply_id}")
        
        # Обновляем order_data с актуальными заказами из API (включая случай с пустым списком)
        try:
            # Получаем исходные данные поставки для сохранения структуры
            hanging_supply = await self.hanging_supplies_model.get_hanging_supply_by_id(supply_id, account)
            if hanging_supply and hanging_supply.get('order_data'):
                # Десериализуем order_data если это строка
                order_data = hanging_supply['order_data']
                if isinstance(order_data, str):
                    try:
                        order_data = json.loads(order_data)
                    except json.JSONDecodeError as e:
                        logger.error(f"Ошибка десериализации order_data для поставки {supply_id} ({account}): {str(e)}")
                        return
                
                if not isinstance(order_data, dict):
                    logger.error(f"order_data должен быть словарем для поставки {supply_id} ({account})")
                    return
                
                # Обновляем только orders, сохраняя остальную структуру order_data
                updated_order_data = order_data.copy()
                updated_order_data['orders'] = current_orders  # Может быть пустым списком
                
                # Обновляем order_data в БД
                success = await self.hanging_supplies_model.update_order_data(supply_id, account, updated_order_data)
                if success:
                    logger.info(f"Обновлен order_data для поставки {supply_id} ({account}): {len(current_orders)} заказов")
                else:
                    logger.error(f"Не удалось обновить order_data для поставки {supply_id} ({account})")
            else:
                logger.warning(f"Поставка {supply_id} ({account}) не найдена или не содержит order_data")
        except Exception as e:
            logger.error(f"Ошибка при обновлении order_data для поставки {supply_id} ({account}): {str(e)}")
        
        # Логируем изменения в changes_log
        if changes_log_entries:
            await self._update_changes_log(supply_id, account, changes_log_entries)
            logger.info(f"Залогировано {len(changes_log_entries)} изменений для поставки {supply_id} ({account})")
    
    async def _update_changes_log(self, supply_id: str, account: str, changes_log_entries: List[Dict[str, Any]]) -> None:
        """
        Обновляет changes_log в БД через модель.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            changes_log_entries: Список новых записей для лога
        """
        success = await self.hanging_supplies_model.update_changes_log(supply_id, account, changes_log_entries)
        if not success:
            raise Exception(f"Не удалось обновить changes_log для поставки {supply_id} ({account})")
    
    async def get_changes_log(self, supply_id: str, account: str) -> List[Dict[str, Any]]:
        """
        Получает лог изменений для конкретной поставки.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            
        Returns:
            List[Dict[str, Any]]: Лог изменений
        """
        return await self.hanging_supplies_model.get_changes_log(supply_id, account)
    
    async def get_changes_statistics(self) -> Dict[str, Any]:
        """
        Получает статистику изменений по всем висячим поставкам.
        
        Returns:
            Dict[str, Any]: Статистика изменений
        """
        return await self.hanging_supplies_model.get_changes_statistics()

    async def _sync_conversion_supply_into_fictitious_shipment(self):
        """Логика автоперевода висячих поставок."""
        without_overdue_marker, without_invalid_marker = False, False # маркеры отсутствия поставок

        logger.info("Поиск висячих поставок с просроченными сборочными заданиями более 60 часов")
        overdue_supplies = await self.hanging_supplies_model._sync_get_hanging_supplies_by_status()
        # если поставки с просроченными сборочными отсутствуют
        if len(overdue_supplies) == 0:
            logger.info("Поставок с просроченными сборочными заданиями не найдено")
            without_overdue_marker = True

        logger.info("Поиск висячих поставок с отправленными сборочными заданиями")
        supplies_with_invalid_statuses = await self.hanging_supplies_model._sync_get_hanging_supplies_with_invalid_status()
        # если поставки с отправленными сборочными отсутствуют
        if len(supplies_with_invalid_statuses) == 0:
            logger.info("Поставок с отправленными сборочными заданиями не найдено")
            without_invalid_marker = True

        if not without_overdue_marker:
            supplies_ids = set(supply.supply_id for supply in overdue_supplies)
            logger.info(f"Количество поставок с просроченными сборочными заданиями: {len(supplies_ids)}")
            logger.info("Выполняется автоперевод в фиктивную доставку")
            await self.hanging_supplies_model._sync_conversion_supply_into_fictitious_shipment(supplies_ids)
            logger.info("Автоперевод выполнен")

            logger.info(f'Выполняется перевод сборочных заданий в статус "IN_HANGING_SUPPLY"')
            data = [
                (overdue_supply.order_id, overdue_supply.supply_id, overdue_supply.account)
                for overdue_supply in overdue_supplies
            ]
            await self.hanging_supplies_model._sync_update_orders_status(data)
            logger.info(f'Присвоение статусов выполнено успешно')

        if not without_invalid_marker:
            invalid_statuses_supplies_ids = set(supply.supply_id for supply in supplies_with_invalid_statuses)
            logger.info(f"Количество поставок с отправленными сборочными заданиями: {len(invalid_statuses_supplies_ids)}")
            logger.info("Выполняется автоперевод в фиктивную доставку")
            await self.hanging_supplies_model._sync_conversion_supply_into_fictitious_shipment(invalid_statuses_supplies_ids)
            logger.info("Автоперевод выполнен")

        if without_overdue_marker and without_invalid_marker:
            logger.info("Поставок с просроченными или отправленными сборочными заданиями не найдено.")
            return None
        return None


@celery_app.task(name='sync_hanging_supplies_with_data', soft_time_limit=600, time_limit=600)
def sync_hanging_supplies_with_data(supplies_data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Фоновая задача для синхронизации висячих поставок с переданными данными из API.
    
    Задача запускается из метода warm_up_cache после получения актуальных данных поставок,
    чтобы не делать дополнительных запросов к API.
    
    Args:
        supplies_data: Данные поставок из API (результат group_result)
        
    Returns:
        Dict[str, Any]: Результат синхронизации со статистикой
    """
    try:
        logger.info("Запуск фоновой синхронизации висячих поставок с переданными данными")
        
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
        result = loop.run_until_complete(_sync_hanging_supplies_async(supplies_data))
        
        logger.info(f"Фоновая синхронизация завершена успешно: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Ошибка в фоновой синхронизации висячих поставок: {str(e)}")
        logger.error(f"Тип переданных данных: {type(supplies_data)}")
        raise


async def _sync_hanging_supplies_async(supplies_data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Асинхронная синхронизация висячих поставок с переданными данными.
    
    Args:
        supplies_data: Данные поставок из API
        
    Returns:
        Dict[str, Any]: Результат синхронизации
    """
    from src.settings import settings
    import asyncpg
    
    # Создаем собственный пул для этой задачи
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
            hanging_supplies_service = HangingSuppliesService(connection)
            
            # Выполняем синхронизацию с переданными данными
            sync_result = await hanging_supplies_service.sync_hanging_supplies_with_current_data(supplies_data)
            
            # Добавляем общую информацию о задаче
            result = {
                "task_status": "success",
                "task_timestamp": datetime.utcnow().isoformat(),
                "api_data_accounts": len(supplies_data),
                "api_data_supplies": sum(len(supplies) for supplies in supplies_data.values()),
                **sync_result
            }
            
            logger.info(f"Синхронизация завершена: обработано {result['processed_supplies']} поставок")
            return result
        
    except Exception as e:
        logger.error(f"Ошибка в асинхронной синхронизации: {str(e)}")
        return {
            "task_status": "error",
            "task_timestamp": datetime.utcnow().isoformat(),
            "error": str(e)
        }
    finally:
        # Обязательно закрываем пул
        if pool:
            await pool.close()


@celery_app.task(name='get_hanging_supplies_statistics', soft_time_limit=600, time_limit=600)
def get_hanging_supplies_statistics() -> Dict[str, Any]:
    """
    Получает статистику изменений по всем висячим поставкам.
    
    Returns:
        Dict[str, Any]: Статистика изменений
    """
    try:
        logger.info("Запуск получения статистики висячих поставок")
        
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
        result = loop.run_until_complete(_get_statistics_async())
        
        logger.info(f"Статистика получена: {result.get('total_supplies_with_changes', 0)} поставок с изменениями")
        return result
        
    except Exception as e:
        logger.error(f"Ошибка получения статистики висячих поставок: {str(e)}")
        raise


async def _get_statistics_async() -> Dict[str, Any]:
    """
    Асинхронное получение статистики изменений.
    
    Returns:
        Dict[str, Any]: Статистика изменений
    """
    from src.settings import settings
    import asyncpg
    
    # Создаем собственный пул для этой задачи
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
            hanging_supplies_model = HangingSupplies(connection)
            statistics = await hanging_supplies_model.get_changes_statistics()
            
            return {
                "task_status": "success",
                "task_timestamp": datetime.utcnow().isoformat(),
                **statistics
            }
        
    except Exception as e:
        logger.error(f"Ошибка получения статистики: {str(e)}")
        return {
            "task_status": "error",
            "task_timestamp": datetime.utcnow().isoformat(),
            "error": str(e)
        }
    finally:
        # Обязательно закрываем пул
        if pool:
            await pool.close()


@celery_app.task(name='cleanup_old_changes_log', soft_time_limit=600, time_limit=600)
def cleanup_old_changes_log(days_to_keep: int = 30) -> Dict[str, Any]:
    """
    Очищает старые записи из changes_log для экономии места.
    
    Args:
        days_to_keep: Количество дней для хранения логов (по умолчанию 30)
        
    Returns:
        Dict[str, Any]: Результат очистки
    """
    try:
        logger.info(f"Запуск очистки старых записей changes_log (старше {days_to_keep} дней)")
        
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
        result = loop.run_until_complete(_cleanup_old_logs_async(days_to_keep))
        
        logger.info(f"Очистка завершена: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Ошибка очистки старых записей: {str(e)}")
        raise


async def _cleanup_old_logs_async(days_to_keep: int) -> Dict[str, Any]:
    """
    Асинхронная очистка старых записей из changes_log.
    
    Args:
        days_to_keep: Количество дней для хранения
        
    Returns:
        Dict[str, Any]: Результат очистки
    """
    from src.settings import settings
    import asyncpg
    
    # Создаем собственный пул для этой задачи
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
            hanging_supplies_model = HangingSupplies(connection)
            cleanup_result = await hanging_supplies_model.cleanup_old_changes_log(days_to_keep)
            
            result = {
                "task_status": "success",
                "task_timestamp": datetime.utcnow().isoformat(),
                **cleanup_result
            }
            
            return result
        
    except Exception as e:
        logger.error(f"Ошибка в асинхронной очистке: {str(e)}")
        return {
            "task_status": "error",
            "task_timestamp": datetime.utcnow().isoformat(),
            "error": str(e)
        }
    finally:
        # Обязательно закрываем пул
        if pool:
            await pool.close()

@celery_app.task(name="auto_conversion_hanging_supplies")
def auto_conversion_hanging_supplies():
    try:
        logger.info("Выполнение фоновой периодической задачи перевода висячих поставок в фиктивную доставку")
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError("Event loop is closed")
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        loop.run_until_complete(_auto_conversion())

        logger.info("Автоперевод висячих поставок в фиктивную доставку выполнен успешно")

    except Exception as error:
        logger.error(f"Ошибка в выполненнии автоперевода висячих поставок в фиктивную доставку: {error}")

async def _auto_conversion():
    from src.settings import settings
    import asyncpg

    pool = None
    try:
        pool = await asyncpg.create_pool(
                host=settings.db_app_host,
                port=settings.db_app_port,
                user=settings.db_app_user,
                password=settings.db_app_password,
                database=settings.dp_app_name,
                min_size=5,
                max_size=10,
                command_timeout=60
            )

        async with pool.acquire() as connection:
            hanging_supplies_service = HangingSuppliesService(connection)
            await hanging_supplies_service._sync_conversion_supply_into_fictitious_shipment()

    except Exception as error:
        logger.error(f"Ошибка в выполнении фоновой периодической задачи перевода висячих поставок в фиктивную доставку: {error}")

    finally:
        if pool:
            await pool.close()
