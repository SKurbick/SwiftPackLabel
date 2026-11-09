import json
from datetime import datetime
from typing import Dict, Any, List, Set

from src.models.hanging_supplies import HangingSupplies
from src.logger import app_logger as logger


class HangingSuppliesService:
    """Сервис для синхронизации висячих поставок с актуальными данными API."""
    
    def __init__(self, db):
        self.db = db
        self.hanging_supplies_model = HangingSupplies(db)
    
    async def get_all_hanging_supplies(self) -> List[Dict[str, Any]]:
        """
        Получает все висячие поставки из БД.
        """
        return await self.hanging_supplies_model.get_hanging_supplies()
    
    def _validate_supplies_data(self, supplies_data: Dict[str, Dict[str, Any]]) -> None:
        """
        Валидация входных данных поставок.
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
        """
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
        """
        changes_log_entries = []
        timestamp = datetime.utcnow().isoformat()
        
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
        """
        return await self.hanging_supplies_model.get_changes_statistics()

    async def _sync_conversion_supply_into_fictitious_shipment(self):
        logger.info("Поиск висячих поставок")
        overdue_supplies = await self.hanging_supplies_model._sync_get_hanging_supplies_by_status()

        if len(overdue_supplies) == 0:
            logger.info("Поставок с просроченными сборочными заданиями не найдено")
            return None

        logger.info(f"Количество поставок с просроченными сборочными заданиями: {len(overdue_supplies)}")
        logger.info("Выполняется автоперевод в фиктивную доставку")

        supplies_ids = [supply.supply_id for supply in overdue_supplies]

        await self.hanging_supplies_model._sync_conversion_supply_into_fictitious_shipment(supplies_ids)
        logger.info("Автоперевод выполнен")
