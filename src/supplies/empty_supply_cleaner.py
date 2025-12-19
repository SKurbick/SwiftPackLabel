import json
import asyncio
from typing import Set, Dict, Any
from src.logger import app_logger as logger
from src.wildberries_api.supplies import Supplies
from src.utils import get_wb_tokens
from src.db import db as main_db
from src.models.hanging_supplies import HangingSupplies

class EmptySupplyCleaner:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.REDIS_KEY = "empty_supplies_tracking"
    
    async def get_saved_empty_supplies(self) -> Set[str]:
        """Получает сохраненный список пустых поставок"""
        try:
            data = await self.redis.get(self.REDIS_KEY)
            return set(json.loads(data.decode())) if data else set()
        except Exception as e:
            logger.error(f"Ошибка получения сохраненных пустых поставок: {e}")
            return set()
    
    async def save_empty_supplies(self, empty_supplies: Set[str]):
        """Сохраняет список пустых поставок"""
        try:
            data = json.dumps(list(empty_supplies))
            await self.redis.set(self.REDIS_KEY, data)
            logger.info(f"Сохранено {len(empty_supplies)} пустых поставок в Redis")
        except Exception as e:
            logger.error(f"Ошибка сохранения пустых поставок: {e}")
    
    def _make_supply_key(self, supply_id: str, account: str) -> str:
        """Создает уникальный ключ поставки"""
        return f"{account}:{supply_id}"
    
    async def get_supplies_from_cache(self) -> Dict[str, int]:
        """
        Получает ВСЕ поставки из кэша с количеством заказов
        Возвращает: {"account:supply_id": orders_count}
        """
        supplies_data = {}
        
        try:
            # Получаем обычные поставки
            cache_key_normal = "cache:supplies_all:hanging_only:False"
            cached_normal = await self.redis.get(cache_key_normal)
            
            # Получаем висячие поставки  
            cache_key_hanging = "cache:supplies_all:hanging_only:True"
            cached_hanging = await self.redis.get(cache_key_hanging)
            
            if not cached_normal and not cached_hanging:
                logger.warning("Кэш поставок пустой, получаем данные напрямую")
                return await self._get_supplies_direct()
            
            import pickle
            
            # Обрабатываем обычные поставки
            if cached_normal:
                normal_response = pickle.loads(cached_normal)
                for supply in normal_response.supplies:
                    supply_key = self._make_supply_key(supply.supply_id, supply.account)
                    orders_count = len(supply.orders) if supply.orders else 0
                    supplies_data[supply_key] = orders_count
            
            # Обрабатываем висячие поставки
            if cached_hanging:
                hanging_response = pickle.loads(cached_hanging)
                for supply in hanging_response.supplies:
                    supply_key = self._make_supply_key(supply.supply_id, supply.account)
                    orders_count = len(supply.orders) if supply.orders else 0
                    supplies_data[supply_key] = orders_count
            
            logger.info(f"Получено из кэша данных о {len(supplies_data)} поставках (обычные + висячие)")
            return supplies_data
            
        except Exception as e:
            logger.error(f"Ошибка получения данных из кэша: {e}")
            # Fallback на прямое получение
            return await self._get_supplies_direct()
    
    async def _get_supplies_direct(self) -> Dict[str, int]:
        """Fallback метод для прямого получения поставок из WB API"""
        supplies_data = {}
        wb_tokens = get_wb_tokens()
        
        try:
            # Получаем все поставки по всем аккаунтам
            tasks = []
            for account, token in wb_tokens.items():
                task = Supplies(account, token).get_supplies_filter_done()
                tasks.append(task)
            
            all_supplies = await asyncio.gather(*tasks)
            
            # Обрабатываем каждую поставку
            for account_supplies in all_supplies:
                for account, supplies in account_supplies.items():
                    if account not in wb_tokens:
                        continue
                    
                    # Получаем количество заказов для каждой поставки
                    # Используем get_supply_order_ids вместо get_supply_orders для скорости
                    # (не нужны детали заказов, только количество)
                    supplies_api = Supplies(account, wb_tokens[account])
                    for supply in supplies:
                        supply_id = supply['id']
                        supply_key = self._make_supply_key(supply_id, account)

                        try:
                            order_ids = await supplies_api.get_supply_order_ids(supply_id)
                            orders_count = len(order_ids)
                            supplies_data[supply_key] = orders_count

                        except Exception as e:
                            logger.warning(f"Ошибка получения заказов {supply_key}: {e}")
                            # БЕЗОПАСНОСТЬ: В случае ошибки НЕ включаем поставку в анализ
                            # Это предотвращает случайное удаление поставок при недоступности API
                            continue
            
            logger.info(f"Получено напрямую данных о {len(supplies_data)} поставках")
            return supplies_data
            
        except Exception as e:
            logger.error(f"Ошибка прямого получения данных о поставках: {e}")
            return {}
    
    async def delete_supplies(self, supply_keys: Set[str], current_supplies_data: Dict[str, int]) -> int:
        """Удаляет поставки по списку ключей с финальной проверкой по актуальным данным"""
        if not supply_keys:
            return 0
        
        deleted_count = 0
        wb_tokens = get_wb_tokens()
        
        logger.info(f"Начинаем удаление {len(supply_keys)} поставок")
        
        for supply_key in supply_keys:
            try:
                account, supply_id = supply_key.split(':', 1)
                
                if account not in wb_tokens:
                    logger.warning(f"Токен для аккаунта {account} не найден")
                    continue
                
                # ФИНАЛЬНАЯ ПРОВЕРКА 1: используем актуальные данные, полученные при обновлении кэша
                current_orders_count = current_supplies_data.get(supply_key, -1)
                
                if current_orders_count < 0:
                    logger.warning(f"БЕЗОПАСНОСТЬ: Нет актуальных данных для поставки {supply_id} ({account}) - пропускаем удаление")
                    continue
                
                if current_orders_count > 0:
                    logger.warning(f"БЕЗОПАСНОСТЬ: Поставка {supply_id} ({account}) содержит {current_orders_count} заказов - удаление отменено")
                    continue
                
                # ФИНАЛЬНАЯ ПРОВЕРКА 2: дополнительный запрос к WB API для абсолютной уверенности
                if not await self._verify_empty_supply_via_api(supply_id, account, wb_tokens[account]):
                    logger.warning(f"БЕЗОПАСНОСТЬ: Поставка {supply_id} ({account}) не прошла API проверку - пропускаем удаление")
                    continue
                
                # Удаляем через WB API
                supplies_api = Supplies(account, wb_tokens[account])
                await supplies_api.delete_supply(supply_id)
                deleted_count += 1
                logger.info(f"✓ Удалена поставка {supply_id} ({account}) из WB API")

                # Удаляем запись из hanging_supplies если она там есть
                try:
                    async with main_db.connection() as db_conn:
                        hanging_model = HangingSupplies(db_conn)
                        was_deleted = await hanging_model.delete_hanging_supply(supply_id, account)
                        if was_deleted:
                            logger.info(f"✓ Удалена запись из hanging_supplies: {supply_id} ({account})")
                        else:
                            logger.debug(f"Запись не найдена в hanging_supplies: {supply_id} ({account})")
                except Exception as e:
                    logger.warning(f"Ошибка удаления из hanging_supplies {supply_id}: {e}")

            except Exception as e:
                logger.error(f"Ошибка удаления {supply_key}: {e}")
        
        logger.info(f"Удалено {deleted_count} из {len(supply_keys)} поставок")
        return deleted_count
    
    async def _verify_empty_supply_via_api(self, supply_id: str, account: str, token: str) -> bool:
        """
        Дополнительная проверка пустоты поставки через прямой запрос к WB API.
        Возвращает True только если поставка действительно пустая.
        """
        try:
            logger.info(f"Выполняется дополнительная API проверка поставки {supply_id} ({account})")

            supplies_api = Supplies(account, token)

            # Получаем только order_ids (быстрее чем get_supply_orders)
            order_ids = await supplies_api.get_supply_order_ids(supply_id)
            orders_count = len(order_ids)

            if orders_count > 0:
                logger.warning(f"API ПРОВЕРКА: Поставка {supply_id} ({account}) содержит {orders_count} заказов")
                return False

            logger.info(f"API ПРОВЕРКА: Поставка {supply_id} ({account}) подтверждена как пустая")
            return True

        except Exception as e:
            logger.error(f"API ПРОВЕРКА: Ошибка при проверке поставки {supply_id} ({account}): {e}")
            # В случае ошибки НЕ разрешаем удаление (принцип безопасности)
            return False
    
    async def process_empty_supplies(self) -> Dict[str, Any]:
        """
        Главный метод обработки пустых поставок
        Логика: находим пустые → сравниваем с сохраненными → удаляем пересечение → обновляем сохраненные
        """
        logger.info("Начинаем обработку пустых поставок")
        
        try:
            # 1. Получаем ранее сохраненные пустые поставки
            saved_empty = await self.get_saved_empty_supplies()
            logger.info(f"Ранее сохранено пустых: {len(saved_empty)}")
            
            # 2. Получаем текущее состояние всех поставок из кэша
            current_supplies = await self.get_supplies_from_cache()
            
            if not current_supplies:
                logger.warning("Не получены данные о поставках")
                return {'error': 'Не удалось получить данные о поставках'}
            
            # 3. Находим текущие пустые поставки
            current_empty = {key for key, orders_count in current_supplies.items() if orders_count == 0}
            logger.info(f"Текущих пустых: {len(current_empty)}")
            
            # 4. Находим поставки для удаления (были пустые И остались пустые)
            to_delete = saved_empty.intersection(current_empty)
            logger.info(f"К удалению (пустые 2 раза): {len(to_delete)}")
            
            # 5. Удаляем поставки
            deleted_count = 0
            if to_delete:
                deleted_count = await self.delete_supplies(to_delete, current_supplies)
            
            # 6. Обновляем сохраненные пустые поставки
            # Убираем удаленные и несуществующие, оставляем текущие пустые
            all_existing = set(current_supplies.keys())
            new_empty_to_save = current_empty - to_delete  # Текущие пустые минус удаленные
            
            # Очищаем от несуществующих поставок
            cleaned_nonexistent = len(saved_empty) - len(saved_empty.intersection(all_existing))
            
            await self.save_empty_supplies(new_empty_to_save)
            
            result = {
                'success': True,
                'saved_empty_before': len(saved_empty),
                'current_empty_found': len(current_empty),
                'marked_for_deletion': len(to_delete),
                'successfully_deleted': deleted_count,
                'cleaned_nonexistent': cleaned_nonexistent,
                'saved_empty_after': len(new_empty_to_save),
                'total_supplies_checked': len(current_supplies)
            }
            
            logger.info(f"Обработка завершена: удалено {deleted_count}, сохранено {len(new_empty_to_save)} пустых")
            return result
            
        except Exception as e:
            logger.error(f"Критическая ошибка обработки пустых поставок: {e}")
            return {'error': str(e), 'success': False}

    async def auto_clean_empty_supplies(self):
        """Автоматическая очистка при обновлении кэша"""
        try:
            result = await self.process_empty_supplies()
            
            if result.get('success'):
                logger.info(f"Автоочистка: удалено {result.get('successfully_deleted', 0)} пустых поставок")
            
        except Exception as e:
            logger.error(f"Ошибка автоочистки пустых поставок: {e}")