import asyncio
import json
import pickle
import time
from typing import Any, Optional, Dict, List
from datetime import datetime, timedelta
import redis.asyncio as redis

from src.orders.schema import OrderDetail
from src.settings import settings
from src.logger import app_logger as logger
from src.supplies.schema import SupplyIdResponseSchema

from src.supplies.supplies import SuppliesService
from src.orders.orders import OrdersService
from src.models.shipment_of_goods import ShipmentOfGoods
from src.models.hanging_supplies import HangingSupplies

from src.celery_app.tasks.hanging_supplies_sync import sync_hanging_supplies_with_data



class GlobalCache:
    """
    Глобальный кэш на основе Redis для улучшения производительности приложения.
    
    Предоставляет следующие возможности:
    - Кэширование данных с настраиваемым TTL
    - Автоматическое фоновое обновление кэша
    - Прогрев кэша при запуске приложения
    - Асинхронная работа с Redis
    """
    
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.background_tasks: List[asyncio.Task] = []
        self.is_connected = False
        # Флаг для тестирования оптимизированного метода кэширования
        self.use_optimized_cache = True  # Изменить на True для тестирования нового метода
        
    async def connect(self) -> None:
        """Подключение к Redis серверу."""
        try:
            redis_url = f"redis://"
            if settings.REDIS_PASSWORD:
                redis_url += f":{settings.REDIS_PASSWORD}@"
            redis_url += f"{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}"
            
            self.redis_client = redis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=False,  # Работаем с bytes для pickle
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            
            # Проверка подключения
            await self.redis_client.ping()
            self.is_connected = True
            logger.info(f"Успешное подключение к Redis: {settings.REDIS_HOST}:{settings.REDIS_PORT}")
            
        except Exception as e:
            logger.error(f"Ошибка подключения к Redis: {str(e)}")
            self.is_connected = False
            raise
    
    async def disconnect(self) -> None:
        """Отключение от Redis и остановка фоновых задач."""
        try:
            # Останавливаем фоновые задачи
            for task in self.background_tasks:
                if not task.done():
                    task.cancel()
                    
            if self.background_tasks:
                await asyncio.gather(*self.background_tasks, return_exceptions=True)
                
            # Закрываем соединение с Redis
            if self.redis_client:
                await self.redis_client.close()
                
            self.is_connected = False
            logger.info("Отключение от Redis выполнено успешно")
            
        except Exception as e:
            logger.error(f"Ошибка при отключении от Redis: {str(e)}")
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Получение значения из кэша.
        
        Args:
            key: Ключ для получения данных
            
        Returns:
            Кэшированные данные или None если ключ не найден
        """
        if not self.is_connected or not self.redis_client:
            logger.warning("Redis не подключен, пропускаем получение из кэша")
            return None
            
        try:
            cached_data = await self.redis_client.get(key)
            if cached_data:
                # Десериализация данных
                data = pickle.loads(cached_data)
                logger.debug(f"Кэш HIT для ключа: {key}")
                return data
            else:
                logger.debug(f"Кэш MISS для ключа: {key}")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка при получении данных из кэша для ключа {key}: {str(e)}")
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Сохранение значения в кэш.
        
        Args:
            key: Ключ для сохранения данных
            value: Данные для кэширования
            ttl: Время жизни в секундах (по умолчанию из настроек)
            
        Returns:
            True если данные успешно сохранены, False иначе
        """
        if not self.is_connected or not self.redis_client:
            logger.warning("Redis не подключен, пропускаем сохранение в кэш")
            return False
            
        try:
            # Удаляем старый ключ перед записью нового для экономии памяти
            await self.redis_client.delete(key)
            
            # Сериализация данных
            serialized_data = pickle.dumps(value)
            
            # Установка TTL
            cache_ttl = ttl or settings.CACHE_TTL
            
            # Сохранение в Redis
            await self.redis_client.setex(key, cache_ttl, serialized_data)
            logger.debug(f"Данные сохранены в кэш для ключа: {key}, TTL: {cache_ttl}s")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных в кэш для ключа {key}: {str(e)}")
            return False
    
    async def delete(self, key: str) -> bool:
        """
        Удаление значения из кэша.
        
        Args:
            key: Ключ для удаления
            
        Returns:
            True если ключ был удален, False иначе
        """
        if not self.is_connected or not self.redis_client:
            logger.warning("Redis не подключен, пропускаем удаление из кэша")
            return False
            
        try:
            result = await self.redis_client.delete(key)
            logger.debug(f"Ключ {key} удален из кэша: {bool(result)}")
            return bool(result)
            
        except Exception as e:
            logger.error(f"Ошибка при удалении ключа {key} из кэша: {str(e)}")
            return False
    
    async def clear_all(self) -> bool:
        """
        Очистка всего кэша.
        
        Returns:
            True если кэш очищен успешно, False иначе
        """
        if not self.is_connected or not self.redis_client:
            logger.warning("Redis не подключен, пропускаем очистку кэша")
            return False
            
        try:
            await self.redis_client.flushdb()
            logger.info("Кэш полностью очищен")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при очистке кэша: {str(e)}")
            return False
    
    async def cleanup_expired_keys(self) -> int:
        """
        Очистка истекших ключей кэша для экономии памяти.
        
        Returns:
            Количество удаленных ключей
        """
        if not self.is_connected or not self.redis_client:
            return 0
            
        try:
            # Получаем все ключи кэша
            all_keys = await self.redis_client.keys("cache:*")
            expired_keys = []
            
            for key in all_keys:
                try:
                    ttl = await self.redis_client.ttl(key)
                    # TTL = -2 означает, что ключ истек, но еще не удален
                    # TTL = -1 означает, что ключ существует без TTL
                    if ttl == -2:
                        expired_keys.append(key)
                except Exception:
                    continue
            
            # Удаляем истекшие ключи
            if expired_keys:
                await self.redis_client.delete(*expired_keys)
                logger.info(f"Очищено {len(expired_keys)} истекших ключей кэша")
                return len(expired_keys)
            
            return 0
            
        except Exception as e:
            logger.error(f"Ошибка при очистке истекших ключей: {str(e)}")
            return 0
    
    async def get_cache_info(self) -> Dict[str, Any]:
        """
        Получение информации о состоянии кэша.
        
        Returns:
            Словарь с информацией о кэше
        """
        if not self.is_connected or not self.redis_client:
            return {"connected": False, "keys_count": 0}
            
        try:
            info = await self.redis_client.info()
            keys_count = await self.redis_client.dbsize()
            
            return {
                "connected": True,
                "keys_count": keys_count,
                "used_memory": info.get("used_memory_human", "Unknown"),
                "connected_clients": info.get("connected_clients", 0),
                "uptime": info.get("uptime_in_seconds", 0)
            }
            
        except Exception as e:
            logger.error(f"Ошибка при получении информации о кэше: {str(e)}")
            return {"connected": False, "error": str(e)}
    
    async def warm_up_cache(self) -> None:
        """
        Прогрев кэша при запуске приложения.
        Автоматически выбирает оптимизированный или legacy метод.
        """
        if self.use_optimized_cache:
            logger.info("Используется ОПТИМИЗИРОВАННЫЙ метод прогрева кэша")
            return await self.warm_up_cache_optimized()
        else:
            logger.info("Используется LEGACY метод прогрева кэша")
            return await self.warm_up_cache_legacy()

    async def warm_up_cache_legacy(self) -> None:
        """
        LEGACY: Прогрев кэша при запуске приложения (старый метод).
        Заполняет кэш наиболее часто используемыми данными.
        """
        if not self.is_connected:
            logger.warning("Redis не подключен, пропускаем прогрев кэша")
            return
            
        logger.info("Начинаем прогрев кэша...")
        
        # Очищаем старые ключи перед прогревом для экономии памяти
        try:
            old_keys = await self.redis_client.keys("cache:*")
            if old_keys:
                await self.redis_client.delete(*old_keys)
                logger.info(f"Удалено {len(old_keys)} старых ключей кэша")
        except Exception as e:
            logger.warning(f"Не удалось очистить старые ключи: {str(e)}")
        
        try:
            
            # Прогрев данных поставок для всех комбинаций hanging_only и is_delivery
            try:
                from src.db import db as main_db
                async with main_db.connection() as connection:
                    supplies_service = SuppliesService(connection)
                    
                    # Прогрев кэша для всех комбинаций параметров
                    cache_combinations = [
                        {"hanging_only": False, "is_delivery": False},  # Обычные поставки из WB API
                        {"hanging_only": True, "is_delivery": False},   # Висячие поставки из WB API
                        {"hanging_only": False, "is_delivery": True},   # Обычные поставки из доставки
                        {"hanging_only": True, "is_delivery": True},    # Висячие поставки из доставки
                    ]
                    
                    for combination in cache_combinations:
                        hanging_only = combination["hanging_only"]
                        is_delivery = combination["is_delivery"]
                        
                        try:
                            logger.info(f"Прогрев кэша для hanging_only={hanging_only}, is_delivery={is_delivery}")
                            
                            # Получаем данные для конкретной комбинации параметров
                            supplies_response = await supplies_service.get_list_supplies(
                                hanging_only=hanging_only, 
                                is_delivery=is_delivery
                            )
                            
                            # Формируем ключ кэша с параметрами
                            cache_key = f"cache:supplies_all:hanging_only:{hanging_only}|is_delivery:{is_delivery}"
                            
                            # Сохраняем в кэш
                            await self.set(cache_key, supplies_response)
                            
                            logger.info(f"Кэш прогрет для {cache_key}: {len(supplies_response.supplies)} поставок")
                            
                        except Exception as e:
                            logger.error(f"Ошибка прогрева кэша для hanging_only={hanging_only}, is_delivery={is_delivery}: {str(e)}")
                            continue
                    
                    # Запускаем очистку пустых поставок только для данных из WB API
                    try:
                        from src.supplies.empty_supply_cleaner import EmptySupplyCleaner
                        cleaner = EmptySupplyCleaner(self.redis_client)
                        await cleaner.auto_clean_empty_supplies()
                        logger.info("Автоочистка пустых поставок завершена")
                    except Exception as e:
                        logger.error(f"Ошибка автоочистки пустых поставок: {str(e)}")
                    
                    # Запускаем фоновую синхронизацию висячих поставок только для данных из WB API
                    try:
                        # Получаем данные из WB API для синхронизации
                        supplies_ids = await supplies_service.get_information_to_supplies()
                        supplies = supplies_service.group_result(await supplies_service.get_information_orders_to_supplies(supplies_ids))
                        
                        sync_hanging_supplies_with_data.delay(supplies)
                        logger.info("Запущена фоновая синхронизация висячих поставок")
                    except Exception as e:
                        logger.error(f"Ошибка запуска фоновой синхронизации висячих поставок: {str(e)}")
                        
            except Exception as e:
                logger.error(f"Ошибка при прогреве кэша поставок: {str(e)}")
            
            # Прогрев данных заказов (с правильными ключами и полными объектами)
            try:
                from src.db import db as main_db
                async with main_db.connection() as connection:
                    orders_service = OrdersService(connection)
                    
                    # Базовый запрос: time_delta=1.0, wild=None
                    orders_data = await orders_service.get_filtered_orders(time_delta=1.0, article=None)
                    
                    # Создаем полные объекты OrderDetail
                    order_details = [OrderDetail(**order) for order in orders_data]
                    grouped_orders = await orders_service.group_orders_by_wild(order_details)
                    
                    # Правильный ключ с параметрами - используем увеличенный TTL
                    cache_key_orders = "cache:orders_all:time_delta:1.0|wild:None"
                    await self.set(cache_key_orders, grouped_orders)
                    
                    logger.info("Кэш заказов прогрет успешно")
            except Exception as e:
                logger.error(f"Ошибка при прогреве кэша заказов: {str(e)}")
                
            logger.info("Прогрев кэша завершен")
            
        except Exception as e:
            logger.error(f"Общая ошибка при прогреве кэша: {str(e)}")

    async def warm_up_cache_optimized(self) -> None:
        """
        УЛЬТРА-ОПТИМИЗИРОВАННЫЙ: Прогрев кэша при запуске приложения.
        Получает ВСЕ данные WB API и delivery ОДИН РАЗ, затем генерирует все 4 комбинации.
        Сокращает количество API запросов в ~10 раз!
        """
        if not self.is_connected:
            logger.warning("Redis не подключен, пропускаем прогрев кэша")
            return
            
        logger.info("Начинаем УЛЬТРА-ОПТИМИЗИРОВАННЫЙ прогрев кэша...")
        start_time = time.time()
        
        # Очищаем старые ключи перед прогревом для экономии памяти
        try:
            old_keys = await self.redis_client.keys("cache:*")
            if old_keys:
                await self.redis_client.delete(*old_keys)
                logger.info(f"Удалено {len(old_keys)} старых ключей кэша")
        except Exception as e:
            logger.warning(f"Не удалось очистить старые ключи: {str(e)}")
        
        try:
            from src.db import db as main_db
            async with main_db.connection() as connection:
                supplies_service = SuppliesService(connection)

                unified_data = await self._get_all_supplies_data_ultra_optimized(supplies_service)

                all_combinations = await self._generate_all_combinations_from_unified_data(
                    unified_data, supplies_service
                )
                
                # 3. КЭШИРУЕМ ВСЕ 4 КОМБИНАЦИИ
                cache_mappings = [
                    {"key": "hanging_only:False|is_delivery:False", "data": all_combinations['wb_normal']},
                    {"key": "hanging_only:True|is_delivery:False", "data": all_combinations['wb_hanging']},
                    {"key": "hanging_only:False|is_delivery:True", "data": all_combinations['delivery_normal']},
                    {"key": "hanging_only:True|is_delivery:True", "data": all_combinations['delivery_hanging']},
                ]
                
                for mapping in cache_mappings:
                    try:
                        cache_key = f"cache:supplies_all:{mapping['key']}"
                        await self.set(cache_key, mapping['data'])
                        
                        logger.info(f"УЛЬТРА-ОПТИМИЗИРОВАННЫЙ кэш прогрет для {cache_key}: {len(mapping['data'].supplies)} поставок")
                        
                    except Exception as e:
                        logger.error(f"Ошибка кэширования {mapping['key']}: {str(e)}")
                        continue
                
                # 4. ОСТАЛЬНЫЕ ОПЕРАЦИИ (используем уже полученные данные)
                try:
                    from src.supplies.empty_supply_cleaner import EmptySupplyCleaner
                    cleaner = EmptySupplyCleaner(self.redis_client)
                    await cleaner.auto_clean_empty_supplies()
                    logger.info("Автоочистка пустых поставок завершена")
                except Exception as e:
                    logger.error(f"Ошибка автоочистки пустых поставок: {str(e)}")
                
                # 5. СИНХРОНИЗАЦИЯ ВИСЯЧИХ ПОСТАВОК (используем уже полученные WB данные)
                try:
                    # Используем уже полученные данные вместо повторного API вызова
                    sync_hanging_supplies_with_data.delay(unified_data['wb_supplies_grouped'])
                    logger.info("Запущена фоновая синхронизация висячих поставок (ультра-оптимизация)")
                except Exception as e:
                    logger.error(f"Ошибка запуска фоновой синхронизации висячих поставок: {str(e)}")
                        
        except Exception as e:
            logger.error(f"Ошибка при ультра-оптимизированном прогреве кэша поставок: {str(e)}")
        
        # 6. ПРОГРЕВ ДАННЫХ ЗАКАЗОВ (оставляем как есть)
        try:
            from src.db import db as main_db
            async with main_db.connection() as connection:
                orders_service = OrdersService(connection)
                
                # Базовый запрос: time_delta=1.0, wild=None
                orders_data = await orders_service.get_filtered_orders(time_delta=1.0, article=None)
                
                # Создаем полные объекты OrderDetail
                order_details = [OrderDetail(**order) for order in orders_data]
                grouped_orders = await orders_service.group_orders_by_wild(order_details)
                
                # Правильный ключ с параметрами - используем увеличенный TTL
                cache_key_orders = "cache:orders_all:time_delta:1.0|wild:None"
                await self.set(cache_key_orders, grouped_orders)
                
                logger.info("Кэш заказов прогрет успешно")
        except Exception as e:
            logger.error(f"Ошибка при прогреве кэша заказов: {str(e)}")
        
        elapsed_time = time.time() - start_time
        logger.info(f"УЛЬТРА-ОПТИМИЗИРОВАННЫЙ прогрев кэша завершен за {elapsed_time:.2f} секунд")

    async def _get_all_supplies_data_ultra_optimized(self, supplies_service) -> Dict[str, Any]:
        """
        УЛЬТРА-ОПТИМИЗИРОВАННОЕ получение ВСЕХ данных единым блоком:
        1. WB API вызывается ОДИН РАЗ для всех случаев
        2. БД запросы делаются ОДИН РАЗ
        3. Все 4 комбинации генерируются из одних данных
        """
        logger.info("Получение ВСЕХ данных поставок ультра-оптимизированным способом...")
        
        # 1. ЕДИНСТВЕННЫЙ вызов WB API для получения ВСЕХ поставок
        logger.info("1/4: Получение ВСЕХ WB поставок...")
        wb_supplies_ids = await supplies_service.get_information_to_supplies()  # ОДИН РАЗ!
        logger.info("2/4: Получение ВСЕХ WB заказов...")
        wb_orders_data = await supplies_service.get_information_orders_to_supplies(wb_supplies_ids)  # ОДИН РАЗ!
        wb_supplies_grouped = supplies_service.group_result(wb_orders_data)
        
        # 2. ЕДИНСТВЕННЫЕ запросы к БД для delivery данных  
        logger.info("3/4: Получение delivery данных из БД...")
        basic_supplies_ids = await ShipmentOfGoods(supplies_service.db).get_weekly_supply_ids()  # ОДИН РАЗ!
        fictitious_supplies_ids = await HangingSupplies(supplies_service.db).get_weekly_fictitious_supplies_ids(
            is_fictitious_delivered=True)  # ОДИН РАЗ!
        
        # 3. Подготовка delivery данных (используем уже полученные wb_supplies_ids!)
        logger.info("4/4: Обработка delivery поставок...")
        all_db_supplies_ids = supplies_service._merge_supplies_data(basic_supplies_ids, fictitious_supplies_ids)
        filtered_delivery_supplies_ids = supplies_service._exclude_wb_active_from_db_supplies(
            all_db_supplies_ids, wb_supplies_ids  # ПЕРЕИСПОЛЬЗУЕМ!
        )
        delivery_supplies_details = await supplies_service.get_information_to_supply_details(filtered_delivery_supplies_ids)
        delivery_orders_data = await supplies_service.get_information_orders_to_supplies(delivery_supplies_details)  # ОДИН РАЗ!
        delivery_supplies_grouped = supplies_service.group_result(delivery_orders_data)
        
        logger.info("ВСЕ данные поставок получены ультра-оптимизированным способом!")
        
        return {
            'wb_supplies_ids': wb_supplies_ids,
            'wb_supplies_grouped': wb_supplies_grouped,
            'delivery_supplies_ids': delivery_supplies_details, 
            'delivery_supplies_grouped': delivery_supplies_grouped
        }

    async def _generate_all_combinations_from_unified_data(self, unified_data, supplies_service):
        """
        Генерирует все 4 комбинации из единых данных без дополнительных API вызовов.
        УЛЬТРА-ОПТИМИЗАЦИЯ: вся фильтрация происходит в памяти!
        """
        logger.info("Генерация всех 4 комбинаций из единых данных...")
        
        combinations = {}
        
        try:
            # 1. WB Normal (hanging_only=False, is_delivery=False)
            logger.info("Генерация WB Normal...")
            combinations['wb_normal'] = await self._filter_wb_supplies_ultra_optimized(
                unified_data['wb_supplies_grouped'], 
                unified_data['wb_supplies_ids'], 
                hanging_only=False,
                supplies_service=supplies_service
            )
            
            # 2. WB Hanging (hanging_only=True, is_delivery=False)  
            logger.info("Генерация WB Hanging...")
            combinations['wb_hanging'] = await self._filter_wb_supplies_ultra_optimized(
                unified_data['wb_supplies_grouped'],
                unified_data['wb_supplies_ids'], 
                hanging_only=True,
                supplies_service=supplies_service
            )
            
            # 3. Delivery Normal (hanging_only=False, is_delivery=True)
            logger.info("Генерация Delivery Normal...")
            combinations['delivery_normal'] = await self._filter_delivery_supplies_ultra_optimized(
                unified_data['delivery_supplies_grouped'],
                unified_data['delivery_supplies_ids'],
                hanging_only=False,
                supplies_service=supplies_service
            )
            
            # 4. Delivery Hanging (hanging_only=True, is_delivery=True)
            logger.info("Генерация Delivery Hanging...")
            combinations['delivery_hanging'] = await self._filter_delivery_supplies_ultra_optimized(
                unified_data['delivery_supplies_grouped'],
                unified_data['delivery_supplies_ids'], 
                hanging_only=True,
                supplies_service=supplies_service
            )
            
            logger.info("Все 4 комбинации сгенерированы успешно!")
            return combinations
            
        except Exception as e:
            logger.error(f"Ошибка генерации комбинаций: {str(e)}")
            raise

    async def _filter_wb_supplies_ultra_optimized(
        self, 
        wb_supplies_grouped: Dict[str, Dict], 
        wb_supplies_ids: List[Dict],
        hanging_only: bool,
        supplies_service
    ) -> SupplyIdResponseSchema:
        """
        УЛЬТРА-ОПТИМИЗИРОВАННАЯ фильтрация WB поставок - БЕЗ дополнительных API вызовов.
        """
        try:
            result = []
            supplies_ids_dict = {key: value for d in wb_supplies_ids for key, value in d.items()}
            
            for account, value in wb_supplies_grouped.items():
                for supply_id, orders in value.items():
                    # Формируем данные поставки (не delivery)
                    supply = {
                        data["id"]: {"name": data["name"], "createdAt": data['createdAt']}
                        for data in supplies_ids_dict[account] if not data['done']
                    }
                    
                    result.append(supplies_service.create_supply_result(supply, supply_id, account, orders))
            
            # Применяем фильтр по hanging_only
            filtered_result = await supplies_service.filter_supplies_by_hanging(result, hanging_only)
            return SupplyIdResponseSchema(supplies=filtered_result)
            
        except Exception as e:
            logger.error(f"Ошибка ультра-фильтрации WB поставок: {str(e)}")
            raise

    async def _filter_delivery_supplies_ultra_optimized(
        self, 
        delivery_supplies_grouped: Dict[str, Dict], 
        delivery_supplies_ids: List[Dict],
        hanging_only: bool,
        supplies_service
    ) -> SupplyIdResponseSchema:
        """
        УЛЬТРА-ОПТИМИЗИРОВАННАЯ фильтрация delivery поставок - БЕЗ дополнительных API вызовов.
        """
        try:
            result = []
            supplies_ids_dict = {key: value for d in delivery_supplies_ids for key, value in d.items()}
            
            for account, value in delivery_supplies_grouped.items():
                for supply_id, orders in value.items():
                    # Формируем данные поставки (delivery)
                    supply = {
                        data["id"]: {"name": data["name"], "createdAt": data['createdAt']}
                        for data in supplies_ids_dict[account]
                    }
                    
                    result.append(supplies_service.create_supply_result(supply, supply_id, account, orders))
            
            # Применяем фильтр по hanging_only
            filtered_result = await supplies_service.filter_supplies_by_hanging(result, hanging_only)
            return SupplyIdResponseSchema(supplies=filtered_result)
            
        except Exception as e:
            logger.error(f"Ошибка ультра-фильтрации delivery поставок: {str(e)}")
            raise
    
    async def _background_refresh_task(self) -> None:
        """Фоновая задача для обновления кэша."""
        refresh_count = 0
        while True:
            try:
                # Обновляем каждые 8 минут вместо 10, чтобы данные не успевали истечь
                refresh_interval = min(int(settings.CACHE_REFRESH_INTERVAL * 0.8), settings.CACHE_TTL - 120)  # На 2 минуты раньше истечения TTL
                await asyncio.sleep(refresh_interval)
                refresh_count += 1
                
                logger.info(f"[Refresh #{refresh_count}] Запуск фонового обновления кэша...")
                logger.info(f"Интервал обновления: {settings.CACHE_REFRESH_INTERVAL} секунд")
                
                # Проверяем состояние Redis перед обновлением
                if not self.is_connected:
                    logger.error("Redis недоступен, пропускаем обновление кэша")
                    continue
                
                # Логируем текущее состояние кэша
                cache_info = await self.get_cache_info()
                logger.info(f"Состояние кэша перед обновлением: {cache_info}")
                
                # Очищаем истекшие ключи перед обновлением
                cleaned_keys = await self.cleanup_expired_keys()
                if cleaned_keys > 0:
                    logger.info(f"Очищено {cleaned_keys} истекших ключей перед обновлением")
                
                # Обновляем кэш
                await self.warm_up_cache()
                
                # Логируем состояние после обновления
                cache_info_after = await self.get_cache_info()
                logger.info(f"[Refresh #{refresh_count}] Фоновое обновление кэша завершено")
                logger.info(f"Состояние кэша после обновления: {cache_info_after}")
                
            except asyncio.CancelledError:
                logger.info("Фоновая задача обновления кэша отменена")
                break
            except Exception as e:
                logger.error(f"КРИТИЧЕСКАЯ ОШИБКА в фоновой задаче обновления кэша (refresh #{refresh_count}): {str(e)}")
                logger.error(f"Попытаемся продолжить через {settings.CACHE_REFRESH_INTERVAL} секунд...")
                # Продолжаем работу, даже если произошла ошибка
                continue
    
    async def start_background_refresh_all(self) -> None:
        """Запуск фонового обновления кэша."""
        if not self.is_connected:
            logger.warning("Redis не подключен, пропускаем запуск фонового обновления")
            return
            
        logger.info("Запускаем фоновое обновление кэша...")
        logger.info(f"Интервал обновления: {settings.CACHE_REFRESH_INTERVAL} секунд")
        task = asyncio.create_task(self._background_refresh_task())
        self.background_tasks.append(task)
    
    async def diagnose_cache_keys(self) -> dict:
        """Диагностика ключей кэша для отладки."""
        if not self.is_connected or not self.redis_client:
            return {"error": "Redis недоступен"}
        
        try:
            all_keys = await self.redis_client.keys("cache:*")
            keys_info = {}
            
            for key in all_keys:
                key_str = key.decode('utf-8') if isinstance(key, bytes) else str(key)
                try:
                    ttl = await self.redis_client.ttl(key)
                    size = await self.redis_client.memory_usage(key)
                    keys_info[key_str] = {
                        "ttl": ttl,
                        "size_bytes": size or 0
                    }
                except Exception as e:
                    keys_info[key_str] = {"error": str(e)}
            
            return {
                "total_keys": len(all_keys),
                "keys": keys_info,
                "expected_keys": [
                    "cache:supplies_all:hanging_only:False|is_delivery:False",
                    "cache:supplies_all:hanging_only:True|is_delivery:False", 
                    "cache:supplies_all:hanging_only:False|is_delivery:True",
                    "cache:supplies_all:hanging_only:True|is_delivery:True",
                    "cache:orders_all:time_delta:1.0|wild:None"
                ]
            }
            
        except Exception as e:
            return {"error": f"Ошибка диагностики: {str(e)}"}
            
    async def force_refresh_cache(self) -> bool:
        """Принудительное обновление кэша."""
        logger.info("Принудительное обновление кэша...")
        try:
            await self.warm_up_cache()
            logger.info("Принудительное обновление кэша завершено успешно")
            return True
        except Exception as e:
            logger.error(f"Ошибка при принудительном обновлении кэша: {str(e)}")
            return False

    async def refresh_specific_cache(self, cache_type: str, hanging_only: bool = None, is_delivery: bool = None) -> bool:
        """
        Принудительное обновление конкретного типа кэша.
        
        Args:
            cache_type: Тип кэша ('supplies' или 'orders')
            hanging_only: Для поставок - только висячие (True) или обычные (False), None - все
            is_delivery: Для поставок - из доставки (True) или WB API (False), None - все
            
        Returns:
            bool: True если обновление успешно
        """
        logger.info(f"Селективное обновление кэша: type={cache_type}, hanging_only={hanging_only}, is_delivery={is_delivery}")
        
        if not self.is_connected:
            logger.warning("Redis не подключен, пропускаем селективное обновление кэша")
            return False
        
        try:
            if cache_type == "supplies":
                return await self._refresh_supplies_cache(hanging_only, is_delivery)
            else:
                logger.error(f"Неизвестный тип кэша: {cache_type}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка при селективном обновлении кэша {cache_type}: {str(e)}")
            return False
    
    async def _refresh_supplies_cache(self, hanging_only: bool = None, is_delivery: bool = None) -> bool:
        """Обновление кэша поставок с фильтрацией."""
        try:
            from src.db import db as main_db

            # Определяем какие комбинации обновлять
            combinations_to_update = []

            if hanging_only is None and is_delivery is None:
                # Обновляем все комбинации
                combinations_to_update = [
                    {"hanging_only": False, "is_delivery": False},
                    {"hanging_only": True, "is_delivery": False},
                    {"hanging_only": False, "is_delivery": True},
                    {"hanging_only": True, "is_delivery": True},
                ]
            elif hanging_only is None:
                combinations_to_update.extend([
                    {"hanging_only": False, "is_delivery": is_delivery},
                    {"hanging_only": True, "is_delivery": is_delivery},
                ])
            elif is_delivery is None:
                combinations_to_update.extend([
                    {"hanging_only": hanging_only, "is_delivery": False},
                    {"hanging_only": hanging_only, "is_delivery": True},
                ])
            else:
                combinations_to_update.append({
                    "hanging_only": hanging_only, 
                    "is_delivery": is_delivery
                })

            async with main_db.connection() as connection:
                supplies_service = SuppliesService(connection)

                success_count = 0
                for combination in combinations_to_update:
                    try:
                        h_only = combination["hanging_only"]
                        delivery = combination["is_delivery"]

                        logger.info(f"Обновление кэша поставок: hanging_only={h_only}, is_delivery={delivery}")

                        # Получаем свежие данные
                        supplies_response = await supplies_service.get_list_supplies(
                            hanging_only=h_only,
                            is_delivery=delivery
                        )

                        # Формируем ключ кэша
                        cache_key = f"cache:supplies_all:hanging_only:{h_only}|is_delivery:{delivery}"

                        # Удаляем старый ключ и сохраняем новые данные
                        await self.delete(cache_key)
                        await self.set(cache_key, supplies_response)

                        logger.info(f"Кэш обновлен для {cache_key}: {len(supplies_response.supplies)} поставок")
                        success_count += 1

                    except Exception as e:
                        logger.error(f"Ошибка обновления кэша для {combination}: {str(e)}")
                        continue

                logger.info(f"Обновление кэша поставок завершено: {success_count}/{len(combinations_to_update)} успешно")
                return success_count == len(combinations_to_update)

        except Exception as e:
            logger.error(f"Критическая ошибка при обновлении кэша поставок: {str(e)}")
            return False
    

    def _extract_supply_ids_from_cache(self, cached_data) -> set:
        """
        Извлекает supply_id из кэшированных данных.
        
        Args:
            cached_data: Кэшированные данные типа SupplyIdResponseSchema
            
        Returns:
            set: Множество supply_id из кэша
        """
        if not cached_data or not hasattr(cached_data, 'supplies'):
            return set()
        return {supply.supply_id for supply in cached_data.supplies}

    def _calculate_supply_differences(self, cached_ids: set, current_ids: set) -> Dict[str, Any]:
        """
        Вычисляет различия между множествами supply_id.
        
        Args:
            cached_ids: Множество supply_id из кэша
            current_ids: Множество текущих supply_id из БД
            
        Returns:
            Dict с результатами сравнения
        """
        new_supplies = list(current_ids - cached_ids)
        removed_supplies = list(cached_ids - current_ids)
        
        return {
            "cached_count": len(cached_ids),
            "current_count": len(current_ids),
            "new_supplies": new_supplies,
            "removed_supplies": removed_supplies,
            "has_changes": len(new_supplies) > 0 or len(removed_supplies) > 0
        }

    async def check_delivery_supplies_diff(self) -> Dict[str, Any]:
        """
        Проверяет различия между актуальными и кэшированными поставками доставки.
        
        Сравнивает только номера supply_id для эффективности.
        Проверяет оба типа delivery поставок:
        - Обычные (hanging_only=False, is_delivery=True)  
        - Висячие (hanging_only=True, is_delivery=True)
        
        Returns:
            Dict с результатами сравнения и метриками
        """
        if not self.is_connected:
            return {"error": "Redis недоступен"}
        
        start_time = datetime.utcnow()
        
        try:
            from src.db import db as main_db
            
            # Получаем кэшированные данные
            cached_delivery_normal = await self.get("cache:supplies_all:hanging_only:False|is_delivery:True")
            cached_delivery_hanging = await self.get("cache:supplies_all:hanging_only:True|is_delivery:True")
            
            # Извлекаем supply_id из кэша
            cached_normal_ids = self._extract_supply_ids_from_cache(cached_delivery_normal)
            cached_hanging_ids = self._extract_supply_ids_from_cache(cached_delivery_hanging)
            
            # Получаем текущие supply_id напрямую из БД (только номера)
            async with main_db.connection() as connection:
                supplies_service = SuppliesService(connection)
                
                current_normal_ids = await supplies_service.get_delivery_supplies_ids_only(hanging_only=False)
                current_hanging_ids = await supplies_service.get_delivery_supplies_ids_only(hanging_only=True)
            
            # Вычисляем различия
            normal_diff = self._calculate_supply_differences(cached_normal_ids, current_normal_ids)
            hanging_diff = self._calculate_supply_differences(cached_hanging_ids, current_hanging_ids)
            
            duration = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            result = {
                "timestamp": start_time.isoformat(),
                "check_duration_ms": round(duration, 2),
                "method": "optimized_supply_ids_only",
                
                "delivery_normal": {
                    **normal_diff,
                    "cache_key": "cache:supplies_all:hanging_only:False|is_delivery:True"
                },
                
                "delivery_hanging": {
                    **hanging_diff,
                    "cache_key": "cache:supplies_all:hanging_only:True|is_delivery:True"
                },
                
                "summary": {
                    "total_changes": normal_diff["has_changes"] or hanging_diff["has_changes"],
                    "total_new": len(normal_diff["new_supplies"]) + len(hanging_diff["new_supplies"]),
                    "total_removed": len(normal_diff["removed_supplies"]) + len(hanging_diff["removed_supplies"]),
                    "affected_cache_keys": sum([normal_diff["has_changes"], hanging_diff["has_changes"]])
                }
            }
            
            if result["summary"]["total_changes"]:
                logger.info(f"Обнаружены изменения в delivery поставках: {result['summary']}")
            else:
                logger.info("Изменений в delivery поставках не обнаружено")
                
            return result
            
        except Exception as e:
            logger.error(f"Ошибка проверки различий delivery поставок: {str(e)}")
            return {"error": str(e)}


# Глобальный экземпляр кэша
global_cache = GlobalCache()
