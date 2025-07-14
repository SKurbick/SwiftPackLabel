import asyncio
import json
import pickle
from typing import Any, Optional, Dict, List
from datetime import datetime, timedelta
import redis.asyncio as redis

from src.settings import settings
from src.logger import app_logger as logger


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
            # Импортируем сервисы для прогрева
            from src.supplies.supplies import SuppliesService
            from src.orders.orders import OrdersService
            from src.db import get_db_connection
            
            # Прогрев данных поставок (оптимизированный - один запрос к API)
            try:
                db_gen = get_db_connection()
                db = await db_gen.__anext__()
                try:
                    supplies_service = SuppliesService(db)
                    
                    logger.info("Выполняем один запрос к API WB для получения всех поставок...")
                    
                    # Получаем ВСЕ данные поставок один раз (внутренний вызов без фильтрации)
                    supplies_ids = await supplies_service.get_information_to_supplies()
                    supplies = supplies_service.group_result(await supplies_service.get_information_orders_to_supplies(supplies_ids))
                    result = []
                    supplies_ids_dict = {key: value for d in supplies_ids for key, value in d.items()}
                    
                    for account, value in supplies.items():
                        for supply_id, orders in value.items():
                            supply = {data["id"]: {"name": data["name"], "createdAt": data['createdAt']}
                                     for data in supplies_ids_dict[account] if not data['done']}
                            result.append(supplies_service.create_supply_result(supply, supply_id, account, orders))
                    
                    logger.info(f"Получено {len(result)} поставок для кэширования")
                    
                    # Фильтруем и кэшируем обычные поставки (hanging_only=False)
                    supplies_data_normal = await supplies_service.filter_supplies_by_hanging(result, hanging_only=False)
                    cache_key_normal = "cache:supplies_all:hanging_only:False"
                    from src.supplies.schema import SupplyIdResponseSchema
                    response_normal = SupplyIdResponseSchema(supplies=supplies_data_normal)
                    await self.set(cache_key_normal, response_normal)
                    
                    # Фильтруем и кэшируем висячие поставки (hanging_only=True)
                    supplies_data_hanging = await supplies_service.filter_supplies_by_hanging(result, hanging_only=True)
                    cache_key_hanging = "cache:supplies_all:hanging_only:True"
                    response_hanging = SupplyIdResponseSchema(supplies=supplies_data_hanging)
                    await self.set(cache_key_hanging, response_hanging)
                    
                    logger.info(f"Кэш поставок прогрет успешно: обычные={len(supplies_data_normal)}, висячие={len(supplies_data_hanging)}")
                finally:
                    await db_gen.aclose()
            except Exception as e:
                logger.error(f"Ошибка при прогреве кэша поставок: {str(e)}")
            
            # Прогрев данных заказов (с правильными ключами и полными объектами)
            try:
                db_gen = get_db_connection()
                db = await db_gen.__anext__()
                try:
                    orders_service = OrdersService(db)
                    
                    # Базовый запрос: time_delta=1.0, wild=None
                    orders_data = await orders_service.get_filtered_orders(time_delta=1.0, article=None)
                    
                    # Создаем полные объекты OrderDetail
                    from src.orders.schema import OrderDetail
                    order_details = [OrderDetail(**order) for order in orders_data]
                    grouped_orders = await orders_service.group_orders_by_wild(order_details)
                    
                    # Правильный ключ с параметрами - используем увеличенный TTL
                    cache_key_orders = "cache:orders_all:time_delta:1.0|wild:None"
                    await self.set(cache_key_orders, grouped_orders)
                    
                    logger.info("Кэш заказов прогрет успешно")
                finally:
                    await db_gen.aclose()
            except Exception as e:
                logger.error(f"Ошибка при прогреве кэша заказов: {str(e)}")
                
            logger.info("Прогрев кэша завершен")
            
        except Exception as e:
            logger.error(f"Общая ошибка при прогреве кэша: {str(e)}")
    
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
                    "cache:supplies_all:hanging_only:False",
                    "cache:supplies_all:hanging_only:True", 
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


# Глобальный экземпляр кэша
global_cache = GlobalCache()