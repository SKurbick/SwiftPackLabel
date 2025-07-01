"""
Простая система глобального кэширования с автоматическим обновлением каждые 5 минут.
Все пользователи получают данные из одного глобального кэша.
"""

import json
import asyncio
import time
from functools import wraps
from typing import Any, Optional, Dict, Callable
import redis.asyncio as redis
from src.settings import settings
from src.logger import get_logger

logger = get_logger()

# Глобальные структуры для управления кэшированием
_registered_functions: Dict[str, Dict] = {}
_scheduled_tasks: Dict[str, asyncio.Task] = {}
_refresh_locks: Dict[str, asyncio.Lock] = {}


class GlobalCacheService:
    """Сервис для автоматического обновления глобального кэша каждые 5 минут."""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis_client = redis_client
    
    async def start_background_refresh(self, cache_key: str):
        """Запуск фонового обновления кэша."""
        if cache_key in _scheduled_tasks:
            logger.debug(f"Background refresh already running for {cache_key}")
            return
            
        if cache_key not in _registered_functions:
            logger.warning(f"Function not registered for background refresh: {cache_key}")
            return
            
        task = asyncio.create_task(self._background_refresh_loop(cache_key))
        _scheduled_tasks[cache_key] = task
        logger.info(f"Started background refresh task for {cache_key}")
    
    async def _background_refresh_loop(self, cache_key: str):
        """Основной цикл фонового обновления с настраиваемым интервалом."""
        func_data = _registered_functions[cache_key]
        interval = settings.CACHE_REFRESH_INTERVAL  # Берем из настроек
        
        # Создаем lock для этого ключа если его нет
        if cache_key not in _refresh_locks:
            _refresh_locks[cache_key] = asyncio.Lock()
        
        while True:
            try:
                await asyncio.sleep(interval)
                
                # Блокируем обновление, чтобы не было дублирования
                async with _refresh_locks[cache_key]:
                    logger.info(f"Starting background refresh for {cache_key}")
                    
                    # Выполняем функцию обновления
                    func = func_data['func']
                    args = func_data['args']
                    kwargs = func_data['kwargs']
                    
                    result = await func(*args, **kwargs)
                    
                    # Обновляем кэш
                    await self._update_cache(cache_key, result)
                    
                    logger.info(f"Background refresh completed for {cache_key}")
                
            except Exception as e:
                logger.error(f"Background refresh failed for {cache_key}: {e}")
                # Продолжаем работу несмотря на ошибку
                await asyncio.sleep(30)  # Пауза перед повтором при ошибке
    
    async def _update_cache(self, cache_key: str, value: Any):
        """Обновление значения в кэше."""
        try:
            ttl = settings.CACHE_TTL  # Берем из настроек
            serialized_value = json.dumps(self._serialize_value(value), ensure_ascii=False)
            
            await self.redis_client.setex(cache_key, ttl, serialized_value)
            logger.debug(f"Cache updated: {cache_key}")
        except Exception as e:
            logger.error(f"Failed to update cache for {cache_key}: {e}")
    
    def _serialize_value(self, value: Any) -> Any:
        """Сериализация значения для хранения в кэше."""
        if hasattr(value, 'model_dump'):  # Pydantic v2
            return value.model_dump()
        elif hasattr(value, 'dict'):  # Pydantic v1
            return value.dict()
        elif isinstance(value, list):
            return [self._serialize_value(item) for item in value]
        elif isinstance(value, dict):
            return {k: self._serialize_value(v) for k, v in value.items()}
        else:
            return value
    
    async def stop_background_refresh(self, cache_key: str):
        """Остановка фонового обновления."""
        if cache_key in _scheduled_tasks:
            _scheduled_tasks[cache_key].cancel()
            del _scheduled_tasks[cache_key]
            logger.info(f"Stopped background refresh for {cache_key}")
    
    async def stop_all(self):
        """Остановка всех фоновых обновлений."""
        for cache_key in list(_scheduled_tasks.keys()):
            await self.stop_background_refresh(cache_key)


class GlobalCacheManager:
    """
    Глобальный менеджер кэша с автоматическим обновлением каждые 5 минут.
    Все пользователи получают данные из одного кэша.
    """
    
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.cache_service: Optional[GlobalCacheService] = None
        self._connected = False

    async def connect(self):
        """Подключение к Redis и инициализация сервисов."""
        logger.info(f"Attempting to connect to Redis...")
        logger.info(f"Redis config: host={settings.REDIS_HOST}, port={settings.REDIS_PORT}, db={settings.REDIS_DB}")
        
        try:
            self.redis_client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                password=settings.REDIS_PASSWORD if settings.REDIS_PASSWORD else None,
                decode_responses=True,
                retry_on_timeout=True
            )
            logger.info(f"Redis client created, testing connection...")
            
            await self.redis_client.ping()
            logger.info(f"Redis ping successful!")
            
            self._connected = True
            logger.info(f"Connection status set to True")
            
            # Инициализация сервиса глобального кэша
            self.cache_service = GlobalCacheService(self.redis_client)
            logger.info(f"GlobalCacheService initialized")
            
            logger.info(f"Global cache connected to Redis at {settings.REDIS_HOST}:{settings.REDIS_PORT}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            logger.error(f"Exception type: {type(e)}")
            self._connected = False
            logger.error(f"Connection status set to False")

    async def disconnect(self):
        """Отключение от Redis и остановка всех задач."""
        if self.cache_service:
            await self.cache_service.stop_all()
        
        if self.redis_client:
            await self.redis_client.close()
            self._connected = False
            logger.info("Global cache disconnected from Redis")

    def is_connected(self) -> bool:
        """Проверка подключения к Redis."""
        return self._connected

    def _generate_key(self, key: str) -> str:
        """Генерация ключа кэша (просто возвращаем исходный ключ)."""
        return key

    async def get(self, key: str) -> Optional[Any]:
        """Получение данных из кэша."""
        logger.info(f"Cache.get() called for key: {key}")
        
        if not self.is_connected():
            logger.warning(f"Cache.get() - not connected to Redis")
            return None
        
        try:
            logger.info(f"Executing Redis GET for key: {key}")
            cached_data = await self.redis_client.get(key)
            logger.info(f"Redis GET result: exists={cached_data is not None}, size={len(cached_data) if cached_data else 0}")
            
            if cached_data:
                logger.info(f"Cache HIT for key: {key}")
                parsed_data = json.loads(cached_data)
                logger.info(f"Cache data parsed successfully, type: {type(parsed_data)}")
                return parsed_data
            
            logger.info(f"Cache MISS for key: {key}")
            return None
        except Exception as e:
            logger.error(f"Error getting cache for key {key}: {e}")
            logger.error(f"Exception type: {type(e)}")
            return None

    def _serialize_value(self, value: Any) -> Any:
        """Сериализация значения для хранения в кэше."""
        if hasattr(value, 'model_dump'):  # Pydantic v2
            return value.model_dump()
        elif hasattr(value, 'dict'):  # Pydantic v1
            return value.dict()
        elif isinstance(value, list):
            return [self._serialize_value(item) for item in value]
        elif isinstance(value, dict):
            return {k: self._serialize_value(v) for k, v in value.items()}
        else:
            return value

    async def set(self, key: str, value: Any, ttl: int = None) -> bool:
        """Сохранение данных в кэш."""
        logger.info(f"Cache.set() called for key: {key}")
        
        if not self.is_connected():
            logger.warning(f"Cache.set() - not connected to Redis")
            return False
        
        try:
            ttl = ttl or settings.CACHE_TTL  # Берем из настроек
            logger.info(f"Using TTL: {ttl} seconds for key: {key}")
            
            logger.info(f"Serializing value for cache, type: {type(value)}")
            serialized_value = json.dumps(self._serialize_value(value), ensure_ascii=False)
            logger.info(f"Serialized value size: {len(serialized_value)} chars")
            
            logger.info(f"Executing Redis SETEX for key: {key}")
            await self.redis_client.setex(key, ttl, serialized_value)
            logger.info(f"Cache SET successful: {key} (TTL: {ttl}s)")
            return True
        except Exception as e:
            logger.error(f"Error setting cache for key {key}: {e}")
            logger.error(f"Exception type: {type(e)}")
            logger.error(f"Value type that failed: {type(value)}")
            return False

    async def delete_pattern(self, pattern: str) -> bool:
        """Удаление ключей по паттерну."""
        if not self.is_connected():
            return False
        
        try:
            keys = await self.redis_client.keys(pattern)
            if keys:
                await self.redis_client.delete(*keys)
                logger.info(f"Deleted {len(keys)} cache keys matching pattern: {pattern}")
            return True
        except Exception as e:
            logger.error(f"Error deleting cache pattern {pattern}: {e}")
            return False

    async def warm_up_cache(self):
        """Начальная инициализация всех зарегистрированных кэшей."""
        logger.info("Starting cache warm-up...")
        
        if not _registered_functions:
            logger.warning("No functions registered for warm-up. Skipping cache initialization.")
            return
        
        logger.info(f"Found {len(_registered_functions)} functions to warm up")
        
        for cache_key, func_data in _registered_functions.items():
            try:
                logger.info(f"Warming up cache for {cache_key}")
                
                func = func_data['func']
                args = func_data['args']
                kwargs = func_data['kwargs']
                
                # Выполняем функцию и кэшируем результат
                result = await func(*args, **kwargs)
                
                # Сохраняем результат в кэш
                await self.set(cache_key, result, settings.CACHE_TTL)  # TTL из настроек
                logger.info(f"Cache warmed up for {cache_key}")
                
            except Exception as e:
                logger.error(f"Failed to warm up cache for {cache_key}: {e}")
        
        logger.info("Cache warm-up completed")

    async def start_background_refresh_all(self):
        """Запуск всех зарегистрированных фоновых обновлений."""
        if not self.cache_service:
            logger.error("Cache service not initialized")
            return
            
        logger.info("Starting all background refresh tasks...")
        
        for cache_key in _registered_functions.keys():
            await self.cache_service.start_background_refresh(cache_key)
        
        logger.info("All background refresh tasks started")


# Глобальный экземпляр менеджера кэша
global_cache = GlobalCacheManager()


def _register_cache_function(cache_key: str, func: Callable, args: tuple = (), kwargs: dict = None):
    """Внутренняя функция для регистрации функций кэширования."""
    if kwargs is None:
        kwargs = {}
        
    _registered_functions[cache_key] = {
        'func': func,
        'args': args,
        'kwargs': kwargs
    }


def global_cached(key: str = None):
    """
    Декоратор для глобального кэширования с автоматическим фоновым обновлением каждые 5 минут.
    
    Args:
        key: Ключ кэша (по умолчанию имя функции)
    
    Example:
        @global_cached(key="supplies_all")
        async def get_supplies():
            return await SuppliesService().get_list_supplies()
    """
    def decorator(func):
        # Регистрируем функцию для глобального кэширования
        cache_key = key or func.__name__
        
        # Регистрируем функцию для кэширования
        _register_cache_function(cache_key, func, (), {})
        
        @wraps(func)
        async def wrapper(*args, **kwargs):
            logger.info(f"Cache wrapper called for {func.__name__}")
            logger.info(f"Global cache connection status: {global_cache.is_connected()}")
            
            if not global_cache.is_connected():
                logger.warning(f"Cache not available for {func.__name__}, executing without cache")
                logger.warning(f"Redis client status: {global_cache.redis_client}")
                logger.warning(f"Cache service status: {global_cache.cache_service}")
                return await func(*args, **kwargs)
            
            # Генерация ключа кэша (игнорируем параметры для глобального кэша)
            final_key = global_cache._generate_key(cache_key)
            logger.info(f"Generated cache key: {final_key}")
            
            # Попытка получить из кэша
            logger.info(f"Attempting to get cached data for key: {final_key}")
            cached_result = await global_cache.get(final_key)
            logger.info(f"Cache get result: {type(cached_result)} (None={cached_result is None})")
            
            if cached_result is not None:
                logger.info(f"Cache HIT: Returning cached result for {func.__name__} (size: {len(str(cached_result))} chars)")
                return cached_result
            
            # Если кэша нет - выполняем функцию и кэшируем результат
            logger.info(f"Cache MISS: No cache found for {final_key}, executing function {func.__name__}")
            result = await func(*args, **kwargs)
            logger.info(f"Function executed, result size: {len(str(result))} chars")
            
            # Сохраняем в кэш
            logger.info(f"Setting cache for key: {final_key} with TTL: {settings.CACHE_TTL}")
            await global_cache.set(final_key, result, settings.CACHE_TTL)
            logger.info(f"Cache SET: Successfully cached result for {func.__name__}")
            
            return result
        
        return wrapper
    return decorator


# Для обратной совместимости
hybrid_cached = global_cached


# Вспомогательные функции для ручного управления кэшем
async def invalidate_cache(pattern: str) -> bool:
    """Инвалидация кэша по паттерну."""
    return await global_cache.delete_pattern(pattern)


async def clear_function_cache(function_name: str) -> bool:
    """Очистка кэша для конкретной функции."""
    return await global_cache.delete_pattern(f"{function_name}*")