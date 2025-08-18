import functools
from typing import Any, Callable, Optional
import asyncio
import inspect

from .global_cache import global_cache
from src.logger import app_logger as logger


def global_cached(key: str, ttl: Optional[int] = None, cache_only: bool = False):
    """
    Декоратор для кэширования результатов функций.
    
    Args:
        key: Ключ для кэширования
        ttl: Время жизни кэша в секундах (необязательно)
        cache_only: Если True, данные берутся ТОЛЬКО из кэша (по умолчанию True)
        
    Usage:
        @global_cached(key="my_function_cache", cache_only=True)
        async def my_function():
            return expensive_operation()
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            # Генерируем ключ кэша
            cache_key = f"cache:{key}"
            
            # Добавляем параметры к ключу если они есть
            if args or kwargs:
                params_str = _serialize_params(args, kwargs)
                cache_key = f"{cache_key}:{params_str}"
            
            # Проверяем подключение к кэшу
            if not global_cache.is_connected:
                if cache_only:
                    logger.error(f"КРИТИЧЕСКАЯ ОШИБКА: Кэш недоступен для {func.__name__}, но cache_only=True")
                    raise Exception(f"Cache unavailable for cache-only function {func.__name__}")
                else:
                    logger.warning(f"Кэш недоступен для функции {func.__name__}, выполняем без кэширования")
                    return await func(*args, **kwargs)
            
            try:
                # Пытаемся получить данные из кэша
                cached_result = await global_cache.get(cache_key)
                if cached_result is not None:
                    logger.debug(f"Cache HIT для {func.__name__} с ключом {cache_key}")
                    return cached_result
                
                # Если данных в кэше нет
                if cache_only:
                    logger.error(f"КРИТИЧЕСКАЯ ОШИБКА: Данные отсутствуют в кэше для {func.__name__} (cache_only=True)")
                    logger.error(f"Ключ кэша: {cache_key}")
                    
                    # Попытаемся найти похожие ключи для диагностики
                    try:
                        if global_cache.redis_client:
                            all_keys = await global_cache.redis_client.keys("cache:*")
                            matching_keys = [k.decode('utf-8') if isinstance(k, bytes) else k for k in all_keys if key in str(k)]
                            logger.error(f"Доступные ключи с '{key}': {matching_keys}")
                    except Exception:
                        pass
                    
                    # Вместо исключения попытаемся выполнить функцию как fallback
                    logger.warning(f"Fallback: выполняем {func.__name__} без кэша из-за отсутствия данных")
                    result = await func(*args, **kwargs)
                    # Сохраняем результат в кэш для следующих запросов
                    await global_cache.set(cache_key, result, ttl)
                    return result
                
                # Если cache_only=False, выполняем функцию
                logger.warning(f"Cache MISS для {func.__name__}, выполняем функцию")
                result = await func(*args, **kwargs)
                
                # Сохраняем результат в кэш
                await global_cache.set(cache_key, result, ttl)
                logger.debug(f"Результат {func.__name__} сохранен в кэш с ключом {cache_key}")
                
                return result
                
            except Exception as e:
                if cache_only:
                    logger.error(f"КРИТИЧЕСКАЯ ОШИБКА в кэше для {func.__name__}: {str(e)}")
                    raise
                else:
                    logger.error(f"Ошибка при работе с кэшем для функции {func.__name__}: {str(e)}")
                    # В случае ошибки кэша, выполняем функцию без кэширования
                    return await func(*args, **kwargs)
        
        return wrapper
    return decorator


def _serialize_params(args: tuple, kwargs: dict) -> str:
    """
    Сериализация параметров функции для создания уникального ключа кэша.
    
    Args:
        args: Позиционные аргументы
        kwargs: Именованные аргументы
        
    Returns:
        Строковое представление параметров
    """
    try:
        # Фильтруем специальные объекты, которые не нужно включать в ключ
        filtered_kwargs = {}
        for k, v in kwargs.items():
            # Пропускаем объекты соединения с БД, запросы, пользователей
            if k in ['db', 'request', 'user']:
                continue
            # Включаем None значения для корректного кэширования
            filtered_kwargs[k] = v
        
        # Создаем строку из параметров
        params_list = []
        
        # Добавляем args (исключая первые элементы, которые могут быть объектами)
        for i, arg in enumerate(args):
            if i == 0:  # Обычно первый аргумент - это self или объекты
                continue
            params_list.append(f"arg{i}:{str(arg)}")
        
        # Добавляем kwargs с None значениями
        for k, v in sorted(filtered_kwargs.items()):
            if v is None:
                params_list.append(f"{k}:None")
            else:
                params_list.append(f"{k}:{str(v)}")
        
        return "|".join(params_list) if params_list else "no_params"
        
    except Exception as e:
        logger.warning(f"Ошибка при сериализации параметров: {str(e)}")
        return "serialization_error"


def cache_invalidate(key: str):
    """
    Декоратор для инвалидации (удаления) кэша.
    
    Args:
        key: Ключ кэша для удаления
        
    Usage:
        @cache_invalidate(key="my_function_cache")
        async def update_data():
            # Функция, которая изменяет данные и должна очистить кэш
            pass
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            # Выполняем функцию
            result = await func(*args, **kwargs)
            
            # Удаляем из кэша
            if global_cache.is_connected:
                cache_key = f"cache:{key}"
                await global_cache.delete(cache_key)
                logger.debug(f"Кэш с ключом {cache_key} удален после выполнения {func.__name__}")
            
            return result
        return wrapper
    return decorator


def cache_warm_up(keys: list[str]):
    """
    Декоратор для прогрева кэша.
    
    Args:
        keys: Список ключей кэша для прогрева
        
    Usage:
        @cache_warm_up(keys=["supplies_all", "orders_all"])
        async def warm_up_function():
            pass
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            result = await func(*args, **kwargs)
            
            # Прогреваем указанные ключи
            if global_cache.is_connected:
                for key in keys:
                    try:
                        # Здесь можно добавить логику прогрева конкретных ключей
                        logger.debug(f"Прогрев кэша для ключа: {key}")
                    except Exception as e:
                        logger.error(f"Ошибка при прогреве кэша для ключа {key}: {str(e)}")
            
            return result
        return wrapper
    return decorator