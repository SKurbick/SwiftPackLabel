import functools
from typing import Any, Callable, Optional
import asyncio
import inspect

from .global_cache import global_cache
from src.logger import app_logger as logger

from fastapi import HTTPException

from src.cache_state import is_snapshot
from src.db import db as main_db


def global_cached(key: str, ttl: Optional[int] = None, cache_only: bool = False):
    """Возвращает данные из кеша, а снимки СЗ и поставок обновляет в фоне."""
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        signature = inspect.signature(func)

        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            bound = signature.bind(*args, **kwargs)
            bound.apply_defaults()
            params = dict(bound.arguments)
            for name, parameter in signature.parameters.items():
                if parameter.annotation is float and isinstance(params.get(name), (int, float)):
                    params[name] = float(params[name])
            cache_key = f"cache:{key}:{_serialize_params((), params)}"
            if not global_cache.is_connected:
                if cache_only:
                    raise HTTPException(
                        status_code=503,
                        detail="Кеш временно недоступен",
                        headers={"Retry-After": "5"},
                    )
                return await func(*args, **kwargs)

            cached = await global_cache.get(cache_key)
            if is_snapshot(cache_key):
                async def load():
                    loader_params = dict(bound.arguments)
                    if "db" in loader_params:
                        loader_params["db"] = main_db
                    return await func(**loader_params)

                try:
                    if cached is None or not await global_cache.is_fresh(cache_key):
                        await global_cache.request_refresh(
                            cache_key, loader=load if key == "orders_all" else None,
                        )
                except Exception:
                    if cached is None:
                        raise
                    logger.exception(
                        f"Не удалось запустить обновление {cache_key}; возвращаем прежний снимок"
                    )
                if cached is not None:
                    return cached
                raise HTTPException(
                    status_code=503,
                    detail="Первоначальная загрузка данных выполняется. Повторите запрос через несколько секунд",
                    headers={"Retry-After": "5"},
                )

            if cached is not None:
                return cached
            result = await func(*args, **kwargs)
            await global_cache.set(cache_key, result, ttl)
            return result

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