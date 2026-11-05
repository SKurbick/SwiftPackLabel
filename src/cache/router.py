from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from enum import Enum
from src.auth.dependencies import get_current_user
from src.cache.global_cache import global_cache
from src.logger import app_logger as logger


class CacheType(str, Enum):
    """Типы кэша для обновления"""
    ALL = "all"
    SUPPLIES_WB_NORMAL = "supplies-wb-normal"
    SUPPLIES_WB_HANGING = "supplies-wb-hanging" 
    SUPPLIES_DELIVERY_NORMAL = "supplies-delivery-normal"
    SUPPLIES_DELIVERY_HANGING = "supplies-delivery-hanging"


cache = APIRouter(prefix='/cache', tags=['Cache'])


@cache.post("/refresh",
            status_code=status.HTTP_200_OK,
            summary="Обновление кэша",
            description="Запускает фоновое обновление. Состояние доступно в GET /cache/status")
async def refresh_cache(
        cache_type: CacheType = Query(CacheType.ALL, description="Тип кэша для обновления"),
        user: dict = Depends(get_current_user)
) -> dict:
    """Запускает обновление и сразу возвращает состояние задачи."""
    logger.info(f"Обновление кэша типа '{cache_type}' запущено пользователем {user.get('username', 'unknown')}")
    
    if not global_cache.is_connected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis кэш недоступен. Проверьте подключение к Redis серверу."
        )
    
    key = "all"
    if cache_type != CacheType.ALL:
        hanging = cache_type in (CacheType.SUPPLIES_WB_HANGING, CacheType.SUPPLIES_DELIVERY_HANGING)
        delivery = cache_type in (CacheType.SUPPLIES_DELIVERY_NORMAL, CacheType.SUPPLIES_DELIVERY_HANGING)
        key = f"cache:supplies_all:hanging_only:{hanging}|is_delivery:{delivery}"
    result = await global_cache.request_refresh(key)
    return {
        "success": True,
        "message": "Обновление запущено" if result["started"] else "Обновление уже выполняется",
        "cache_type": cache_type,
        "cache_key": key,
        **result,
    }


@cache.get("/status", summary="Состояние фонового обновления кеша")
async def cache_status(user: dict = Depends(get_current_user)) -> dict:
    """Возвращает время обновления снимков и состояние фоновых задач."""
    return await global_cache.get_cache_info()