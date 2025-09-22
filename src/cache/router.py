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
            description="Обновляет кэш системы - весь или конкретный тип")
async def refresh_cache(
        cache_type: CacheType = Query(CacheType.ALL, description="Тип кэша для обновления"),
        user: dict = Depends(get_current_user)
) -> dict:
    """
    Обновление кэша системы.
    
    Args:
        cache_type: Тип кэша для обновления:
            - all: весь кэш
            - supplies-wb-normal: обычные поставки из WB (hanging_only=False, is_delivery=False)
            - supplies-wb-hanging: висячие поставки из WB (hanging_only=True, is_delivery=False)
            - supplies-delivery-normal: обычные поставки на доставке (hanging_only=False, is_delivery=True)
            - supplies-delivery-hanging: висячие поставки на доставке (hanging_only=True, is_delivery=True)
        user: Данные текущего пользователя (требуется авторизация)
        
    Returns:
        dict: Результат обновления кэша
    """
    logger.info(f"Обновление кэша типа '{cache_type}' запущено пользователем {user.get('username', 'unknown')}")
    
    if not global_cache.is_connected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis кэш недоступен. Проверьте подключение к Redis серверу."
        )
    
    try:
        # Определяем какой кэш обновлять
        if cache_type == CacheType.ALL:
            success = await global_cache.force_refresh_cache()
            cache_key = "all"
            message = "Весь кэш обновлен" if success else "Ошибка обновления всего кэша"
            
        elif cache_type == CacheType.SUPPLIES_WB_NORMAL:
            success = await global_cache.refresh_specific_cache("supplies", hanging_only=False, is_delivery=False)
            cache_key = "cache:supplies_all:hanging_only:False|is_delivery:False"
            message = "Кэш WB обычных поставок обновлен" if success else "Ошибка обновления WB обычных поставок"
            
        elif cache_type == CacheType.SUPPLIES_WB_HANGING:
            success = await global_cache.refresh_specific_cache("supplies", hanging_only=True, is_delivery=False)
            cache_key = "cache:supplies_all:hanging_only:True|is_delivery:False"
            message = "Кэш WB висячих поставок обновлен" if success else "Ошибка обновления WB висячих поставок"
            
        elif cache_type == CacheType.SUPPLIES_DELIVERY_NORMAL:
            success = await global_cache.refresh_specific_cache("supplies", hanging_only=False, is_delivery=True)
            cache_key = "cache:supplies_all:hanging_only:False|is_delivery:True"
            message = "Кэш delivery обычных поставок обновлен" if success else "Ошибка обновления delivery обычных поставок"
            
        elif cache_type == CacheType.SUPPLIES_DELIVERY_HANGING:
            success = await global_cache.refresh_specific_cache("supplies", hanging_only=True, is_delivery=True)
            cache_key = "cache:supplies_all:hanging_only:True|is_delivery:True"
            message = "Кэш delivery висячих поставок обновлен" if success else "Ошибка обновления delivery висячих поставок"
            
        else:
            raise HTTPException(status_code=400, detail=f"Неизвестный тип кэша: {cache_type}")
        
        if success:
            logger.info(f"Обновление кэша '{cache_type}' завершено успешно пользователем {user.get('username', 'unknown')}")
        else:
            logger.error(f"Обновление кэша '{cache_type}' завершилось с ошибкой")
            
        return {
            "success": success,
            "message": message,
            "cache_type": cache_type,
            "cache_key": cache_key
        }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Критическая ошибка при обновлении кэша '{cache_type}': {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Внутренняя ошибка при обновлении кэша: {str(e)}"
        )