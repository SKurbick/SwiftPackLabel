"""
Маршрутизатор API для работы с карточками товаров Wildberries.
"""
from fastapi import APIRouter, Depends, Body, HTTPException, status

from src.logger import app_logger as logger
from src.auth.dependencies import get_current_user
from src.cards.cards import CardsService
from src.cards.schema import DimensionsUpdateRequest
from src.db import get_db_connection, AsyncGenerator


cards = APIRouter(prefix='/cards', tags=['Cards'])


@cards.post(
    "/update-dimensions",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Обновление размеров и веса товара",
    description="Обновляет размеры и вес товара по артикулу продавца (wild)"
)
async def update_dimensions(
    dimensions: DimensionsUpdateRequest = Body(..., description="Данные о размерах и весе товара"),
    db: AsyncGenerator = Depends(get_db_connection),
    user: dict = Depends(get_current_user)
) -> None:
    """
    Обновляет размеры и вес товара по артикулу продавца (wild).
    
    Args:
        dimensions: Данные о размерах и весе товара
        db: Соединение с базой данных
        user: Данные текущего пользователя
        
    Returns:
        None: Возвращает 204 No Content при успешном обновлении
    """
    logger.info(f"Запрос на обновление размеров и веса товара от {user.get('username', 'unknown')}")
    logger.info(f"Артикул: {dimensions.wild}, Размеры: {dimensions.width}x{dimensions.length}x{dimensions.height} см, Вес: {dimensions.weight} г")
    
    # Обновляем размеры и вес через сервис, передавая соединение с БД
    result = await CardsService.update_dimensions(
        wild=dimensions.wild,
        width=dimensions.width,
        length=dimensions.length,
        height=dimensions.height,
        weight=dimensions.weight,
        db=db
    )
    
    # Логируем найденные vendor_codes из базы данных
    if "vendor_codes" in result and result["vendor_codes"]:
        logger.info(f"Найденные vendor_codes в базе данных: {', '.join(result['vendor_codes'])}")
    
    if not result["success"]:
        # Определяем код ошибки в зависимости от содержания сообщения
        if "Не найдено карточек" in result["error"] and not result.get("vendor_codes"):
            status_code = status.HTTP_404_NOT_FOUND
        elif "Не найдены токены" in result["error"]:
            status_code = status.HTTP_400_BAD_REQUEST
        else:
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            
        raise HTTPException(
            status_code=status_code,
            detail=result["error"]
        )
    
    # Если все прошло успешно, возвращаем 204 No Content
    return None