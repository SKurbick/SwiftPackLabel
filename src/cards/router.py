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
    description="Обновляет размеры и вес товара по артикулу продавца (wild). Обновляются только переданные параметры."
)
async def update_dimensions(
    dimensions: DimensionsUpdateRequest = Body(..., description="Данные о размерах и весе товара"),
    db: AsyncGenerator = Depends(get_db_connection),
    user: dict = Depends(get_current_user)
) -> None:
    """
    Обновляет размеры и вес товара по артикулу продавца (wild).
    """
    username = user.get('username', 'unknown')
    logger.info(f"Пользователь {username} запросил обновление размеров для артикула {dimensions.wild}")

    # Создаем сервис
    cards_service = CardsService(db)
    
    # Обновляем размеры
    result = await cards_service.update_dimensions(
        wild=dimensions.wild,
        width=dimensions.width,
        length=dimensions.length,
        height=dimensions.height,
        weight=dimensions.weight
    )
    
    # Проверяем результат
    if not result["success"]:
        if "Не найдено карточек" in result["error"]:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=result["error"]
            )
        elif "Не указаны параметры" in result["error"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result["error"]
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result["error"]
            )
    
    logger.info(f"Успешно обновлено {result.get('updated_count', 0)} карточек для артикула {dimensions.wild}")
