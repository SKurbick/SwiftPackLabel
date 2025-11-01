"""
Маршрутизатор API для работы с отмененными заказами в поставках Wildberries.
"""
from fastapi import APIRouter, Depends, HTTPException, status

from src.logger import app_logger as logger
from src.auth.dependencies import get_current_user
from src.canceled_orders.service import CanceledOrdersService
from src.canceled_orders.schema import (
    SupplyCanceledCheckRequest,
    SupplyCanceledCheckResponse,
    BulkSupplyCanceledCheckRequest,
    BulkSupplyCanceledCheckResponse
)
from src.db import get_db_connection, AsyncGenerator

canceled_orders = APIRouter(prefix='/canceled-orders', tags=['Canceled Orders'])


@canceled_orders.post(
    "/check-supply",
    response_model=SupplyCanceledCheckResponse,
    status_code=status.HTTP_200_OK,
    summary="Проверка наличия отмененных заказов в поставке",
    description="Проверяет наличие заказов со статусом 'canceled_by_client' в указанной поставке"
)
async def check_supply_canceled(
    request: SupplyCanceledCheckRequest,
    db: AsyncGenerator = Depends(get_db_connection),
    user: dict = Depends(get_current_user)
) -> SupplyCanceledCheckResponse:
    """
    Проверяет наличие отмененных заказов в поставке.

    Для каждого заказа берется последний статус (по created_at_db DESC).
    """
    username = user.get('username', 'unknown')
    logger.info(f"Пользователь {username} запросил проверку отмененных заказов для поставки {request.supply_id}")

    service = CanceledOrdersService(db)

    try:
        result = await service.check_supply_has_canceled(request.supply_id)

        return SupplyCanceledCheckResponse(
            supply_id=request.supply_id,
            has_canceled=result["has_canceled"],
            canceled_order_ids=result["canceled_order_ids"]
        )

    except Exception as e:
        logger.error(f"Ошибка при проверке поставки {request.supply_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка при проверке поставки: {str(e)}"
        )


@canceled_orders.post(
    "/check-supplies",
    response_model=BulkSupplyCanceledCheckResponse,
    status_code=status.HTTP_200_OK,
    summary="Массовая проверка наличия отмененных заказов в поставках",
    description="Проверяет наличие заказов со статусом 'canceled_by_client' в списке поставок"
)
async def check_supplies_canceled(
    request: BulkSupplyCanceledCheckRequest,
    db: AsyncGenerator = Depends(get_db_connection),
    user: dict = Depends(get_current_user)
) -> BulkSupplyCanceledCheckResponse:
    """
    Проверяет наличие отмененных заказов в списке поставок.

    Оптимизирован для массовой проверки - делает один запрос к БД.
    Для каждого заказа берется последний статус (по created_at_db DESC).
    """
    username = user.get('username', 'unknown')
    logger.info(
        f"Пользователь {username} запросил массовую проверку отмененных заказов "
        f"для {len(request.supply_ids)} поставок"
    )

    service = CanceledOrdersService(db)

    try:
        results = await service.check_supplies_has_canceled(request.supply_ids)

        response_results = [
            SupplyCanceledCheckResponse(
                supply_id=result["supply_id"],
                has_canceled=result["has_canceled"],
                canceled_order_ids=result["canceled_order_ids"]
            )
            for result in results
        ]

        return BulkSupplyCanceledCheckResponse(results=response_results)

    except Exception as e:
        logger.error(f"Ошибка при массовой проверке поставок: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка при массовой проверке поставок: {str(e)}"
        )
