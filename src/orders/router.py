import asyncio
import time

from src.logger import app_logger as logger
from src.orders.orders import OrdersService
from src.auth.dependencies import get_current_user
from src.db import get_db_connection, AsyncGenerator
from src.orders.schema import OrderDetail, GroupedOrderInfo, OrdersWithSupplyNameIn, SupplyAccountWildOut, WildInfo, SupplyInfo
from typing import Dict, Any, Coroutine

from fastapi import APIRouter, Depends, status, Request, HTTPException, Query, Body

orders = APIRouter(prefix='/orders', tags=['Orders'])


@orders.get("/", response_model=Dict[str, GroupedOrderInfo], status_code=status.HTTP_200_OK)
async def get_orders(
        request: Request,
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user),
        time_delta: float = Query(1, description="Фильтрация по времени создания заказа (в часах)"),
        wild: str = Query(None, description="Фильтрация по wild")
) -> Dict[str, GroupedOrderInfo]:
    """
    Получить сгруппированные по wild заказы с расширенной информацией:
    Args:
        request: Объект запроса FastAPI
        db: Соединение с базой данных
        user: Данные текущего пользователя
        time_delta: Количество часов для фильтрации по времени создания
        wild: Артикул для фильтрации (обрабатывается через process_local_vendor_code)
    Returns:
        Dict[str, GroupedOrderInfo]: Словарь с данными о заказах по артикулам
    """
    start_time = time.time()
    logger.info(f"Запрос на получение сгруппированных заказов от {user.get('username', 'unknown')}")
    try:
        orders_service = OrdersService(db)
        filtered_orders = await orders_service.get_filtered_orders(time_delta=time_delta, article=wild)
        order_details = [OrderDetail(**order) for order in filtered_orders]
        grouped_orders = await orders_service.group_orders_by_wild(order_details)

        elapsed_time = time.time() - start_time
        logger.info(f"Заказы сгруппированы успешно. Всего: {len(filtered_orders)}. Время: {elapsed_time:.2f} сек.")
        return grouped_orders
    except Exception as e:
        logger.error(f"Ошибка при получении заказов: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Произошла ошибка при получении заказов",
        ) from e


@orders.post("/with-supply-name", response_model=SupplyAccountWildOut, status_code=status.HTTP_201_CREATED)
async def add_fact_orders_and_supply_name(
        payload: OrdersWithSupplyNameIn = Body(...),
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user)
) -> SupplyAccountWildOut:
    """
    Создает поставки на основе фактического количества заказов для каждого wild.
    Args:
        payload: Данные о заказах и имя поставки
        db: Соединение с базой данных
        user: Данные текущего пользователя
    Returns:
        SupplyAccountWildOut: Результаты создания поставок
    """
    start_time = time.time()
    logger.info(f"Запрос на создание поставок от {user.get('username', 'unknown')}")

    try:
        orders_service = OrdersService(db)
        result = await orders_service.process_orders_with_fact_count(payload)
        elapsed_time = time.time() - start_time
        logger.info(f"Поставки созданы успешно. Время: {elapsed_time:.2f} сек.")
        return result
    except Exception as e:
        logger.error(f"Ошибка при создании поставок: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Произошла ошибка при создании поставок: {str(e)}",
        )

@orders.post("/with-supply-name-mock", response_model=SupplyAccountWildOut, status_code=status.HTTP_200_OK)
async def add_fact_orders_and_supply_name_mock(
        payload: OrdersWithSupplyNameIn = Body(...),
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user)
) -> SupplyAccountWildOut:
    """
    MOCK: Возвращает фиктивные данные для SupplyAccountWildOut без вызова бизнес-логики.
    """
    await asyncio.timeout(10)
    return SupplyAccountWildOut(
        wilds=[
            WildInfo(wild="123456", count=2),
            WildInfo(wild="654321", count=1)
        ],
        supply_ids=[
            SupplyInfo(supply_id="WB-GI-1234567", account="test_account_1", order_ids=[111, 112]),
            SupplyInfo(supply_id="WB-GI-7654321", account="test_account_2", order_ids=[113])
        ],
        order_wild_map={
            111: "123456",
            112: "123456",
            113: "654321"
        }
    )
