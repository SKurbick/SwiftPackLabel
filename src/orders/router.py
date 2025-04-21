from fastapi import APIRouter, Depends, status, Request, HTTPException, Query
import time
from datetime import datetime, timezone, timedelta

from src.auth.dependencies import get_current_user
from src.db import get_db_connection, AsyncGenerator
from src.orders.orders import OrdersService
from src.orders.schema import OrdersResponse, OrderDetail
from src.logger import app_logger as logger
from src.utils import process_local_vendor_code

orders = APIRouter(prefix='/orders', tags=['Orders'])


@orders.get("/", response_model=OrdersResponse, status_code=status.HTTP_200_OK)
async def get_orders(
        request: Request,
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user),
        time_delta: float = Query(None, description="Фильтрация по времени создания заказа (в часах)"),
        wild: str = Query(None, description="Фильтрация по wild")
) -> OrdersResponse:
    """
    Получить отформатированные данные по всем заказам в едином списке,
    отсортированные по дате создания, с возможностью фильтрации по времени и артикулу.
    Args:
        request: Объект запроса FastAPI
        db: Соединение с базой данных
        user: Данные текущего пользователя
        time_delta: Количество часов для фильтрации по времени создания
        wild: Артикул для фильтрации (обрабатывается через process_local_vendor_code)
    Returns:
        OrdersResponse: Отформатированный ответ с заказами
    """
    start_time = time.time()
    logger.info(f"Запрос на получение заказов от {user.get('username', 'unknown')}")
    try:
        orders_service = OrdersService(db)
        filtered_orders = await orders_service.get_filtered_orders(time_delta=time_delta, article=wild)
        response = OrdersResponse(orders=[OrderDetail(**order) for order in filtered_orders])
        elapsed_time = time.time() - start_time
        logger.info(f"Заказы получены успешно. Всего: {len(filtered_orders)}. Время: {elapsed_time:.2f} сек.")
        return response
    except Exception as e:
        logger.error(f"Ошибка при получении заказов: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Произошла ошибка при получении заказов",
        ) from e
