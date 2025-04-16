from fastapi import APIRouter, Depends, status, Request, HTTPException
import time

from src.auth.dependencies import get_current_user
from src.db import get_db_connection, AsyncGenerator
from src.orders.orders import OrdersService
from src.orders.schema import OrdersResponse, OrderDetail
from src.logger import app_logger as logger

orders = APIRouter(prefix='/orders', tags=['Orders'])


@orders.get("/", response_model=OrdersResponse, status_code=status.HTTP_200_OK)
async def get_orders(
        request: Request,
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user)
) -> OrdersResponse:
    """
    Получить отформатированные данные по всем заказам в едином списке,
    отсортированные по дате создания.
    Args:
        request: Объект запроса FastAPI
        db: Соединение с базой данных
        user: Данные текущего пользователя
    Returns:
        OrdersResponse: Отформатированный ответ с заказами
    """
    start_time = time.time()
    logger.info(f"Запрос на получение заказов от {user.get("username", "unknown")}")

    try:
        orders_service = OrdersService(db)
        all_orders = await orders_service.get_all_orders()

        formatted_orders = []
        for orders_list in all_orders.values():
            formatted_orders.extend(orders_list)

        formatted_orders = sorted(
            formatted_orders,
            key=lambda x: x["created_at"],
        )
        response = OrdersResponse(orders=[OrderDetail(**order) for order in formatted_orders])
        elapsed_time = time.time() - start_time
        logger.info(f"Заказы получены успешно. Всего: {len(formatted_orders)}. Время: {elapsed_time:.2f} сек.")

        return response
    except Exception as e:
        logger.error(f"Ошибка при получении заказов: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Произошла ошибка при получении заказов",
        ) from e
