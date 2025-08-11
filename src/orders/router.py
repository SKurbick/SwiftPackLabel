import time

from src.logger import app_logger as logger
from src.orders.orders import OrdersService
from src.auth.dependencies import get_current_user
from src.db import get_db_connection, AsyncGenerator
from src.orders.schema import OrderDetail, GroupedOrderInfo, OrdersWithSupplyNameIn, SupplyAccountWildOut, OrdersResponse
from src.cache import global_cached
from typing import Dict

from fastapi import APIRouter, Depends, status, Request, HTTPException, Query, Body, Path
from starlette.responses import StreamingResponse

orders = APIRouter(prefix='/orders', tags=['Orders'])


@orders.get("/", response_model=Dict[str, GroupedOrderInfo], status_code=status.HTTP_200_OK)
# @global_cached(key="orders_all", cache_only=True)
async def get_orders(
        request: Request,
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user),
        time_delta: float = Query(1, description="Фильтрация по времени создания заказа (в часах)"),
        wild: str = Query(None, description="Фильтрация по wild")
) -> Dict[str, GroupedOrderInfo]:
    """
    Получить сгруппированные по wild заказы с расширенной информацией:
    
    Примечание: Cache middleware проверяет глобальный кэш ПЕРЕД авторизацией.
    Эта функция вызывается только если нет кэша или нужно обновление.
    
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
        payload: Данные о заказах и имя поставки. Если payload.is_hanging=True, 
                поставки будут помечены как "висячие".
        db: Соединение с базой данных
        user: Данные текущего пользователя
    Returns:
        SupplyAccountWildOut: Результаты создания поставок
    """
    start_time = time.time()
    logger.info(f"Запрос на создание поставок от {user.get('username', 'unknown')}")
    logger.info(f"Поставки будут помечены как висячие: {payload.is_hanging}")

    try:
        orders_service = OrdersService(db)
        result = await orders_service.process_orders_with_fact_count(payload, user.get('username', 'unknown'))
        elapsed_time = time.time() - start_time
        logger.info(f"Поставки созданы успешно. Время: {elapsed_time:.2f} сек.")
        return result
    except Exception as e:
        logger.error(f"Ошибка при создании поставок: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Произошла ошибка при создании поставок: {str(e)}",
        )


@orders.get("/sticker/{order_id}",
           status_code=status.HTTP_200_OK,
           response_description="PNG стикер для сборочного задания",
           responses={200: {"content": {"image/png": {}},
                           "description": "PNG файл стикера"},
                     404: {"description": "Сборочное задание не найдено"},
                     422: {"description": "Ошибка валидации параметров"}})
async def get_order_sticker(
        order_id: int = Path(..., description="Номер сборочного задания"),
        account: str = Query(..., description="Наименование аккаунта"),
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user)
) -> StreamingResponse:
    """
    Получить PNG стикер для конкретного сборочного задания.
    
    Args:
        order_id: Номер сборочного задания
        account: Наименование аккаунта
        db: Соединение с базой данных
        user: Данные текущего пользователя
        
    Returns:
        StreamingResponse: PNG файл стикера
    """
    try:
        orders_service = OrdersService(db)
        png_buffer = await orders_service.get_single_order_sticker(order_id, account)
        
        # Safe filename
        safe_account = "".join(c for c in account if c.isalnum() or c in "._-")
        filename = f'sticker_{order_id}.png'
        
        return StreamingResponse(
            png_buffer,
            media_type="image/png",
            headers={
                'Content-Disposition': f'attachment; filename={filename}'
            }
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )