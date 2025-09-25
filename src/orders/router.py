import time
import uuid
from typing import Dict, Optional

from src.logger import app_logger as logger
from src.orders.orders import OrdersService
from src.auth.dependencies import get_current_user
from src.db import get_db_connection, AsyncGenerator
from src.orders.schema import OrderDetail, GroupedOrderInfo, OrdersWithSupplyNameIn, SupplyAccountWildOut, OrdersResponse, OperationResult, OperationHistoryResponse
from src.models.supply_operations import SupplyOperationsDB
from src.cache import global_cached

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
        wild: str = Query(None, description="Фильтрация по wild"),
        positive_stock: bool = Query(False, description="Фильтрация по остатку: True - положительные, False - нулевые и отрицательные")
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
        filtered_by_stock = orders_service.filter_orders_by_stock(grouped_orders, positive_stock)

        elapsed_time = time.time() - start_time
        logger.info(f"Заказы сгруппированы успешно. Всего: {len(filtered_orders)}. Время: {elapsed_time:.2f} сек.")
        return filtered_by_stock
    except Exception as e:
        logger.error(f"Ошибка при получении заказов: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Произошла ошибка при получении заказов",
        ) from e


@orders.post("/with-supply-name", response_model=SupplyAccountWildOut, status_code=status.HTTP_201_CREATED)
async def add_fact_orders_and_supply_name(
        payload: OrdersWithSupplyNameIn = Body(...),
        operation_id: Optional[str] = Query(None, description="Опциональный ID операции для восстановления результата"),
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user)
) -> SupplyAccountWildOut:
    """
    Создает поставки на основе фактического количества заказов для каждого wild.
    
    Args:
        payload: Данные о заказах и имя поставки. Если payload.is_hanging=True, 
                поставки будут помечены как "висячие".
        operation_id: Опциональный ID операции. Если не передан, будет сгенерирован автоматически.
        db: Соединение с базой данных
        user: Данные текущего пользователя
        
    Returns:
        SupplyAccountWildOut: Результаты создания поставок
        
    Note:
        Если соединение разорвется, можно восстановить результат через:
        GET /api/v1/orders/operations/{operation_id} или
        GET /api/v1/orders/operations/latest
    """
    start_time = time.time()
    
    # Генерируем ID операции если не передан
    if not operation_id:
        operation_id = f"supply_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    
    logger.info(f"Обработка операции {operation_id} от {user.get('username', 'unknown')}")
    logger.info(f"Поставки будут помечены как висячие: {payload.is_hanging}")

    try:
        # Сохраняем начало операции
        await SupplyOperationsDB.save_operation_start(operation_id, user['id'], payload.dict())
        
        # Выполняем основную логику
        orders_service = OrdersService(db)
        result = await orders_service.process_orders_with_fact_count(payload, user.get('username', 'unknown'))
        
        # Сохраняем успешный результат
        await SupplyOperationsDB.save_operation_success(operation_id, result.dict())
        
        elapsed_time = time.time() - start_time
        logger.info(f"Операция {operation_id} завершена успешно. Время: {elapsed_time:.2f} сек.")
        return result
        
    except Exception as e:
        # Сохраняем ошибку в БД
        await SupplyOperationsDB.save_operation_error(operation_id, str(e))
        
        logger.error(f"Ошибка в операции {operation_id}: {str(e)}")
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


@orders.get("/operations/{operation_id}", response_model=OperationResult, status_code=status.HTTP_200_OK)
async def get_operation_result(
    operation_id: str = Path(..., description="ID операции"),
    db: AsyncGenerator = Depends(get_db_connection),
    user: dict = Depends(get_current_user)
) -> OperationResult:
    """
    Получить результат операции по ID.
    
    Args:
        operation_id: Уникальный идентификатор операции
        db: Соединение с базой данных
        user: Данные текущего пользователя
        
    Returns:
        OperationResult: Результат операции со всеми деталями
        
    Raises:
        HTTPException: 404 если операция не найдена, 403 если нет доступа
    """
    try:
        operation = await SupplyOperationsDB.get_operation_by_id(operation_id)
        
        if not operation:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Операция {operation_id} не найдена"
            )
        
        if operation['user_id'] != user['id']:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Нет доступа к данной операции"
            )
        
        logger.info(f"Получен результат операции {operation_id} для пользователя {user.get('username')}")
        
        return OperationResult(
            operation_id=operation['operation_id'],
            status=operation['status'],
            result=SupplyAccountWildOut(**operation['response_data']) if operation['response_data'] else None,
            error=operation['error_message'],
            created_at=operation['created_at'],
            completed_at=operation['completed_at']
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при получении операции {operation_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Произошла ошибка при получении операции: {str(e)}"
        )


@orders.get("/operations/latest", response_model=OperationResult, status_code=status.HTTP_200_OK)
async def get_latest_operation(
    db: AsyncGenerator = Depends(get_db_connection),
    user: dict = Depends(get_current_user)
) -> OperationResult:
    """
    Получить последнюю операцию пользователя.
    
    Args:
        db: Соединение с базой данных
        user: Данные текущего пользователя
        
    Returns:
        OperationResult: Последняя операция пользователя
        
    Raises:
        HTTPException: 404 если операции не найдены
    """
    try:
        operation = await SupplyOperationsDB.get_latest_user_operation(user['id'])
        
        if not operation:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Операции не найдены"
            )
        
        logger.info(f"Получена последняя операция {operation['operation_id']} для пользователя {user.get('username')}")
        
        return OperationResult(
            operation_id=operation['operation_id'],
            status=operation['status'],
            result=SupplyAccountWildOut(**operation['response_data']) if operation['response_data'] else None,
            error=operation['error_message'],
            created_at=operation['created_at'],
            completed_at=operation['completed_at']
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при получении последней операции пользователя {user['id']}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Произошла ошибка при получении операции: {str(e)}"
        )


@orders.get("/operations/history", response_model=OperationHistoryResponse, status_code=status.HTTP_200_OK)
async def get_operations_history(
    limit: int = Query(10, ge=1, le=100, description="Количество операций для получения"),
    offset: int = Query(0, ge=0, description="Смещение для пагинации"),
    db: AsyncGenerator = Depends(get_db_connection),
    user: dict = Depends(get_current_user)
) -> OperationHistoryResponse:
    """
    Получить историю операций пользователя с пагинацией.
    
    Args:
        limit: Максимальное количество операций (1-100)
        offset: Смещение для пагинации
        db: Соединение с базой данных
        user: Данные текущего пользователя
        
    Returns:
        OperationHistoryResponse: Список операций с общим количеством
    """
    try:
        operations = await SupplyOperationsDB.get_user_operations_history(user['id'], limit, offset)
        
        history_items = [
            {
                "operation_id": op['operation_id'],
                "status": op['status'],
                "created_at": op['created_at'],
                "completed_at": op['completed_at'],
                "error": op['error_message']
            }
            for op in operations
        ]
        
        logger.info(f"Получена история из {len(history_items)} операций для пользователя {user.get('username')}")
        
        return OperationHistoryResponse(
            operations=history_items,
            total_count=len(history_items)  # В реальности здесь должен быть отдельный запрос для подсчета
        )
        
    except Exception as e:
        logger.error(f"Ошибка при получении истории операций пользователя {user['id']}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Произошла ошибка при получении истории: {str(e)}"
        )