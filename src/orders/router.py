import time
import uuid
from typing import Dict
from datetime import datetime

from src.logger import app_logger as logger
from src.orders.orders import OrdersService
from src.orders.order_status_service import OrderStatusService
from src.auth.dependencies import get_current_user
from src.db import get_db_connection, AsyncGenerator
from src.orders.schema import OrderDetail, GroupedOrderInfo, GroupedOrderInfoWithFact, OrdersWithSupplyNameIn, SupplyAccountWildOut, OrdersResponse
from src.cache import global_cached
from src.models.supply_operations import SupplyOperationsDB

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

        # 1. Получаем отфильтрованные заказы
        filtered_orders = await orders_service.get_filtered_orders(time_delta=time_delta, article=wild)

        # 2. Логируем новые заказы в order_status_log
        status_service = OrderStatusService(db)
        logged_count = await status_service.process_and_log_new_orders(filtered_orders)
        logger.info(f"Залогировано {logged_count} новых заказов в order_status_log")

        # 3. Продолжаем обычную обработку
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
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user)
) -> list[SupplyAccountWildOut]:
    """
    Создает поставки на основе фактического количества заказов для каждого wild.
    
    Args:
        payload: Данные о заказах и имя поставки. Если payload.is_hanging=True, 
                поставки будут помечены как "висячие".
        db: Соединение с базой данных
        user: Данные текущего пользователя
        
    Returns:
        SupplyAccountWildOut: Результаты создания поставок
        
    Note:
        Если соединение разорвется, можно восстановить результат через:
        GET /api/v1/orders/operations/{operation_id} или
        GET /api/v1/orders/operations/latest


    UPDATE 17.03.26!!!
    payload делится на 2 части:
    - ЮР: заказы, где OrderDetail.is_b2b is True
    - ФИЗ: заказы, где OrderDetail.is_b2b is False

    Для каждой части создается отдельная операция и отдельный operation_id.
    """
    start_time = time.time()

    base_operation_id = f"supply_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    
    logger.info(f"Обработка операции {base_operation_id} от {user.get('username', 'unknown')}")
    logger.info(f"Поставки будут помечены как висячие: {payload.is_hanging}")

    def _split_payload_by_b2b(
            source_payload: OrdersWithSupplyNameIn,
    ) -> tuple[OrdersWithSupplyNameIn, OrdersWithSupplyNameIn]:
        """
        Делит исходный payload на две части:
        - b2b_payload: только заказы с is_b2b is True
        - fiz_payload: заказы с is_b2b is False
        """
        b2b_orders: dict[str, GroupedOrderInfoWithFact] = {}
        fiz_orders: dict[str, GroupedOrderInfoWithFact] = {}

        base_name = source_payload.name_supply

        for wild_key, grouped_info in source_payload.orders.items():
            b2b_only_orders = [order for order in grouped_info.orders if order.is_b2b is True]
            fiz_only_orders = [order for order in grouped_info.orders if order.is_b2b is not True]

            if b2b_only_orders:
                b2b_orders[wild_key] = grouped_info.model_copy(update={"orders": b2b_only_orders})

            if fiz_only_orders:
                fiz_orders[wild_key] = grouped_info.model_copy(update={"orders": fiz_only_orders})

        b2b_payload = (
            OrdersWithSupplyNameIn(
                orders=b2b_orders,
                name_supply=f"{base_name}_ЮР",
                is_hanging=source_payload.is_hanging,
            )
            if b2b_orders
            else None
        )

        fiz_payload = (
            OrdersWithSupplyNameIn(
                orders=fiz_orders,
                name_supply=f"{base_name}_ФИЗ",
                is_hanging=source_payload.is_hanging,
            )
            if fiz_orders
            else None
        )

        return b2b_payload, fiz_payload

    async def _process_one_payload(
        branch_payload: OrdersWithSupplyNameIn,
        branch_suffix: str,
    ) -> SupplyAccountWildOut:
        branch_operation_id = f"{base_operation_id}_{branch_suffix}"

        try:
            if not branch_payload.is_hanging:
                await SupplyOperationsDB.save_operation_start(
                    branch_operation_id,
                    user["id"],
                    branch_payload.model_dump(),
                    supply_name=branch_payload.name_supply,
                    supply_date=datetime.now().isoformat(),
                )
                logger.info(
                    f"Операция {branch_operation_id} начата и сохранена в supply_operations "
                    f"(технический круг)"
                )
            else:
                logger.info(
                    f"Операция {branch_operation_id} начата "
                    f"(висячий круг, не сохраняем в supply_operations)"
                )

            orders_service = OrdersService(db)
            result = await orders_service.process_orders_with_fact_count(
                branch_payload,
                user.get('username', 'unknown'),
            )

            status_service = OrderStatusService(db)
            logged_count = await status_service.process_and_log_orders_in_supplies(
                result,
                branch_payload.is_hanging,
            )

            logger.info(
                f"Залогировано {logged_count} заказов со статусом "
                f"{'IN_HANGING_SUPPLY' if branch_payload.is_hanging else 'IN_TECHNICAL_SUPPLY'}"
            )

            result.operation_id = branch_operation_id

            if not branch_payload.is_hanging:
                await SupplyOperationsDB.save_operation_success(
                    branch_operation_id,
                    result.model_dump(),
                )
                logger.info(
                    f"Операция {branch_operation_id} завершена успешно и сохранена в supply_operations "
                    f"(технический круг)"
                )
            else:
                logger.info(
                    f"Операция {branch_operation_id} завершена успешно "
                    f"(висячий круг, не сохраняем в supply_operations)"
                )

            return result

        except Exception as e:
            if not branch_payload.is_hanging:
                await SupplyOperationsDB.save_operation_error(branch_operation_id, str(e))
                logger.error(
                    f"Ошибка в операции {branch_operation_id} (технический круг) "
                    f"сохранена в supply_operations: {str(e)}"
                )
            else:
                logger.error(f"Ошибка в операции {branch_operation_id} (висячий круг): {str(e)}")

            raise

    try:
        b2b_payload, fiz_payload = _split_payload_by_b2b(payload)

        results: list[SupplyAccountWildOut] = []

        if b2b_payload is not None:
            results.append(await _process_one_payload(b2b_payload, "b2b"))

        if fiz_payload is not None:
            results.append(await _process_one_payload(fiz_payload, "fiz"))

        if not results:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="В payload нет заказов для обработки",
            )

        elapsed_time = time.time() - start_time
        logger.info(
            f"Операция {base_operation_id} завершена успешно. "
            f"Создано результатов: {len(results)}. Время: {elapsed_time:.2f} сек."
        )

        return results

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка в операции {base_operation_id}: {str(e)}")
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

@orders.get("/sessions", status_code=status.HTTP_200_OK)
async def get_sessions_list(
        limit: int = Query(50, ge=1, le=200, description="Количество сессий для получения"),
        offset: int = Query(0, ge=0, description="Смещение для пагинации"),
        user: dict = Depends(get_current_user)
):
    """
    Получить общий список всех сессий с базовой информацией.
    
    Args:
        limit: Максимальное количество сессий для возврата (1-200)
        offset: Количество сессий для пропуска
        user: Данные текущего пользователя
        
    Returns:
        List: Список сессий с базовой информацией (operation_id, supply_name, created_at, status)
    """
    try:
        sessions = await SupplyOperationsDB.get_sessions_list(limit=limit, offset=offset)
        
        return {
            'sessions': sessions,
            'total_count': len(sessions),
            'limit': limit,
            'offset': offset
        }
        
    except Exception as e:
        logger.error(f"Ошибка при получении списка сессий: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Произошла ошибка при получении списка сессий: {str(e)}"
        )


@orders.get("/sessions/{operation_id}", status_code=status.HTTP_200_OK)
async def get_session_full_info(
        operation_id: str = Path(..., description="ID сессии для получения полной информации"),
        user: dict = Depends(get_current_user)
):
    """
    Получить полную информацию о сессии по ID.
    
    Args:
        operation_id: Уникальный идентификатор сессии
        user: Данные текущего пользователя
        
    Returns:
        Dict: Полная информация о сессии включая request_payload и response_data
        
    Raises:
        404: Сессия не найдена
    """
    try:
        session = await SupplyOperationsDB.get_session_full_info(operation_id)
        
        if not session:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Сессия {operation_id} не найдена"
            )
        
        return session
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при получении информации о сессии {operation_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Произошла ошибка при получении информации о сессии: {str(e)}"
        )