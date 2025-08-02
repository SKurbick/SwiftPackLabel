from typing import List, Dict

from starlette.responses import StreamingResponse, JSONResponse
from fastapi import APIRouter, Depends, Body, status, HTTPException

from src.logger import app_logger as logger
from src.auth.dependencies import get_current_user
from src.supplies.schema import SupplyIdResponseSchema, SupplyIdBodySchema, WildFilterRequest, DeliverySupplyInfo, SupplyIdWithShippedBodySchema, MoveOrdersRequest, MoveOrdersResponse
from src.supplies.supplies import SuppliesService
from src.db import get_db_connection, AsyncGenerator
from src.service.service_pdf import collect_images_sticker_to_pdf, create_table_pdf
from src.service.zip_service import create_zip_archive
from src.archives.archives import Archives
from src.supplies.integration_1c import OneCIntegration
from src.cache import global_cached

supply = APIRouter(prefix='/supplies', tags=['Supplies'])


@supply.get("/", response_model=SupplyIdResponseSchema, status_code=status.HTTP_200_OK)
@global_cached(key="supplies_all", cache_only=True)
async def get_supplies(
    hanging_only: bool = False,
    db: AsyncGenerator = Depends(get_db_connection),
    user: dict = Depends(get_current_user)
) -> SupplyIdResponseSchema:
    """
    Получить список поставок с фильтрацией по висячим.
    
    Примечание: Cache middleware проверяет глобальный кэш ПЕРЕД авторизацией.
    Эта функция вызывается только если нет кэша или нужно обновление.
    
    Args:
        hanging_only: Если True - вернуть только висячие поставки, если False - только обычные (не висячие)
        db: Соединение с базой данных
        user: Текущий пользователь
    Returns:
        SupplyIdResponseSchema: Список поставок с их деталями
    """
    logger.info("get_supplies function called (no cache available)")
    return await SuppliesService(db).get_list_supplies(hanging_only=hanging_only)


@supply.post("/upload_stickers",
             status_code=status.HTTP_201_CREATED,
             response_description="ZIP-архив с файлами стикеров и листом подбора",
             responses={201: {"content": {"application/zip": {}},
                              "description": "ZIP-архив, содержащий два PDF файла: стикеры и лист подбора"},
                        422: {"description": "Ошибка валидации входных данных"}})
async def upload_stickers_to_orders(
        supply_ids: SupplyIdBodySchema = Body(),
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user)
) -> StreamingResponse:
    """
    Генерирует и возвращает ZIP-архив, содержащий PDF со стикерами и лист подбора
    для указанных поставок.
    Args:
        supply_ids: Информация о поставках для которых нужно создать стикеры
        db: Соединение с базой данных
    Returns:
        StreamingResponse: ZIP-архив, содержащий два PDF файла:
            - stickers.pdf: PDF с стикерами для печати
            - selection_sheet.pdf: PDF с листом подбора
    """
    supplies_service = SuppliesService(db)
    result_stickers = await supplies_service.filter_and_fetch_stickers(supply_ids)
    selection_sheet_content = await create_table_pdf(result_stickers)
    pdf_sticker = await collect_images_sticker_to_pdf(result_stickers)
    zip_buffer = create_zip_archive({
        "stickers.pdf": pdf_sticker.getvalue(),
        "selection_sheet.pdf": selection_sheet_content.getvalue()
    })
    await Archives(db).save_archive(zip_buffer, account_name=user.get('username'))
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={'Content-Disposition': 'attachment; filename=stickers_package.zip'}
    )


@supply.post("/stickers_by_wild",
             status_code=status.HTTP_201_CREATED,
             response_description="PDF-файл со стикерами для конкретного wild",
             responses={201: {"content": {"application/pdf": {}},
                              "description": "PDF-файл с стикерами для печати"},
                        422: {"description": "Ошибка валидации входных данных"}})
async def generate_stickers_by_wild(
        wild_filter: WildFilterRequest = Body(...),
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user)
) -> StreamingResponse:
    """
    Генерирует и возвращает PDF-файл со стикерами для конкретного wild и указанных заказов в поставках.
    Args:
        wild_filter: Информация о wild, поставках и заказах для которых нужно создать стикеры
        db: Соединение с базой данных
        user: Данные текущего пользователя
    Returns:
        StreamingResponse: PDF-файл со стикерами для печати
    """
    supplies_service = SuppliesService(db)
    result_stickers = await supplies_service.filter_and_fetch_stickers_by_wild(wild_filter)
    pdf_sticker = await collect_images_sticker_to_pdf(result_stickers)

    return StreamingResponse(
        pdf_sticker,
        media_type="application/pdf",
        headers={'Content-Disposition': f'attachment; filename=stickers_{wild_filter.wild}.pdf'}
    )


@supply.post("/delivery",
             status_code=status.HTTP_201_CREATED,
             summary="Перевод поставок в статус доставки",
             description="Переводит указанные поставки в статус доставки")
async def deliver_supplies(
        supply_ids: List[DeliverySupplyInfo] = Body(..., description="Список поставок для перевода в статус доставки"),
        order_wild_map: Dict[str, str] = Body(..., description="Соответствие заказов и артикулов wild"),
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user)
) -> JSONResponse:
    """
    Переводит указанные поставки в статус доставки и формирует структурированные данные для 1C.
    Args:
        supply_ids: Список поставок для перевода в статус доставки
        order_wild_map: Соответствие заказов и артикулов wild
        db: Соединение с базой данных
        user: Данные текущего пользователя
    Returns:
        Словарь с ключом "accounts", содержащий список данных по аккаунтам, wild-артикулам и поставкам
    """
    logger.info(f"Запрос на перевод поставок в статус доставки от {user.get('username', 'unknown')}")
    logger.info(f"Получен запрос на доставку для {len(supply_ids)} поставок и {len(order_wild_map)} заказов")
    try:
        supply_service = SuppliesService(db)
        await supply_service.process_delivery_supplies(supply_ids)
        integration = OneCIntegration()
        integration_result = await integration.format_delivery_data(supply_ids, order_wild_map)
        shipment_result = await supply_service.save_shipments(supply_ids, order_wild_map,
                                                              user.get('username', "Не найден"))

        integration_success = isinstance(integration_result, dict) and integration_result.get("status_code") == 200

        response_content = {
            "success": integration_success and shipment_result,
            "message": "Все операции выполнены успешно" if (integration_success and shipment_result)
                      else "Возникли ошибки при обработке",
            "integration_result": integration_result,
            "shipment_result": shipment_result
        }

        return JSONResponse(
            content=response_content,
            status_code=status.HTTP_201_CREATED
        )

    except Exception as e:
        logger.error(f"Ошибка при обработке доставки поставок: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка при обработке доставки поставок: {str(e)}"
        )


@supply.post("/delivery-hanging",
             status_code=status.HTTP_201_CREATED,
             summary="Перевод висячих поставок в статус доставки",
             description="Переводит указанные висячие поставки в статус доставки")
async def deliver_supplies_hanging(
        supply_ids: List[DeliverySupplyInfo] = Body(..., description="Список поставок для перевода в статус доставки"),
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user)
):
    """
    Переводит указанные висячие поставки в статус доставки.
    Args:
        supply_ids: Список поставок для перевода в статус доставки
        db: Соединение с базой данных
        user: Данные текущего пользователя
    Returns:
        dict: Словарь с информацией об успешности выполнения
    """
    logger.info(f"Запрос на перевод висячих поставок в статус доставки от {user.get('username', 'unknown')}")
    logger.info(f"Получен запрос на доставку для {len(supply_ids)} поставок : {supply_ids}")
    try:
        supply_service = SuppliesService(db)
        await supply_service.process_delivery_supplies(supply_ids)
    except Exception as e:
        logger.error(f"Ошибка при обработке доставки висячих поставок: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка при обработке доставки поставок: {str(e)}",
        ) from e


@supply.post("/shipment-hanging-actual",
             status_code=status.HTTP_201_CREATED,
             summary="Отгрузка фактического количества висячих поставок",
             description="Отгружает фактическое количество товаров из висячих поставок")
async def shipment_hanging_actual_quantity(
        supply_data: SupplyIdWithShippedBodySchema = Body(..., description="Данные висячих поставок с фактическим количеством для отгрузки"),
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user)
) -> JSONResponse:
    """
    Отгружает фактическое количество товаров из висячих поставок.
    Args:
        supply_data: Данные висячих поставок с фактическим количеством для отгрузки
        db: Соединение с базой данных
        user: Данные текущего пользователя
    Returns:
        JSONResponse: Результат отгрузки фактического количества
    """
    logger.info(f"Запрос на отгрузку фактического количества висячих поставок от {user.get('username', 'unknown')}")
    logger.info(f"Получен запрос для {len(supply_data.supplies)} висячих поставок с фактическим количеством={supply_data.shipped_count}")
    
    try:
        supply_service = SuppliesService(db)
        result = await supply_service.shipment_hanging_actual_quantity_implementation(supply_data, user)
        
        return JSONResponse(
            content=result,
            status_code=status.HTTP_201_CREATED)
        
    except Exception as e:
        logger.error(f"Ошибка при отгрузке фактического количества висячих поставок: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка при отгрузке фактического количества висячих поставок: {str(e)}"
        )


@supply.post("/move-orders",
             status_code=status.HTTP_200_OK,
             response_model=MoveOrdersResponse,
             summary="Перемещение заказов между поставками",
             description="Перемещает сборочные задания из одной поставки в другую")
async def move_orders_between_supplies(
        request_data: MoveOrdersRequest = Body(..., description="Данные о заказах для перемещения"),
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user)
) -> MoveOrdersResponse:
    """
    Перемещает сборочные задания между поставками.
    
    Args:
        request_data: Данные о заказах сгруппированные по wild-кодам
        db: Соединение с базой данных
        user: Данные текущего пользователя
        
    Returns:
        MoveOrdersResponse: Результат операции перемещения
    """
    logger.info(f"Запрос на перемещение заказов от {user.get('username', 'unknown')}")
    total_remove_count = sum(wild_item.remove_count for wild_item in request_data.orders.values())
    logger.info(f"Получен запрос на перемещение {total_remove_count} заказов из {len(request_data.orders)} wild-кодов")
    
    try:
        supply_service = SuppliesService(db)
        result = await supply_service.move_orders_between_supplies_implementation(request_data, user)
        
        return MoveOrdersResponse(
            success=result.get("success", True),
            message=result.get("message", "Операция перемещения заказов выполнена"),
            removed_order_ids=result.get("removed_order_ids", []),
            processed_supplies=result.get("processed_supplies", 0),
            processed_wilds=result.get("processed_wilds", 0)
        )
        
    except Exception as e:
        logger.error(f"Ошибка при перемещении заказов: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка при перемещении заказов: {str(e)}"
        )
