from typing import List, Dict

from starlette.responses import StreamingResponse, JSONResponse
from fastapi import APIRouter, Depends, Body, status, HTTPException, Path, Query

from src.logger import app_logger as logger
from src.auth.dependencies import get_current_user
from src.supplies.schema import SupplyIdResponseSchema, SupplyIdBodySchema, WildFilterRequest, DeliverySupplyInfo, \
    SupplyIdWithShippedBodySchema, MoveOrdersRequest, MoveOrdersResponse, SupplyBarcodeListRequest, \
    FictitiousDeliveryRequest, FictitiousDeliveryResponse, FictitiousShipmentRequest
from src.supplies.supplies import SuppliesService
from src.db import get_db_connection, AsyncGenerator
from src.service.service_pdf import collect_images_sticker_to_pdf, create_table_pdf
from src.service.zip_service import create_zip_archive
from src.archives.archives import Archives
from src.supplies.integration_1c import OneCIntegration
from src.cache import global_cached
from src.supplies.empty_supply_cleaner import EmptySupplyCleaner
from src.cache.global_cache import global_cache

supply = APIRouter(prefix='/supplies', tags=['Supplies'])


@supply.get("/", response_model=SupplyIdResponseSchema, status_code=status.HTTP_200_OK)
@global_cached(key="supplies_all", cache_only=True)
async def get_supplies(
        hanging_only: bool = False,
        is_delivery: bool = False,
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user)
) -> SupplyIdResponseSchema:
    """
    Получить список поставок с фильтрацией по висячим и доставке.
    
    Примечание: Cache middleware проверяет глобальный кэш ПЕРЕД авторизацией.
    Эта функция вызывается только если нет кэша или нужно обновление.
    
    Args:
        hanging_only: Если True - вернуть только висячие поставки, если False - только обычные (не висячие)
        is_delivery: Если True - получать поставки из отгрузок за неделю (таблица shipment_of_goods), 
                    если False - получать из WB API
        db: Соединение с базой данных
        user: Текущий пользователь
    Returns:
        SupplyIdResponseSchema: Список поставок с их деталями
    """
    logger.info("get_supplies function called (no cache available)")
    return await SuppliesService(db).get_list_supplies(hanging_only=hanging_only, is_delivery=is_delivery)


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


@supply.post("/delivery-fictitious",
             status_code=status.HTTP_201_CREATED,
             response_model=FictitiousDeliveryResponse,
             summary="Перевод фиктивной поставки в статус доставки",
             description="Переводит фиктивную висячую поставку в статус доставки с отметкой в БД")
async def deliver_fictitious_supply(
        request: FictitiousDeliveryRequest = Body(..., description="Данные фиктивной поставки"),
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user)
) -> FictitiousDeliveryResponse:
    """
    Переводит фиктивные висячие поставки в статус доставки.
    
    Функция выполняет следующие операции для каждой поставки:
    1. Проверяет существование поставки в таблице hanging_supplies
    2. Вызывает существующий метод deliver_supply для перевода в доставку
    3. Помечает поставку как фиктивно доставленную в БД
    
    Args:
        request: Данные запроса с объектом supplies {supply_id: account}
        db: Соединение с базой данных
        user: Данные текущего пользователя
        
    Returns:
        FictitiousDeliveryResponse: Результат операции с подробной информацией
        
    Raises:
        HTTPException: В случае ошибки обработки запроса
    """
    logger.info(f"Запрос на перевод фиктивных поставок {list(request.supplies.keys())} "
                f"в статус доставки от {user.get('username', 'unknown')}")

    try:
        supply_service = SuppliesService(db)
        operator = user.get('username', 'unknown')

        # Обработка поставок (одной или нескольких)
        logger.info(f"Обработка {len(request.supplies)} фиктивных поставок")
        result = await supply_service.deliver_fictitious_supplies_batch(
            supplies=request.supplies,
            operator=operator
        )

        return FictitiousDeliveryResponse(
            success=result["success"],
            message=result["message"],
            total_processed=result["total_processed"],
            successful_count=result["successful_count"],
            failed_count=result["failed_count"],
            results=result["results"],
            processing_time_seconds=result["processing_time_seconds"],
            operator=result["operator"],
        )
    except Exception as e:
        error_detail = f"Произошла ошибка при обработке фиктивных поставок: {str(e)}"
        logger.error(f"Ошибка при обработке фиктивных поставок {list(request.supplies.keys())}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_detail
        ) from e


@supply.post("/shipment_of_fictions",
             status_code=status.HTTP_201_CREATED,
             summary="Фиктивная отгрузка поставок с указанным количеством",
             description="Выполняет фиктивную отгрузку указанного количества заказов из поставок",
             response_description="PDF файл с QR-стикерами отгруженных заказов",
             responses={201: {"content": {"application/pdf": {}},
                              "description": "PDF файл с QR-стикерами для фиктивно отгруженных заказов"}})
async def shipment_of_fictions_supply(
        request: FictitiousShipmentRequest = Body(..., description="Данные для фиктивной отгрузки"),
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user)
) -> StreamingResponse:
    """
    Выполняет фиктивную отгрузку заказов из поставок.
    
    Алгоритм:
    1. Получает актуальные данные о заказах через get_information_to_supply_details
    2. Фильтрует уже фиктивно отгруженные заказы из БД (поле fictitious_shipped_order_ids)
    3. Сортирует по времени создания (старые сначала)
    4. Выбирает указанное количество заказов
    5. Имитирует отгрузку (заглушка)
    6. Сохраняет отгруженные order_id в fictitious_shipped_order_ids
    
    Args:
        request: Данные запроса с поставками и количеством для отгрузки
        db: Соединение с базой данных
        user: Данные текущего пользователя
        
    Returns:
        FictitiousDeliveryResponse: Результат операции с подробной информацией
        
    Raises:
        HTTPException: В случае ошибки обработки запроса или недостатка заказов
    """
    logger.info(f"Запрос на фиктивную отгрузку {request.shipped_quantity} заказов "
                f"из {len(request.supplies)} поставок от {user.get('username', 'unknown')}")
    
    try:
        supply_service = SuppliesService(db)
        operator = user.get('username', 'unknown')
        
        result = await supply_service.shipment_fictitious_supplies_with_quantity(
            supplies=request.supplies,
            shipped_quantity=request.shipped_quantity,
            operator=operator
        )
        
        # Всегда возвращаем PDF стикеры
        if result.get("stickers_pdf"):
            filename = f"fictitious_shipment_stickers_{request.shipped_quantity}_{operator}.pdf"
            return StreamingResponse(
                result["stickers_pdf"],
                media_type="application/pdf", 
                headers={'Content-Disposition': f'attachment; filename={filename}'}
            )
        else:
            # Если стикеры не удалось сгенерировать, возвращаем ошибку
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Не удалось сгенерировать QR-стикеры для отгруженных заказов"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        error_detail = f"Произошла ошибка при фиктивной отгрузке: {str(e)}"
        logger.error(f"Ошибка фиктивной отгрузки: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_detail
        ) from e


@supply.post("/shipment-hanging-actual",
             status_code=status.HTTP_201_CREATED,
             summary="Отгрузка фактического количества висячих поставок",
             description="Отгружает фактическое количество товаров из висячих поставок")
async def shipment_hanging_actual_quantity(
        supply_data: SupplyIdWithShippedBodySchema = Body(...,
                                                          description="Данные висячих поставок с фактическим количеством для отгрузки"),
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
    logger.info(
        f"Получен запрос для {len(supply_data.supplies)} висячих поставок с фактическим количеством={supply_data.shipped_count}")

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


@supply.post("/clean-empty-supplies",
             status_code=status.HTTP_200_OK,
             summary="Очистка пустых поставок",
             description="Находит и удаляет поставки без заказов после двойной проверки")
async def clean_empty_supplies(
        user: dict = Depends(get_current_user)
) -> JSONResponse:
    """
    Обработка пустых поставок:
    1. Находит текущие пустые поставки
    2. Сравнивает с ранее сохраненными
    3. Удаляет поставки пустые два раза подряд
    4. Обновляет список отслеживаемых
    """
    logger.info(f"Запуск очистки пустых поставок от {user.get('username')}")

    try:

        cleaner = EmptySupplyCleaner(global_cache.redis)
        result = await cleaner.process_empty_supplies()

        return JSONResponse(content=result, status_code=status.HTTP_200_OK)

    except Exception as e:
        logger.error(f"Ошибка очистки пустых поставок: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка очистки: {str(e)}"
        )


@supply.post("/barcode",
             status_code=status.HTTP_200_OK,
             response_description="PNG файл с объединенными стикерами поставок",
             responses={200: {"content": {"image/png": {}},
                              "description": "PNG файл с объединенными стикерами поставок"},
                        404: {"description": "Поставки не найдены"},
                        422: {"description": "Ошибка валидации параметров"}})
async def get_supply_stickers(
        request: SupplyBarcodeListRequest = Body(...),
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user)
) -> StreamingResponse:
    """
    Получить PNG файл с объединенными стикерами для списка поставок.

    Args:
        request: Словарь поставок с привязкой к аккаунтам {supply_id: account_name}
        db: Соединение с базой данных
        user: Данные текущего пользователя

    Returns:
        StreamingResponse: PNG файл с объединенными стикерами
    """
    try:
        supply_service = SuppliesService()
        png_buffer = await supply_service.get_multiple_supply_stickers(request.supplies)
        
        filename = f'stickers_{len(request.supplies)}_supplies.png'

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