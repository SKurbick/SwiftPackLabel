from typing import List, Dict

from starlette.responses import StreamingResponse
from fastapi import APIRouter, Depends, Body, status

from src.logger import app_logger as logger

from src.auth.dependencies import get_current_user
from src.orders.schema import SupplyAccountWildOut
from src.supplies.schema import SupplyIdResponseSchema, SupplyIdBodySchema, WildFilterRequest, DeliverySupplyInfo
from src.supplies.supplies import SuppliesService
from src.db import get_db_connection, AsyncGenerator
from src.service.service_pdf import collect_images_sticker_to_pdf, create_table_pdf
from src.service.zip_service import create_zip_archive
from src.archives.archives import Archives

supply = APIRouter(prefix='/supplies', tags=['Supplies'])


@supply.get("/", response_model=SupplyIdResponseSchema, status_code=status.HTTP_200_OK)
async def get_supplies(user: dict = Depends(get_current_user)) -> SupplyIdResponseSchema:
    """
    Получить список всех поставок.
    Returns:
        SupplyIdResponseSchema: Список поставок с их деталями
    """
    return await SuppliesService().get_list_supplies()


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
    # Получаем данные стикеров
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
             status_code=status.HTTP_200_OK,
             summary="Перевод поставок в статус доставки",
             description="Переводит указанные поставки в статус доставки")
async def deliver_supplies(
        supply_ids: List[DeliverySupplyInfo] = Body(..., description="Список поставок для перевода в статус доставки"),
        order_wild_map: Dict[str, str] = Body(..., description="Соответствие заказов и артикулов wild"),
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user)
) -> None:
    """
    Заглушка для перевода поставок в статус доставки.
    Только принимает запрос и возвращает статус 200 без тела ответа.
    
    Args:
        supply_ids: Список поставок для перевода в статус доставки
        order_wild_map: Соответствие заказов и артикулов wild
        db: Соединение с базой данных
        user: Данные текущего пользователя
    """
    logger.info(f"Запрос на перевод поставок в статус доставки от {user.get('username', 'unknown')}")
    logger.info(f"Получен запрос на доставку для {len(supply_ids)} поставок и {len(order_wild_map)} заказов")    
