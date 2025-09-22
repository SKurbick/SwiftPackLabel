from fastapi import APIRouter, Body, Depends, HTTPException, Query, status

from src.auth.dependencies import get_current_user
from src.db import get_db_connection, AsyncGenerator
from src.qr_parser.schema import WildParserRequest, WildParserResponse, QRLookupRequest, QRLookupResponse
from src.qr_parser.service import WildParserService, QRLookupService
from src.logger import app_logger as logger

qr_parser = APIRouter(prefix='/qr-parser', tags=['QR Parser'])


@qr_parser.post(
    "/parse",
    response_model=WildParserResponse,
    status_code=status.HTTP_200_OK,
    summary="Parse wild string",
    description="Parses a string in the format 'wild123/23' to extract product information"
)
async def parse_wild_string(
        request: WildParserRequest = Body(...),
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user),
) -> WildParserResponse:
    """
    Парсит строку формата 'wild123/23' и возвращает информацию о товаре.
    Args:
        request: Запрос со строкой для парсинга
        user: Данные текущего пользователя
    Returns:
        WildParserResponse: Информация о товаре, извлеченная из строки
    """
    try:
        logger.info(
            f"Запрос на парсинг строки wild от пользователя {user.get('username', 'unknown')}: {request.wild_string}")
        parser_service = WildParserService(db=db)
        result = await parser_service.parse_wild_string(request.wild_string)
        logger.info(f"Успешно разобрана строка wild: {request.wild_string}")
        return result
    except ValueError as e:
        logger.error(f"Ошибка валидации при парсинге строки wild: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Внутренняя ошибка при парсинге строки wild: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Произошла внутренняя ошибка: {str(e)}"
        )


@qr_parser.get(
    "/lookup",
    response_model=QRLookupResponse,
    status_code=status.HTTP_200_OK,
    summary="Поиск данных по QR-коду",
    description="Ищет данные в таблице qr_scans по QR-коду и возвращает связанную информацию о заказе"
)
async def lookup_by_qr_code(
        qr_data: str = Query(..., description="QR код стикера, например '*CN+tGIpw'"),
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user),
) -> QRLookupResponse:
    """
    Ищет данные по QR-коду стикера и возвращает информацию о заказе.
    
    Args:
        qr_data: QR код для поиска
        db: Соединение с базой данных
        user: Данные текущего пользователя
        
    Returns:
        QRLookupResponse: Найденные данные QR-скана и связанного заказа
    """
    try:
        logger.info(
            f"Запрос на поиск по QR-коду от пользователя {user.get('username', 'unknown')}: {qr_data}")
        
        lookup_service = QRLookupService(db=db)
        result = await lookup_service.find_by_qr_data(qr_data)
        
        if result.found:
            logger.info(f"Успешно найдены данные по QR-коду: {qr_data}")
        else:
            logger.info(f"Данные по QR-коду не найдены: {qr_data}")
            
        return result
        
    except Exception as e:
        logger.error(f"Внутренняя ошибка при поиске по QR-коду: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Произошла внутренняя ошибка: {str(e)}"
        )
