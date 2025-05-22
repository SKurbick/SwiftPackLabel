from fastapi import APIRouter, Body, Depends, HTTPException, status

from src.auth.dependencies import get_current_user
from src.db import get_db_connection, AsyncGenerator
from src.qr_parser.schema import WildParserRequest, WildParserResponse
from src.qr_parser.service import WildParserService
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
