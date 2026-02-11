"""
API роутер для парсинга PDF листов подбора
"""

from fastapi import APIRouter, File, UploadFile, HTTPException, status, Depends, Body
from fastapi.responses import JSONResponse

from src.auth import UserPermissions, get_info_from_token
from src.pdf_parser.service import DocumentProcessingService
from src.db import get_db_connection

document_parser_router = APIRouter(prefix='/document-parser', tags=['Document Parser'])



@document_parser_router.post(
    "/parse-and-ship",
    status_code=status.HTTP_201_CREATED,
    summary="Парсинг документа и отправка в фиктивную отгрузку",
    description="Загружает PDF или Excel лист подбора, парсит его и сразу отправляет данные в систему фиктивной отгрузки"
)
async def parse_document_and_ship(
    file: UploadFile = File(..., description="PDF или Excel файл листа подбора"),
    account: str = Body(..., description="Аккаунт Wildberries для поставки"),
    db=Depends(get_db_connection),
    user: UserPermissions = Depends(get_info_from_token)
) -> JSONResponse:
    """
    Парсит PDF или Excel лист подбора и сразу отправляет данные в фиктивную отгрузку.
    """
    if not user.creating_a_delivery:
        raise HTTPException(status_code=status.HTTP_423_LOCKED, detail="permission locked")
    # Проверяем тип файла
    filename_lower = file.filename.lower()
    if not (filename_lower.endswith('.pdf') or filename_lower.endswith(('.xlsx', '.xls'))):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Поддерживаются только PDF и Excel файлы (.pdf, .xlsx, .xls)"
        )
    
    # Читаем содержимое файла
    content = await file.read()
    
    # Используем сервис для обработки
    service = DocumentProcessingService(db)
    response_data = await service.parse_and_ship(content, file.filename, account, user)
    
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_201_CREATED if response_data["success"] else status.HTTP_206_PARTIAL_CONTENT
    )