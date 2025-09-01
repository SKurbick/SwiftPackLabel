"""
API роутер для парсинга PDF листов подбора
"""
from typing import Dict, Any

from fastapi import APIRouter, File, UploadFile, HTTPException, status, Depends, Body
from fastapi.responses import JSONResponse

from src.auth.dependencies import get_current_user
from src.pdf_parser.service import PDFProcessingService
from src.db import get_db_connection

pdf_parser_router = APIRouter(prefix='/pdf-parser', tags=['PDF Parser'])



@pdf_parser_router.post(
    "/parse-and-ship",
    status_code=status.HTTP_201_CREATED,
    summary="Парсинг PDF и отправка в фиктивную отгрузку",
    description="Загружает PDF лист подбора, парсит его и сразу отправляет данные в систему фиктивной отгрузки"
)
async def parse_pdf_and_ship(
    file: UploadFile = File(..., description="PDF файл листа подбора"),
    account: str = Body(..., description="Аккаунт Wildberries для поставки"),
    db=Depends(get_db_connection),
    user: dict = Depends(get_current_user)
) -> JSONResponse:
    """
    Парсит PDF лист подбора и сразу отправляет данные в фиктивную отгрузку.
    """
    # Проверяем тип файла
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Поддерживаются только PDF файлы"
        )
    
    # Читаем содержимое файла
    content = await file.read()
    
    # Используем сервис для обработки
    service = PDFProcessingService(db)
    response_data = await service.parse_and_ship(content, file.filename, account, user)
    
    return JSONResponse(
        content=response_data,
        status_code=status.HTTP_201_CREATED if response_data["success"] else status.HTTP_206_PARTIAL_CONTENT
    )