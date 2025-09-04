from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from typing import Optional

from src.auth.dependencies import get_current_user
from src.excel_data.schema import (
    WildModelResponse, MessageResponse, WildModelListResponse,
    WildModelRecord, WildModelCreate, WildModelUpdate
)
from src.excel_data.service import ExcelDataService
from src.logger import app_logger as logger

excel_data = APIRouter(prefix='/excel-data', tags=['Excel Data'])


@excel_data.post("/upload", 
                response_model=MessageResponse, 
                status_code=status.HTTP_201_CREATED)
async def upload_excel_file(
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user)
) -> MessageResponse:
    """
    Загружает Excel-файл с данными формата wild-модель.
    Файл должен содержать только два столбца: 'wild' и 'модель'.
    Все значения должны быть строками, и файл должен содержать только один лист.
    Args:
        file: Загружаемый Excel-файл (.xlsx)
        user: Текущий авторизованный пользователь
    Returns:
        MessageResponse: Сообщение о успешной загрузке
    Raises:
        HTTPException: Если файл не соответствует требованиям
    """
    if not file.filename.endswith('.xlsx'):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Загрузка разрешена только для файлов .xlsx"
        )
    file_content = await file.read()
    excel_service = ExcelDataService()
    excel_service.upload_excel(file_content)
    
    logger.info(f"Пользователь {user.get('username')} загрузил новый Excel-файл")
    return MessageResponse(message="Файл успешно загружен и данные обновлены")


@excel_data.get("/download", 
               response_description="Excel-файл с текущими данными")
async def download_excel_file(
    user: dict = Depends(get_current_user)
) -> StreamingResponse:
    """
    Скачивает текущие данные в формате Excel-файла.
    Args:
        user: Текущий авторизованный пользователь
    Returns:
        StreamingResponse: Excel-файл с данными
    """
    excel_service = ExcelDataService()
    excel_buffer = excel_service.download_excel()
    
    logger.info(f"Пользователь {user.get('username')} скачал текущий Excel-файл")
    return StreamingResponse(
        excel_buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=wild_model_data.xlsx"}
    )


# Новые CRUD эндпоинты
@excel_data.get("/records", response_model=WildModelListResponse)
async def get_all_records(
    user: dict = Depends(get_current_user)
) -> WildModelListResponse:
    """
    Возвращает все записи из файла с индексами.
    
    Returns:
        WildModelListResponse: Список всех записей с индексами
    """
    excel_service = ExcelDataService()
    return excel_service.get_all_records()


@excel_data.put("/records/{index}", response_model=WildModelRecord)
async def update_record(
    index: int,
    record: WildModelUpdate,
    user: dict = Depends(get_current_user)
) -> WildModelRecord:
    """
    Обновляет запись по номеру строки (индексу).
    
    Args:
        index: Номер строки для обновления (начиная с 0)
        record: Новые данные (wild и model)
        user: Текущий пользователь
        
    Returns:
        WildModelRecord: Обновленная запись
    """
    excel_service = ExcelDataService()
    return excel_service.update_record(index, record)


@excel_data.post("/records", response_model=WildModelRecord, status_code=status.HTTP_201_CREATED)
async def create_record(
    record: WildModelCreate,
    user: dict = Depends(get_current_user)
) -> WildModelRecord:
    """
    Добавляет новую запись в файл.
    
    Args:
        record: Данные для создания (wild и model)
        user: Текущий пользователь
        
    Returns:
        WildModelRecord: Созданная запись с индексом
    """
    excel_service = ExcelDataService()
    return excel_service.create_record(record)


@excel_data.delete("/records/{index}", response_model=MessageResponse)
async def delete_record(
    index: int,
    user: dict = Depends(get_current_user)
) -> MessageResponse:
    """
    Удаляет запись по номеру строки (индексу).
    
    Args:
        index: Номер строки для удаления (начиная с 0)
        user: Текущий пользователь
        
    Returns:
        MessageResponse: Сообщение об успешном удалении
    """
    excel_service = ExcelDataService()
    
    if not excel_service.delete_record(index):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Запись с индексом {index} не найдена"
        )
    
    return MessageResponse(message=f"Запись с индексом {index} успешно удалена")


