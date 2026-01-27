from fastapi import APIRouter, Depends, Response, status, HTTPException

from src.auth import UserPermissions, get_info_from_token
from src.db import get_db_connection, AsyncGenerator
from src.wild_logs.schema import WildLogCreate, ShiftSupervisorData
from src.wild_logs.service import WildLogService
from src.logger import app_logger as logger

wild_logs = APIRouter(prefix='/wild-logs', tags=['Wild Logs'])


@wild_logs.post(
    "/", 
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Записать лог операции с wild"
)
async def create_wild_log(
    log_data: WildLogCreate,
    db: AsyncGenerator = Depends(get_db_connection),
    user: UserPermissions = Depends(get_info_from_token)
) -> Response:
    """
    Записывает информацию об операции с wild-кодом в базу данных.
    Не возвращает никаких данных в случае успеха.
    Args:
        log_data: Данные для записи в лог
        db: Соединение с базой данных
        user: Данные текущего пользователя
    Returns:
        Response: Пустой ответ со статусом 204 No Content
    """
    if not user.viewing:
        raise HTTPException(status_code=status.HTTP_423_LOCKED, detail="permission locked")
    service = WildLogService(db)
    await service.create_log(log_data)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@wild_logs.post(
    "/supervisor",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Обновить информацию о старшем операторе"
)
async def update_supervisor_info(
    supervisor_data: ShiftSupervisorData,
    db: AsyncGenerator = Depends(get_db_connection),
    user: UserPermissions = Depends(get_info_from_token)
) -> Response:
    """
    Обновляет информацию о старшем операторе для записей с указанным session_id.
    Не возвращает никаких данных в случае успеха.
    Args:
        supervisor_data: Данные для обновления информации о старшем операторе
        db: Соединение с базой данных
        user: Данные текущего пользователя
    Returns:
        Response: Пустой ответ со статусом 204 No Content
    """
    if not user.viewing:
        raise HTTPException(status_code=status.HTTP_423_LOCKED, detail="permission locked")
    service = WildLogService(db)
    await service.update_supervisor_info(supervisor_data)
    return Response(status_code=status.HTTP_204_NO_CONTENT)