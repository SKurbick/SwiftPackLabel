from asyncpg import InterfaceError, PostgresError
from fastapi import APIRouter, Depends, HTTPException, status

from src.assembly_tasks.schema import AssemblyTasksLookupRequest, AssemblyTasksLookupResponse
from src.auth.dependencies import get_current_user
from src.db import DatabaseManager, get_db_connection
from src.diagnostics import safe_error
from src.logger import app_logger as logger
from src.models.assembly_tasks import AssemblyTasks


assembly_tasks = APIRouter(prefix="/assembly-tasks", tags=["Assembly Tasks"])


@assembly_tasks.post(
    "/by-rids",
    response_model=AssemblyTasksLookupResponse,
    dependencies=[Depends(get_current_user)],
)
async def lookup_assembly_tasks(
    payload: AssemblyTasksLookupRequest,
    db: DatabaseManager = Depends(get_db_connection),
) -> AssemblyTasksLookupResponse:
    """ищет номера сз"""
    try:
        items = await AssemblyTasks(db).find_by_rids(payload.rids)
    except (PostgresError, InterfaceError, TimeoutError, OSError) as exc:
        logger.error(f"не удалось найти сз по rid: {safe_error(exc)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="не удалось получить сз",
        ) from exc
    return AssemblyTasksLookupResponse(items=items)
