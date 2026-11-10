from secrets import compare_digest

from fastapi import Depends, Security
from fastapi.security import APIKeyHeader

from src.auth.dependencies import get_current_user, oauth2_scheme
from src.auth.service import AuthService
from src.settings import settings


assembly_tasks_api_key = APIKeyHeader(
    name="X-API-Key",
    scheme_name="AssemblyTasksApiKey",
    description="секретный ключ для поиска сз по rid",
    auto_error=False,
)


async def authorize_assembly_tasks(
    api_key: str | None = Security(assembly_tasks_api_key),
    token: str | None = Depends(oauth2_scheme),
    auth_service: AuthService = Depends(),
) -> None:
    """разрешает поиск по секрет кею"""
    expected_key = settings.ASSEMBLY_TASKS_API_KEY.get_secret_value()
    if expected_key and api_key and compare_digest(api_key.encode("utf-8"), expected_key.encode("utf-8")):
        return
    await get_current_user(token=token, auth_service=auth_service)
