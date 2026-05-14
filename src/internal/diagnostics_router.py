import asyncio
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, status

try:
    from redis.exceptions import AuthenticationError
except Exception:
    class AuthenticationError(Exception):
        pass

from src.diagnostics import diagnostics, safe_error, utc_now_iso
from src.settings import settings

internal_diagnostics = APIRouter(prefix="/internal", tags=["Internal Diagnostics"])

INTERNAL_ACCESS_DESCRIPTION = (
    "Внутренний доступ к диагностике. Если задан `INTERNAL_DIAGNOSTICS_TOKEN`, "
    "передайте его в заголовке `X-Internal-Token`. Если токен не задан, доступ "
    "разрешен только запросам, которые FastAPI видит как localhost."
)

LIVE_DESCRIPTION = (
    "Проверяет, что FastAPI-процесс жив и может отвечать на HTTP-запросы. "
    "Endpoint не выполняет проверку PostgreSQL, Redis, Celery, RabbitMQ или Wildberries."
)

READY_DESCRIPTION = (
    "Легкая проверка готовности приложения. Выполняет быстрый `SELECT 1` в PostgreSQL "
    "и легкий Redis ping, если внешние проверки не отключены через "
    "`DIAGNOSTICS_NO_EXTERNAL_CHECKS=true`. Не запускает прогрев кэша и не делает WB-запросы."
)

STATUS_DESCRIPTION = (
    "Общий диагностический снимок процесса: память, file descriptors, количество asyncio tasks, "
    "event loop lag, состояние PostgreSQL pool, Redis, Celery, текущий/последний WB refresh "
    "и счетчики in-memory ring buffers. Используется как первая точка входа при инциденте."
)

DB_DESCRIPTION = (
    "Показывает состояние asyncpg pool и последние события по PostgreSQL: долгое ожидание "
    "`pool.acquire()`, долго удерживаемые соединения и медленные SQL-запросы. SQL args, "
    "пароли и токены не возвращаются."
)

WB_REFRESH_DESCRIPTION = (
    "Показывает, активен ли сейчас refresh кэша Wildberries, его `refresh_id`, источник запуска, "
    "длительность, счетчики запросов/retry/404 и последние связанные события. Endpoint только читает "
    "in-memory состояние и сам refresh не запускает."
)

RECENT_EVENTS_DESCRIPTION = (
    "Возвращает последние диагностические события из in-memory ring buffer текущего процесса. "
    "Можно ограничить количество событий и отфильтровать по точному имени события."
)

PROCESS_DESCRIPTION = (
    "Возвращает легкие метрики текущего процесса: pid, RSS memory, open file descriptors, "
    "количество asyncio tasks, threads и uptime."
)

REDIS_DESCRIPTION = (
    "Проверяет Redis легким ping и читает безопасные поля INFO: connected_clients, blocked_clients, "
    "used_memory_human, rejected_connections и instantaneous_ops_per_sec. Не возвращает пароль или URL подключения."
)

CELERY_DESCRIPTION = (
    "Минимальная диагностика Celery через Redis: длина основной очереди `celery`, если Redis доступен. "
    "Если проверить безопасно нельзя, возвращает `skipped` или `unavailable`."
)


def _get_global_cache():
    try:
        from src.cache import global_cache

        return global_cache, None
    except Exception as exc:
        return None, safe_error(exc)


async def check_internal_access(
    request: Request,
    x_internal_token: Optional[str] = Header(
        default=None,
        alias="X-Internal-Token",
        description=(
            "Внутренний токен диагностики. Обязателен, если в окружении задан "
            "`INTERNAL_DIAGNOSTICS_TOKEN`. Не является JWT и не связан с пользовательской авторизацией."
        ),
    ),
) -> None:
    if settings.INTERNAL_DIAGNOSTICS_TOKEN:
        if x_internal_token != settings.INTERNAL_DIAGNOSTICS_TOKEN:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Доступ к internal diagnostics запрещен")
        return

    client_host = request.client.host if request.client else ""
    if client_host not in {"127.0.0.1", "::1", "localhost"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Доступ к internal diagnostics разрешен только с localhost")


async def _db_status(timeout: float = 1.5) -> str:
    if settings.DIAGNOSTICS_NO_EXTERNAL_CHECKS:
        return "skipped"
    try:
        from src.db import db

        await asyncio.wait_for(db.fetchrow("SELECT 1"), timeout=timeout)
        return "ok"
    except Exception:
        return "fail"


async def _redis_snapshot(timeout: float = 1.5) -> dict:
    if settings.DIAGNOSTICS_NO_EXTERNAL_CHECKS:
        return {"status": "skipped", "message": "Проверка Redis пропущена: включен DIAGNOSTICS_NO_EXTERNAL_CHECKS"}
    try:
        global_cache, import_error = _get_global_cache()
        if not global_cache:
            return {
                "status": "not_configured",
                "message": "Redis-клиент не загружен в диагностическом процессе",
                "reason": import_error,
            }
        if not global_cache.redis_client:
            return {"status": "not_configured", "message": "Redis-клиент не настроен в процессе приложения"}
        pong = await asyncio.wait_for(global_cache.redis_client.ping(), timeout=timeout)
        info = await asyncio.wait_for(global_cache.redis_client.info(), timeout=timeout)
        return {
            "status": "ok" if pong else "fail",
            "message": "Redis доступен" if pong else "Redis не ответил на ping",
            "connected_clients": info.get("connected_clients"),
            "blocked_clients": info.get("blocked_clients"),
            "used_memory_human": info.get("used_memory_human"),
            "rejected_connections": info.get("rejected_connections"),
            "instantaneous_ops_per_sec": info.get("instantaneous_ops_per_sec"),
        }
    except AuthenticationError:
        return {"status": "auth_required", "message": "Redis требует авторизацию"}
    except Exception as exc:
        return {"status": "unavailable", "message": "Redis недоступен или не ответил за отведенное время", "error": safe_error(exc)}


async def _celery_snapshot(timeout: float = 1.5) -> dict:
    if settings.DIAGNOSTICS_NO_EXTERNAL_CHECKS:
        return {"status": "skipped", "message": "Проверка Celery пропущена: включен DIAGNOSTICS_NO_EXTERNAL_CHECKS"}
    try:
        global_cache, import_error = _get_global_cache()
        if not global_cache:
            return {
                "status": "skipped",
                "message": "Проверка Celery пропущена: Redis-клиент не загружен в диагностическом процессе",
                "reason": import_error,
            }
        if not global_cache.redis_client:
            return {"status": "skipped", "message": "Проверка Celery пропущена: Redis-клиент недоступен", "reason": "redis_unavailable"}
        length = await asyncio.wait_for(global_cache.redis_client.llen("celery"), timeout=timeout)
        return {"status": "ok", "message": "Очередь Celery прочитана из Redis", "queues": {"celery": length}}
    except AuthenticationError:
        return {"status": "auth_required", "message": "Redis для проверки Celery требует авторизацию"}
    except Exception as exc:
        return {"status": "unavailable", "message": "Не удалось получить состояние Celery через Redis", "error": safe_error(exc)}


@internal_diagnostics.get(
    "/health/live",
    dependencies=[Depends(check_internal_access)],
    summary="Проверка, что процесс жив",
    description=LIVE_DESCRIPTION + "\n\n" + INTERNAL_ACCESS_DESCRIPTION,
)
async def live() -> dict:
    return {
        "status": "ok",
        "message": "Процесс приложения жив",
        "service": "swift_pack_label",
        "time": utc_now_iso(),
    }


@internal_diagnostics.get(
    "/health/ready",
    dependencies=[Depends(check_internal_access)],
    summary="Проверка готовности приложения",
    description=READY_DESCRIPTION + "\n\n" + INTERNAL_ACCESS_DESCRIPTION,
)
async def ready() -> dict:
    db_status = await _db_status()
    redis_status = await _redis_snapshot()
    status_value = "ok" if db_status == "ok" and redis_status.get("status") in {"ok", "not_configured"} else "degraded"
    return {
        "status": status_value,
        "message": "Приложение готово принимать запросы" if status_value == "ok" else "Готовность приложения снижена, проверьте компоненты ниже",
        "db": db_status,
        "redis": redis_status.get("status", "skipped"),
    }


@internal_diagnostics.get(
    "/diagnostics/status",
    dependencies=[Depends(check_internal_access)],
    summary="Общий диагностический снимок",
    description=STATUS_DESCRIPTION + "\n\n" + INTERNAL_ACCESS_DESCRIPTION,
)
async def diagnostics_status() -> dict:
    from src.db import db

    redis_status = await _redis_snapshot()
    celery_status = await _celery_snapshot()
    snapshot = diagnostics.snapshot()
    db_pool = db.get_pool_stats()
    status_value = "ok"
    if redis_status.get("status") not in {"ok", "not_configured"}:
        status_value = "degraded"
    return {
        "status": status_value,
        "message": "Диагностический снимок получен" if status_value == "ok" else "Диагностический снимок получен, есть признаки деградации",
        "process": snapshot.get("process"),
        "event_loop": snapshot.get("event_loop"),
        "db_pool": db_pool,
        "wb_refresh": snapshot.get("wb_refresh"),
        "redis": redis_status,
        "celery": celery_status,
        "recent_counts": snapshot.get("recent_counts"),
    }


@internal_diagnostics.get(
    "/diagnostics/db",
    dependencies=[Depends(check_internal_access)],
    summary="Диагностика PostgreSQL pool",
    description=DB_DESCRIPTION + "\n\n" + INTERNAL_ACCESS_DESCRIPTION,
)
async def diagnostics_db() -> dict:
    from src.db import db

    return {
        "message": "Состояние пула PostgreSQL и последние медленные операции",
        "pool": db.get_pool_stats(),
        "recent_slow_acquires": list(diagnostics.recent_db_acquires),
        "recent_long_holds": list(diagnostics.recent_db_holds),
        "recent_slow_queries": list(diagnostics.recent_db_queries),
    }


@internal_diagnostics.get(
    "/diagnostics/wb-refresh",
    dependencies=[Depends(check_internal_access)],
    summary="Состояние refresh кэша Wildberries",
    description=WB_REFRESH_DESCRIPTION + "\n\n" + INTERNAL_ACCESS_DESCRIPTION,
)
async def diagnostics_wb_refresh() -> dict:
    return {
        "message": "Состояние текущего или последнего обновления кэша Wildberries",
        "wb_refresh": diagnostics.get_wb_refresh_state(),
        "recent_events": list(diagnostics.recent_wb_refreshes),
    }


@internal_diagnostics.get(
    "/diagnostics/recent-events",
    dependencies=[Depends(check_internal_access)],
    summary="Последние диагностические события",
    description=RECENT_EVENTS_DESCRIPTION + "\n\n" + INTERNAL_ACCESS_DESCRIPTION,
)
async def diagnostics_recent_events(
    limit: int = Query(
        default=100,
        ge=1,
        le=500,
        description=(
            "Максимальное количество событий в ответе. По умолчанию 100, максимум 500. "
            "Большие значения не поддерживаются, чтобы endpoint оставался легким."
        ),
    ),
    event_type: Optional[str] = Query(
        default=None,
        description=(
            "Фильтр по точному имени события. Примеры: `LOGIN_START`, `DB_ACQUIRE_SLOW`, "
            "`DB_QUERY_SLOW`, `EVENT_LOOP_LAG_WARNING`, `WB_REFRESH_START`, `WB_REQUEST_RETRY`."
        ),
    ),
) -> dict:
    return {"message": "Последние диагностические события из памяти процесса", "events": diagnostics.recent(limit=limit, event_type=event_type)}


@internal_diagnostics.get(
    "/diagnostics/process",
    dependencies=[Depends(check_internal_access)],
    summary="Метрики процесса",
    description=PROCESS_DESCRIPTION + "\n\n" + INTERNAL_ACCESS_DESCRIPTION,
)
async def diagnostics_process() -> dict:
    process = diagnostics.snapshot().get("process", {})
    return {"message": "Легкие метрики процесса приложения", **process}


@internal_diagnostics.get(
    "/diagnostics/redis",
    dependencies=[Depends(check_internal_access)],
    summary="Диагностика Redis",
    description=REDIS_DESCRIPTION + "\n\n" + INTERNAL_ACCESS_DESCRIPTION,
)
async def diagnostics_redis() -> dict:
    return await _redis_snapshot()


@internal_diagnostics.get(
    "/diagnostics/celery",
    dependencies=[Depends(check_internal_access)],
    summary="Диагностика Celery",
    description=CELERY_DESCRIPTION + "\n\n" + INTERNAL_ACCESS_DESCRIPTION,
)
async def diagnostics_celery() -> dict:
    return await _celery_snapshot()
