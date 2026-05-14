import asyncio

from starlette.middleware.cors import CORSMiddleware
from fastapi import FastAPI, status, Body
from src.settings import settings
from src.db import check_db_connected, check_db_disconnected
from src.routes import router
from src.auth.init_superuser import create_initial_superuser
from src.cache import global_cache
from src.diagnostics import current_refresh_source, diagnostics_summary_logger, event_loop_lag_monitor
from src.internal.diagnostics_router import internal_diagnostics
from src.middleware import DuplicateRequestMiddleware, RequestIdMiddleware
from src.broker import broker_manager, RoutingKey, ExchangeName
from src.logger import app_logger as logger


def include_router(application: FastAPI) -> None:
    application.include_router(router)
    application.include_router(internal_diagnostics)


def add_middleware(application: FastAPI, *args, **kwargs) -> None: # noqa
    application.add_middleware(
        *args,
        **kwargs
    )

def start_application() -> FastAPI:
    application = FastAPI(title='SwiftPackLabel', debug=settings.debug)
    include_router(application)

    # CORS middleware (внешний слой)
    add_middleware(
        application,
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*']
    )

    # Защита от дублирующих запросов (двойной клик)
    # Выполняется ПОСЛЕ CORS, использует Redis для блокировки
    application.add_middleware(DuplicateRequestMiddleware)
    application.add_middleware(RequestIdMiddleware)

    return application


app = start_application()

@app.on_event('startup')
async def startup() -> None:
    await check_db_connected()
    await global_cache.connect()
    await create_initial_superuser()
    app.state.diagnostics_tasks = []
    if settings.DIAGNOSTICS_ENABLED:
        from src.db import db

        app.state.diagnostics_tasks.append(asyncio.create_task(event_loop_lag_monitor(settings)))
        app.state.diagnostics_tasks.append(asyncio.create_task(diagnostics_summary_logger(settings, db)))
    # Начальная инициализация кэша
    if settings.CACHE_WARMUP_ON_STARTUP:
        source_token = current_refresh_source.set("startup")
        try:
            await global_cache.warm_up_cache()
        finally:
            current_refresh_source.reset(source_token)
    else:
        logger.info("Стартовый прогрев кэша отключен через CACHE_WARMUP_ON_STARTUP=false")

    # Запуск автоматического фонового обновления кэша
    if settings.CACHE_BACKGROUND_REFRESH_ENABLED:
        await global_cache.start_background_refresh_all()
    else:
        logger.info("Фоновое обновление кэша отключено через CACHE_BACKGROUND_REFRESH_ENABLED=false")
    if settings.RABBITMQ_ENABLED:
        await broker_manager.get_broker().start()
    else:
        logger.info("RabbitMQ broker отключен через RABBITMQ_ENABLED=false")


@app.on_event('shutdown')
async def shutdown() -> None:
    for task in getattr(app.state, "diagnostics_tasks", []):
        if not task.done():
            task.cancel()
    if getattr(app.state, "diagnostics_tasks", []):
        await asyncio.gather(*app.state.diagnostics_tasks, return_exceptions=True)
    await check_db_disconnected()
    await global_cache.disconnect()
    if settings.RABBITMQ_ENABLED:
        await broker_manager.get_broker().stop()


@app.get('/', status_code=status.HTTP_200_OK)
async def check_alive() -> dict:
    return {'status': 'alive'}
