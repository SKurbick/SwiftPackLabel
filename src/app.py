from starlette.middleware.cors import CORSMiddleware
from fastapi import FastAPI, status
from src.settings import settings
from src.db import check_db_connected, check_db_disconnected
from src.routes import router
from src.auth.init_superuser import create_initial_superuser
from src.cache import global_cache


def include_router(application: FastAPI) -> None:
    application.include_router(router)


def add_middleware(application: FastAPI, *args, **kwargs) -> None: # noqa
    application.add_middleware(
        *args,
        **kwargs
    )

def start_application() -> FastAPI:
    application = FastAPI(title='SwiftPackLabel', debug=settings.debug)
    include_router(application)
    add_middleware(
        application,
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*']
    )
    return application


app = start_application()


@app.on_event('startup')
async def startup() -> None:
    await check_db_connected()
    await global_cache.connect()
    await create_initial_superuser()
    # Начальная инициализация кэша
    await global_cache.warm_up_cache()
    # Запуск автоматического фонового обновления каждые 5 минут
    await global_cache.start_background_refresh_all()


@app.on_event('shutdown')
async def shutdown() -> None:
    await check_db_disconnected()
    await global_cache.disconnect()


@app.get('/', status_code=status.HTTP_200_OK)
async def check_alive() -> dict:
    return {'status': 'alive'}
