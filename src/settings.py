import os

from pydantic_settings import BaseSettings
from functools import lru_cache
from src.utils import get_wb_tokens
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    debug: bool = True
    tokens: dict = get_wb_tokens()
    db_app_host: str = os.environ.get("POSTGRES_HOST", "localhost")
    db_app_port: int = os.environ.get("POSTGRES_PORT", 5432)
    db_app_user: str = os.environ.get("POSTGRES_USER")
    db_app_password: str = os.environ.get("POSTGRES_PASSWORD")
    dp_app_name: str = os.environ.get("POSTGRES_DB")

    echo: bool = True
    async_pg_pool_size: int = 5

    connection_timeout: float = 10.0  # seconds
    statement_timeout: float = 30.0  # seconds

    max_connection_lifetime: float = 3600.0  # 1 hour
    max_connection_idle_time: float = 600.0

    SECRET_KEY: str = os.getenv("SECRET_KEY", "your_secret_key_here")
    ALGORITHM: str = os.getenv("ALGORITHM", "HS256")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 30)

    INIT_SUPERUSER_USERNAME: str = os.getenv("INIT_SUPERUSER_USERNAME", "admin")
    INIT_SUPERUSER_PASSWORD: str = os.getenv("INIT_SUPERUSER_PASSWORD", "adminpassword")
    INIT_SUPERUSER_EMAIL: str = os.getenv("INIT_SUPERUSER_EMAIL", "admin@example.com")

    ONEC_HOST: str = os.getenv("ONEC_HOST","")
    ONEC_USER: str = os.getenv("ONEC_USER","")
    ONEC_PASSWORD: str = os.getenv("ONEC_PASSWORD","")

    REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", 6379))
    REDIS_DB: int = int(os.getenv("REDIS_DB", 0))
    REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD", "")
    CACHE_TTL: int = int(os.getenv("CACHE_TTL", 600))  # 10 минут по умолчанию
    
    # Настройки глобального кэша
    CACHE_REFRESH_INTERVAL: int = int(os.getenv("CACHE_REFRESH_INTERVAL", 300))  # 5 минут по умолчанию
    
    # Настройки Celery
    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL", f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/1")
    CELERY_RESULT_BACKEND: str = os.getenv("CELERY_RESULT_BACKEND", f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/2")
    CELERY_TIMEZONE: str = os.getenv("CELERY_TIMEZONE", "UTC")
    CELERY_RESULT_EXPIRES: int = int(os.getenv("CELERY_RESULT_EXPIRES", 3600))  # 1 час
    CELERY_WORKER_PREFETCH_MULTIPLIER: int = int(os.getenv("CELERY_WORKER_PREFETCH_MULTIPLIER", 1))
    CELERY_WORKER_MAX_TASKS_PER_CHILD: int = int(os.getenv("CELERY_WORKER_MAX_TASKS_PER_CHILD", 1000))
    CELERY_TASK_SOFT_TIME_LIMIT: int = int(os.getenv("CELERY_TASK_SOFT_TIME_LIMIT", 600))  # 10 минут
    CELERY_TASK_TIME_LIMIT: int = int(os.getenv("CELERY_TASK_TIME_LIMIT", 600))  # 10 минут
    
    # Настройки API отгрузки
    SHIPMENT_API_URL: str = os.getenv("SHIPMENT_API_URL", "http://1c_routing_api:8002/api/shipment_of_goods/update")
    
    # Настройки резервации товаров для висячих поставок
    PRODUCT_RESERVATION_API_URL: str = os.getenv("PRODUCT_RESERVATION_API_URL", "http://1c_routing_api:8002/api/shipment_of_goods/create_reserve")
    PRODUCT_RESERVATION_WAREHOUSE_ID: int = int(os.getenv("PRODUCT_RESERVATION_WAREHOUSE_ID", 1))
    PRODUCT_RESERVATION_DELIVERY_TYPE: str = os.getenv("PRODUCT_RESERVATION_DELIVERY_TYPE", "ФБС")
    PRODUCT_RESERVATION_EXPIRES_DAYS: int = int(os.getenv("PRODUCT_RESERVATION_EXPIRES_DAYS", 10))
    
    # Настройки отправки данных об отгрузке висячих поставок
    SHIPPED_GOODS_API_URL: str = os.getenv("SHIPPED_GOODS_API_URL", "http://1c_routing_api:8002/api/shipment_of_goods/add_shipped_goods")

@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
