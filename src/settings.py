import json
import os

from pathlib import Path
from pydantic_settings import BaseSettings
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()


def get_wb_tokens() -> dict:
    """Читает токены кабинетов"""
    tokens_path = Path(__file__).parent / "tokens.json"
    with tokens_path.open("r", encoding="utf-8") as file:
        return json.load(file)


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
    CACHE_TTL: int = int(os.getenv("CACHE_TTL", 2400))  # 10 минут по умолчанию
    
    # Настройки глобального кэша
    CACHE_REFRESH_INTERVAL: int = int(os.getenv("CACHE_REFRESH_INTERVAL", 1800))  # 30 минут по умолчанию
    CACHE_WARMUP_ON_STARTUP: bool = os.getenv("CACHE_WARMUP_ON_STARTUP", "true").lower() in ("1", "true", "yes", "on")
    CACHE_BACKGROUND_REFRESH_ENABLED: bool = os.getenv("CACHE_BACKGROUND_REFRESH_ENABLED", "true").lower() in ("1", "true", "yes", "on")
    
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

    #RABBIT
    RABBITMQ_HOST: str = os.getenv("RABBITMQ_HOST", "localhost")
    RABBITMQ_PORT: int = int(os.getenv("RABBITMQ_PORT", 5672))
    RABBITMQ_USER: str = os.getenv("RABBITMQ_USER", "guest")
    RABBITMQ_PASSWORD: str = os.getenv("RABBITMQ_PASSWORD", "")
    RABBITMQ_VHOST: str = os.getenv("RABBIT_VHOST", "/")
    RABBITMQ_ENABLED: bool = os.getenv("RABBITMQ_ENABLED", "true").lower() in ("1", "true", "yes", "on")

    HTTP_TIMEOUT_SEC: float = float(os.getenv("HTTP_TIMEOUT_SEC", 120))
    HTTP_MAX_ATTEMPTS: int = int(os.getenv("HTTP_MAX_ATTEMPTS", 4))
    HTTP_RETRY_BACKOFF_BASE_SEC: float = float(os.getenv("HTTP_RETRY_BACKOFF_BASE_SEC", 1.0))
    HTTP_RETRY_BACKOFF_MAX_SEC: float = float(os.getenv("HTTP_RETRY_BACKOFF_MAX_SEC", 30.0))
    HTTP_RETRY_AFTER_MAX_SEC: float = float(os.getenv("HTTP_RETRY_AFTER_MAX_SEC", 60.0))
    # Пауза после 429, когда сервер не прислал Retry-After: лимиты WB живут в
    # минутном окне, повтор через секунду упрётся в тот же 429 и сожжёт попытку
    HTTP_RATE_LIMIT_BACKOFF_BASE_SEC: float = float(os.getenv("HTTP_RATE_LIMIT_BACKOFF_BASE_SEC", 10.0))
    HTTP_MAX_CONCURRENT_REQUESTS_PER_HOST: int = int(os.getenv("HTTP_MAX_CONCURRENT_REQUESTS_PER_HOST", 8))
    HTTP_CONNECTION_POOL_SIZE: int = int(os.getenv("HTTP_CONNECTION_POOL_SIZE", 100))

    # 1с
    ONEC_TIMEOUT_SEC: float = float(os.getenv("ONEC_TIMEOUT_SEC", 240))
    ONEC_MAX_ATTEMPTS: int = int(os.getenv("ONEC_MAX_ATTEMPTS", 5))
    ONEC_RETRY_BACKOFF_BASE_SEC: float = float(os.getenv("ONEC_RETRY_BACKOFF_BASE_SEC", 5.0))

    # Пул потоков воркеркс
    BLOCKING_POOL_MAX_WORKERS: int = int(os.getenv("BLOCKING_POOL_MAX_WORKERS", 8))

    DIAGNOSTICS_ENABLED: bool = os.getenv("DIAGNOSTICS_ENABLED", "true").lower() in ("1", "true", "yes", "on")
    DIAGNOSTICS_LOG_INTERVAL_SEC: int = int(os.getenv("DIAGNOSTICS_LOG_INTERVAL_SEC", 60))
    DIAGNOSTICS_DB_ACQUIRE_SLOW_MS: int = int(os.getenv("DIAGNOSTICS_DB_ACQUIRE_SLOW_MS", 1000))
    DIAGNOSTICS_DB_HOLD_LONG_MS: int = int(os.getenv("DIAGNOSTICS_DB_HOLD_LONG_MS", 30000))
    DIAGNOSTICS_DB_QUERY_SLOW_MS: int = int(os.getenv("DIAGNOSTICS_DB_QUERY_SLOW_MS", 1000))
    DIAGNOSTICS_REQUEST_SLOW_MS: int = int(os.getenv("DIAGNOSTICS_REQUEST_SLOW_MS", 5000))
    DIAGNOSTICS_EVENT_LOOP_LAG_WARN_MS: int = int(os.getenv("DIAGNOSTICS_EVENT_LOOP_LAG_WARN_MS", 500))
    DIAGNOSTICS_EVENT_LOOP_LAG_CRITICAL_MS: int = int(os.getenv("DIAGNOSTICS_EVENT_LOOP_LAG_CRITICAL_MS", 3000))
    DIAGNOSTICS_NO_EXTERNAL_CHECKS: bool = os.getenv("DIAGNOSTICS_NO_EXTERNAL_CHECKS", "false").lower() in ("1", "true", "yes", "on")
    INTERNAL_DIAGNOSTICS_TOKEN: str = os.getenv("INTERNAL_DIAGNOSTICS_TOKEN", "")

@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
