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
    CACHE_TTL: int = int(os.getenv("CACHE_TTL", 1200))  # 10 минут по умолчанию
    
    # Настройки глобального кэша
    CACHE_REFRESH_INTERVAL: int = int(os.getenv("CACHE_REFRESH_INTERVAL", 600))  # 5 минут по умолчанию


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
