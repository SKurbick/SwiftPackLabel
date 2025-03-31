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


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
