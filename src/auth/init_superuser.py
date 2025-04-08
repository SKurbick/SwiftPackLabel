import asyncio
import os

from src.logger import app_logger as logger
from src.auth.service import AuthService
from src.settings import settings


async def create_initial_superuser():
    """Create the initial superuser if no users exist"""
    auth_service = AuthService()
    users = await auth_service.get_all_users()

    if not users:
        try:
            print("Creating initial superuser...")
            await auth_service.create_user(
                username=settings.INIT_SUPERUSER_USERNAME,
                password=settings.INIT_SUPERUSER_PASSWORD,
                email=settings.INIT_SUPERUSER_EMAIL,
                is_superuser=True
            )
            logger.info(f"Initial superuser '{settings.INIT_SUPERUSER_USERNAME}' created successfully!")
        except ValueError as e:
            logger.warning(f"Error creating initial superuser: {e}")
    else:
        logger.info("Users already exist, skipping superuser creation.")


if __name__ == "__main__":
    asyncio.run(create_initial_superuser())
