from io import BytesIO
from typing import List, Optional

from src.logger import app_logger as logger
from src.models.archives import ArchivesDB
from src.db import AsyncGenerator
from src.archives.schema import ArchiveInfo, ArchiveDetail


class Archives:
    """Service class for archive operations"""

    def __init__(self, db: AsyncGenerator = None):
        self.db = db
        self.archives_db = ArchivesDB(self.db)

    async def save_archive(self, zip_archive: BytesIO, account_name: Optional[str] = None) -> None:
        """Save archive to database
        Args:
            zip_archive: Binary archive data
            account_name: Optional account name
        """
        logger.info('Сохранение архива в БД')
        await self.archives_db.save_archive_to_db(zip_archive, account_name)

    async def get_archives_by_ids(self, archive_ids: List[int]) -> List[ArchiveDetail]:
        """Get archives by IDs
        Args:
            archive_ids: List of archive IDs to retrieve
        Returns:
            List of archive details with binary data
        """
        logger.info('Получение архивов по ID')
        result = await self.archives_db.get_archives_from_db_to_id(archive_ids)
        return [
            ArchiveDetail(
                id=record['id'],
                name=record['name'] or f"archive_{record['id']}.zip",
                archive=record['archive'],
                created_at=record['created_at'],
                user_name=record['user_name']
            )
            for record in result
        ]

    async def get_all_archives(self) -> List[ArchiveInfo]:
        """Get all archives information without binary data
        Returns:
            List of archive information
        """
        logger.info('Получение всех архивов')
        result = await self.archives_db.get_all_archive_from_db()
        return [
            ArchiveInfo(
                name=data["name"],
                date=data["created_at"],
                id=data["id"],
                user=data["user_name"]
            )
            for data in result
        ]
