import os
import datetime
from io import BytesIO
from typing import List, Optional
import json
import glob

from src.logger import app_logger as logger
from src.models.archives import ArchivesDB
from src.db import AsyncGenerator
from src.archives.schema import ArchiveInfo, ArchiveDetail


class FileSystemArchiveStorage:
    """Class for handling file system archive storage"""

    def __init__(self):
        current_file_path = os.path.abspath(__file__)
        current_dir = os.path.dirname(current_file_path)
        self.storage_dir = os.path.join(current_dir, "data")
        os.makedirs(self.storage_dir, exist_ok=True)
        self.metadata_file = os.path.join(self.storage_dir, "metadata.json")

        if not os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'w') as f:
                json.dump([], f)

    def _get_metadata(self):
        """Read metadata from file"""
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'r') as f:
                return json.load(f)
        return []

    def _save_metadata(self, metadata):
        """Save metadata to file"""
        with open(self.metadata_file, 'w') as f:
            json.dump(metadata, f, default=str)

    async def save_archive_to_fs(self, zip_archive: BytesIO, account_name: Optional[str] = None,
                                 name_archive: Optional[str] = None) -> int:
        """Save archive to file system with additional compression
        Args:
            zip_archive: Binary archive data
            account_name: Optional account name
            name_archive: Optional archive name
        Returns:
            ID of the saved archive
        """
        import gzip
        import zlib

        # Generate timestamp for filename
        timestamp = datetime.datetime.now()
        timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")

        # Generate file ID
        metadata = self._get_metadata()
        archive_id = len(metadata) + 1

        # Create filename
        if name_archive:
            filename = f"{name_archive}_{timestamp_str}.gz"
        else:
            filename = f"archive_{archive_id}_{timestamp_str}.gz"

        # Apply additional compression to the archive and save
        file_path = os.path.join(self.storage_dir, filename)
        with gzip.open(file_path, 'wb', compresslevel=9) as f:
            f.write(zip_archive.getvalue())

        # Store the original filename with .zip extension for serving
        original_filename = filename.replace('.gz', '.zip')

        # Update metadata
        metadata.append({
            "id": archive_id,
            "name": name_archive or original_filename,
            "created_at": timestamp.isoformat(),
            "filename": filename,
            "original_filename": original_filename,
            "user_name": account_name,
            "file_path": file_path
        })

        self._save_metadata(metadata)
        return archive_id

    async def delete_archive(self, archive_id: int) -> bool:
        """Delete archive from file system
        Args:
            archive_id: ID of the archive to delete
        Returns:
            True if archive was deleted, False otherwise
        """
        metadata = self._get_metadata()
        updated_metadata = []
        deleted = False

        for item in metadata:
            if item["id"] == archive_id:
                # Delete the file if it exists
                if os.path.exists(item["file_path"]):
                    os.remove(item["file_path"])
                deleted = True
            else:
                updated_metadata.append(item)

        if deleted:
            self._save_metadata(updated_metadata)

        return deleted

    async def get_all_archives_info(self):
        """Get information about all archives
        Returns:
            List of archive information
        """
        return self._get_metadata()

    async def get_archives_by_ids(self, archive_ids: List[int]):
        """Get archives by IDs
        Args:
            archive_ids: List of archive IDs
        Returns:
            List of archive details
        """
        import gzip

        metadata = self._get_metadata()
        result = []

        for item in metadata:
            if item["id"] in archive_ids:
                # Read and decompress the archive file
                try:
                    with gzip.open(item["file_path"], 'rb') as f:
                        archive_data = f.read()

                    # Add to results
                    result.append({
                        "id": item["id"],
                        "name": item.get("original_filename", item["name"]),
                        "archive": archive_data,
                        "created_at": datetime.datetime.fromisoformat(item["created_at"]),
                        "user_name": item["user_name"]
                    })
                except Exception as e:
                    logger.error(f"Error reading archive {item['id']}: {str(e)}")

        return result


class Archives:
    """Service class for archive operations"""

    def __init__(self, db: AsyncGenerator = None):
        self.db = db
        self.archives_db = ArchivesDB(self.db)
        self.fs_storage = FileSystemArchiveStorage()

    async def save_archive(self, zip_archive: BytesIO, account_name: Optional[str] = None) -> None:
        """Save archive to file system
        Args:
            zip_archive: Binary archive data
            account_name: Optional account name
        """
        logger.info('Сохранение архива в файловую систему')
        await self.fs_storage.save_archive_to_fs(zip_archive, account_name)

    async def get_archives_by_ids(self, archive_ids: List[int]) -> List[ArchiveDetail]:
        """Get archives by IDs
        Args:
            archive_ids: List of archive IDs to retrieve
        Returns:
            List of archive details with binary data
        """
        logger.info('Получение архивов по ID')
        result = await self.fs_storage.get_archives_by_ids(archive_ids)
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
        result = await self.fs_storage.get_all_archives_info()
        return [
            ArchiveInfo(
                name=data["name"],
                date=datetime.datetime.fromisoformat(data["created_at"]) if isinstance(data["created_at"], str) else
                data["created_at"],
                id=data["id"],
                user=data["user_name"]
            )
            for data in result
        ]

    async def delete_archive(self, archive_id: int) -> bool:
        """Delete archive by ID (for superuser only)
        Args:
            archive_id: ID of the archive to delete
        Returns:
            True if archive was deleted, False otherwise
        """
        logger.info(f'Удаление архива с ID {archive_id}')
        return await self.fs_storage.delete_archive(archive_id)
