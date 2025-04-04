from io import BytesIO
from typing import List


class ArchivesDB:

    def __init__(self, db):
        self.db = db

    async def save_archive_to_db(self, zip_archive: BytesIO, account_name: str = None, name_archive: str = None):
        query = """
                INSERT INTO archives (archive, name, user_name)
                VALUES ($1, $2, $3)
                """
        await self.db.execute(query, zip_archive.getvalue(), name_archive, account_name)

    async def get_all_archive_from_db(self):
        query = """
                SELECT id,name,created_at,user_name FROM archives
                """
        return await self.db.fetch(query)

    async def get_archives_from_db_to_id(self, id_archive: List[int]):
        query = """
                SELECT * FROM archives WHERE id = ANY($1)
                """
        return await self.db.fetch(query, id_archive)

    async def get_all_archives_from_db(self):
        query = """
                SELECT * FROM archives
                """
        return await self.db.fetch(query)
