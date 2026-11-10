from src.db import DatabaseManager


class AssemblyTasks:
    """Читает соответствия RID и СЗ из складского учёта."""

    def __init__(self, db: DatabaseManager):
        self.db = db

    async def find_by_rids(self, rids: list[str]) -> list[dict]:
        """Возвращает найденные СЗ по списку RID."""
        query = """
            SELECT DISTINCT rid, task_id
            FROM public.assembly_task
            WHERE rid = ANY($1::text[])
            AND task_id IS NOT NULL
        """
        rows = await self.db.fetch(query, rids)
        return [dict(row) for row in rows]
