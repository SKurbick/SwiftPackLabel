import asyncio
from datetime import date
from fastapi import Depends
from asyncpg.exceptions import PostgresConnectionError, InternalServerError, ConnectionFailureError, ConnectionDoesNotExistError
from asyncpg.protocol import Record

from src.db import get_db_connection, DatabaseManager
from src.utils import error_handler_http


class AvailableQuantityRepository:
    def __init__(self, db: DatabaseManager):
        self.db = db

    @error_handler_http(
        status_code=500,
        message='Ошибка базы данных',
        exceptions=(
            PostgresConnectionError,
            InternalServerError,
            ConnectionFailureError,
            ConnectionDoesNotExistError
        )
    )
    async def get_available_quantity(
            self,
            product_id: str | None,
            start_date: date | None,
            end_date: date | None
    ) -> list[Record]:

        query = """
        SELECT * FROM available_quantity
        WHERE
            $1::varchar IS NULL OR product_id = $1::varchar AND
            $2::date IS NULL or created_at >= $2::date AND
            $3::date IS NULL or created_at <= $3::date
            ORDER BY created_at DESC;
        """

        result = await self.db.fetch(query, product_id, start_date, end_date)
        return result

    async def _sync_update_available_quantity(self):
        query = """
        WITH calculated_quantities AS (
            SELECT 
                cb.product_id, 
                cb.warehouse_id, 
                (cb.physical_quantity - cb.reserved_quantity) AS available_quantity
            FROM current_balances cb
        )
        INSERT INTO available_quantity (product_id, warehouse_id, available_quantity)
        SELECT product_id, warehouse_id, available_quantity
        FROM calculated_quantities;
        """

        await self.db.execute(query)



def get_available_quantity_repository(
        db: DatabaseManager = Depends(get_db_connection)
) -> AvailableQuantityRepository:
    return AvailableQuantityRepository(db)