"""
Модель для работы с таблицей assembly_task_status_model.

Таблица содержит историю статусов сборочных заданий (заказов) Wildberries.
Используется для получения данных заказов без обращения к WB API.
"""
from typing import List, Dict, Any
from src.logger import app_logger as logger


class AssemblyTaskStatus:
    """
    Работа с таблицей assembly_task_status_model.

    Таблица содержит информацию о статусах заказов с полными данными из WB API.

    Columns:
        id (int8): ID заказа (order_id)
        nm_id (int8): Номер номенклатуры на WB
        converted_price (int4): Цена в рублях
        account (varchar): Аккаунт WB
        supplier_status (text): Статус от поставщика
        wb_status (text): Статус WB
        created_at (timestamptz): Время создания заказа
        created_at_db (timestamptz): Время записи в БД
        ... и другие поля
    """

    def __init__(self, db):
        self.db = db

    async def get_orders_for_1c_integration(
        self,
        account: str,
        order_ids: List[int]
    ) -> List[Dict[str, Any]]:
        """
        Получает данные заказов для интеграции с 1С.

        Для каждого order_id берется последняя актуальная запись
        (по created_at_db DESC), так как в таблице может быть несколько
        записей с разными статусами для одного заказа.

        Args:
            account: Аккаунт Wildberries
            order_ids: Список ID заказов

        Returns:
            List[Dict[str, Any]]: Список заказов с полями:
                - id (order_id)
                - nmId (nm_id)
                - convertedPrice (converted_price)
                - createdAt (created_at в ISO формате)
        """
        if not order_ids:
            logger.warning("Пустой список order_ids для получения данных")
            return []

        try:
            query = """
                SELECT DISTINCT ON (id)
                    id,
                    nm_id,
                    converted_price,
                    created_at
                FROM assembly_task_status_model
                WHERE id = ANY($1)
                  AND account = $2
                ORDER BY id, created_at_db DESC
            """

            result = await self.db.fetch(query, order_ids, account)

            # Преобразуем в формат совместимый с WB API
            orders = []
            for row in result:
                orders.append({
                    "id": row["id"],
                    "nmId": row["nm_id"],
                    "convertedPrice": row["converted_price"],
                    "createdAt": row["created_at"].isoformat() if row["created_at"] else ""
                })

            logger.info(
                f"Получено {len(orders)} из {len(order_ids)} заказов "
                f"из assembly_task_status_model для аккаунта {account}"
            )

            return orders

        except Exception as e:
            logger.error(
                f"Ошибка получения заказов из assembly_task_status_model "
                f"для аккаунта {account}: {str(e)}"
            )
            return []
