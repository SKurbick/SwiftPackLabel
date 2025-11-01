"""
Сервис для работы с отмененными заказами в поставках Wildberries.
"""
from typing import Dict, List
from src.logger import app_logger as logger


class CanceledOrdersService:
    """Сервис для проверки наличия отмененных заказов в поставке."""

    def __init__(self, db):
        """
        Инициализирует сервис для работы с отмененными заказами.

        Args:
            db: Соединение с базой данных
        """
        self.db = db

    async def check_supply_has_canceled(self, supply_id: str) -> Dict[str, any]:
        """
        Проверяет наличие заказов со статусом 'canceled_by_client' в поставке.

        Использует паттерн DISTINCT ON для получения последнего статуса каждого заказа
        (аналогично AssemblyTaskStatus.get_orders_for_1c_integration).

        Args:
            supply_id: ID поставки

        Returns:
            Dict с ключами:
                - has_canceled (bool): True если есть хотя бы один заказ со статусом canceled_by_client
                - canceled_order_ids (List[int]): Список ID заказов со статусом canceled_by_client
        """
        try:
            query = """
                SELECT DISTINCT ON (id)
                    id,
                    wb_status
                FROM assembly_task_status_model
                WHERE supply_id = $1
                ORDER BY id, created_at_db DESC
            """

            result = await self.db.fetch(query, supply_id)

            if not result:
                logger.info(f"Поставка {supply_id} не найдена или не содержит заказов")
                return {"has_canceled": False, "canceled_order_ids": []}

            # Собираем ID заказов со статусом canceled_by_client
            canceled_order_ids = [
                row["id"] for row in result
                if row["wb_status"] == "canceled_by_client"
            ]

            has_canceled = len(canceled_order_ids) > 0

            logger.info(
                f"Поставка {supply_id}: найдено {len(result)} заказов, "
                f"отмененных: {len(canceled_order_ids)}"
            )

            return {
                "has_canceled": has_canceled,
                "canceled_order_ids": canceled_order_ids
            }

        except Exception as e:
            logger.error(
                f"Ошибка при проверке отмененных заказов в поставке {supply_id}: {str(e)}"
            )
            raise
