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

    async def check_supplies_has_canceled(self, supply_ids: List[str]) -> List[Dict[str, any]]:
        """
        Проверяет наличие заказов со статусом 'canceled_by_client' в списке поставок.

        Оптимизированный метод для массовой проверки нескольких поставок одним запросом.

        Args:
            supply_ids: Список ID поставок

        Returns:
            List[Dict] со структурой для каждой поставки:
                - supply_id (str): ID поставки
                - has_canceled (bool): Флаг наличия отмененных заказов
                - canceled_order_ids (List[int]): Список ID отмененных заказов
        """
        if not supply_ids:
            logger.warning("Пустой список supply_ids для проверки")
            return []

        try:
            query = """
                SELECT DISTINCT ON (id)
                    id,
                    supply_id,
                    wb_status
                FROM assembly_task_status_model
                WHERE supply_id = ANY($1)
                ORDER BY id, created_at_db DESC
            """

            result = await self.db.fetch(query, supply_ids)

            # Группируем результаты по supply_id
            supplies_data = {}
            for row in result:
                supply_id = row["supply_id"]
                if supply_id not in supplies_data:
                    supplies_data[supply_id] = []
                supplies_data[supply_id].append(row)

            # Формируем результаты для каждой поставки
            results = []
            for supply_id in supply_ids:
                orders = supplies_data.get(supply_id, [])

                if not orders:
                    logger.info(f"Поставка {supply_id} не найдена или не содержит заказов")
                    results.append({
                        "supply_id": supply_id,
                        "has_canceled": False,
                        "canceled_order_ids": []
                    })
                    continue

                # Собираем ID отмененных заказов
                canceled_order_ids = [
                    row["id"] for row in orders
                    if row["wb_status"] == "canceled_by_client"
                ]

                has_canceled = len(canceled_order_ids) > 0

                logger.info(
                    f"Поставка {supply_id}: найдено {len(orders)} заказов, "
                    f"отмененных: {len(canceled_order_ids)}"
                )

                results.append({
                    "supply_id": supply_id,
                    "has_canceled": has_canceled,
                    "canceled_order_ids": canceled_order_ids
                })

            return results

        except Exception as e:
            logger.error(
                f"Ошибка при массовой проверке отмененных заказов: {str(e)}"
            )
            raise
