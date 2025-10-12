"""
Модель для работы с таблицей order_status_log.

Отвечает за все операции с базой данных для отслеживания статусов сборочных заданий.
"""

from typing import List, Dict, Any
from enum import Enum
from src.logger import app_logger as logger


class OrderStatus(str, Enum):
    """Статусы сборочных заданий в жизненном цикле"""
    NEW = "NEW"
    IN_HANGING_SUPPLY = "IN_HANGING_SUPPLY"
    IN_TECHNICAL_SUPPLY = "IN_TECHNICAL_SUPPLY"
    IN_FINAL_SUPPLY = "IN_FINAL_SUPPLY"
    SENT_TO_1C = "SENT_TO_1C"
    DELIVERED = "DELIVERED"
    FICTITIOUS_DELIVERED = "FICTITIOUS_DELIVERED"
    PARTIALLY_SHIPPED = "PARTIALLY_SHIPPED"


class OrderStatusLog:
    """
    Класс для работы с таблицей order_status_log.

    Таблица содержит историю всех изменений статусов заказов.

    Структура таблицы:
        id (serial, primary key)
        order_id (integer): ID сборочного задания
        status (varchar): Статус заказа
        supply_id (varchar): ID поставки (может быть NULL)
        account (varchar): Аккаунт Wildberries
        created_at (timestamp): Время создания записи
    """

    def __init__(self, db):
        """
        Инициализация модели.

        Args:
            db: Соединение с базой данных
        """
        self.db = db

    async def insert_orders_batch(
        self,
        orders_data: List[Dict[str, Any]]
    ) -> int:
        """
        Универсальный метод для batch-вставки статусов заказов в БД.

        Принимает подготовленные данные и вставляет в БД.
        НЕ содержит бизнес-логику - только работа с БД.
        Используется для ВСЕХ типов статусов (NEW, IN_TECHNICAL_SUPPLY, DELIVERED и т.д.).

        Использует ON CONFLICT DO NOTHING для автоматического игнорирования дубликатов.

        Args:
            orders_data: Список словарей с данными для вставки
                [
                    {
                        'order_id': 12345,              # ОБЯЗАТЕЛЬНО: bigint
                        'status': 'NEW',                # ОБЯЗАТЕЛЬНО: str (из OrderStatus)
                        'supply_id': None,              # ОПЦИОНАЛЬНО: str или None
                        'account': 'account1'           # ОБЯЗАТЕЛЬНО: str
                    },
                    ...
                ]

        Returns:
            int: Количество обработанных записей (попыток вставки)

        Raises:
            Exception: При ошибке работы с БД
        """
        if not orders_data:
            logger.debug("Нет данных для вставки в order_status_log")
            return 0

        try:
            query = """
            INSERT INTO public.order_status_log (order_id, status, supply_id, account)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT DO NOTHING
            """

            # Формируем значения для executemany
            values = [
                (
                    order.get('order_id'),
                    order.get('status'),
                    order.get('supply_id'),
                    order.get('account')
                )
                for order in orders_data
            ]

            # Выполняем batch insert
            await self.db.executemany(query, values)

            logger.info(
                f"Выполнено {len(values)} вставок в order_status_log "
                f"(дубликаты автоматически пропущены БД)"
            )

            return len(values)

        except Exception as e:
            logger.error(f"Ошибка при batch-вставке в order_status_log: {str(e)}")
            raise

    async def get_order_ids_by_supplies(
        self,
        supplies_data: List[Dict[str, str]]
    ) -> List[Dict[str, Any]]:
        """
        Получает уникальные order_id по списку поставок.

        Args:
            supplies_data: Список словарей с ключами supply_id и account
                [
                    {'supply_id': 'WB-GI-12345678', 'account': 'Кабинет1'},
                    {'supply_id': 'WB-GI-87654321', 'account': 'Кабинет2'}
                ]

        Returns:
            List[Dict[str, Any]]: Список уникальных записей с order_id и supply_id
                [
                    {'order_id': 12345, 'supply_id': 'WB-GI-12345678'},
                    {'order_id': 67890, 'supply_id': 'WB-GI-87654321'}
                ]
        """
        if not supplies_data:
            logger.debug("Нет данных о поставках для получения order_id")
            return []

        try:
            # Формируем список кортежей (supply_id, account) для фильтрации
            supply_filters = [(s['supply_id'], s['account']) for s in supplies_data]

            query = """
            SELECT DISTINCT order_id, supply_id
            FROM public.order_status_log
            WHERE (supply_id, account) = ANY($1::record[])
            ORDER BY order_id
            """

            rows = await self.db.fetch(query, supply_filters)

            logger.info(
                f"Найдено {len(rows)} уникальных order_id для {len(supplies_data)} поставок"
            )

            return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"Ошибка при получении order_id по поставкам: {str(e)}")
            raise
