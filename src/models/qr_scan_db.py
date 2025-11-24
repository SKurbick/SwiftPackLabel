"""
Модель для работы с таблицей qr_scans.
Предоставляет методы для получения QR-данных заказов.
"""

from typing import List, Dict, Optional
from src.logger import app_logger as logger


class QRScanDB:
    """
    Модель для работы с таблицей qr_scans.
    Предоставляет batch-методы для эффективного получения QR-кодов заказов.
    """

    def __init__(self, db):
        """
        Инициализация модели.

        Args:
            db: Соединение с базой данных (asyncpg connection)
        """
        self.db = db

    async def get_qr_codes_by_order_ids(
        self,
        order_ids: List[int]
    ) -> Dict[int, str]:
        """
        Получает QR-коды для списка заказов одним batch-запросом.

        Для каждого order_id берется последняя запись (по created_at DESC).
        QR-код формируется как конкатенация part_a + part_b (без разделителя).

        Args:
            order_ids: Список ID заказов для получения QR-кодов

        Returns:
            Dict[int, str]: Словарь {order_id: "part_apart_b"}

        Example:
            qr_db = QRScanDB(db)
            qr_codes = await qr_db.get_qr_codes_by_order_ids([123, 456, 789])
            print(qr_codes)
            {
                123: "wild123A001",
                456: "wild456B002",
                789: None  # QR-код не найден
            }
        """
        if not order_ids:
            logger.debug("Пустой список order_ids для получения QR-кодов")
            return {}

        try:
            # Используем DISTINCT ON для получения последней записи для каждого order_id
            # (если есть несколько QR-кодов для одного заказа, берем самый свежий)
            query = """
            SELECT DISTINCT ON (order_id)
                order_id,
                part_a,
                part_b,
                qr_data
            FROM public.qr_scans
            WHERE order_id = ANY($1::bigint[])
            ORDER BY order_id, created_at DESC
            """

            rows = await self.db.fetch(query, order_ids)

            # Формируем словарь с объединенными QR-кодами
            qr_codes = {}
            for row in rows:
                order_id = row['order_id']
                part_a = row['part_a']
                part_b = row['part_b']

                # Объединяем part_a и part_b БЕЗ разделителя
                # Приоритет: part_a + part_b > part_a > part_b > qr_data
                if part_a and part_b:
                    qr_code = f"{part_a}{part_b}"
                elif part_a:
                    qr_code = part_a
                elif part_b:
                    qr_code = part_b
                else:
                    # Fallback: используем raw qr_data если части пустые
                    qr_code = row['qr_data'] if row['qr_data'] else None

                if qr_code:
                    qr_codes[order_id] = qr_code

            logger.info(
                f"Получено {len(qr_codes)} QR-кодов из {len(order_ids)} запрошенных заказов"
            )

            return qr_codes

        except Exception as e:
            logger.error(f"Ошибка при получении QR-кодов для заказов: {str(e)}")
            return {}

    async def get_qr_code_by_order_id(self, order_id: int) -> Optional[str]:
        """
        Получает QR-код для одного заказа.

        Args:
            order_id: ID заказа

        Returns:
            Optional[str]: QR-код в формате "part_apart_b" или None если не найден

        Example:
            >>> qr_db = QRScanDB(db)
            >>> qr_code = await qr_db.get_qr_code_by_order_id(123456789)
            >>> print(qr_code)
            "wild123A001"
        """
        result = await self.get_qr_codes_by_order_ids([order_id])
        return result.get(order_id)

    async def has_qr_code(self, order_id: int) -> bool:
        """
        Проверяет наличие QR-кода для заказа.

        Args:
            order_id: ID заказа

        Returns:
            bool: True если QR-код найден, False если нет
        """
        qr_code = await self.get_qr_code_by_order_id(order_id)
        return qr_code is not None
