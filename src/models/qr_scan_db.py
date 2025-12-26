"""
Модель для работы с таблицей qr_scans.
Предоставляет методы для получения QR-данных заказов.
"""

from typing import List, Dict, Optional, Tuple
from src.logger import app_logger as logger
from src.utils import process_local_vendor_code


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

    async def fetch_orders_by_qr_codes(
        self,
        qr_codes: List[str]
    ) -> Tuple[List[dict], List[str]]:
        """
        Получает полные данные заказов по QR-кодам для операции перемещения.

        Комбинирует резолвинг QR-кодов и получение полных данных заказов
        в один метод для избежания дублирования кода.

        Поддерживаемые форматы QR:
        - Barcode (qr_data): начинается с '*', например '*CN+tGIpw'
        - Part format: part_a + part_b, например 'wild1440015'

        Args:
            qr_codes: Список QR-кодов в смешанном формате

        Returns:
            Tuple[List[dict], List[str]]:
                - orders_data: Список заказов с полными данными (дедуплицированный)
                - not_found_qr_codes: QR-коды, которые не были найдены
        """
        if not qr_codes:
            logger.warning("Пустой список QR-кодов")
            return [], []

        logger.info(f"Получение данных заказов для {len(qr_codes)} QR-кодов")

        # Разделяем по формату
        barcodes = [qr for qr in qr_codes if qr.startswith('*')]
        part_formats = [qr for qr in qr_codes if not qr.startswith('*')]

        # Собираем order_ids и отслеживаем какой QR резолвился
        order_ids_set = set()
        qr_to_order = {}

        # Batch-поиск по barcode (qr_data)
        if barcodes:
            query_barcode = """
                SELECT qr.qr_data as qr_code, qr.order_id
                FROM qr_scans qr
                WHERE qr.qr_data = ANY($1::text[])
            """
            try:
                rows = await self.db.fetch(query_barcode, barcodes)
                for row in rows:
                    order_ids_set.add(row['order_id'])
                    qr_to_order[row['qr_code']] = row['order_id']
                logger.debug(f"Barcode резолвинг: {len(rows)} найдено из {len(barcodes)}")
            except Exception as e:
                logger.error(f"Ошибка резолвинга barcode: {str(e)}")
                raise

        # Batch-поиск по part_a + part_b
        if part_formats:
            query_parts = """
                SELECT CONCAT(qr.part_a, qr.part_b) as qr_code, qr.order_id
                FROM qr_scans qr
                WHERE CONCAT(qr.part_a, qr.part_b) = ANY($1::text[])
            """
            try:
                rows = await self.db.fetch(query_parts, part_formats)
                for row in rows:
                    order_ids_set.add(row['order_id'])
                    qr_to_order[row['qr_code']] = row['order_id']
                logger.debug(f"Part format резолвинг: {len(rows)} найдено из {len(part_formats)}")
            except Exception as e:
                logger.error(f"Ошибка резолвинга part format: {str(e)}")
                raise

        # Определяем ненайденные QR-коды
        not_found = [qr for qr in qr_codes if qr not in qr_to_order]

        if not order_ids_set:
            logger.warning("Не найдено ни одного заказа по QR-кодам")
            return [], not_found

        order_ids = list(order_ids_set)
        logger.info(f"Резолвинг: {len(qr_codes)} QR → {len(order_ids)} уникальных order_id")

        # Получаем полные данные заказов
        # DISTINCT ON избегает дублирования при JOIN с qr_scans
        query_orders = """
            SELECT DISTINCT ON (o.id)
                o.id,
                o.order_uid,
                o.article,
                o.nm_id as "nmId",
                o.chrt_id as "chrtId",
                o.supply_id,
                o.created_at as "createdAt",
                qr.account,
                atsm.supplier_status,
                atsm.wb_status
            FROM orders_wb o
            LEFT JOIN qr_scans qr ON o.id = qr.order_id
            LEFT JOIN assembly_task_status_model atsm ON o.id = atsm.id
            WHERE o.id = ANY($1::bigint[])
            ORDER BY o.id, qr.created_at DESC
        """

        try:
            rows = await self.db.fetch(query_orders, order_ids)

            orders_data = []
            for row in rows:
                row_dict = dict(row)

                # Обогащаем данные для совместимости с методами перемещения
                order_data = {
                    'id': row_dict['id'],
                    'order_id': row_dict['id'],  # Дублируем для совместимости
                    'order_uid': row_dict.get('order_uid'),
                    'article': row_dict.get('article', ''),
                    'nmId': row_dict.get('nmId'),
                    'chrtId': row_dict.get('chrtId'),
                    'supply_id': row_dict.get('supply_id'),
                    'original_supply_id': row_dict.get('supply_id'),
                    'createdAt': str(row_dict.get('createdAt', '')),
                    'account': row_dict.get('account', ''),
                    'supplier_status': row_dict.get('supplier_status'),
                    'wb_status': row_dict.get('wb_status'),
                    # Вычисляем wild_code
                    'wild_code': process_local_vendor_code(row_dict.get('article', ''))
                }

                orders_data.append(order_data)

            logger.info(f"Получено {len(orders_data)} заказов из БД")
            return orders_data, not_found

        except Exception as e:
            logger.error(f"Ошибка получения данных заказов: {str(e)}")
            raise
