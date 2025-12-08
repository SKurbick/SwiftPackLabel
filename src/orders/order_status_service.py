"""
Сервис для обработки и подготовки данных о статусах сборочных заданий.

Отвечает за бизнес-логику работы со статусами:
- Обработка новых заказов из WB API
- Подготовка данных для записи в БД
- Валидация и преобразование данных
"""

from typing import List, Dict, Any, Optional
from src.models.order_status_log import OrderStatusLog, OrderStatus
from src.logger import app_logger as logger


class OrderStatusService:
    """
    Сервис для обработки статусов сборочных заданий.

    Обрабатывает данные из различных источников и подготавливает для записи в БД.
    """

    def __init__(self, db):
        """
        Инициализация сервиса.

        Args:
            db: Соединение с базой данных
        """
        self.db = db
        self.status_log = OrderStatusLog(db)

    async def process_and_log_new_orders(
        self,
        filtered_orders: List[Dict[str, Any]]
    ) -> int:
        """
        Обрабатывает новые заказы из get_orders() и логирует в БД.

        Принимает список заказов после фильтрации, извлекает необходимые поля,
        валидирует данные и записывает в БД через OrderStatusLog.

        Args:
            filtered_orders: Список заказов из orders_service.get_filtered_orders()
                Формат: [
                    {
                        'id': 12345,
                        'article': 'WILD001',
                        'account': 'account1',
                        'created_at': '2025-10-10T10:00:00Z',
                        'nm_id': 67890,
                        'photo': 'url',
                        'subject_name': 'Название',
                        'price': 1000,
                        'elapsed_time': '1ч 30мин'
                    },
                    ...
                ]

        Returns:
            int: Количество обработанных заказов
        """
        if not filtered_orders:
            logger.debug("Нет новых заказов для обработки")
            return 0

        try:
            # 1. Извлекаем и валидируем необходимые поля
            prepared_data = self._prepare_new_orders_data(filtered_orders)

            if not prepared_data:
                logger.warning("После валидации не осталось данных для записи")
                return 0

            # 2. Записываем в БД через модель
            count = await self.status_log.insert_orders_batch(prepared_data)

            logger.info(
                f"Обработано {len(filtered_orders)} заказов, "
                f"подготовлено {len(prepared_data)} записей, "
                f"отправлено в БД {count}"
            )

            return count

        except Exception as e:
            logger.error(f"Ошибка обработки новых заказов: {str(e)}")
            return 0

    def _prepare_new_orders_data(
        self,
        filtered_orders: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Подготавливает данные новых заказов для записи в БД.

        Извлекает необходимые поля (order_id, account) и формирует
        структуру данных для вставки в order_status_log.

        Args:
            filtered_orders: Список заказов из WB API

        Returns:
            List[Dict]: Подготовленные данные для insert_orders_batch()
                [
                    {
                        'order_id': 12345,
                        'status': 'NEW',
                        'supply_id': None,
                        'account': 'account1'
                    },
                    ...
                ]
        """
        prepared_data = []

        for order in filtered_orders:
            # Извлекаем необходимые поля
            order_id = order.get('id')
            account = order.get('account')

            # Валидация: order_id и account обязательны
            if not order_id or not account:
                logger.warning(
                    f"Пропущен заказ: order_id={order_id}, account={account} "
                    f"(отсутствуют обязательные поля)"
                )
                continue

            # Формируем запись для БД
            prepared_data.append({
                'order_id': order_id,
                'status': OrderStatus.NEW.value,
                'supply_id': None,  # Для новых заказов supply_id = NULL
                'account': account
            })

        logger.debug(
            f"Подготовлено {len(prepared_data)} записей из {len(filtered_orders)} заказов"
        )

        return prepared_data

    async def process_and_log_orders_in_supplies(self,result,is_hanging: bool) -> int:
        """
        Логирует добавление заказов в поставки на основе результата создания.

        Используется после process_orders_with_fact_count() для логирования
        статусов заказов, добавленных в технические или висячие поставки.

        Args:
            result: Результат из process_orders_with_fact_count() (SupplyAccountWildOut)
                Содержит:
                - supply_ids: List[SupplyInfo]
                    [
                        {supply_id: "WB-GI-123", account: "acc1", order_ids: [1,2,3]},
                        {supply_id: "WB-GI-456", account: "acc2", order_ids: [4,5,6]}
                    ]
                - order_wild_map: Dict[int, str] - маппинг order_id → wild

            is_hanging: Тип поставки
                True = висячая поставка (IN_HANGING_SUPPLY)
                False = техническая поставка (IN_TECHNICAL_SUPPLY)

        Returns:
            int: Количество залогированных заказов

        Algorithm:
            1. Определяем статус (IN_HANGING_SUPPLY или IN_TECHNICAL_SUPPLY)
            2. Для каждой поставки из result.supply_ids:
                - Извлекаем supply_id, account, order_ids
                - Подготавливаем данные для batch insert
            3. Вызываем insert_orders_batch() один раз для всех заказов

        """
        if not result.supply_ids:
            logger.debug("Нет поставок для логирования")
            return 0

        try:
            # 1. Определяем статус по типу поставки
            status = (OrderStatus.IN_HANGING_SUPPLY if is_hanging
                     else OrderStatus.IN_TECHNICAL_SUPPLY)

            # 2. Собираем все данные для batch insert
            all_orders_data = []

            for supply_info in result.supply_ids:
                supply_id = supply_info.supply_id
                account = supply_info.account
                order_ids = supply_info.order_ids

                # Подготавливаем данные для каждого заказа
                for order_id in order_ids:
                    all_orders_data.append({
                        'order_id': order_id,
                        'status': status.value,
                        'supply_id': supply_id,
                        'account': account
                    })

            # 3. Вставляем все заказы одним batch
            if all_orders_data:
                count = await self.status_log.insert_orders_batch(all_orders_data)

                supply_type = "висячие" if is_hanging else "технические"
                logger.info(
                    f"Залогировано {count} заказов в {supply_type} поставки "
                    f"(поставок: {len(result.supply_ids)})"
                )

                return count

            return 0

        except Exception as e:
            logger.error(f"Ошибка логирования заказов в поставки: {str(e)}")
            return 0

    async def process_and_log_moved_orders(
        self,
        moved_orders_details: List[Dict[str, Any]],
        move_to_final: bool,
        operator: Optional[str] = None
    ) -> int:
        """
        Логирует перемещение заказов между поставками.

        Используется после move_orders_between_supplies() для логирования
        новых статусов заказов, перемещенных в финальные или висячие поставки.

        Args:
            moved_orders_details: Детали перемещенных заказов
                [
                    {
                        'order_id': 12345,
                        'supply_id': 'WB-GI-999_ФИНАЛ',
                        'account': 'acc1',
                        'wild': 'wild273'
                    },
                    ...
                ]
            move_to_final: Направление перемещения
                True = в финальную поставку (IN_FINAL_SUPPLY)
                False = в висячую поставку (IN_HANGING_SUPPLY)
            operator: Оператор, выполнивший операцию (опционально)

        Returns:
            int: Количество залогированных заказов
        """
        if not moved_orders_details:
            logger.debug("Нет перемещенных заказов для логирования")
            return 0

        try:
            # 1. Определяем статус по направлению перемещения
            status = (OrderStatus.IN_FINAL_SUPPLY if move_to_final
                     else OrderStatus.IN_HANGING_SUPPLY)

            # 2. Подготавливаем данные для batch insert
            prepared_data = []
            for detail in moved_orders_details:
                prepared_data.append({
                    'order_id': detail['order_id'],
                    'status': status.value,
                    'supply_id': detail['supply_id'],
                    'account': detail['account'],
                    'operator': operator
                })

            # 3. Вставляем в БД через модель
            count = await self.status_log.insert_orders_batch(prepared_data)

            target_type = "финальные" if move_to_final else "висячие"
            logger.info(
                f"Залогировано {count} перемещенных заказов в {target_type} поставки "
                f"(статус: {status.value})"
            )

            return count

        except Exception as e:
            logger.error(f"Ошибка логирования перемещенных заказов: {str(e)}")
            return 0

    async def process_and_log_sent_to_1c(
        self,
        sent_orders_details: List[Dict[str, Any]]
    ) -> int:
        """
        Логирует отправку заказов в 1C.

        Используется после успешной отправки данных в 1C через integration_1c.py

        Args:
            sent_orders_details: Детали отправленных заказов
                [
                    {
                        'order_id': 12345,
                        'supply_id': 'WB-GI-999_ФИНАЛ',
                        'account': 'acc1'
                    },
                    ...
                ]

        Returns:
            int: Количество залогированных заказов
        """
        if not sent_orders_details:
            logger.debug("Нет заказов для логирования отправки в 1C")
            return 0

        try:
            # Подготавливаем данные для batch insert
            prepared_data = []
            for detail in sent_orders_details:
                prepared_data.append({
                    'order_id': detail['order_id'],
                    'status': OrderStatus.SENT_TO_1C.value,
                    'supply_id': detail['supply_id'],
                    'account': detail['account']
                })

            # Вставляем в БД через модель
            count = await self.status_log.insert_orders_batch(prepared_data)

            logger.info(f"Залогировано {count} заказов со статусом SENT_TO_1C")

            return count

        except Exception as e:
            logger.error(f"Ошибка логирования отправки в 1C: {str(e)}")
            return 0

    async def process_and_log_delivered(
        self,
        delivery_supplies: List[Dict[str, Any]]
    ) -> int:
        """
        Обрабатывает список поставок для перевода в статус DELIVERED.
        Создает записи DELIVERED только для order_ids из входных данных.
        Выполняет сверку с БД и логирует расхождения.

        Args:
            delivery_supplies: Список словарей с ключами:
                - supply_id: номер поставки
                - account: кабинет
                - order_ids: фактические номера сборочных заданий для доставки
                [
                    {
                        'supply_id': 'WB-GI-12345678',
                        'account': 'Кабинет1',
                        'order_ids': [111, 222, 333]
                    }
                ]

        Returns:
            int: Количество залогированных заказов
        """
        if not delivery_supplies:
            return 0

        # 1. Собираем фактические order_id из входных данных и создаем маппинги
        actual_order_ids_set = set()
        supply_actual_orders_map = {}  # supply_id -> set(order_ids)
        supply_account_map = {}  # supply_id -> account

        for supply in delivery_supplies:
            supply_id = supply['supply_id']
            account = supply['account']
            order_ids = supply.get('order_ids', [])

            actual_order_ids_set.update(order_ids)
            supply_actual_orders_map[supply_id] = set(order_ids)
            supply_account_map[supply_id] = account

        # 2. Получаем order_id из БД для сверки
        order_status_log = OrderStatusLog(self.db)
        orders_from_db = await order_status_log.get_order_ids_by_supplies(delivery_supplies)

        # 3. Группируем order_id из БД по поставкам для сверки
        db_orders_by_supply = {}
        for order_data in orders_from_db:
            supply_id = order_data['supply_id']
            if supply_id not in db_orders_by_supply:
                db_orders_by_supply[supply_id] = set()
            db_orders_by_supply[supply_id].add(order_data['order_id'])

        # 4. Сверка и логирование расхождений
        for supply_id, actual_orders in supply_actual_orders_map.items():
            db_orders = db_orders_by_supply.get(supply_id, set())

            # Находим заказы, которые есть в БД, но нет в запросе
            missing_in_request = db_orders - actual_orders

            if missing_in_request:
                account = supply_account_map[supply_id]
                logger.warning(
                    f"Поставка {supply_id} ({account}):\n"
                    f"  - В БД: {len(db_orders)} заказов\n"
                    f"  - В запросе: {len(actual_orders)} заказов\n"
                    f"  - Заказы из БД, которые НЕ будут помечены DELIVERED: {sorted(missing_in_request)}"
                )

        # 5. Подготавливаем данные для записи в БД - ТОЛЬКО фактические order_ids
        prepared_data = []
        for supply in delivery_supplies:
            supply_id = supply['supply_id']
            account = supply['account']
            order_ids = supply.get('order_ids', [])

            for order_id in order_ids:
                prepared_data.append({
                    'order_id': order_id,
                    'status': OrderStatus.DELIVERED,
                    'supply_id': supply_id,
                    'account': account
                })

        if not prepared_data:
            logger.info("Нет заказов для логирования статуса DELIVERED")
            return 0

        # 6. Записываем в БД
        logged_count = await order_status_log.insert_orders_batch(prepared_data)

        return logged_count

    async def process_and_log_fictitious_delivered(
        self,
        fictitious_delivered_data: List[Dict[str, Any]]
    ) -> int:
        """
        Логирует фиктивную доставку заказов из висячих поставок.

        Используется после успешного перевода висячих поставок в статус доставки
        через deliver_fictitious_supply() в supplies.py

        Args:
            fictitious_delivered_data: Детали фиктивно доставленных заказов
                [
                    {
                        'order_id': 12345,
                        'supply_id': 'WB-GI-12345',
                        'account': 'acc1'
                    },
                    ...
                ]

        Returns:
            int: Количество залогированных заказов
        """
        if not fictitious_delivered_data:
            logger.debug("Нет заказов для логирования фиктивной доставки")
            return 0

        try:
            # Подготавливаем данные для batch insert
            prepared_data = []
            for detail in fictitious_delivered_data:
                prepared_data.append({
                    'order_id': detail['order_id'],
                    'status': OrderStatus.FICTITIOUS_DELIVERED.value,
                    'supply_id': detail['supply_id'],
                    'account': detail['account']
                })

            # Вставляем в БД через модель
            count = await self.status_log.insert_orders_batch(prepared_data)

            logger.info(f"Залогировано {count} заказов со статусом FICTITIOUS_DELIVERED")

            return count

        except Exception as e:
            logger.error(f"Ошибка логирования фиктивной доставки: {str(e)}")
            return 0

    async def process_and_log_partially_shipped(
        self,
        partially_shipped_data: List[Dict[str, Any]],
        operator: Optional[str] = None
    ) -> int:
        """
        Логирует частичную отгрузку заказов из висячих поставок.

        Используется после успешной отгрузки фактического количества через
        shipment_hanging_actual_quantity_implementation() в supplies.py

        Args:
            partially_shipped_data: Детали частично отгруженных заказов
                [
                    {
                        'order_id': 12345,
                        'supply_id': 'WB-GI-99999',
                        'account': 'acc1'
                    },
                    ...
                ]
            operator: Оператор, выполнивший операцию (опционально)

        Returns:
            int: Количество залогированных заказов
        """
        if not partially_shipped_data:
            logger.debug("Нет заказов для логирования частичной отгрузки")
            return 0

        try:
            # Подготавливаем данные для batch insert
            prepared_data = []
            for detail in partially_shipped_data:
                prepared_data.append({
                    'order_id': detail['order_id'],
                    'status': OrderStatus.PARTIALLY_SHIPPED.value,
                    'supply_id': detail['supply_id'],
                    'account': detail['account'],
                    'operator': operator
                })

            # Вставляем в БД через модель
            count = await self.status_log.insert_orders_batch(prepared_data)

            logger.info(f"Залогировано {count} заказов со статусом PARTIALLY_SHIPPED")

            return count

        except Exception as e:
            logger.error(f"Ошибка логирования частичной отгрузки: {str(e)}")
            return 0

    def _normalize_order_fields(self, order: Dict[str, Any], source: str) -> Optional[Dict[str, Any]]:
        """
        Безопасно извлекает поля из заказа для логирования/обработки.

        Критически важно для корректного учёта остатков!

        Args:
            order: Заказ (может быть из invalid_status_orders или failed_movement_orders)
            source: Источник для логирования ('invalid_status' или 'failed_movement')

        Returns:
            {'order_id': int, 'account': str, 'supplier_status': str, 'supply_id': str | None}
            или None если критичные поля отсутствуют
        """
        # 1. order_id - КРИТИЧНО
        order_id = order.get('id') if 'id' in order else order.get('order_id')
        if order_id is None:
            logger.error(
                f"❌ КРИТИЧНО [{source}]: Отсутствует order_id. "
                f"Доступные ключи: {list(order.keys())}"
            )
            return None

        # 2. account - КРИТИЧНО
        account = order.get('account')
        if not account:
            logger.error(f"❌ КРИТИЧНО [{source}]: Отсутствует account для заказа {order_id}")
            return None

        # 3. supplier_status - для маппинга в OrderStatus
        supplier_status = (
            order.get('blocked_supplier_status') if 'blocked_supplier_status' in order
            else order.get('supplier_status', 'unknown')
        )

        # 4. supply_id - может быть NULL (допустимо)
        supply_id = (
            order.get('supply_id') if 'supply_id' in order and order.get('supply_id')
            else order.get('original_supply_id')
        )

        if not supply_id:
            logger.warning(
                f"⚠️  [{source}]: Отсутствует supply_id для заказа {order_id} ({account}). "
                f"Будет записан NULL в БД."
            )

        return {
            'order_id': order_id,
            'account': account,
            'supplier_status': supplier_status,
            'supply_id': supply_id
        }

    async def log_blocked_orders_status(
        self,
        invalid_status_orders: List[Dict[str, Any]],
        failed_movement_orders: List[Dict[str, Any]],
        operator: Optional[str] = None
    ) -> int:
        """
        Логирует блокированные заказы (с невалидным статусом или ошибками перемещения).

        Используется после move_orders_between_supplies() для логирования заказов,
        которые не удалось переместить из-за:
        - Невалидного статуса WB (supplierStatus != "new"/"confirm")
        - Ошибок при попытке перемещения

        Args:
            invalid_status_orders: Заказы с невалидным статусом WB
                [
                    {
                        'order_id': 12345,
                        'account': 'acc1',
                        'supplier_status': 'complete',  # или 'cancel', 'not_found', и т.д.
                        'original_supply_id': 'WB-GI-123'
                    },
                    ...
                ]
            failed_movement_orders: Заказы с ошибками при перемещении
                [
                    {
                        'order_id': 67890,
                        'account': 'acc2',
                        'original_supply_id': 'WB-GI-456',
                        'error': 'Текст ошибки'
                    },
                    ...
                ]
            operator: Оператор, выполнивший операцию (опционально)

        Returns:
            int: Количество залогированных заказов

        Note:
            Статусы блокировки определяются по supplier_status:
            - 'complete' -> BLOCKED_ALREADY_DELIVERED
            - 'cancel' -> BLOCKED_CANCELED
            - другие -> BLOCKED_INVALID_STATUS
        """
        if not invalid_status_orders and not failed_movement_orders:
            logger.debug("Нет блокированных заказов для логирования")
            return 0

        try:
            prepared_data = []

            # 1. Обрабатываем заказы с невалидным статусом WB
            for order in invalid_status_orders:
                # Используем helper для безопасного извлечения полей
                normalized = self._normalize_order_fields(order, "log_blocked_orders_status:invalid")
                if not normalized:
                    continue  # Критические поля отсутствуют, пропускаем

                # Определяем тип блокировки по supplier_status
                supplier_status = normalized['supplier_status']
                if supplier_status == 'complete':
                    status = OrderStatus.BLOCKED_ALREADY_DELIVERED
                elif supplier_status == 'cancel':
                    status = OrderStatus.BLOCKED_CANCELED
                else:
                    status = OrderStatus.BLOCKED_INVALID_STATUS

                prepared_data.append({
                    'order_id': normalized['order_id'],
                    'status': status.value,
                    'supply_id': normalized['supply_id'],
                    'account': normalized['account'],
                    'operator': operator
                })

            # 2. Обрабатываем заказы с ошибками перемещения (тоже блокируем как INVALID_STATUS)
            for order in failed_movement_orders:
                # Используем helper для безопасного извлечения полей
                normalized = self._normalize_order_fields(order, "log_blocked_orders_status:failed")
                if not normalized:
                    continue  # Критические поля отсутствуют, пропускаем

                prepared_data.append({
                    'order_id': normalized['order_id'],
                    'status': OrderStatus.BLOCKED_INVALID_STATUS.value,
                    'supply_id': normalized['supply_id'],
                    'account': normalized['account'],
                    'operator': operator
                })

            # 3. Вставляем в БД через модель
            if prepared_data:
                count = await self.status_log.insert_orders_batch(prepared_data)

                logger.info(
                    f"Залогировано {count} блокированных заказов "
                    f"(невалидный статус: {len(invalid_status_orders)}, "
                    f"ошибки перемещения: {len(failed_movement_orders)})"
                )

                return count

            return 0

        except Exception as e:
            logger.error(f"Ошибка логирования блокированных заказов: {str(e)}")
            return 0

    async def log_shipped_with_block_status(
        self,
        invalid_status_orders: List[Dict[str, Any]],
        operator: Optional[str] = None
    ) -> int:
        """
        Логирует заблокированные заказы, которые были отгружены с оригинальным supply_id.

        Используется в режиме финального круга, когда заказы с невалидным статусом
        (complete/cancel) не смогли переместиться, но были отгружены в 1C/Shipment
        с номером поставки, где они изначально находились.

        Args:
            invalid_status_orders: Заказы с невалидным статусом WB, которые были отгружены
                [
                    {
                        'id': 12345,  # или 'order_id'
                        'account': 'acc1',
                        'supply_id': 'WB-GI-123',  # оригинальный supply_id
                        ...
                    },
                    ...
                ]
            operator: Оператор, выполнивший операцию (опционально)

        Returns:
            int: Количество залогированных заказов

        Note:
            Этот метод вызывается ПОСЛЕ log_blocked_orders_status() и ТОЛЬКО в режиме
            финального круга (move_to_final=True)
        """
        if not invalid_status_orders:
            logger.debug("Нет заблокированных заказов для логирования SHIPPED_WITH_BLOCK")
            return 0

        try:
            prepared_data = []

            # Обрабатываем заказы с невалидным статусом, которые были отгружены
            for order in invalid_status_orders:
                # Используем helper для безопасного извлечения полей
                normalized = self._normalize_order_fields(order, "log_shipped_with_block_status")
                if not normalized:
                    continue  # Критические поля отсутствуют, пропускаем

                prepared_data.append({
                    'order_id': normalized['order_id'],
                    'status': OrderStatus.SHIPPED_WITH_BLOCK.value,
                    'supply_id': normalized['supply_id'],
                    'account': normalized['account'],
                    'operator': operator
                })

            # Вставляем в БД через модель
            if prepared_data:
                count = await self.status_log.insert_orders_batch(prepared_data)

                logger.info(
                    f"Залогировано {count} заблокированных заказов со статусом SHIPPED_WITH_BLOCK "
                    f"(отгружены с оригинальным supply_id)"
                )

                return count

            return 0

        except Exception as e:
            logger.error(f"Ошибка логирования SHIPPED_WITH_BLOCK: {str(e)}")
            return 0
