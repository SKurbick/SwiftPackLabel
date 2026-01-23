# Детальная Документация Модулей и Сервисов

## Содержание

1. [Supplies Module (ЯДРО)](#supplies-module)
2. [Orders Module](#orders-module)
3. [Wildberries API Module](#wildberries-api-module)
4. [Cache Module](#cache-module)
5. [QR Parser Module](#qr-parser-module)
6. [Authentication Module](#authentication-module)
7. [Database Models (Repository)](#database-models)

---

## Supplies Module

### Назначение

**Ядро системы** - управление полным жизненным циклом поставок товаров

### Структура

```
src/supplies/
├── router.py              # API endpoints (40+ маршрутов)
├── supplies.py            # SuppliesService (5067 строк)
├── schema.py              # Pydantic модели
├── integration_1c.py      # Интеграция с 1C
└── empty_supply_cleaner.py # Удаление пустых поставок
```

### src/supplies/router.py (40KB)

**Ответственность**: HTTP эндпоинты для работы с поставками

**Основные маршруты**:

#### 1. Получение данных

```python
@router.get("/supplies")
async def get_supplies(
    current_user: dict = Depends(get_current_user),
    supplies_service: SuppliesService = Depends()
) -> GetSuppliesResponse:
    """
    GET /api/v1/supplies

    Получает список всех поставок текущего пользователя

    Returns:
    {
        "supplies": [
            {
                "supply_id": "234567890",
                "status": "new",
                "account": "account_name",
                "orders_count": 10,
                "created_at": "2025-01-23T10:00:00"
            }
        ]
    }

    Кэширование: кэшируется в Redis (TTL: 2400 сек)
    """
```

#### 2. Создание техкруга

```python
@router.post("/supplies/create-tech-supply")
async def create_tech_supply(
    request: CreateTechSupplyRequest,
    current_user: dict = Depends(get_current_user),
    supplies_service: SuppliesService = Depends()
) -> CreateSupplyResponse:
    """
    POST /api/v1/supplies/create-tech-supply

    Создание технологической поставки (Техкруг)

    Request:
    {
        "account": "account_name",
        "order_ids": [123456, 234567, 345678]
    }

    Response:
    {
        "supply_id": "234567890",
        "status": "created",
        "orders_added": 3,
        "orders_failed": 0
    }

    Бизнес-логика:
    1. Валидирует заказы в WB API
    2. Создаёт поставку в WB (префикс ТЕХ_)
    3. Добавляет заказы параллельно
    4. Генерирует QR-коды для печати
    5. Логирует в order_status_log и supply_operations
    """
```

#### 3. Создание висячей поставки

```python
@router.post("/supplies/create-hanging-supply")
async def create_hanging_supply(
    request: CreateHangingSupplyRequest,
    current_user: dict = Depends(get_current_user),
    supplies_service: SuppliesService = Depends()
) -> CreateSupplyResponse:
    """
    POST /api/v1/supplies/create-hanging-supply

    Создание "висячей" поставки (для товаров, которых нет на складе)

    Request:
    {
        "account": "account_name",
        "order_ids": [123456, 234567]
    }

    Response:
    {
        "supply_id": "345678901",
        "status": "created",
        "type": "hanging"
    }

    Особенности:
    - Заказы НЕ добавляются в WB API
    - Данные хранятся в таблице hanging_supplies (JSONB)
    - Может быть переведена в фиктивную доставку (PATCH /delivery-fictitious)
    - Может быть отгружена реально позже (POST /delivery-hanging-actual)
    """
```

#### 4. Перемещение заказов

```python
@router.post("/supplies/move-orders")
async def move_orders(
    request: MoveOrdersRequest,
    current_user: dict = Depends(get_current_user),
    supplies_service: SuppliesService = Depends()
) -> MoveOrdersResponse:
    """
    POST /api/v1/supplies/move-orders

    Перемещение заказов между поставками

    Request:
    {
        "from_supply_id": "234567890",
        "to_supply_id": "345678901",
        "order_ids": [123456, 234567]
    }

    Response:
    {
        "orders_moved": 2,
        "orders_failed": 0,
        "failed_orders": []
    }

    Бизнес-логика:
    1. Валидирует заказы в WB API (перед перемещением)
    2. Параллельно добавляет в целевую поставку (asyncio.gather)
    3. Обрабатывает невалидные заказы (статус SHIPPED_WITH_BLOCK)
    4. Удаляет из исходной поставки
    5. Логирует изменения
    6. Инвалидирует кэш

    Защита от дублей: DuplicateRequestMiddleware (120 сек timeout)
    """
```

#### 5. Новое! Перемещение по QR-кодам

```python
@router.post("/supplies/move-orders-by-qr")
async def move_orders_by_qr(
    request: MoveOrdersByQRRequest,
    current_user: dict = Depends(get_current_user),
    supplies_service: SuppliesService = Depends()
) -> MoveOrdersResponse:
    """
    POST /api/v1/supplies/move-orders-by-qr

    Перемещение заказов по QR-кодам вместо явного указания order_ids

    Request:
    {
        "to_supply_id": "345678901",
        "qr_codes": [
            {"format": "barcode", "data": "*CN+tGIpw"},
            {"format": "part_ab", "part_a": "010", "part_b": "312"}
        ]
    }

    Response:
    {
        "orders_moved": 2,
        "orders_failed": 0,
        "failed_orders": []
    }

    Поддерживаемые форматы QR:
    1. barcode - полный QR из WB (например *CN+tGIpw)
    2. part_ab - разделённый формат (part_a + part_b без разделителя)

    Обработка:
    1. Распарсить QR-коды (resolve_qr из QRParserService)
    2. Получить order_ids из qr_scans таблицы
    3. Вызвать move_orders() с полученными ID
    """
```

#### 6. Реальная отгрузка

```python
@router.post("/supplies/delivery")
async def delivery(
    request: DeliveryRequest,
    current_user: dict = Depends(get_current_user),
    supplies_service: SuppliesService = Depends()
) -> DeliveryResponse:
    """
    POST /api/v1/supplies/delivery

    КРИТИЧНАЯ ОПЕРАЦИЯ: Реальная отгрузка поставки

    Request:
    {
        "supply_id": "234567890"
    }

    Response:
    {
        "delivery_id": "delivery_123456",
        "status": "delivered",
        "orders_delivered": 10
    }

    ⚠️ НЕВОЗМОЖНО ОТМЕНИТЬ ПОСЛЕ УСПЕШНОГО ВЫПОЛНЕНИЯ!

    Бизнес-логика:
    1. PATCH /api/v3/supplies/{supply_id}/deliver в WB API
    2. Отправка данных об отгрузке в 1C (OneCIntegration)
    3. Создание записи в delivered_supplies (неизменяемый архив)
    4. Логирование в shipment_of_goods
    5. Создание новой финальной поставки для следующих заказов
    6. Инвалидация кэша

    Защита от дублей: DuplicateRequestMiddleware (120 сек)

    Риск: если произойдёт ошибка в 1C интеграции после успешной
    отгрузки в WB, данные может потребоваться восстанавливать вручную
    """
```

#### 7. Фиктивная отгрузка

```python
@router.patch("/supplies/delivery-fictitious")
async def delivery_fictitious(
    request: DeliveryFictitiousRequest,
    current_user: dict = Depends(get_current_user),
    supplies_service: SuppliesService = Depends()
) -> DeliveryResponse:
    """
    PATCH /api/v1/supplies/delivery-fictitious

    Фиктивная отгрузка (для продления сроков доставки)

    Request:
    {
        "supply_id": "345678901"  # Must be hanging supply
    }

    Response:
    {
        "delivery_id": "fictitious_123456",
        "status": "fictitiously_delivered",
        "orders_marked": 5
    }

    Назначение:
    - Для висячих поставок продлить сроки доставки
    - Маркирует заказы как "фиктивно отгруженные"
    - В WB API заказы показывают статус "доставлено"
    - На самом деле товар может быть в пути или ожидается

    Бизнес-логика:
    1. Проверка что это висячая поставка
    2. Помечает заказы в fictitious_shipped_order_ids
    3. Устанавливает is_fictitious_delivered = true
    4. Логирует в изменения поставки (changes_log)
    5. Отправляет в 1C (фиктивная доставка)
    6. Продление срока на 120 часов (5 дней)

    Отличие от реальной отгрузки:
    - Заказы НЕ удаляются из WB API
    - Данные остаются в hanging_supplies
    - Может быть повторена при необходимости
    """
```

#### 8. Отгрузка из висячей в факт

```python
@router.post("/supplies/delivery-hanging-actual")
async def delivery_hanging_actual(
    request: DeliveryHangingActualRequest,
    current_user: dict = Depends(get_current_user),
    supplies_service: SuppliesService = Depends()
) -> DeliveryResponse:
    """
    POST /api/v1/supplies/delivery-hanging-actual

    Отгрузка заказов из висячей поставки в реальную факт-поставку

    Request:
    {
        "hanging_supply_id": "345678901",
        "orders_to_deliver": [123456, 234567]
    }

    Response:
    {
        "fact_supply_id": "234567890",
        "status": "delivered"
    }

    Бизнес-логика:
    1. Получает висячую поставку из hanging_supplies
    2. Получает или создаёт финальную (final_supplies)
    3. Создаёт новую факт-поставку в WB API
    4. Добавляет выбранные заказы в факт-поставку
    5. Отправляет в 1C (реальная отгрузка)
    6. Помечает висячую как "доставленная"
    """
```

#### 9. Генерация стикеров

```python
@router.post("/supplies/upload_stickers")
async def upload_stickers(
    request: UploadStickersRequest,
    current_user: dict = Depends(get_current_user),
    supplies_service: SuppliesService = Depends()
) -> UploadStickersResponse:
    """
    POST /api/v1/supplies/upload_stickers

    Генерация PDF файла со стикерами (QR-коды) для печати

    Request:
    {
        "supply_id": "234567890"
    }

    Response:
    {
        "pdf_url": "/stickers/234567890.pdf",
        "page_count": 10,
        "orders_in_pdf": 10
    }

    Обработка:
    1. Получает заказы из поставки
    2. Для каждого заказа генерирует QR-код
    3. Генерирует PDF (fpdf2 + Pillow)
    4. По одной странице на заказ
    5. Сохраняет в файловую систему
    6. Возвращает URL для скачивания

    Формат PDF:
    - A4 страница
    - QR-код (50x50 мм)
    - Информация заказа (article, nm_id, price)
    - Весь текст на кириллице
    """
```

### src/supplies/supplies.py (5067 строк!)

**Это СЕРДЦЕ СИСТЕМЫ**

```python
class SuppliesService:
    """Основной сервис управления поставками"""

    def __init__(
        self,
        db: DatabaseManager,
        cache: GlobalCache,
        wildberries_orders: OrdersWB,
        wildberries_supplies: SuppliesWB,
        integration_1c: OneCIntegration
    ):
        self.db = db  # Доступ к БД
        self.cache = cache  # Redis кэш
        self.wildberries_orders = wildberries_orders  # WB Orders API
        self.wildberries_supplies = wildberries_supplies  # WB Supplies API
        self.integration_1c = integration_1c  # 1C интеграция
```

#### Ключевые методы:

**1. create_tech_supply()**
```python
async def create_tech_supply(
    self,
    account: str,
    orders: List[int]
) -> dict:
    """
    Создание техкруга (технологической поставки)

    Шаги:
    1. Валидация заказов в WB API
       - can_add_to_supply_batch(orders)
       - Проверка статусов каждого заказа

    2. Создание поставки в WB
       - POST /api/v3/supplies
       - supply_name: ТЕХ_{timestamp}

    3. Параллельное добавление заказов
       - asyncio.gather() для всех заказов
       - PATCH /api/v3/supplies/{supply_id}/orders/{orderId}
       - Timeout per order: 120 сек

    4. Обработка невалидных заказов
       - Статус: SHIPPED_WITH_BLOCK
       - Логируется, но операция считается успешной

    5. Создание записи в supply_operations
       - Сохранение request payload и response
       - Для отслеживания операции

    6. Генерация QR-кодов
       - PDFService.create_sticker_pdf()
       - На каждый заказ: 1 страница PDF

    7. Логирование статусов
       - OrderStatusLog.insert() для каждого заказа
       - Статус: IN_TECHNICAL_SUPPLY

    8. Инвалидация кэша
       - cache.invalidate(["supplies_all", "orders_all"])

    Return:
    {
        "supply_id": "234567890",
        "status": "created",
        "orders_added": 10,
        "orders_failed": 0,
        "pdf_url": "/stickers/234567890.pdf"
    }
    """
```

**2. create_hanging_supply()**
```python
async def create_hanging_supply(
    self,
    account: str,
    orders: List[int]
) -> dict:
    """
    Создание "висячей" поставки

    Назначение:
    - Для заказов, товар которых ещё не прибыл на склад
    - Заказы НЕ добавляются в WB API
    - Данные хранятся локально (БД)

    Шаги:
    1. Получить информацию по заказам
       - OrdersDB.get_orders_by_ids(orders)
       - Из таблицы assembly_task_status

    2. Создать запись в hanging_supplies
       - supply_id: сгенерированный ID
       - account: пользователь
       - order_data: JSONB с полными данными
       - created_at: текущее время
       - operator: текущий пользователь

    3. Логирование в order_status_log
       - Статус: IN_HANGING_SUPPLY

    4. Отправка в 1C (резервирование товара)
       - OneCIntegration.create_product_reservation()

    5. Инвалидация кэша
       - cache.invalidate(["supplies_all"])

    Return:
    {
        "supply_id": "345678901",
        "status": "created",
        "type": "hanging",
        "orders_count": 10
    }
    """
```

**3. move_orders_between_supplies()**
```python
async def move_orders_between_supplies(
    self,
    from_supply_id: str,
    to_supply_id: str,
    order_ids: List[int]
) -> dict:
    """
    Перемещение заказов между поставками

    Критичная операция!

    Шаги:
    1. Валидация заказов в WB API
       - can_add_to_supply_batch(order_ids)
       - Если заказ не валиден → статус SHIPPED_WITH_BLOCK

    2. Параллельное добавление в целевую поставку
       - asyncio.gather() для ускорения
       - Timeout на каждый заказ: 120 сек
       - Всего максимум: 120 * len(orders) секунд

    3. Обработка ошибок
       - Невалидные заказы логируются отдельно
       - Операция не откатывается (частичное успешное перемещение OK)

    4. Удаление из исходной поставки
       - Удаляется из WB API
       - Или из hanging_supplies если висячая

    5. Логирование
       - order_status_log: обновление статуса
       - Логирование успешных и неудачных

    6. Инвалидация кэша
       - cache.invalidate(["supplies_all", "orders_all"])

    Return:
    {
        "orders_moved": 8,
        "orders_failed": 2,
        "failed_orders": [
            {"order_id": 123456, "reason": "Already shipped"}
        ]
    }
    """
```

**4. deliver_supply()**
```python
async def deliver_supply(
    self,
    supply_id: str
) -> dict:
    """
    КРИТИЧНАЯ ОПЕРАЦИЯ: Реальная отгрузка

    ⚠️ НЕ МОЖЕТ БЫТЬ ОТМЕНЕНА ПОСЛЕ УСПЕШНОГО ВЫПОЛНЕНИЯ!

    Шаги:
    1. Получить информацию о поставке
       - Из WB API или БД

    2. PATCH /api/v3/supplies/{supply_id}/deliver в WB API
       - Переводит поставку в статус "доставлена"
       - Это необратимо!

    3. Отправка данных в 1C
       - OneCIntegration.send_shipment_of_goods()
       - Передача полной информации об отгрузке
       - Обновление учёта товара в 1C

    4. Создание записи в delivered_supplies
       - Архив отгруженных поставок (неизменяемый)
       - ON CONFLICT DO NOTHING
       - Полная структура данных

    5. Логирование в shipment_of_goods
       - account, wild_code, order_id, quantity
       - delivery_type, shipped_date, operator

    6. Создание новой финальной поставки
       - Для следующих заказов
       - Или переиспользование существующей

    7. Инвалидация кэша
       - cache.invalidate(["supplies_all"])

    Return:
    {
        "delivery_id": "delivery_123456",
        "status": "delivered",
        "orders_delivered": 10,
        "new_final_supply_id": "234567890"
    }

    Риск:
    - Если fail в OneCIntegration после успешной WB отгрузки
    - Данные нужно восстанавливать вручную
    - Рекомендуется: внешний мониторинг этого шага
    """
```

### src/supplies/schema.py

**Pydantic модели для валидации запросов/ответов**

```python
class CreateTechSupplyRequest(BaseModel):
    """Request для создания техкруга"""
    account: str
    order_ids: List[int]

    class Config:
        json_schema_extra = {
            "example": {
                "account": "account_name",
                "order_ids": [123456, 234567, 345678]
            }
        }

class CreateSupplyResponse(BaseModel):
    """Response при создании поставки"""
    supply_id: str
    status: Literal["created", "error"]
    orders_added: Optional[int] = None
    orders_failed: Optional[int] = None
    pdf_url: Optional[str] = None

class MoveOrdersRequest(BaseModel):
    """Request для перемещения заказов"""
    from_supply_id: str
    to_supply_id: str
    order_ids: List[int]

class MoveOrdersByQRRequest(BaseModel):
    """Request для перемещения по QR-кодам"""
    to_supply_id: str
    qr_codes: List[Dict[str, str]]
    # Пример: [{"format": "barcode", "data": "*CN+tGIpw"}]
```

### src/supplies/integration_1c.py

**Интеграция с 1C ERP системой**

```python
class OneCIntegration:
    """Отправка данных об отгрузке в 1C"""

    async def send_shipment_of_goods(
        self,
        supplies_data: dict
    ) -> bool:
        """
        Отправка данных об отгрузке

        URL: http://1c_routing_api:8002/api/shipment_of_goods/update

        Структура:
        {
            "author": "app",
            "operator": "current_user",
            "account": "account_name",
            "wild_code": "12345",
            "orders": [
                {
                    "order_id": 123456,
                    "quantity": 1,
                    "price": 1000,
                    "delivery_type": "ФБС"
                }
            ]
        }
        """

    async def create_product_reservation(
        self,
        account: str,
        orders: List[dict]
    ) -> bool:
        """
        Резервирование товара для висячей поставки

        URL: http://1c_routing_api:8002/api/shipment_of_goods/create_reserve
        """

    async def add_shipped_goods(
        self,
        account: str,
        orders: List[dict]
    ) -> bool:
        """
        Добавление отгруженного товара

        URL: http://1c_routing_api:8002/api/shipment_of_goods/add_shipped_goods
        """
```

---

## Orders Module

### Назначение

**Управление заказами** - получение, фильтрация, группировка, логирование

### Структура

```
src/orders/
├── router.py                # API endpoints (10+)
├── orders.py                # OrdersService (44KB)
├── order_status_service.py  # Логирование статусов
└── schema.py                # Pydantic модели
```

### src/orders/router.py

#### GET /orders - сгруппированные заказы

```python
@router.get("/orders")
async def get_orders(
    current_user: dict = Depends(get_current_user),
    skip: int = Query(0),
    limit: int = Query(100),
    from_time: Optional[datetime] = None,
    to_time: Optional[datetime] = None,
    orders_service: OrdersService = Depends()
) -> GetOrdersResponse:
    """
    GET /api/v1/orders

    Получает все заказы, сгруппированные по wild-кодам

    Query параметры:
    - skip: сколько пропустить (пагинация)
    - limit: сколько вернуть (макс 100)
    - from_time: начальная дата (фильтр)
    - to_time: конечная дата (фильтр)

    Response:
    {
        "grouped_orders": {
            "12345": {
                "wild_code": "12345",
                "orders": [
                    {
                        "order_id": 123456,
                        "article": "SKU123",
                        "nm_id": 12345678,
                        "price": 1000.00,
                        "quantity": 1,
                        "status": "awaiting_assembly"
                    }
                ],
                "total_orders": 10,
                "total_price": 10000.00
            }
        }
    }

    Логика:
    1. get_filtered_orders() - фильтр по времени
    2. group_orders_by_wild() - группировка по wild коду
    3. Кэшируется в Redis
    """
```

#### POST /orders/with-supply-name - создание поставок

```python
@router.post("/orders/with-supply-name")
async def create_supplies_with_orders(
    request: CreateSuppliesWithOrdersRequest,
    current_user: dict = Depends(get_current_user),
    orders_service: OrdersService = Depends(),
    supplies_service: SuppliesService = Depends()
) -> CreateSuppliesResponse:
    """
    POST /api/v1/orders/with-supply-name

    Создание поставок из заказов (это главный эндпоинт для конечного пользователя)

    Request:
    {
        "wild_code": "12345",
        "count": 10
    }

    Response:
    {
        "supplies": [
            {
                "supply_id": "234567890",
                "type": "technical",  # или "hanging"
                "orders_added": 10,
                "pdf_url": "/stickers/234567890.pdf"
            }
        ]
    }

    Бизнес-логика:
    1. OrdersService.get_filtered_orders()
       - Получить доступные заказы по wild коду
       - Фильтр: доступное количество (stock_count)

    2. OrdersService.group_orders_by_wild()
       - Группировать по wild кодам

    3. OrdersService.process_orders_with_fact_count()
       - Для каждого wild кода определить тип поставки
       - Если stock_count = 0 → висячая, иначе техкруг

    4. SuppliesService.create_and_add_orders()
       - Создать поставку и добавить заказы
       - Возвращает supply_id

    5. Для техкруга: создание supply_operations записи
       - Для отслеживания сессии

    6. Возвращение результатов

    Защита от дублей: DuplicateRequestMiddleware (120 сек)
    """
```

#### GET /orders/sticker/{order_id} - PNG стикер

```python
@router.get("/orders/sticker/{order_id}")
async def get_single_order_sticker(
    order_id: int,
    current_user: dict = Depends(get_current_user),
    orders_service: OrdersService = Depends()
) -> FileResponse:
    """
    GET /api/v1/orders/sticker/{order_id}

    Получить PNG стикер для одного заказа

    Response:
    - Content-Type: image/png
    - Body: PNG файл (QR-код + информация)
    """
```

#### GET /orders/sessions - сессии операций

```python
@router.get("/orders/sessions")
async def get_sessions(
    skip: int = 0,
    limit: int = 100,
    current_user: dict = Depends(get_current_user)
) -> GetSessionsResponse:
    """
    GET /api/v1/orders/sessions

    Получить список всех сессий операций (создание поставок)

    Response:
    {
        "sessions": [
            {
                "operation_id": "uuid",
                "user_id": 123,
                "supply_name": "234567890",
                "status": "success",
                "created_at": "2025-01-23T10:00:00",
                "orders_created": 10
            }
        ],
        "total": 100
    }
    """
```

#### GET /orders/sessions/{operation_id} - детали сессии

```python
@router.get("/orders/sessions/{operation_id}")
async def get_session_details(
    operation_id: str,
    current_user: dict = Depends(get_current_user)
) -> SessionDetailsResponse:
    """
    GET /api/v1/orders/sessions/{operation_id}

    Получить полную информацию о сессии

    Response:
    {
        "operation_id": "uuid",
        "user_id": 123,
        "supply_name": "234567890",
        "status": "success",
        "request_payload": { ... },
        "response_data": { ... },
        "created_at": "2025-01-23T10:00:00",
        "updated_at": "2025-01-23T10:05:00",
        "error_message": null
    }
    """
```

### src/orders/orders.py (44KB)

```python
class OrdersService:
    """Управление заказами"""

    async def get_filtered_orders(
        self,
        account: str,
        from_time: Optional[datetime] = None,
        to_time: Optional[datetime] = None
    ) -> List[dict]:
        """
        Получить отфильтрованные заказы

        Фильтры:
        1. По аккаунту (текущий пользователь)
        2. По времени создания (опционально)
        3. По статусу (только новые, не доставленные)

        Возвращает:
        - список заказов с полной информацией
        - информацией о доступном количестве (stock_count)
        """

    async def group_orders_by_wild(
        self,
        orders: List[dict]
    ) -> Dict[str, List[dict]]:
        """
        Группировать заказы по wild-кодам

        Return:
        {
            "12345": [order1, order2, ...],
            "23456": [order3, order4, ...]
        }
        """

    async def process_orders_with_fact_count(
        self,
        orders: List[dict],
        count: int
    ) -> List[dict]:
        """
        Обработать заказы с учётом доступного количества

        Логика:
        1. Если stock_count > 0 для заказа → техкруг
        2. Если stock_count = 0 → висячая
        3. Распределить количество на техкруги и висячие
        """

    async def get_single_order_sticker(
        self,
        order_id: int
    ) -> bytes:
        """
        Получить PNG стикер для одного заказа

        Обработка:
        1. Получить информацию заказа
        2. Генерировать QR-код
        3. Генерировать PNG изображение
        4. Возвращать бинарное содержимое
        """
```

### src/orders/order_status_service.py

```python
class OrderStatusService:
    """Логирование статусов заказов"""

    async def process_and_log_new_orders(
        self,
        orders: List[int]
    ) -> None:
        """
        Логирование новых заказов

        SQL: INSERT INTO order_status_log
        Статус: NEW
        """

    async def process_and_log_orders_in_supplies(
        self,
        supply_id: str,
        status: str
    ) -> None:
        """
        Логирование заказов в поставке

        Статусы:
        - IN_TECHNICAL_SUPPLY: в техкруге
        - IN_HANGING_SUPPLY: в висячей
        - DELIVERED: доставлено
        """
```

---

## Wildberries API Module

### Назначение

**Интеграция с Wildberries Marketplace API** для получения и обновления данных о заказах и поставках

### Структура

```
src/wildberries_api/
├── orders.py      # Orders - управление заказами
├── supplies.py    # Supplies - управление поставками
└── cards.py       # Cards - информация о товарах
```

### src/wildberries_api/orders.py

```python
class Orders:
    """Работа с заказами WB API"""

    async def get_orders_statuses(
        self,
        token: str,
        order_ids: List[int]
    ) -> Dict[int, str]:
        """
        POST /api/v3/orders/status

        Получить статусы заказов

        Request:
        {
            "orders": [123456, 234567, 345678]
        }

        Response:
        {
            "orders": [
                {
                    "id": 123456,
                    "status": "awaiting_assembly",
                    "delivery_type": "fbs"
                }
            ]
        }

        Retry логика: 90 попыток с 61 сек между попытками
        Timeout на одно соединение: 120 сек
        """

    async def can_add_to_supply(
        self,
        token: str,
        supply_id: str,
        order_id: int
    ) -> bool:
        """
        Проверить может ли заказ быть добавлен в поставку

        Логика:
        1. Получить статус заказа
        2. Проверить если не "shipped", "lost", "cancelled"
        3. Вернуть True если можно добавить
        """

    async def can_add_to_supply_batch(
        self,
        token: str,
        supply_id: str,
        order_ids: List[int]
    ) -> Dict[int, bool]:
        """
        Batch проверка может ли заказы быть добавлены

        Параллельно проверяет все заказы
        Возвращает словарь: order_id → True/False
        """
```

### src/wildberries_api/supplies.py (15KB)

```python
class Supplies:
    """Работа с поставками WB API"""

    async def get_supplies(
        self,
        token: str
    ) -> List[dict]:
        """
        GET /api/v3/supplies

        Получить все поставки аккаунта

        Response:
        {
            "supplies": [
                {
                    "id": "234567890",
                    "name": "ТЕХ_20250123",
                    "status": "new",
                    "created_at": "2025-01-23T10:00:00"
                }
            ]
        }
        """

    async def get_supply_orders(
        self,
        token: str,
        supply_id: str
    ) -> List[dict]:
        """
        ОПТИМИЗИРОВАННЫЙ подход!

        Получить все заказы в поставке

        Шаги:
        1. GET /api/marketplace/v3/supplies/{id}/order-ids из WB API
           - Возвращает только ID

        2. Получить детали из БД (быстро!)
           - SELECT * FROM assembly_task_status WHERE order_id = ANY($1)
           - Время: ~50ms для 100 заказов

        3. Fallback на WB API для отсутствующих в БД
           - Для заказов новых или редких
           - Время: ~5 сек на один запрос (retry логика)

        4. Объединить результаты

        Преимущество:
        - ~100x быстрее чем только WB API
        - Снижает нагрузку на WB API
        - БД является кэшем статусов
        """

    async def get_supply_order_ids(
        self,
        token: str,
        supply_id: str
    ) -> List[int]:
        """
        GET /api/marketplace/v3/supplies/{id}/order-ids

        Получить только ID заказов в поставке

        Используется для оптимизации get_supply_orders()
        """

    async def create_supply(
        self,
        token: str,
        supply_name: str
    ) -> str:
        """
        POST /api/v3/supplies

        Создать новую поставку

        Response:
        {
            "id": "234567890"
        }
        """

    async def add_order_to_supply(
        self,
        token: str,
        supply_id: str,
        order_id: int
    ) -> bool:
        """
        PATCH /api/v3/supplies/{id}/orders/{orderId}

        Добавить заказ в поставку

        Retry логика: 8 попыток, 61 сек между попытками
        Timeout: 120 сек
        """

    async def deliver_supply(
        self,
        token: str,
        supply_id: str
    ) -> bool:
        """
        PATCH /api/v3/supplies/{id}/deliver

        КРИТИЧНАЯ ОПЕРАЦИЯ: Отгрузить поставку

        ⚠️ НЕ МОЖЕТ БЫТЬ ОТМЕНЕНА!

        Все заказы в поставке переводятся в статус "доставлена"
        """
```

### src/wildberries_api/cards.py

```python
class Cards:
    """Информация о товарах"""

    async def get_card_info(
        self,
        token: str,
        nm_id: int
    ) -> dict:
        """
        Получить информацию о товаре (карточка)

        Response:
        {
            "nm_id": 12345678,
            "article": "SKU123",
            "name": "Товар",
            "price": 1000.00
        }
        """
```

---

## Cache Module

### Назначение

**Redis кэширование** для высокой производительности системы

### Структура

```
src/cache/
├── global_cache.py  # GlobalCache - основной класс
├── decorators.py    # @global_cached декоратор
└── router.py        # API endpoints
```

### src/cache/global_cache.py

```python
class GlobalCache:
    """Redis кэширование с graceful degradation"""

    async def connect(self) -> None:
        """Подключение к Redis"""

    async def disconnect(self) -> None:
        """Отключение от Redis"""

    async def get(self, key: str) -> Optional[dict]:
        """
        Получить значение из кэша

        Graceful degradation:
        - Если Redis недоступен → возвращает None
        - Система продолжает работать, просто медленнее
        """

    async def set(
        self,
        key: str,
        value: dict,
        ttl: int = 2400
    ) -> None:
        """
        Сохранить значение в кэш

        TTL: время жизни в секундах (по умолчанию 40 минут)
        """

    async def warm_up_cache(self) -> None:
        """
        Первоначальный прогрев кэша при startup

        Загружает:
        - Все поставки
        - Все заказы
        - Информацию по аккаунтам
        """

    async def start_background_refresh_all(self) -> None:
        """
        Запуск фонового обновления кэша

        Периодичность: каждые 1800 сек (30 минут)

        Обновляет:
        - supplies_all
        - orders_all
        - orders_by_account_*
        """

    async def invalidate(self, keys: List[str]) -> None:
        """
        Инвалидация (удаление) ключей из кэша

        Примеры:
        - cache.invalidate(["supplies_all", "orders_all"])
        - После каждого изменения данных
        """
```

### Ключи Redis

| Ключ | Содержание | TTL | Обновление |
|------|-----------|-----|-----------|
| `supplies_all` | Все поставки | 2400 сек | 30 мин (фон) |
| `orders_all` | Все заказы | 2400 сек | 30 мин (фон) |
| `orders_by_account_{account}` | Заказы аккаунта | 2400 сек | 30 мин (фон) |

### src/cache/decorators.py

```python
@global_cached(ttl=2400, key="supplies_all")
async def get_all_supplies():
    """
    Декоратор для автоматического кэширования

    Логика:
    1. Проверить Redis кэш
    2. Если есть → вернуть из кэша
    3. Если нет → вызвать функцию
    4. Сохранить результат в кэш
    5. Вернуть результат
    """
```

---

## QR Parser Module

### Назначение

**Парсинг и распознавание QR-кодов** для автоматизации ввода заказов

### Структура

```
src/qr_parser/
├── router.py   # API endpoints
├── service.py  # QRParserService
└── schema.py   # Pydantic модели
```

### src/qr_parser/service.py

```python
class QRParserService:
    """Парсинг QR-кодов"""

    async def resolve_qr(self, qr_data: str) -> dict:
        """
        Распарсить QR-код и определить формат

        Поддерживаемые форматы:

        1. Barcode (полный QR):
           - Формат: *CN+tGIpw (или другой)
           - Это полный QR из WB
           - Резолвится в order_id напрямую

        2. Part A + Part B (разделённый):
           - Формат: 010312... (без разделителя!)
           - Получить part_a и part_b из qr_scans БД
           - Совмещение даёт order_id

        Return:
        {
            "format": "barcode" or "part_ab",
            "order_id": 123456,
            "raw_data": "010312..."
        }
        """

    async def fetch_orders_by_qr_codes(
        self,
        qr_codes: List[str]
    ) -> List[int]:
        """
        Batch резолвинг QR-кодов в order_ids

        Логика:
        1. Для каждого QR вызвать resolve_qr()
        2. Получить list order_ids
        3. Вернуть отфильтрованный список (только валидные)
        """
```

### src/qr_parser/router.py

```python
@router.get("/qr-parser/qr/{qr_code}")
async def parse_qr(
    qr_code: str,
    service: QRParserService = Depends()
) -> QRParseResponse:
    """
    GET /api/v1/qr-parser/qr/{qr_code}

    Разобрать один QR-код

    Response:
    {
        "format": "barcode",
        "order_id": 123456,
        "raw_data": "010312..."
    }
    """

@router.get("/qr-parser/order/{order_id}")
async def get_qr_by_order(
    order_id: int,
    service: QRParserService = Depends()
) -> GetQRResponse:
    """
    GET /api/v1/qr-parser/order/{order_id}

    Получить QR-код по order_id

    Response:
    {
        "order_id": 123456,
        "qr_barcode": "*CN+tGIpw",
        "qr_part_a": "010",
        "qr_part_b": "312",
        "created_at": "2025-01-23T10:00:00"
    }
    """
```

---

## Authentication Module

### Назначение

**JWT аутентификация и управление пользователями**

### Структура

```
src/auth/
├── router.py           # API endpoints (login, register)
├── service.py          # AuthService
├── dependencies.py     # Dependency injection (get_current_user)
├── init_superuser.py   # Создание суперпользователя
└── schema.py           # Pydantic модели
```

### src/auth/service.py

```python
class AuthService:
    """Управление аутентификацией"""

    async def login(
        self,
        username: str,
        password: str
    ) -> str:
        """
        Логин пользователя

        Процесс:
        1. Получить пользователя из БД
        2. Проверить пароль (bcrypt)
        3. Создать JWT токен
        4. Вернуть токен

        JWT claims:
        {
            "sub": "username",
            "exp": timestamp,
            "iat": timestamp,
            "scopes": ["superuser"] или []
        }
        """

    async def create_user(
        self,
        username: str,
        email: str,
        password: str,
        is_superuser: bool = False
    ) -> dict:
        """
        Создание нового пользователя

        ⚠️ Только для superuser!

        Процесс:
        1. Проверить уникальность username
        2. Хешировать пароль (bcrypt)
        3. Сохранить в БД
        4. Вернуть информацию пользователя
        """
```

### src/auth/dependencies.py

```python
async def get_current_user(
    token: str = Depends(oauth2_scheme)
) -> dict:
    """
    Dependency injection для получения текущего пользователя

    Процесс:
    1. Получить JWT токен из Authorization header
    2. Декодировать и верифицировать (SECRET_KEY)
    3. Получить username из claims
    4. Вернуть информацию пользователя

    Используется:
    @router.post("/supplies/create")
    async def create(
        request: CreateRequest,
        current_user: dict = Depends(get_current_user)
    ):
        # current_user содержит информацию пользователя
        account = current_user["account"]
    """

async def get_current_superuser(
    current_user: dict = Depends(get_current_user)
) -> dict:
    """
    Получить текущего суперпользователя

    ⚠️ Выбрасывает исключение если не superuser

    Используется для защищённых операций
    """
```

---

## Database Models (Repository Pattern)

### Назначение

**Репозитории для доступа к БД** через SQL запросы

### Основные Классы

#### OrdersDB (src/models/orders_wb.py)

```python
class OrdersDB:
    """Repository для таблицы assembly_task_status"""

    async def get_orders_by_ids(order_ids: List[int]) -> List[dict]:
        """SELECT * FROM assembly_task_status WHERE order_id = ANY($1)"""

    async def fetch_orders_for_supply(supply_id: str) -> List[dict]:
        """Получить заказы в поставке"""

    async def update_order_status(order_id: int, status: str) -> None:
        """UPDATE assembly_task_status SET status = $1 WHERE order_id = $2"""
```

#### HangingSupplies (src/models/hanging_supplies.py)

```python
class HangingSupplies:
    """Repository для таблицы hanging_supplies"""

    async def get_supply(supply_id: str) -> dict:
        """SELECT * FROM hanging_supplies WHERE supply_id = $1"""

    async def create(
        supply_id: str,
        account: str,
        order_data: dict  # JSONB
    ) -> None:
        """INSERT INTO hanging_supplies ..."""

    async def update_fictitious_delivery(
        supply_id: str,
        fictitious_shipped_order_ids: List[int]
    ) -> None:
        """UPDATE hanging_supplies SET fictitious_shipped_order_ids = $1"""
```

#### FinalSupplies (src/models/final_supplies.py)

```python
class FinalSupplies:
    """Repository для таблицы final_supplies"""

    async def get_active_supply(account: str) -> Optional[dict]:
        """
        SELECT * FROM final_supplies
        WHERE account = $1 AND done = false
        LIMIT 1
        """

    async def create(supply_id: str, account: str, supply_name: str) -> None:
        """INSERT INTO final_supplies ..."""

    async def mark_done(supply_id: str) -> None:
        """UPDATE final_supplies SET done = true WHERE supply_id = $1"""
```

#### DeliveredSupplies (src/models/delivered_supplies.py)

```python
class DeliveredSupplies:
    """Repository для архива доставленных поставок"""

    async def create(
        supply_id: str,
        account: str,
        supply_data: dict  # JSONB - полная структура
    ) -> None:
        """
        INSERT INTO delivered_supplies (supply_id, account, supply_data, delivered_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT DO NOTHING  # Архив неизменяемый
        """
```

#### QRScanDB (src/models/qr_scan_db.py)

```python
class QRScanDB:
    """Repository для таблицы qr_scans"""

    async def get_qr_codes_by_order_ids(
        order_ids: List[int]
    ) -> Dict[int, dict]:
        """
        SELECT * FROM qr_scans WHERE order_id = ANY($1)

        Поддерживает два формата:
        1. barcode - полный QR
        2. part_a + part_b - разделённый формат
        """

    async def save_qr_codes(
        order_data: List[dict]
    ) -> None:
        """INSERT INTO qr_scans (order_id, part_a, part_b, qr_data) ..."""

    async def fetch_orders_by_qr_codes(
        qr_codes: List[str]
    ) -> List[int]:
        """
        Batch резолвинг QR-кодов в order_ids

        Логика:
        1. Для каждого QR разобрать (barcode или part_a+part_b)
        2. Получить order_id из таблицы
        3. Вернуть список order_ids
        """
```

#### OrderStatusLog (src/models/order_status_log.py)

```python
class OrderStatusLog:
    """Repository для логирования статусов заказов"""

    async def create(
        order_id: int,
        supply_id: str,
        status: str,  # NEW, IN_TECHNICAL_SUPPLY, IN_HANGING_SUPPLY, DELIVERED
        operator: str
    ) -> None:
        """INSERT INTO order_status_log ..."""

    async def get_order_history(order_id: int) -> List[dict]:
        """SELECT * FROM order_status_log WHERE order_id = $1 ORDER BY created_at"""
```

---

**Документация продолжается в следующих файлах...**

Этот документ охватывает:
- ✅ Supplies Module (ЯДРО)
- ✅ Orders Module
- ✅ Wildberries API Integration
- ✅ Cache Module
- ✅ QR Parser Module
- ✅ Authentication Module
- ✅ Database Models (Repository Pattern)

Продолжение будет включать:
- 📊 Полную документацию по всем 12+ таблицам БД
- 🔌 Детальное описание Celery задач
- 🔧 Конфигурацию и переменные окружения
- 💡 Практические примеры и сценарии использования
