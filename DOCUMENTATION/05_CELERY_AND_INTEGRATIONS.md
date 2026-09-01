# Celery, Асинхронная Обработка и Интеграции

## Содержание

1. [Celery Configuration](#celery-configuration)
2. [Периодические Задачи](#периодические-задачи)
3. [Task Monitoring](#task-monitoring)
4. [Wildberries API Integration](#wildberries-api-integration)
5. [1C Integration](#1c-integration)
6. [Redis Configuration](#redis-configuration)
7. [Error Handling](#error-handling)

---

## Celery Configuration

### Структура

```
src/celery_app/
├── celery.py          # create_celery_app() - инициализация
└── tasks/
    ├── orders_sync.py # sync_orders_periodic
    ├── hanging_supplies_sync.py # sync_hanging_supplies
    └── available_quantity_sync.py # sync_update_available_quantity
```

### src/celery_app/celery.py

```python
def create_celery_app():
    """Создание и конфигурация Celery приложения"""

    celery_app = Celery(
        'swift_pack_label',
        broker=settings.CELERY_BROKER_URL,      # Redis DB 1
        backend=settings.CELERY_RESULT_BACKEND  # Redis DB 2
    )

    # Конфигурация
    celery_app.conf.update(
        # Broker конфигурация
        broker_url=settings.CELERY_BROKER_URL,
        result_backend=settings.CELERY_RESULT_BACKEND,
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone=settings.CELERY_TIMEZONE,
        enable_utc=True,

        # Worker конфигурация
        worker_pool='prefork',                  # Важно для стабильности!
        worker_concurrency=2,                   # Ограничено (async)
        worker_prefetch_multiplier=1,           # Берёт по 1 задаче за раз
        worker_max_tasks_per_child=1000,        # Перезагрузка процесса

        # Task конфигурация
        task_soft_time_limit=600,               # 10 мин soft limit
        task_time_limit=600,                    # 10 мин hard limit
        task_acks_late=True,                    # Гарантирует обработку
        task_reject_on_worker_lost=True,        # Переробить если worker упал
        task_track_started=True,                # Отслеживать начало

        # Retry конфигурация
        task_autoretry_for=(Exception,),
        task_max_retries=3,
        task_default_retry_delay=60,

        # Result конфигурация
        result_expires=3600,                    # 1 час
        result_persistent=False,

        # Beat конфигурация (периодические задачи)
        beat_scheduler='celery.beat:PersistentScheduler',
        beat_schedule={
            'sync_orders_periodic': {
                'task': 'src.celery_app.tasks.orders_sync.sync_orders_periodic',
                'schedule': crontab(minute=0),  # Каждый час
            },
            'sync_hanging_supplies': {
                'task': 'src.celery_app.tasks.hanging_supplies_sync.sync_hanging_supplies',
                'schedule': crontab(hour='*/6'),  # Каждые 6 часов
            },
            'sync_update_available_quantity': {
                'task': 'src.celery_app.tasks.available_quantity_sync.sync_update_available_quantity',
                'schedule': crontab(hour=23, minute=59),  # 23:59 UTC
            }
        }
    )

    return celery_app

celery_app = create_celery_app()
```

### Параметры Конфигурации

| Параметр | Значение | Описание |
|----------|----------|---------|
| `worker_pool` | prefork | Стабильность, не gevent |
| `worker_concurrency` | 2 | Ограничено для async |
| `worker_prefetch_multiplier` | 1 | Берёт по одной задаче |
| `task_soft_time_limit` | 600 сек | Graceful shutdown |
| `task_time_limit` | 600 сек | Hard kill |
| `task_acks_late` | True | Гарантирует обработку |

---

## Периодические Задачи

### 1. sync_orders_periodic

**Файл**: `src/celery_app/tasks/orders_sync.py`

**Периодичность**: Каждый час (каждое 0-й минуте часа: 10:00, 11:00, 12:00, ...)

**Назначение**: Синхронизация статусов заказов из WB API в БД

```python
@celery_app.task(name='sync_orders_periodic', bind=True)
async def sync_orders_periodic(self):
    """
    Синхронизация заказов из WB API

    Процесс:
    1. Получить все аккаунты из tokens.json
    2. Для каждого аккаунта:
       - GET /api/v3/orders/status из WB API
       - Обновить assembly_task_status
       - Логировать в order_status_log
    3. Обновить кэш в Redis
    4. Логировать результаты
    """

    logger = get_logger(__name__)

    try:
        logger.info("Starting orders synchronization")

        # 1️⃣ Получить все аккаунты
        accounts_tokens = get_wb_tokens()  # из tokens.json

        # 2️⃣ Для каждого аккаунта получить и обновить заказы
        for account, token in accounts_tokens.items():
            logger.info(f"Syncing orders for account: {account}")

            # 🔴 GET из WB API (с retry логикой)
            orders_from_wb = await wildberries_orders.get_orders_statuses(
                token=token,
                retries=90,  # До 90 попыток!
                delay=61     # 61 сек между попытками
            )

            # 🟢 UPDATE в БД
            for order in orders_from_wb:
                await db.execute("""
                    INSERT INTO assembly_task_status
                    (order_id, order_uid, status, price, ...)
                    VALUES ($1, $2, $3, $4, ...)
                    ON CONFLICT (order_id) DO UPDATE SET
                        status = $3, price = $4, updated_at = NOW()
                """, order.id, order.uid, order.status, order.price)

            # 🟡 Логирование в order_status_log
            await order_status_service.process_and_log_new_orders(
                [o.id for o in orders_from_wb if o.status == 'new']
            )

        # 3️⃣ Обновить кэш
        await global_cache.invalidate(['supplies_all', 'orders_all'])
        await global_cache.warm_up_cache()

        logger.info("Orders synchronization completed successfully")
        return {'status': 'success', 'synced_accounts': len(accounts_tokens)}

    except Exception as e:
        logger.error(f"Orders synchronization failed: {e}")
        # Celery автоматически переработает (task_autoretry_for)
        raise
```

**Мониторинг**: Flower (http://localhost:5555)

**Логирование**: `/logging/celery/orders_sync_{timestamp}.log`

---

### 2. sync_hanging_supplies

**Файл**: `src/celery_app/tasks/hanging_supplies_sync.py`

**Периодичность**: Каждые 6 часов (00:00, 06:00, 12:00, 18:00 UTC)

**Сложность**: 28KB кода! КРИТИЧНАЯ задача

**Назначение**: Синхронизация висячих поставок

```python
@celery_app.task(name='sync_hanging_supplies', bind=True)
async def sync_hanging_supplies(self):
    """
    Синхронизация висячих поставок

    Это самая сложная задача в системе!

    Логика:
    1. Получить все висячие поставки из БД
    2. Для каждой висячей поставки:
       - Проверить статусы заказов в WB
       - Если заказ может быть добавлен → автоматически добавить
       - Если товар прибыл → перейти в факт-поставку
       - Если срок прошёл → автоматически фиктивная отгрузка
    3. Обновить данные в hanging_supplies
    4. Логировать изменения
    """

    logger = get_logger(__name__)

    try:
        logger.info("Starting hanging supplies synchronization")

        # 1️⃣ Получить все висячие поставки
        hanging_supplies_list = await db.fetch("""
            SELECT * FROM hanging_supplies
            WHERE is_fictitious_delivered = false
            ORDER BY created_at DESC
        """)

        logger.info(f"Found {len(hanging_supplies_list)} hanging supplies")

        # 2️⃣ Для каждой висячей поставки выполнить логику
        for hanging_supply in hanging_supplies_list:
            supply_id = hanging_supply['supply_id']
            account = hanging_supply['account']
            order_data = hanging_supply['order_data']

            logger.info(f"Processing hanging supply: {supply_id}")

            try:
                # 🔴 Получить заказы из висячей
                orders = order_data['orders']
                order_ids = [o['order_id'] for o in orders]

                # 🔴 Проверить статусы в WB API
                wb_statuses = await wildberries_orders.can_add_to_supply_batch(
                    token=get_token_for_account(account),
                    order_ids=order_ids
                )

                # 🟢 Определить какие можно добавить
                can_add = [oid for oid, can in wb_statuses.items() if can]

                if can_add:
                    logger.info(f"Can add {len(can_add)} orders to supply")

                    # Автоматически добавить в финальную поставку
                    final_supply = await final_supplies.get_active_supply(account)
                    if final_supply:
                        # Добавить параллельно
                        await supplies_service.move_orders_between_supplies(
                            from_supply_id=supply_id,
                            to_supply_id=final_supply['supply_id'],
                            order_ids=can_add
                        )

                # 🟡 Проверить срок (60 часов автоматической висячей)
                created_at = hanging_supply['created_at']
                age_hours = (datetime.now() - created_at).total_seconds() / 3600

                if age_hours > 60 and not hanging_supply['is_fictitious_delivered']:
                    logger.info(f"Auto fictitious delivery after 60 hours")

                    # Автоматическая фиктивная отгрузка
                    await supplies_service.fictitious_delivery(supply_id)

                # 🟡 Логировать изменения
                await db.execute("""
                    UPDATE hanging_supplies
                    SET changes_log = changes_log || $1::jsonb,
                        updated_at = NOW()
                    WHERE supply_id = $2
                """, json.dumps({
                    'timestamp': datetime.now().isoformat(),
                    'action': 'sync_hanging_supplies',
                    'orders_can_add': len(can_add),
                    'age_hours': age_hours
                }), supply_id)

            except Exception as e:
                logger.error(f"Error processing hanging supply {supply_id}: {e}")
                # Продолжить с следующей, не падать

        logger.info("Hanging supplies synchronization completed")
        return {'status': 'success', 'processed': len(hanging_supplies_list)}

    except Exception as e:
        logger.error(f"Hanging supplies sync failed: {e}")
        raise
```

**Особенности**:
- Самая сложная задача в системе
- Может долго выполняться (на большом количестве висячих)
- Много интеграции с WB API
- Нужно осторожно обрабатывать ошибки

---

### 3. sync_update_available_quantity

**Файл**: `src/celery_app/tasks/available_quantity_sync.py`

**Периодичность**: 23:59 UTC (один раз в день перед полуночью)

**Назначение**: Обновление доступного количества товара (синхронизация с 1C)

```python
@celery_app.task(name='sync_update_available_quantity')
async def sync_update_available_quantity():
    """
    Обновление доступного количества товара

    Синхронизируется с 1C системой

    Процесс:
    1. GET /api/onec/available_quantity из 1C API
    2. UPDATE available_quantity таблицу
    3. Кэшировать результаты
    """

    logger = get_logger(__name__)

    try:
        logger.info("Starting available quantity synchronization")

        # 1️⃣ Получить данные из 1C
        quantity_data = await onec_integration.get_available_quantity()

        # 2️⃣ UPDATE в БД
        for item in quantity_data:
            await db.execute("""
                INSERT INTO available_quantity
                (nm_id, quantity, reserved, last_sync)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (nm_id) DO UPDATE SET
                    quantity = $2, reserved = $3, last_sync = NOW()
            """, item.nm_id, item.quantity, item.reserved)

        # 3️⃣ Обновить кэш
        await global_cache.invalidate(['available_quantity_all'])

        logger.info(f"Updated {len(quantity_data)} items")
        return {'status': 'success', 'updated': len(quantity_data)}

    except Exception as e:
        logger.error(f"Available quantity sync failed: {e}")
        raise
```

---

## Task Monitoring

### Flower - Web Interface для Celery

**URL**: http://localhost:5555

**Доступ**: Прямой (без аутентификации в dev)

**Возможности**:
- Просмотр статуса worker'ов
- Просмотр текущих и завершённых задач
- Просмотр истории выполнения
- Отправка задач вручную
- Мониторинг производительности

### Структура Flower

```
┌─────────────────────────────────────────────┐
│ Flower Dashboard (http://localhost:5555)    │
├─────────────────────────────────────────────┤
│ Workers:                                    │
│ - celery_worker (node.celery@...) Active   │
│ - Processes: 2                              │
│ - Tasks: completed: 1000, active: 2        │
│                                             │
│ Tasks:                                      │
│ - sync_orders_periodic                      │
│   Status: SUCCESS                           │
│   Runtime: 45.32s                           │
│                                             │
│ - sync_hanging_supplies                     │
│   Status: PENDING                           │
│   ETA: 0:05:23                              │
│                                             │
│ - sync_update_available_quantity            │
│   Status: SUCCESS                           │
│   Runtime: 12.15s                           │
└─────────────────────────────────────────────┘
```

### Команды управления Celery

```bash
# Просмотр активных worker'ов
celery -A src.celery_app.celery inspect active

# Просмотр статистики
celery -A src.celery_app.celery inspect stats

# Просмотр зарегистрированных задач
celery -A src.celery_app.celery inspect registered

# Отправить задачу вручную
celery -A src.celery_app.celery send_task 'sync_orders_periodic'

# Просмотр расписания (beat schedule)
celery -A src.celery_app.celery beat --loglevel=info

# Очистка очереди
celery -A src.celery_app.celery purge

# Отключение worker'а gracefully
kill -TERM worker_pid
```

---

## Wildberries API Integration

### src/wildberries_api/orders.py

#### Retry Логика

```python
class AsyncHttpClient:
    """Асинхронный HTTP клиент с retry для WB API"""

    def __init__(self, timeout: int = 120, retries: int = 8, delay: int = 61):
        self.timeout = timeout
        self.retries = retries      # Сколько попыток
        self.delay = delay          # Сколько сек между попытками

    async def _make_request(self, method: str, url: str, **kwargs):
        """
        Retry логика:

        WB API часто возвращает ошибки, поэтому нужно много попыток

        По умолчанию: 8 попыток, 61 сек между ними → 8 минут всего

        Для критичных операций можно использовать:
        - 90 попыток, 61 сек → 90 минут (для синхронизации заказов)
        """
        for attempt in range(self.retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.request(method, url, timeout=self.timeout, **kwargs) as response:
                        response.raise_for_status()
                        return await response.text()

            except aiohttp.ClientError as e:
                logger.warning(f"Attempt {attempt + 1}/{self.retries}: {e}")
                await asyncio.sleep(self.delay)

        # Если все попытки исчерпаны
        logger.error(f"Failed after {self.retries} attempts")
        return None
```

#### API Endpoints

| Метод | Endpoint | Назначение |
|-------|----------|-----------|
| POST | /api/v3/orders/status | Получить статусы заказов |
| GET | /api/v3/supplies | Список поставок |
| GET | /api/marketplace/v3/supplies/{id}/order-ids | ID заказов в поставке |
| PATCH | /api/v3/supplies/{id}/orders/{orderId} | Добавить заказ |
| PATCH | /api/v3/supplies/{id}/deliver | Отгрузить поставку |

#### Пример: Получение Статусов Заказов

```python
# src/wildberries_api/orders.py

class Orders:
    async def get_orders_statuses(
        self,
        token: str,
        order_ids: List[int],
        retries: int = 90,
        delay: int = 61
    ) -> List[dict]:
        """
        POST /api/v3/orders/status

        Получить статусы заказов из WB API

        Request:
        {
            "orders": [123456, 234567, ...]
        }

        Response:
        {
            "orders": [
                {
                    "id": 123456,
                    "status": "awaiting_assembly",
                    "delivery_type": "fbs",
                    ...
                }
            ]
        }

        Retry логика:
        - 90 попыток (!)
        - 61 сек между попытками
        - Всего до 90 минут на один запрос

        Это нужно потому что WB API может быть нестабилен
        """

        client = AsyncHttpClient(retries=retries, delay=delay)

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        data = {
            "orders": order_ids
        }

        response = await client.post(
            url="https://api.wildberries.ru/api/v3/orders/status",
            json=data,
            headers=headers
        )

        if response:
            return json.loads(response)['orders']
        else:
            logger.error(f"Failed to get orders statuses after {retries} attempts")
            return []
```

### Основные WB API ограничения

| Ограничение | Значение | Описание |
|-------------|----------|---------|
| Rate limit | не документировано | Можно отправлять запросы быстро |
| Max orders per request | 1000 | Максимум 1000 заказов в одном запросе |
| Timeout | 30 сек | На один запрос |
| Retry strategy | custom | Реализуем свой |

---

## 1C Integration

### src/supplies/integration_1c.py

**Назначение**: Отправка данных об отгрузке в 1C ERP систему

#### Основные Методы

```python
class OneCIntegration:
    """Интеграция с 1C ERP"""

    def __init__(self, host: str, user: str, password: str):
        self.host = host  # http://1c_routing_api:8002
        self.user = user
        self.password = password

    async def send_shipment_of_goods(
        self,
        account: str,
        supplies: List[dict]
    ) -> bool:
        """
        POST http://1c_routing_api:8002/api/shipment_of_goods/update

        Отправка данных об отгрузке в 1C

        Request:
        {
            "author": "app",
            "operator": "username",
            "account": "account_name",
            "shipments": [
                {
                    "wild_code": "12345",
                    "orders": [
                        {
                            "order_id": 123456,
                            "quantity": 1,
                            "price": 1000.00,
                            "delivery_type": "ФБС"
                        }
                    ]
                }
            ]
        }

        Response:
        {
            "status": "success",
            "message": "Shipment recorded",
            "shipments_processed": 1
        }

        ⚠️ Критично для учёта товара в 1C!
        """

        # Форматирование данных для 1C
        shipment_data = self._format_for_1c(account, supplies)

        # Отправка
        response = await AsyncHttpClient().post(
            url=f"{self.host}/api/shipment_of_goods/update",
            json=shipment_data,
            headers=self._get_auth_headers()
        )

        if response:
            result = json.loads(response)
            logger.info(f"1C shipment update successful: {result}")
            return True
        else:
            logger.error("1C shipment update failed")
            return False

    async def create_product_reservation(
        self,
        account: str,
        orders: List[dict]
    ) -> bool:
        """
        POST http://1c_routing_api:8002/api/shipment_of_goods/create_reserve

        Резервирование товара для висячей поставки

        Request:
        {
            "account": "account_name",
            "orders": [
                {
                    "nm_id": 12345678,
                    "quantity": 1,
                    "warehouse_id": 1,
                    "delivery_type": "ФБС",
                    "expires_days": 10
                }
            ]
        }

        Используется для висячих поставок
        """

    async def add_shipped_goods(
        self,
        account: str,
        orders: List[dict]
    ) -> bool:
        """
        POST http://1c_routing_api:8002/api/shipment_of_goods/add_shipped_goods

        Добавление отгруженного товара в 1C

        Используется после реальной отгрузки
        """

    def _format_for_1c(self, account: str, supplies: List[dict]) -> dict:
        """Форматирование данных для 1C API"""

        shipments = []
        for supply in supplies:
            for order in supply['orders']:
                shipments.append({
                    "wild_code": order.get('wild_code'),
                    "order_id": order['order_id'],
                    "quantity": order['quantity'],
                    "price": order['price'],
                    "delivery_type": order.get('delivery_type', 'ФБС')
                })

        return {
            "author": "swift_pack_label",
            "operator": supply.get('operator', 'system'),
            "account": account,
            "shipments": shipments
        }
```

### 1C API Endpoints

| Endpoint | Метод | Назначение |
|----------|-------|-----------|
| `/api/shipment_of_goods/update` | POST | Отправка данных об отгрузке |
| `/api/shipment_of_goods/create_reserve` | POST | Резервирование товара |
| `/api/shipment_of_goods/add_shipped_goods` | POST | Добавление отгруженного |

### Интеграция в Процесс Отгрузки

```python
# Когда пользователь отгружает поставку:

# 1️⃣ PATCH в WB API
await wildberries_supplies.deliver_supply(supply_id)

# 2️⃣ Отправить в 1C
success = await integration_1c.send_shipment_of_goods(
    account=account,
    supplies=[supply_data]
)

if not success:
    # ⚠️ Ошибка! Данные не попали в 1C
    # Но WB API уже получил отгрузку!
    # Нужен мониторинг этого момента
    logger.error("1C integration failed but WB was updated!")

# 3️⃣ Создать запись в архиве
await delivered_supplies.create(supply_id, account, supply_data)
```

### Риск: Race Condition

```
┌─────────────────────────────────────┐
│ POST /supplies/delivery              │
├─────────────────────────────────────┤
│ 1. PATCH /api/v3/supplies/deliver   │ ✅ SUCCESS
│ 2. POST 1C /shipment_of_goods       │ ❌ TIMEOUT
│ 3. INSERT delivered_supplies        │ (не выполнено)
└─────────────────────────────────────┘

Result:
- WB: поставка отгружена ✅
- 1C: товар не учтён ❌
- БД: нет записи в архиве ❌
```

**Рекомендуемое решение**:
1. Добавить внешний мониторинг этого момента
2. Использовать распределённые транзакции
3. Иметь процедуру восстановления

---

## Redis Configuration

### Структура Redis Databases

```
Redis (6 databases)
├── DB 0: Global Cache (supplies_all, orders_all)
├── DB 1: Celery Broker Queue (очередь задач)
└── DB 2: Celery Result Backend (результаты задач)
```

### Кэширование в Redis

```python
# src/cache/global_cache.py

class GlobalCache:
    def __init__(self, host: str, port: int, db: int, password: str):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.redis_client = None

    async def connect(self):
        """Подключение к Redis"""
        url = f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        self.redis_client = await redis.asyncio.from_url(url)

    async def get(self, key: str) -> Optional[dict]:
        """Получить значение из Redis"""
        try:
            data = await self.redis_client.get(key)
            if data:
                return pickle.loads(data)  # Десериализация
        except redis.RedisError as e:
            logger.warning(f"Redis get error: {e}")
        return None

    async def set(self, key: str, value: dict, ttl: int = 2400):
        """Сохранить значение в Redis"""
        try:
            serialized = pickle.dumps(value)
            await self.redis_client.setex(key, ttl, serialized)
        except redis.RedisError as e:
            logger.warning(f"Redis set error: {e}")

    async def warm_up_cache(self):
        """Прогреть кэш при startup"""
        # Загружаем все поставки и заказы в Redis
        supplies = await db.fetch("SELECT * FROM ...")
        orders = await db.fetch("SELECT * FROM ...")

        await self.set('supplies_all', supplies)
        await self.set('orders_all', orders)
```

---

## Error Handling

### Стратегия Обработки Ошибок

#### 1. WB API Ошибки

```python
try:
    await wildberries_orders.get_orders_statuses(
        token=token,
        order_ids=order_ids
    )
except WBAPIError as e:
    logger.error(f"WB API error: {e.status_code}")
    # Retry логика автоматическая (AsyncHttpClient)
except TimeoutError:
    logger.error("WB API timeout")
    # Retry логика срабатывает
except Exception as e:
    logger.critical(f"Unexpected error: {e}")
    # Celery переробит задачу (task_autoretry_for)
```

#### 2. 1C Интеграция Ошибки

```python
try:
    success = await integration_1c.send_shipment_of_goods(...)
    if not success:
        # Отправка не удалась
        logger.error("1C integration failed")
        # ⚠️ Нужен мониторинг!
except OneCAPIError as e:
    logger.error(f"1C API error: {e}")
    # Retry внутри (AsyncHttpClient)
```

#### 3. БД Ошибки

```python
try:
    await db.execute("UPDATE ... WHERE ...")
except asyncpg.UniqueViolationError:
    logger.warning("Duplicate key - skipping")
except asyncpg.ConnectionFailedError:
    logger.error("DB connection lost")
    # Переподключение автоматическое (пул)
```

### Логирование Ошибок

```python
# src/logger.py - loguru конфигурация

logger.add(
    "/logging/celery/orders_sync_{time}.log",
    rotation="100 MB",           # Ротация по размеру
    compression="zip",           # Сжатие старых файлов
    retention="7 days",          # Хранить 7 дней
    level="INFO",
    format="{time} | {level} | {name}:{function}:{line} | {message}"
)
```

---

**Документация по Celery и интеграциям завершена!**

Этот документ охватывает:
- ✅ Конфигурацию Celery
- ✅ Три периодические задачи (детально)
- ✅ Мониторинг через Flower
- ✅ Интеграцию с WB API
- ✅ Интеграцию с 1C
- ✅ Redis конфигурацию
- ✅ Обработку ошибок
