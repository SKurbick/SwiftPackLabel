# SwiftPackLabel - Полная Архитектурная Документация

## Таблица Содержания

1. [Обзор Системы](#обзор-системы)
2. [Архитектурные Принципы](#архитектурные-принципы)
3. [Технический Стек](#технический-стек)
4. [Структура Проекта](#структура-проекта)
5. [Слои Приложения](#слои-приложения)
6. [Жизненный Цикл Приложения](#жизненный-цикл-приложения)
7. [Поток Данных](#поток-данных)
8. [Паттерны Проектирования](#паттерны-проектирования)

---

## Обзор Системы

### Назначение

**SwiftPackLabel** - это промежуточная система управления поставками товаров между складом и маркетплейсом Wildberries (WB). Система обеспечивает полный цикл подготовки товаров к отгрузке:

1. **Получение заказов** из Wildberries API
2. **Создание поставок** (техкруги, висячие, финальные)
3. **Управление заказами** (добавление, перемещение, валидация)
4. **Генерация документов** (PDF стикеры с QR-кодами)
5. **Реальная отгрузка** с интеграцией в 1C ERP-систему
6. **Фиктивная отгрузка** для продления сроков доставки

### Ключевые Характеристики

- **Асинхронная обработка** - полная поддержка async/await с высокой пропускной способностью
- **Масштабируемость** - работает с миллионами заказов и множеством WB аккаунтов
- **Надёжность** - защита от дублирующих запросов, retry логика, graceful degradation
- **Мониторинг** - Flower для Celery, детальное логирование по модулям
- **Гибкость** - поддержка четырёх типов поставок с разной бизнес-логикой

### Бизнес-Контекст

```
Wildberries API
      ↓
┌─────────────────────┐
│  SwiftPackLabel     │ ← Создание поставок, генерация стикеров
│  (управление)       │
└─────────────────────┘
      ↓
  PostgreSQL      Redis       Celery Worker
  (хранилище)   (кэш)       (фоновые задачи)
      ↓
  1C ERP System
  (учёт товара)
```

---

## Архитектурные Принципы

### 1. Слойная Архитектура (Layered Architecture)

Система разделена на чёткие слои с единственной зависимостью вниз:

```
┌─────────────────────────────────────────────┐
│  API Layer (HTTP Handlers)                  │
│  src/supplies/router.py, src/orders/router.py
├─────────────────────────────────────────────┤
│  Business Logic Layer (Services)            │
│  src/supplies/supplies.py, src/orders/orders.py
├─────────────────────────────────────────────┤
│  Data Access Layer (Repository Pattern)     │
│  src/models/*.py (OrdersDB, HangingSupplies)
├─────────────────────────────────────────────┤
│  Infrastructure Layer                       │
│  src/cache/global_cache.py, src/db.py      │
│  src/response.py, src/middleware/*.py      │
├─────────────────────────────────────────────┤
│  External Services (Integration Layer)      │
│  src/wildberries_api, src/supplies/integration_1c.py
└─────────────────────────────────────────────┘
```

**Правило зависимостей**: каждый слой зависит ТОЛЬКО от слоёв ниже, не выше.

### 2. Service-Oriented Architecture

Каждый основной модуль имеет свой сервис, инкапсулирующий бизнес-логику:

- `SuppliesService` - управление поставками
- `OrdersService` - управление заказами
- `OrderStatusService` - логирование статусов
- `OneCIntegration` - интеграция с 1C

### 3. Repository Pattern

Доступ к БД через специализированные классы в `src/models/`:

```python
class OrdersDB:
    """Repository для таблицы assembly_task_status"""
    async def get_orders_by_ids(order_ids: List[int]) -> List[dict]
    async def fetch_orders_for_supply(supply_id: str) -> List[dict]

class HangingSupplies:
    """Repository для таблицы hanging_supplies"""
    async def get_supply(supply_id: str) -> dict
    async def create(supply_id: str, account: str, order_data: dict) -> None
```

### 4. Dependency Injection

Использование FastAPI `Depends` для управления зависимостями:

```python
async def create_supply(
    request: CreateSupplyRequest,
    current_user: dict = Depends(get_current_user),
    supplies_service: SuppliesService = Depends(),
) -> CreateSupplyResponse:
    return await supplies_service.create(request, current_user)
```

### 5. Middleware-Based Security

ASGI middleware слой для:
- CORS (внешний слой)
- Защиты от дублирующих запросов (критичный уровень)

### 6. Cache-Aside Pattern

Кэширование через Redis с fallback на прямой запрос:

```python
async def get_supplies():
    # 1. Пытаемся получить из Redis
    cached = await cache.get("supplies_all")
    if cached:
        return cached

    # 2. Fallback: запрашиваем из БД
    supplies = await db.fetch_all_supplies()

    # 3. Сохраняем в Redis
    await cache.set("supplies_all", supplies, ttl=2400)
    return supplies
```

---

## Технический Стек

### Backend Framework

| Компонент | Версия | Назначение |
|-----------|--------|-----------|
| **FastAPI** | 0.115.12 | REST API framework с async поддержкой |
| **Uvicorn** | 0.30.x | ASGI сервер для запуска FastAPI |
| **Starlette** | встроена в FastAPI | Middleware, CORS, RequestHandler |

### База Данных

| Компонент | Версия | Назначение |
|-----------|--------|-----------|
| **PostgreSQL** | 12+ | Основное хранилище данных (внешний сервис) |
| **asyncpg** | 0.30.0 | Асинхронный драйвер PostgreSQL |

**Пул соединений**:
- Min size: 5 соединений
- Max size: 15 соединений
- Connection timeout: 10.0 сек
- Statement timeout: 30.0 сек

### Cache & Message Broker

| Компонент | Версия | Назначение |
|-----------|--------|-----------|
| **Redis** | 7.x | Кэширование (key-value) + Message broker |
| **redis-py** | 5.x | Python клиент для Redis |

**Использование в Redis**:
- **DB 0**: Глобальный кэш (supplies_all, orders_all и т.д.)
- **DB 1**: Celery broker queue
- **DB 2**: Celery result backend

### Task Queue & Scheduling

| Компонент | Версия | Назначение |
|-----------|--------|-----------|
| **Celery** | 5.6.0 | Асинхронная очередь задач (task queue) |
| **Celery Beat** | встроена в Celery | Планировщик периодических задач |
| **Flower** | 2.0.1 | Веб-интерфейс для мониторинга Celery |

**Конфигурация Celery**:
- Broker: Redis DB 1
- Result backend: Redis DB 2
- Worker pool: prefork (для стабильности)
- Concurrency: 2 (ограничено для async)
- Prefetch multiplier: 1
- Task soft limit: 600 сек (10 мин)
- Task hard limit: 600 сек (10 мин)

### Document Generation

| Компонент | Версия | Назначение |
|-----------|--------|-----------|
| **fpdf2** | 2.8.x | Генерация PDF файлов |
| **qrcode** | 8.1.x | Генерация QR-кодов |
| **Pillow** | 11.x | Обработка изображений |
| **PyMuPDF** | 1.24.x | Работа с PDF документами |

### Authentication & Security

| Компонент | Версия | Назначение |
|-----------|--------|-----------|
| **python-jose** | 3.3.x | JWT токены |
| **passlib** | 1.7.x | Хеширование паролей (bcrypt) |
| **pydantic** | 2.x | Валидация данных и сериализация |

### Logging & Monitoring

| Компонент | Версия | Назначение |
|-----------|--------|-----------|
| **loguru** | 0.7.x | Структурированное логирование |
| **python-dotenv** | 1.0.x | Загрузка переменных окружения |

### HTTP Client Libraries

| Компонент | Версия | Назначение |
|-----------|--------|-----------|
| **aiohttp** | 3.9.x | Асинхронный HTTP клиент |
| **requests** | 2.31.x | Синхронный HTTP клиент (fallback) |

---

## Структура Проекта

### Полная Структура Каталогов

```
/Users/uventus/PycharmProjects/SwiftPackLabel/
│
├── src/                                   # Основной код приложения
│   ├── __init__.py
│   ├── app.py                             # 🔴 КРИТИЧНЫЙ: инициализация FastAPI
│   ├── db.py                              # 🔴 КРИТИЧНЫЙ: управление БД пулом
│   ├── settings.py                        # 🔴 КРИТИЧНЫЙ: конфигурация
│   ├── logger.py                          # Инициализация loguru логгера
│   ├── response.py                        # HttpClient и AsyncHttpClient
│   ├── routes.py                          # Агрегирование всех маршрутов
│   ├── utils.py                           # Утилиты: форматирование, декораторы
│   │
│   ├── auth/                              # Аутентификация (JWT, OAuth2)
│   │   ├── router.py                      # POST /auth/login, POST /auth/users
│   │   ├── service.py                     # AuthService
│   │   ├── dependencies.py                # Dependency injection (get_current_user)
│   │   ├── init_superuser.py              # Создание суперпользователя при startup
│   │   └── schema.py                      # Pydantic модели
│   │
│   ├── models/                            # ORM-подобные модели (Repository pattern)
│   │   ├── orders_wb.py                   # OrdersDB (таблица assembly_task_status)
│   │   ├── final_supplies.py              # FinalSupplies
│   │   ├── hanging_supplies.py            # HangingSupplies (КРИТИЧНАЯ!)
│   │   ├── delivered_supplies.py          # DeliveredSupplies (архив)
│   │   ├── shipment_of_goods.py           # ShipmentOfGoods (журнал отгрузок)
│   │   ├── qr_scan_db.py                  # QRScanDB (QR-коды)
│   │   ├── order_status_log.py            # OrderStatusLog
│   │   ├── supply_operations.py           # SupplyOperationsDB (сессии)
│   │   ├── assembly_task_status.py        # AssemblyTaskStatus
│   │   ├── onec_delivery_log.py           # OneCDeliveryLog
│   │   ├── card_data.py                   # CardData (информация о товарах)
│   │   ├── canceled_orders.py             # CanceledOrders
│   │   ├── available_quantity.py          # AvailableQuantity
│   │   ├── supply_account_mapping.py      # Маппинг поставок на аккаунты
│   │   └── __init__.py
│   │
│   ├── supplies/                          # 🔥 ЯДРО СИСТЕМЫ: управление поставками
│   │   ├── router.py                      # 40+ API маршрутов
│   │   ├── supplies.py                    # 5067 строк! SuppliesService (основная логика)
│   │   ├── schema.py                      # Pydantic схемы запросов/ответов
│   │   ├── integration_1c.py              # OneCIntegration (отправка в 1C)
│   │   ├── empty_supply_cleaner.py        # Удаление пустых поставок
│   │   └── __init__.py
│   │
│   ├── orders/                            # Управление заказами
│   │   ├── router.py                      # 10+ API маршрутов
│   │   ├── orders.py                      # 44KB OrdersService
│   │   ├── order_status_service.py        # OrderStatusService
│   │   ├── schema.py                      # Pydantic модели
│   │   └── __init__.py
│   │
│   ├── wildberries_api/                   # Интеграция с WB API
│   │   ├── orders.py                      # Получение статусов заказов
│   │   ├── supplies.py                    # Работа с поставками в WB
│   │   ├── cards.py                       # Информация о товарах
│   │   └── __init__.py
│   │
│   ├── cache/                             # 💾 Redis кэширование
│   │   ├── global_cache.py                # GlobalCache (основной компонент)
│   │   ├── decorators.py                  # @global_cached декоратор
│   │   ├── router.py                      # API для управления кэшем
│   │   └── __init__.py
│   │
│   ├── middleware/                        # ASGI middleware
│   │   ├── duplicate_request.py           # DuplicateRequestMiddleware (защита от дублей)
│   │   └── __init__.py
│   │
│   ├── celery_app/                        # Celery конфигурация
│   │   ├── celery.py                      # create_celery_app()
│   │   └── tasks/
│   │       ├── orders_sync.py             # sync_orders_periodic (каждые 65 мин)
│   │       ├── hanging_supplies_sync.py   # sync_hanging_supplies (28KB, сложная!)
│   │       ├── available_quantity_sync.py # sync_update_available_quantity
│   │       └── __init__.py
│   │
│   ├── service/                           # Утилиты и сервисы
│   │   ├── service_pdf.py                 # PDFService - генерация PDF
│   │   ├── qr_auto_scanner.py             # Автоматическое сканирование QR
│   │   ├── qr_scanner_service.py          # QR сканер сервис
│   │   ├── qr_direct_processor.py         # Обработка QR по URI
│   │   ├── zip_service.py                 # Создание ZIP архивов
│   │   └── __init__.py
│   │
│   ├── qr_parser/                         # Парсинг QR-кодов
│   │   ├── router.py                      # GET /qr-parser/qr/{qr_code}
│   │   ├── service.py                     # QRParserService
│   │   ├── schema.py                      # Pydantic модели
│   │   └── __init__.py
│   │
│   ├── users/                             # Управление пользователями
│   │   ├── account.py                     # Account (базовый класс)
│   │   └── __init__.py
│   │
│   ├── logging/                           # 📋 Логирование по модулям
│   │   ├── logger/ → logger.log
│   │   ├── orders/ → логирование заказов
│   │   ├── supplies/ → логирование поставок
│   │   ├── celery/ → логирование Celery
│   │   ├── notifications/ → уведомления
│   │   └── ... (другие логи)
│   │
│   ├── archives/                          # Архивирование поставок
│   │   ├── router.py                      # API для работы с архивами
│   │   ├── archives.py                    # Archives сервис
│   │   ├── schema.py                      # Pydantic модели
│   │   └── data/ → хранилище ZIP архивов
│   │
│   ├── qr_scans/                          # QR-коды сканирования
│   │   └── data/ → хранилище данных QR
│   │
│   ├── images/                            # Загружаемые изображения
│   │   ├── router.py                      # API для работы с изображениями
│   │   ├── service.py                     # ImageService
│   │   ├── uploads/ → директория загрузок
│   │   └── schema.py
│   │
│   ├── cards/                             # Карточки товаров
│   │   ├── router.py
│   │   ├── cards.py                       # Работа с данными карточек
│   │   └── schema.py
│   │
│   ├── excel_data/                        # Работа с Excel файлами
│   │   ├── router.py
│   │   ├── service.py
│   │   ├── data/ → хранилище Excel файлов
│   │   └── schema.py
│   │
│   ├── available_quantity/                # Доступное количество товара
│   │   ├── router.py
│   │   ├── service.py
│   │   ├── repository.py
│   │   └── schema.py
│   │
│   ├── canceled_orders/                   # Отменённые заказы
│   │   ├── router.py
│   │   ├── repository.py
│   │   └── schema.py
│   │
│   ├── pdf_parser/                        # Парсинг PDF
│   │   └── router.py
│   │
│   ├── wild_logs/                         # Логирование по wild кодам
│   │   ├── router.py
│   │   └── schema.py
│   │
│   ├── one_time_tasks/                    # Одноразовые скрипты
│   │   └── ... (различные утилиты)
│   │
│   └── __pycache__/                       # Кэш Python
│
├── migrations/                            # SQL миграции для PostgreSQL
│   ├── add_fictitious_shipped_order_ids.sql
│   ├── add_operator_to_order_status_log.sql
│   ├── add_part_a_part_b_to_qr_scans.sql
│   ├── create_delivered_supplies_table.sql
│   ├── create_final_supplies_table.sql
│   ├── create_qr_scans_table.sql
│   ├── create_supply_operations_table.sql
│   ├── fix_order_id_type.sql
│   └── ... (другие миграции)
│
├── scripts/                               # Скрипты для запуска
│   └── ... (various startup scripts)
│
├── DOCUMENTATION/                         # 📚 Документация (создаётся)
│   └── (документы будут здесь)
│
├── docker-compose.yml                     # 🐳 Docker Compose конфигурация
├── Dockerfile                             # Docker образ приложения
├── requirements.txt                       # Python зависимости
├── .env                                   # Переменные окружения
├── .gitignore
│
└── README.md                              # Основная документация

```

### Критичные Файлы (🔴 отмечены выше)

| Файл | Строк | Роль | Критичность |
|------|-------|------|------------|
| `src/app.py` | 65 | Инициализация FastAPI, конфиг middleware, startup/shutdown | 🔴 КРИТИЧНЫЙ |
| `src/db.py` | 125 | DatabaseManager для asyncpg пула | 🔴 КРИТИЧНЫЙ |
| `src/settings.py` | 78 | Pydantic конфигурация из переменных окружения | 🔴 КРИТИЧНЫЙ |
| `src/supplies/supplies.py` | 5067 | SuppliesService - ОСНОВНАЯ ЛОГИКА | 🔴 КРИТИЧНЫЙ |
| `src/supplies/router.py` | 40KB | API endpoints поставок | 🔴 КРИТИЧНЫЙ |
| `src/cache/global_cache.py` | - | GlobalCache - производительность | 🔴 КРИТИЧНЫЙ |
| `src/middleware/duplicate_request.py` | - | DuplicateRequestMiddleware - защита | 🔴 КРИТИЧНЫЙ |
| `src/models/hanging_supplies.py` | - | HangingSupplies Repository | ⚠️ ВАЖНЫЙ |
| `src/wildberries_api/supplies.py` | 15KB | Интеграция WB API | ⚠️ ВАЖНЫЙ |

---

## Слои Приложения

### 1. API Layer (src/*/router.py)

**Ответственность**: HTTP обработка, валидация входных данных, статус коды

```python
# src/supplies/router.py - пример

@router.post("/supplies/create-tech-supply",
             status_code=status.HTTP_201_CREATED)
async def create_tech_supply(
    request: CreateTechSupplyRequest,
    current_user: dict = Depends(get_current_user),
    supplies_service: SuppliesService = Depends()
) -> CreateSupplyResponse:
    """
    Создание технологической поставки

    Args:
        request: Данные для создания поставки
        current_user: Текущий аутентифицированный пользователь
        supplies_service: Инъекция сервиса

    Returns:
        CreateSupplyResponse: ID и статус созданной поставки
    """
    return await supplies_service.create_tech_supply(
        account=current_user["account"],
        orders=request.order_ids
    )
```

**Ответственность router слоя**:
- ✅ Получение HTTP запроса
- ✅ Валидация Pydantic схемой
- ✅ Извлечение данных из request
- ✅ Вызов сервиса
- ✅ Формирование HTTP ответа
- ✅ Правильные статус коды

**Не входит в ответственность**:
- ❌ Бизнес-логика
- ❌ Доступ к БД
- ❌ Интеграции с внешними системами

### 2. Business Logic Layer (src/*/supplies.py, orders.py, *_service.py)

**Ответственность**: Бизнес-правила, валидация, координация операций

```python
# src/supplies/supplies.py - пример

class SuppliesService:
    """Основной сервис управления поставками"""

    async def create_tech_supply(
        self,
        account: str,
        orders: List[int]
    ) -> dict:
        """
        Создаёт техкруг и добавляет заказы

        Бизнес-логика:
        1. Валидирует заказы в WB API (can_add_to_supply_batch)
        2. Создаёт поставку в WB с префиксом ТЕХ_
        3. Добавляет заказы параллельно (asyncio.gather)
        4. Создаёт запись в supply_operations (для отслеживания)
        5. Логирует статусы в order_status_log
        6. Генерирует QR-коды
        """
        # Шаг 1: Валидация
        valid_orders = await self._validate_orders(orders)

        # Шаг 2: Создание в WB
        supply_id = await self._create_in_wb(account, prefix="ТЕХ_")

        # Шаг 3: Параллельное добавление
        results = await asyncio.gather(
            *[self._add_order_to_wb(supply_id, order)
              for order in valid_orders],
            return_exceptions=True
        )

        # Шаг 4: Логирование
        await self._log_operation(supply_id, results)

        return {"supply_id": supply_id, "status": "created"}
```

**Ответственность business logic слоя**:
- ✅ Валидация по бизнес-правилам
- ✅ Координация операций
- ✅ Обработка ошибок
- ✅ Вызов repository методов
- ✅ Интеграция с другими сервисами
- ✅ Логирование бизнес-событий

**Не входит в ответственность**:
- ❌ SQL запросы (делает repository)
- ❌ HTTP запросы в роутеры (делает API layer)
- ❌ Трансформация данных для ответов (делает API layer)

### 3. Data Access Layer (src/models/*.py)

**Ответственность**: Доступ к БД через SQL запросы

```python
# src/models/orders_wb.py - пример

class OrdersDB:
    """Repository для таблицы assembly_task_status"""

    def __init__(self, connection):
        self.connection = connection

    async def get_orders_by_ids(
        self,
        order_ids: List[int]
    ) -> List[dict]:
        """
        Получает заказы из БД по ID

        SQL:
        SELECT order_id, order_uid, article, nm_id, price, ...
        FROM assembly_task_status
        WHERE order_id = ANY($1)
        """
        query = """
            SELECT * FROM assembly_task_status
            WHERE order_id = ANY($1)
        """
        return await self.connection.fetch(query, order_ids)

    async def fetch_orders_for_supply(
        self,
        supply_id: str
    ) -> List[dict]:
        """Получает все заказы в поставке"""
        # SQL запрос...
```

**Ответственность data access слоя**:
- ✅ SQL запросы
- ✅ Управление transactions
- ✅ Маппинг строк БД на Python объекты
- ✅ Кэширование на уровне запросов

### 4. Infrastructure Layer

**Подкомпоненты**:

#### 4.1 Database Management (src/db.py)

```python
class DatabaseManager:
    """Управление пулом asyncpg соединений"""

    async def create_pool(
        self,
        min_size: int = 5,
        max_size: int = 15
    ):
        """Создание пула соединений"""
        self.pool = await asyncpg.create_pool(
            host=host, port=port,
            user=user, password=password,
            database=database,
            min_size=min_size,
            max_size=max_size
        )

    @asynccontextmanager
    async def connection(self):
        """Context manager для получения соединения"""
        async with self.pool.acquire() as connection:
            yield connection
```

**Особенности**:
- Три отдельных пула: основной (FastAPI) + два для Celery (orders + hanging_supplies)
- Connection timeout: 10 сек
- Statement timeout: 30 сек
- Connection lifetime: 1 час
- Connection idle time: 10 мин

#### 4.2 Cache Management (src/cache/global_cache.py)

```python
class GlobalCache:
    """Redis кэширование с graceful degradation"""

    async def connect(self):
        """Подключение к Redis"""
        self.redis_client = await redis.asyncio.from_url(
            f"redis://:{password}@{host}:{port}/{db}"
        )

    async def get(self, key: str) -> Optional[dict]:
        """Получение из кэша с graceful degradation"""
        if not self.is_connected:
            return None  # Fallback на прямой запрос

        cached = await self.redis_client.get(key)
        return pickle.loads(cached) if cached else None

    async def set(
        self,
        key: str,
        value: dict,
        ttl: int = 2400
    ):
        """Сохранение в кэш"""
        if not self.is_connected:
            return  # Graceful degradation

        serialized = pickle.dumps(value)
        await self.redis_client.setex(key, ttl, serialized)
```

#### 4.3 HTTP Clients (src/response.py)

```python
class AsyncHttpClient:
    """Асинхронный HTTP клиент с retry логикой"""

    def __init__(self, timeout: int = 120, retries: int = 8, delay: int = 61):
        self.timeout = timeout
        self.retries = retries
        self.delay = delay

    async def _make_request(self, method: str, url: str, **kwargs):
        """
        Retry логика:
        - До 8 попыток
        - 61 сек задержка между попытками
        - Всего до 8 минут на один запрос
        """
        for attempt in range(self.retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.request(method, url, **kwargs) as response:
                        response.raise_for_status()
                        return await response.text()
            except aiohttp.ClientError as e:
                logger.warning(f"Попытка {attempt + 1}: {e}")
                await asyncio.sleep(self.delay)
        return None
```

#### 4.4 Middleware Layer (src/middleware/duplicate_request.py)

```python
class DuplicateRequestMiddleware:
    """ASGI middleware для защиты от дублирующих запросов"""

    async def __call__(self, scope, receive, send):
        # Хеширование: path + token + body (исключая dynamic fields)
        lock_key = self._generate_lock_key(scope)

        # Попытка захватить lock в Redis (SETNX)
        acquired = await redis.setnx(lock_key, "locked", ex=120)

        if not acquired:
            # Дублирующий запрос - возвращаем ошибку
            return await send_error_response(
                "Operation already in progress",
                status_code=409
            )

        # Обработка запроса...
        # В finally: удаляем lock
```

### 5. Integration Layer (src/wildberries_api, src/supplies/integration_1c.py)

**Wildberries API Integration**:

```python
# src/wildberries_api/supplies.py

class Supplies:
    """Работа с поставками в WB API"""

    async def get_supplies(self, account: str) -> List[dict]:
        """GET /api/v3/supplies"""

    async def get_supply_orders(
        self,
        supply_id: str
    ) -> List[dict]:
        """
        Оптимизированный подход:
        1. GET /api/marketplace/v3/supplies/{id}/order-ids из WB API
        2. Fetch деталей из БД (быстро!)
        3. Fallback на WB API для отсутствующих
        """
        # Шаг 1: Получить ID заказов из WB
        order_ids = await self.get_supply_order_ids(supply_id)

        # Шаг 2: Получить данные из БД
        from_db = await OrdersDB.get_orders_by_ids(order_ids)

        # Шаг 3: Fallback на WB для отсутствующих
        missing_ids = set(order_ids) - {o["order_id"] for o in from_db}
        from_wb = await self._fetch_from_wb(missing_ids) if missing_ids else []

        return from_db + from_wb
```

**1C Integration**:

```python
# src/supplies/integration_1c.py

class OneCIntegration:
    """Отправка данных об отгрузке в 1C ERP"""

    async def send_shipment_of_goods(
        self,
        supplies_data: dict
    ) -> bool:
        """
        POST http://1c_routing_api:8002/api/shipment_of_goods/update

        Структура:
        {
            "account": "account_name",
            "wild_code": "12345",
            "orders": [
                {
                    "order_id": 123456,
                    "quantity": 1,
                    "delivery_type": "ФБС"
                }
            ]
        }
        """
```

---

## Жизненный Цикл Приложения

### Startup (src/app.py::startup)

```python
@app.on_event('startup')
async def startup() -> None:
    # 1️⃣ Подключение к БД (создание пула соединений)
    await check_db_connected()
    # → Создаёт 3 пула: основной + 2 для Celery

    # 2️⃣ Подключение к Redis
    await global_cache.connect()
    # → Проверяет доступность Redis

    # 3️⃣ Создание суперпользователя при первом запуске
    await create_initial_superuser()
    # → Создаёт пользователя из INIT_SUPERUSER_*

    # 4️⃣ Первоначальный прогрев кэша
    await global_cache.warm_up_cache()
    # → Загружает все поставки и заказы в Redis

    # 5️⃣ Запуск фонового обновления кэша
    await global_cache.start_background_refresh_all()
    # → Каждые 5 минут обновляет кэш
```

**Инициализация в порядке**:
1. **БД** → без неё приложение не работает
2. **Cache** → нужна для производительности
3. **Auth** → нужна для защиты эндпоинтов
4. **Кэш прогрев** → инициальная загрузка данных
5. **Фоновые задачи** → непрерывное обновление

### Shutdown (src/app.py::shutdown)

```python
@app.on_event('shutdown')
async def shutdown() -> None:
    # 1️⃣ Закрытие БД (закрытие пула)
    await check_db_disconnected()

    # 2️⃣ Отключение от Redis
    await global_cache.disconnect()
```

---

## Поток Данных

### Сценарий: Создание Поставки с Заказами

```
1. HTTP REQUEST
   ├─ POST /orders/with-supply-name
   ├─ Body: { "wild_code": "12345", "count": 10 }
   ├─ Headers: { "Authorization": "Bearer token..." }
   └─ Timeout: 30 сек

2. API LAYER (src/orders/router.py)
   ├─ FastAPI парсит request
   ├─ Pydantic валидирует schema
   ├─ get_current_user извлекает пользователя из JWT
   └─ → Вызывает OrdersService.create_supplies_with_orders()

3. BUSINESS LOGIC LAYER (src/orders/orders.py)
   ├─ OrdersService.create_supplies_with_orders()
   │  ├─ 1. Фильтрует заказы по wild коду и доступности
   │  ├─ 2. Группирует по wild кодам (group_orders_by_wild)
   │  ├─ 3. Для каждой группы вызывает SuppliesService
   │  └─ → Вызывает SuppliesService.create_and_add_orders()
   └─ Возвращает { "supply_ids": [...], "status": "created" }

4. BUSINESS LOGIC LAYER (src/supplies/supplies.py)
   └─ SuppliesService.create_and_add_orders()
      ├─ 1. Определяет тип поставки (техкруг или висячая)
      │     └─ Если stock_count = 0 → висячая, иначе техкруг
      │
      ├─ 2. Создаёт поставку в WB API (PATCH /api/v3/supplies)
      │     └─ Возвращает supply_id
      │
      ├─ 3. Добавляет заказы ПАРАЛЛЕЛЬНО (asyncio.gather)
      │     └─ Для каждого заказа: PATCH /api/v3/supplies/{id}/orders/{orderId}
      │     └─ Обрабатывает исключения (невалидные заказы → SHIPPED_WITH_BLOCK)
      │
      ├─ 4. Генерирует QR-коды (PDFService.create_sticker_pdf)
      │     └─ Для каждого заказа: 1 PDF страница со стикером
      │
      ├─ 5. Сохраняет в БД (repository методы)
      │     ├─ OrderStatusLog: логирование статусов
      │     ├─ SupplyOperations: запись о сессии
      │     └─ QRScanDB: сохранение QR-кодов (если техкруг)
      │
      ├─ 6. Отправляет в 1C (OneCIntegration.create_product_reservation)
      │     └─ Для висячих: резервирование товара
      │
      └─ Инвалидирует кэш: cache.invalidate(["supplies_all", "orders_all"])

5. DATA ACCESS LAYER (src/models/*.py)
   ├─ OrderStatusLog.insert(order_id, supply_id, status, ...)
   │  └─ INSERT INTO order_status_log VALUES (...)
   │
   ├─ SupplyOperations.create(operation_id, ...)
   │  └─ INSERT INTO supply_operations VALUES (...)
   │
   └─ QRScanDB.save_qr_codes(order_ids, qr_data)
      └─ INSERT INTO qr_scans (order_id, part_a, part_b) VALUES (...)

6. EXTERNAL INTEGRATION LAYER
   ├─ WB API: PATCH /api/v3/supplies
   │  └─ Возвращает созданный supply_id
   │
   └─ 1C API: POST /api/shipment_of_goods/create_reserve
      └─ Резервирует товар в 1C

7. HTTP RESPONSE
   ├─ Status: 201 Created
   ├─ Body: {
   │    "supply_id": "234567890",
   │    "status": "created",
   │    "orders_added": 10,
   │    "orders_failed": 0,
   │    "pdf_url": "/stickers/234567890.pdf"
   │  }
   └─ Timeout: 30 сек
```

### Асинхронный Поток: Синхронизация Заказов (Celery)

```
Celery Beat (каждые 65 минут)
├─ Trigger: sync_orders_periodic
│
└─ Task: src/celery_app/tasks/orders_sync.py::sync_orders_periodic
   │
   ├─ 1. Получить все аккаунты из tokens.json
   │
   ├─ 2. Для каждого аккаунта:
   │  │  ├─ GET /api/v3/orders/status (WB API)
   │  │  │  └─ Получить статусы всех заказов этого аккаунта
   │  │  │
   │  │  ├─ Обновить assembly_task_status (INSERT OR UPDATE)
   │  │  │  └─ UPDATE assembly_task_status SET ... WHERE order_id = ...
   │  │  │
   │  │  └─ Вызвать OrderStatusService для логирования
   │  │     └─ INSERT INTO order_status_log
   │  │
   │  └─ Параллельно обновлять кэш
   │     └─ Redis: SET supplies_all {новые данные}
   │
   └─ 3. Логирование в /logging/celery/orders_sync_{timestamp}.log
```

---

## Паттерны Проектирования

### 1. Dependency Injection (DI)

**FastAPI встроенная система DI**:

```python
# В router.py
async def create_supply(
    request: CreateSupplyRequest,
    current_user: dict = Depends(get_current_user),
    supplies_service: SuppliesService = Depends(),
    db_connection = Depends(get_db_connection)
) -> Response:
    """
    FastAPI автоматически:
    1. Вызывает get_current_user() → извлекает из JWT
    2. Создаёт SuppliesService() → инициализирует сервис
    3. Вызывает get_db_connection() → получает соединение из пула
    """
```

### 2. Repository Pattern

```python
# src/models/orders_wb.py
class OrdersDB:
    """Абстракция доступа к таблице assembly_task_status"""

    async def get_by_id(self, order_id: int) -> dict:
        """Получить заказ по ID"""

    async def get_by_ids(self, order_ids: List[int]) -> List[dict]:
        """Получить список заказов"""

    async def update_status(self, order_id: int, status: str) -> None:
        """Обновить статус заказа"""

# В service используем:
orders = await OrdersDB().get_by_ids(order_ids)
```

### 3. Service Locator Pattern (Dependency Container)

```python
# Celery tasks получают зависимости через dependency injection

async def sync_orders_periodic():
    # Celery автоматически создаёт все нужные объекты:
    db = await get_celery_orders_db()
    cache = await GlobalCache.get_instance()
    orders_service = OrdersService(db, cache)

    # Используем сервис
    await orders_service.sync_all_accounts()
```

### 4. Strategy Pattern

```python
# Различные стратегии создания поставок

class SuppliesService:

    async def create_and_add_orders(self, account, orders, stock_count):
        # Strategy выбирается в runtime
        if stock_count == 0:
            # Стратегия 1: создание висячей поставки
            return await self._create_hanging_supply(account, orders)
        else:
            # Стратегия 2: создание техкруга
            return await self._create_tech_supply(account, orders)
```

### 5. Observer Pattern (Celery Tasks)

```python
# Асинхронные "наблюдатели" отслеживают состояние системы

# Наблюдатель 1: каждые 65 мин
@celery_app.task
def sync_orders_periodic():
    """Синхронизирует заказы из WB"""

# Наблюдатель 2: по расписанию
@celery_app.task
def sync_hanging_supplies():
    """Синхронизирует висячие поставки"""

# Наблюдатель 3: в определённое время
@celery_app.task
def sync_update_available_quantity():
    """Обновляет доступное количество товара"""
```

### 6. Chain of Responsibility (Middleware)

```python
# Стек middleware обрабатывает запрос последовательно

Request
  ↓
CORSMiddleware (разрешить cross-origin)
  ↓
DuplicateRequestMiddleware (проверить дубли)
  ↓
AuthMiddleware (неявная через get_current_user)
  ↓
Router (обработать запрос)
  ↓
Response
```

### 7. Adapter Pattern

```python
# AsyncHttpClient адаптирует aiohttp к нашему интерфейсу

class AsyncHttpClient:
    """Адаптер для aiohttp с retry логикой"""

    async def get(self, url, headers=None):
        """Стандартный интерфейс GET запроса"""
        return await self._make_request("GET", url, headers=headers)

# Использование
client = AsyncHttpClient(retries=90, delay=61)
response = await client.get("https://api.wildberries.ru/...")
```

---

## Ключевые Компоненты (Детальный Разбор)

### GlobalCache (src/cache/global_cache.py)

**Назначение**: Кэширование часто используемых данных в Redis

**Ключи**:
- `supplies_all` → список всех поставок
- `orders_all` → список всех заказов
- `orders_by_account_{account}` → заказы конкретного аккаунта

**TTL**: 2400 секунд (40 минут) по умолчанию

**Refresh interval**: 1800 секунд (30 минут) для фонового обновления

**Graceful Degradation**:
```python
async def get(self, key: str):
    if not self.is_connected or not self.redis_client:
        logger.warning("Redis не подключен, пропускаем получение")
        return None  # Fallback → запросим из БД
```

### DuplicateRequestMiddleware (src/middleware/duplicate_request.py)

**Назначение**: Защита от двойного клика на критичных операциях

**Механизм**:
1. Генерирует lock_key = hash(path + token + body)
2. Пытается захватить lock в Redis (SETNX)
3. Если lock уже существует → возвращает 409 Conflict
4. Если lock захвачен → обрабатывает запрос
5. В finally → удаляет lock

**Защищённые endpoints**:
- `/api/v1/supplies/move-orders`
- `/api/v1/supplies/delivery`
- `/api/v1/supplies/delivery-hanging`
- `/api/v1/supplies/delivery-fictitious`
- `/api/v1/supplies/shipment_of_fictions`
- `/api/v1/supplies/shipment-hanging-actual`
- `/api/v1/orders/with-supply-name`

**Timeout**: 120 сек (долгие операции вроде move-orders могут занять время)

### SuppliesService (src/supplies/supplies.py - 5067 строк!)

**Это ЯДРО СИСТЕМЫ**

**Основные методы**:
- `create_tech_supply()` - создание техкруга
- `create_hanging_supply()` - создание висячей
- `create_and_add_orders()` - создание и добавление заказов
- `move_orders_between_supplies()` - перемещение между поставками
- `deliver_supply()` - реальная отгрузка
- `fictitious_delivery()` - фиктивная отгрузка
- `process_orders_from_hanging()` - отгрузка из висячей
- `get_list_supplies()` - получение списка (кэшируется)

**Критичные операции**:
1. Валидация в WB API перед любой операцией
2. Параллельное добавление заказов (asyncio.gather)
3. Обработка невалидных заказов (SHIPPED_WITH_BLOCK)
4. Логирование каждого шага
5. Инвалидация кэша после изменений

---

**Документация продолжается в следующем документе...**

Этот документ охватывает:
- ✅ Общую архитектуру
- ✅ Слои приложения
- ✅ Технический стек
- ✅ Структуру проекта
- ✅ Жизненный цикл
- ✅ Поток данных
- ✅ Паттерны проектирования

Продолжение будет включать:
- 📚 Детальную документацию каждого модуля
- 📊 Описание всех таблиц БД
- 🔌 API документацию (все endpoints)
- 🎯 Бизнес-логику по сценариям
- 🔧 Интеграции и конфигурацию
