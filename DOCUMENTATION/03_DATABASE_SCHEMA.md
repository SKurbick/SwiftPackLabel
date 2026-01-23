# Полная Документация БД и Таблиц

## Содержание

1. [Обзор БД](#обзор-бд)
2. [Таблицы Заказов](#таблицы-заказов)
3. [Таблицы Поставок](#таблицы-поставок)
4. [Таблицы Логирования](#таблицы-логирования)
5. [Таблицы QR-кодов](#таблицы-qr-кодов)
6. [Таблицы Аккаунтов](#таблицы-аккаунтов)
7. [Связи между Таблицами](#связи-между-таблицами)
8. [Индексы и Производительность](#индексы-и-производительность)

---

## Обзор БД

### Технические Параметры

- **DBMS**: PostgreSQL 12+
- **Driver**: asyncpg 0.30.0 (асинхронный)
- **Пул соединений**: min=5, max=15
- **Statement timeout**: 30 сек
- **Connection timeout**: 10 сек

### Общая структура

```
┌─────────────────────────────────────────────────┐
│         ЗАКАЗЫ (Orders)                         │
│ assembly_task_status - основная таблица        │
│ canceled_orders - отменённые                    │
│ available_quantity - доступное количество       │
└─────────────────────────────────────────────────┘
              ↓ связь ↓
┌─────────────────────────────────────────────────┐
│    ПОСТАВКИ (Supplies)                          │
│ Три типа: техкруги, висячие, финальные        │
│ hanging_supplies, final_supplies, ...           │
└─────────────────────────────────────────────────┘
              ↓ связь ↓
┌─────────────────────────────────────────────────┐
│      ЛОГИРОВАНИЕ И ИСТОРИЯ (Logs)              │
│ order_status_log - история статусов             │
│ shipment_of_goods - журнал отгрузок            │
│ supply_operations - сессии операций             │
│ onec_delivery_log - интеграция с 1C            │
└─────────────────────────────────────────────────┘
```

---

## Таблицы Заказов

### 1. assembly_task_status (КРИТИЧНАЯ!)

**Назначение**: Основная таблица со всеми заказами из WB API

**Описание**: Кэш статусов заказов из Wildberries. Обновляется каждые 65 минут через Celery задачу `sync_orders_periodic`.

```sql
CREATE TABLE assembly_task_status (
    order_id BIGINT PRIMARY KEY,           -- ID заказа (уникален)
    order_uid VARCHAR NOT NULL,            -- UUID заказа в WB
    article VARCHAR NOT NULL,              -- SKU товара
    nm_id BIGINT NOT NULL,                 -- Артикул WB
    price DECIMAL(12,2),                   -- Цена товара (рубли)
    delivery_type VARCHAR,                 -- Тип доставки (FBS, FBO, ...)
    status VARCHAR NOT NULL,               -- Статус: awaiting_assembly, shipped, ...
    supply_id VARCHAR,                     -- ID поставки (если добавлен)
    created_at TIMESTAMP DEFAULT NOW(),    -- Когда заказ создан в WB
    updated_at TIMESTAMP DEFAULT NOW(),    -- Последнее обновление

    -- Индексы для оптимизации
    INDEX idx_nm_id (nm_id),
    INDEX idx_status (status),
    INDEX idx_supply_id (supply_id),
    INDEX idx_created_at (created_at)
);
```

**Основные поля**:

| Поле | Тип | Описание |
|------|-----|---------|
| `order_id` | BIGINT | PRIMARY KEY - уникальный ID заказа |
| `order_uid` | VARCHAR | UUID заказа в WB системе |
| `article` | VARCHAR | SKU товара (артикул продавца) |
| `nm_id` | BIGINT | Артикул Wildberries (номенклатурный номер) |
| `price` | DECIMAL | Цена товара в рублях |
| `delivery_type` | VARCHAR | Тип: FBS, FBO, FULFILLMENT и т.д. |
| `status` | VARCHAR | awaiting_assembly, shipped, lost, cancelled и т.д. |
| `supply_id` | VARCHAR | ID поставки если заказ добавлен |
| `created_at` | TIMESTAMP | Время создания заказа в WB |
| `updated_at` | TIMESTAMP | Время последнего обновления |

**Использование в коде**:

```python
# src/models/orders_wb.py
class OrdersDB:
    async def get_orders_by_ids(order_ids: List[int]) -> List[dict]:
        query = """
            SELECT * FROM assembly_task_status
            WHERE order_id = ANY($1)
        """
        return await connection.fetch(query, order_ids)
```

**Обновление данных**:

```python
# src/celery_app/tasks/orders_sync.py - каждые 65 минут
@celery_app.task
async def sync_orders_periodic():
    # 1. Получить статусы от WB API
    orders_from_wb = await wildberries_orders.get_orders_statuses(token, all_orders)

    # 2. UPDATE в БД (INSERT OR UPDATE)
    for order in orders_from_wb:
        await db.execute("""
            INSERT INTO assembly_task_status (order_id, order_uid, status, ...)
            VALUES ($1, $2, $3, ...)
            ON CONFLICT (order_id) DO UPDATE SET
                status = $3, updated_at = NOW()
        """, order.id, order.uid, order.status, ...)
```

**Производительность**:
- Размер: ~100K-1M строк (в зависимости от кол-ва аккаунтов и времени жизни)
- Основной индекс: `PRIMARY KEY (order_id)`
- Дополнительные индексы: `idx_nm_id`, `idx_status`, `idx_supply_id`
- Типичный запрос: ~50ms для 100 заказов

### 2. canceled_orders

**Назначение**: Архив отменённых заказов

```sql
CREATE TABLE canceled_orders (
    order_id BIGINT PRIMARY KEY,
    reason VARCHAR,                        -- Причина отмены
    canceled_at TIMESTAMP DEFAULT NOW(),

    INDEX idx_canceled_at (canceled_at)
);
```

**Использование**: Отслеживание отменённых заказов для предотвращения их добавления в поставки

### 3. available_quantity

**Назначение**: Доступное количество товара (синхронизируется с 1C)

```sql
CREATE TABLE available_quantity (
    nm_id BIGINT PRIMARY KEY,              -- Артикул WB
    quantity INT NOT NULL DEFAULT 0,       -- Доступное количество на складе
    reserved INT DEFAULT 0,                -- Зарезервировано
    shipped INT DEFAULT 0,                 -- Отгружено
    last_sync TIMESTAMP DEFAULT NOW(),

    INDEX idx_quantity (quantity)
);
```

**Логика**:
- Используется при создании поставок (определяет тип: техкруг или висячая)
- `available_quantity > 0` → техкруг (товар в наличии)
- `available_quantity = 0` → висячая (товар ждём)

---

## Таблицы Поставок

### 1. hanging_supplies (КРИТИЧНАЯ - JSONB!)

**Назначение**: Висячие поставки (для товаров которых нет на складе)

```sql
CREATE TABLE hanging_supplies (
    supply_id VARCHAR PRIMARY KEY,
    account VARCHAR NOT NULL,              -- Аккаунт пользователя
    order_data JSONB NOT NULL,             -- JSONB с полными данными заказов
    fictitious_shipped_order_ids JSONB,    -- JSONB с ID фиктивно отгруженных
    is_fictitious_delivered BOOLEAN DEFAULT false,
    changes_log JSONB DEFAULT '[]'::jsonb, -- JSONB история изменений
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    operator VARCHAR,                      -- Кто создал

    INDEX idx_account (account),
    INDEX idx_created_at (created_at)
);
```

**Структура JSONB полей**:

```json
// order_data - полные данные заказов
{
  "orders": [
    {
      "order_id": 123456,
      "order_uid": "uuid",
      "article": "SKU123",
      "nm_id": 12345678,
      "price": 1000.00,
      "delivery_type": "FBS",
      "quantity": 1
    }
  ],
  "total_orders": 1,
  "total_price": 1000.00
}

// fictitious_shipped_order_ids - массив ID
[123456, 234567, 345678]

// changes_log - история изменений
[
  {
    "timestamp": "2025-01-23T10:00:00",
    "operator": "user123",
    "action": "created",
    "details": "Created hanging supply"
  },
  {
    "timestamp": "2025-01-23T11:00:00",
    "operator": "user123",
    "action": "fictitious_delivery",
    "details": "Marked as fictitiously delivered"
  }
]
```

**Использование**:

```python
# src/models/hanging_supplies.py
class HangingSupplies:
    async def create(supply_id: str, account: str, order_data: dict):
        query = """
            INSERT INTO hanging_supplies (supply_id, account, order_data, created_at, operator)
            VALUES ($1, $2, $3, NOW(), $4)
        """
        await connection.execute(query, supply_id, account, json.dumps(order_data), operator)

    async def update_fictitious_delivery(supply_id: str, order_ids: List[int]):
        query = """
            UPDATE hanging_supplies
            SET fictitious_shipped_order_ids = $1,
                is_fictitious_delivered = true,
                updated_at = NOW()
            WHERE supply_id = $2
        """
        await connection.execute(query, json.dumps(order_ids), supply_id)
```

**Особенности**:
- JSONB позволяет хранить произвольную структуру
- Можно индексировать по полям внутри JSONB (например: `order_data -> 'orders'`)
- Используется для отслеживания висячих поставок в полной мере

### 2. final_supplies

**Назначение**: Финальные поставки (целевые поставки для отгрузки)

```sql
CREATE TABLE final_supplies (
    supply_id VARCHAR PRIMARY KEY,
    account VARCHAR NOT NULL,              -- Аккаунт пользователя
    supply_name VARCHAR NOT NULL,          -- Имя (обязательно содержит _ФИНАЛ)
    done BOOLEAN DEFAULT false,            -- Отгружена ли
    created_at TIMESTAMP DEFAULT NOW(),
    delivered_at TIMESTAMP,                -- Когда отгружена

    INDEX idx_account (account),
    INDEX idx_done (done),
    UNIQUE(account, done)                  -- Только одна активная на аккаунт
);
```

**Логика**:
- Максимум ОДНА активная (done=false) финальная поставка на аккаунт
- При отгрузке (done=true) создаётся новая финальная поставка
- Переиспользуется если существует активная

### 3. delivered_supplies (АРХИВ - НЕИЗМЕНЯЕМЫЙ!)

**Назначение**: Архив доставленных поставок (неизменяемое хранилище)

```sql
CREATE TABLE delivered_supplies (
    supply_id VARCHAR PRIMARY KEY,
    account VARCHAR NOT NULL,              -- Аккаунт пользователя
    supply_data JSONB NOT NULL,            -- Полная структура поставки
    delivered_at TIMESTAMP DEFAULT NOW(),
    operator VARCHAR,                      -- Кто отгрузил

    INDEX idx_account (account),
    INDEX idx_delivered_at (delivered_at)
);
```

**Структура supply_data (JSONB)**:

```json
{
  "supply_id": "234567890",
  "supply_name": "ТЕХ_20250123",
  "type": "technical",
  "orders": [
    {
      "order_id": 123456,
      "article": "SKU123",
      "nm_id": 12345678,
      "price": 1000.00,
      "quantity": 1
    }
  ],
  "total_orders": 10,
  "total_price": 10000.00,
  "created_at": "2025-01-23T10:00:00",
  "delivered_at": "2025-01-23T14:30:00"
}
```

**Особенность**: `ON CONFLICT DO NOTHING` - это архив, данные неизменяемы

```python
# src/models/delivered_supplies.py
async def create(supply_id: str, account: str, supply_data: dict):
    query = """
        INSERT INTO delivered_supplies (supply_id, account, supply_data, operator)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (supply_id) DO NOTHING
    """
    await connection.execute(query, supply_id, account, json.dumps(supply_data), operator)
```

---

## Таблицы Логирования

### 1. order_status_log (ЛОГИРОВАНИЕ СТАТУСОВ)

**Назначение**: Полная история статусов каждого заказа

```sql
CREATE TABLE order_status_log (
    id SERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL,              -- Ссылка на заказ
    supply_id VARCHAR,                     -- Ссылка на поставку
    status VARCHAR NOT NULL,               -- NEW, IN_TECHNICAL_SUPPLY, IN_HANGING_SUPPLY, DELIVERED, SHIPPED_WITH_BLOCK
    operator VARCHAR,                      -- Кто выполнил операцию
    details JSONB,                         -- Дополнительные детали
    created_at TIMESTAMP DEFAULT NOW(),

    INDEX idx_order_id (order_id),
    INDEX idx_supply_id (supply_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
);
```

**Возможные статусы**:

| Статус | Описание |
|--------|---------|
| `NEW` | Новый заказ из WB API |
| `IN_TECHNICAL_SUPPLY` | Добавлен в техкруг |
| `IN_HANGING_SUPPLY` | Добавлен в висячую поставку |
| `SHIPPED_WITH_BLOCK` | Невозможно добавить (заказ в неправильном статусе) |
| `DELIVERED` | Доставлено |

**Использование**:

```python
# src/orders/order_status_service.py
class OrderStatusService:
    async def process_and_log_new_orders(orders: List[int]):
        for order_id in orders:
            await db.execute("""
                INSERT INTO order_status_log (order_id, status, created_at)
                VALUES ($1, 'NEW', NOW())
            """, order_id)

    async def process_and_log_orders_in_supplies(supply_id: str, status: str):
        await db.execute("""
            INSERT INTO order_status_log (order_id, supply_id, status, created_at)
            SELECT order_id, $1, $2, NOW()
            FROM jsonb_to_recordset(
                (SELECT order_data FROM hanging_supplies WHERE supply_id = $1)
            ) AS x(order_id BIGINT)
        """, supply_id, status)
```

### 2. shipment_of_goods (ЖУРНАЛ ОТГРУЗОК)

**Назначение**: Запись всех отгрузок для интеграции с 1C

```sql
CREATE TABLE shipment_of_goods (
    id SERIAL PRIMARY KEY,
    account VARCHAR NOT NULL,              -- Аккаунт
    wild_code VARCHAR NOT NULL,            -- Wild код (для 1C)
    order_id BIGINT NOT NULL,              -- Заказ
    quantity INT NOT NULL DEFAULT 1,       -- Количество
    delivery_type VARCHAR,                 -- Тип доставки
    shipped_date TIMESTAMP DEFAULT NOW(),  -- Дата отгрузки
    operator VARCHAR,                      -- Оператор

    INDEX idx_account (account),
    INDEX idx_wild_code (wild_code),
    INDEX idx_order_id (order_id),
    INDEX idx_shipped_date (shipped_date)
);
```

**Использование**: Интеграция с 1C (отправка в 1C API для учёта товара)

### 3. supply_operations (СЕССИИ ОПЕРАЦИЙ)

**Назначение**: Отслеживание каждой операции создания/изменения поставки

```sql
CREATE TABLE supply_operations (
    operation_id VARCHAR PRIMARY KEY,      -- UUID сессии
    user_id INT,                           -- ID пользователя
    supply_name VARCHAR NOT NULL,          -- Имя созданной поставки
    status VARCHAR NOT NULL,               -- success, error, partial
    request_payload JSONB,                 -- Полный запрос пользователя
    response_data JSONB,                   -- Результат операции
    error_message TEXT,                    -- Если была ошибка
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    INDEX idx_user_id (user_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
);
```

**Структура JSONB**:

```json
// request_payload
{
  "account": "account_name",
  "order_ids": [123456, 234567],
  "wild_code": "12345"
}

// response_data
{
  "supply_id": "234567890",
  "orders_added": 2,
  "orders_failed": 0,
  "pdf_url": "/stickers/234567890.pdf"
}
```

**Использование**: `GET /orders/sessions` - получение истории операций

### 4. onec_delivery_log (ЛОГИРОВАНИЕ 1C)

**Назначение**: Логирование всех отправок в 1C систему

```sql
CREATE TABLE onec_delivery_log (
    id SERIAL PRIMARY KEY,
    supply_id VARCHAR,                     -- Какую поставку отправляли
    operation_type VARCHAR,                -- shipment, reservation, ...
    request_data JSONB,                    -- Данные которые отправили
    response_data JSONB,                   -- Ответ от 1C
    status VARCHAR,                        -- success, error, timeout
    error_message TEXT,                    -- Если ошибка
    sent_at TIMESTAMP DEFAULT NOW(),

    INDEX idx_supply_id (supply_id),
    INDEX idx_status (status),
    INDEX idx_sent_at (sent_at)
);
```

---

## Таблицы QR-кодов

### qr_scans (QR-КОДЫ ЗАКАЗОВ)

**Назначение**: Хранение QR-кодов для каждого заказа (поддержка двух форматов)

```sql
CREATE TABLE qr_scans (
    order_id BIGINT PRIMARY KEY,           -- Ссылка на заказ
    part_a VARCHAR,                        -- Часть A QR (если разделённый формат)
    part_b VARCHAR,                        -- Часть B QR (если разделённый формат)
    qr_data VARCHAR,                       -- Полный barcode QR (если есть)
    created_at TIMESTAMP DEFAULT NOW(),

    INDEX idx_part_a (part_a),
    INDEX idx_part_b (part_b),
    INDEX idx_qr_data (qr_data)
);
```

**Поддерживаемые форматы**:

```
1. Barcode (полный QR):
   - qr_data: "*CN+tGIpw" (или другой полный QR)
   - Это полный QR-код из WB, уникален для каждого заказа

2. Part A + Part B (разделённый):
   - part_a: "010" (первая часть)
   - part_b: "312..." (вторая часть без разделителя!)
   - Используется когда QR разделён на две части
```

**Использование**:

```python
# src/models/qr_scan_db.py
class QRScanDB:
    async def fetch_orders_by_qr_codes(qr_codes: List[str]) -> List[int]:
        """
        Резолвинг QR-кодов в order_ids

        Логика:
        1. Для каждого QR разобрать формат (barcode или part_a+part_b)
        2. SELECT order_id FROM qr_scans WHERE qr_data = $1 OR (part_a = $2 AND part_b = $3)
        3. Вернуть список order_ids
        """

    async def save_qr_codes(order_data: List[dict]) -> None:
        """Сохранить QR-коды для заказов"""
        for order in order_data:
            await db.execute("""
                INSERT INTO qr_scans (order_id, part_a, part_b, qr_data)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (order_id) DO UPDATE SET
                    part_a = $2, part_b = $3, qr_data = $4
            """, order.id, order.part_a, order.part_b, order.barcode)
```

---

## Таблицы Аккаунтов

### supply_account_mapping

**Назначение**: Маппинг поставок на аккаунты

```sql
CREATE TABLE supply_account_mapping (
    supply_id VARCHAR PRIMARY KEY,
    account VARCHAR NOT NULL,

    INDEX idx_account (account)
);
```

---

## Связи между Таблицами

### ER диаграмма

```
┌─────────────────────────────────────────────┐
│     assembly_task_status                    │
│ (Заказы из WB)                             │
├─────────────────────────────────────────────┤
│ order_id (PK)                               │
│ article, nm_id, price, status               │
│ supply_id (FK)                              │
└────────────┬────────────────────────────────┘
             │ 1:N
             │
    ┌────────┴────────┬──────────────────┐
    │                 │                  │
    ▼                 ▼                  ▼
┌────────────┐ ┌──────────────┐ ┌──────────────┐
│ hanging_   │ │ final_       │ │ delivered_   │
│ supplies   │ │ supplies     │ │ supplies     │
│ (JSONB)    │ │              │ │ (JSONB)      │
└────────────┘ └──────────────┘ └──────────────┘
    │                 │                  │
    └─────────────────┼──────────────────┘
                      │ 1:1
                      │
            ┌─────────▼──────────┐
            │ order_status_log   │
            │ (История статусов) │
            └────────────────────┘
```

### Основные связи

1. **assembly_task_status → hanging_supplies/final_supplies**
   - Foreign key: `supply_id`
   - Один заказ добавляется в одну поставку

2. **assembly_task_status → order_status_log**
   - Foreign key: `order_id`
   - Один заказ может иметь множество записей логирования (история)

3. **supply_orders → shipment_of_goods**
   - Foreign key: `order_id`
   - Каждый отгруженный заказ записывается в журнал

4. **hanging_supplies/final_supplies → supply_operations**
   - Foreign key: `supply_id`
   - Одна операция может создать одну поставку

---

## Индексы и Производительность

### Критичные индексы

```sql
-- assembly_task_status
PRIMARY KEY (order_id)                     -- Быстрый поиск по ID
CREATE INDEX idx_nm_id ON assembly_task_status(nm_id);           -- Поиск по товару
CREATE INDEX idx_status ON assembly_task_status(status);         -- Фильтр по статусу
CREATE INDEX idx_supply_id ON assembly_task_status(supply_id);   -- Заказы в поставке
CREATE INDEX idx_created_at ON assembly_task_status(created_at); -- Фильтр по времени

-- order_status_log
CREATE INDEX idx_order_id ON order_status_log(order_id);         -- История заказа
CREATE INDEX idx_status ON order_status_log(status);             -- Фильтр по статусу
CREATE INDEX idx_created_at ON order_status_log(created_at);     -- Лента по времени

-- hanging_supplies
CREATE INDEX idx_account ON hanging_supplies(account);           -- Поставки аккаунта
CREATE INDEX idx_created_at ON hanging_supplies(created_at);     -- По времени создания

-- qr_scans
CREATE INDEX idx_part_a ON qr_scans(part_a);                     -- Поиск по QR A
CREATE INDEX idx_part_b ON qr_scans(part_b);                     -- Поиск по QR B
CREATE INDEX idx_qr_data ON qr_scans(qr_data);                   -- Поиск по barcode
```

### Типичные запросы и их производительность

```python
# 1. Получить заказы в поставке (~50ms)
SELECT * FROM assembly_task_status
WHERE supply_id = 'supply_123'
# Использует индекс: idx_supply_id

# 2. Получить историю заказа (~10ms)
SELECT * FROM order_status_log
WHERE order_id = 123456
ORDER BY created_at DESC
# Использует индекс: idx_order_id

# 3. Фильтр заказов по статусу (~100ms для 1M)
SELECT * FROM assembly_task_status
WHERE status = 'awaiting_assembly'
# Использует индекс: idx_status

# 4. Поиск QR-кода (~5ms)
SELECT order_id FROM qr_scans
WHERE qr_data = 'barcode_value'
# Использует индекс: idx_qr_data

# 5. Резолвинг QR в order_id (100ms для batch)
SELECT order_id FROM qr_scans
WHERE qr_data = ANY($1)
  OR (part_a = ANY($2) AND part_b = ANY($3))
# Использует индексы: idx_qr_data, idx_part_a, idx_part_b
```

### Оптимизация JSONB запросов

```sql
-- Получить order_id из JSONB массива
SELECT jsonb_to_recordset(
    (SELECT order_data FROM hanging_supplies WHERE supply_id = 'supply_123')
    ->'orders'
) AS x(order_id BIGINT)

-- Индексирование JSONB поля для быстрого поиска
CREATE INDEX idx_order_ids ON hanging_supplies USING GIN (order_data);

-- Поиск в JSONB
SELECT * FROM hanging_supplies
WHERE order_data @> '{"orders":[{"order_id":123456}]}'
```

---

## Миграции и Версионирование

### SQL Миграции

```
/migrations/
├── create_final_supplies_table.sql
├── create_qr_scans_table.sql
├── create_delivered_supplies_table.sql
├── create_supply_operations_table.sql
├── add_fictitious_shipped_order_ids.sql
├── add_operator_to_order_status_log.sql
├── add_part_a_part_b_to_qr_scans.sql
└── fix_order_id_type.sql
```

### Процесс миграции

```python
# При развёртывании:
1. Применить все миграции (alembic или manual)
2. Создать индексы
3. Инициализировать суперпользователя
4. Прогреть кэш из БД
```

---

## Примеры SQL запросов

### 1. Получить все заказы в техкруге

```sql
SELECT o.* FROM assembly_task_status o
WHERE o.supply_id = 'supply_123'
  AND o.status IN ('awaiting_assembly', 'in_process')
ORDER BY o.created_at;
```

### 2. История заказа

```sql
SELECT * FROM order_status_log
WHERE order_id = 123456
ORDER BY created_at DESC;
```

### 3. Статистика по аккаунту

```sql
SELECT
    account,
    COUNT(*) as total_orders,
    COUNT(CASE WHEN status = 'delivered' THEN 1 END) as delivered,
    COUNT(CASE WHEN status = 'awaiting_assembly' THEN 1 END) as pending
FROM assembly_task_status
GROUP BY account;
```

### 4. Найти невалидные заказы

```sql
SELECT * FROM order_status_log
WHERE status = 'SHIPPED_WITH_BLOCK'
  AND created_at > NOW() - INTERVAL '24 hours'
ORDER BY created_at DESC;
```

### 5. Статистика висячих поставок

```sql
SELECT
    account,
    COUNT(*) as hanging_count,
    COUNT(CASE WHEN is_fictitious_delivered THEN 1 END) as fictitiously_delivered
FROM hanging_supplies
GROUP BY account;
```

---

## Резервная копия и восстановление

### Backup

```bash
# Полная резервная копия
pg_dump -h localhost -U postgres -d swift_pack_label > backup.sql

# Compressed backup
pg_dump -h localhost -U postgres -d swift_pack_label | gzip > backup.sql.gz

# Только данные
pg_dump -h localhost -U postgres --data-only -d swift_pack_label > data_backup.sql
```

### Restore

```bash
# Из SQL файла
psql -h localhost -U postgres -d swift_pack_label < backup.sql

# Из compressed файла
gunzip -c backup.sql.gz | psql -h localhost -U postgres -d swift_pack_label
```

---

**Документация по БД завершена!**

Этот документ охватывает:
- ✅ Все основные таблицы (11 таблиц)
- ✅ JSONB структуры
- ✅ Индексы и производительность
- ✅ Связи между таблицами
- ✅ Примеры SQL запросов
- ✅ Оптимизацию
- ✅ Миграции
