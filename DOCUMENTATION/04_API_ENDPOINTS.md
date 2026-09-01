# Полная API Документация

## Содержание

1. [Базовая Информация](#базовая-информация)
2. [Аутентификация](#аутентификация)
3. [Supplies API](#supplies-api)
4. [Orders API](#orders-api)
5. [QR Parser API](#qr-parser-api)
6. [Cache API](#cache-api)
7. [Archives API](#archives-api)
8. [Примеры Использования](#примеры-использования)

---

## Базовая Информация

### API Version
```
/api/v1
```

### Base URL
```
http://localhost:8301/api/v1
```

### Content-Type
```
application/json
```

### Authentication
```
Authorization: Bearer {JWT_TOKEN}
```

### Response Format

**Успешный ответ**:
```json
{
  "status": "success",
  "data": { ... }
}
```

**Ошибка**:
```json
{
  "status": "error",
  "error_code": "VALIDATION_ERROR",
  "message": "Description of error",
  "details": { ... }
}
```

### HTTP Status Codes

| Code | Описание |
|------|----------|
| 200 | OK - успешно |
| 201 | Created - ресурс создан |
| 400 | Bad Request - ошибка валидации |
| 401 | Unauthorized - не аутентифицирован |
| 403 | Forbidden - не авторизирован |
| 409 | Conflict - дублирующий запрос |
| 500 | Internal Server Error - ошибка сервера |

---

## Аутентификация

### POST /auth/login

**Назначение**: Получить JWT токен

**Метод**: POST

**URL**: `/api/v1/auth/login`

**Request Body**:
```json
{
  "username": "user@example.com",
  "password": "password123"
}
```

**Response (200 OK)**:
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer",
  "expires_in": 1800
}
```

**Curl пример**:
```bash
curl -X POST http://localhost:8301/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "username": "user@example.com",
    "password": "password123"
  }'
```

### POST /auth/users

**Назначение**: Создать нового пользователя

**Метод**: POST

**URL**: `/api/v1/auth/users`

**⚠️ Требует**: Superuser роль

**Request Body**:
```json
{
  "username": "newuser",
  "email": "newuser@example.com",
  "password": "securepassword123",
  "is_superuser": false
}
```

**Response (201 Created)**:
```json
{
  "user_id": 2,
  "username": "newuser",
  "email": "newuser@example.com",
  "created_at": "2025-01-23T10:00:00"
}
```

---

## Supplies API

### GET /supplies

**Назначение**: Получить все поставки

**Метод**: GET

**URL**: `/api/v1/supplies`

**Параметры**: нет

**Headers**:
```
Authorization: Bearer {token}
```

**Response (200 OK)**:
```json
{
  "supplies": [
    {
      "supply_id": "234567890",
      "supply_name": "ТЕХ_20250123",
      "account": "account_name",
      "status": "new",
      "type": "technical",
      "orders_count": 10,
      "total_price": 10000.00,
      "created_at": "2025-01-23T10:00:00"
    },
    {
      "supply_id": "345678901",
      "supply_name": "HANGING_20250123",
      "account": "account_name",
      "status": "pending",
      "type": "hanging",
      "orders_count": 5,
      "is_fictitious_delivered": false,
      "created_at": "2025-01-23T11:00:00"
    }
  ]
}
```

**Curl пример**:
```bash
curl -X GET http://localhost:8301/api/v1/supplies \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
```

---

### POST /supplies/create-tech-supply

**Назначение**: Создать технологическую поставку (Техкруг)

**Метод**: POST

**URL**: `/api/v1/supplies/create-tech-supply`

**Request Body**:
```json
{
  "account": "account_name",
  "order_ids": [123456, 234567, 345678]
}
```

**Response (201 Created)**:
```json
{
  "supply_id": "234567890",
  "supply_name": "ТЕХ_20250123",
  "status": "created",
  "type": "technical",
  "orders_added": 3,
  "orders_failed": 0,
  "pdf_url": "/stickers/234567890.pdf"
}
```

**Возможные ошибки**:

| Код | Описание |
|-----|----------|
| INVALID_ORDERS | Некоторые заказы невалидны |
| ACCOUNT_NOT_FOUND | Аккаунт не существует |
| WB_API_ERROR | Ошибка в WB API |

**Curl пример**:
```bash
curl -X POST http://localhost:8301/api/v1/supplies/create-tech-supply \
  -H "Authorization: Bearer {token}" \
  -H "Content-Type: application/json" \
  -d '{
    "account": "account_name",
    "order_ids": [123456, 234567, 345678]
  }'
```

---

### POST /supplies/create-hanging-supply

**Назначение**: Создать висячую поставку

**Метод**: POST

**URL**: `/api/v1/supplies/create-hanging-supply`

**Request Body**:
```json
{
  "account": "account_name",
  "order_ids": [123456, 234567]
}
```

**Response (201 Created)**:
```json
{
  "supply_id": "345678901",
  "supply_name": "HANGING_20250123",
  "status": "created",
  "type": "hanging",
  "orders_count": 2
}
```

---

### POST /supplies/move-orders

**Назначение**: Перемещение заказов между поставками

**Метод**: POST

**URL**: `/api/v1/supplies/move-orders`

**⚠️ Защита от дублей**: 120 сек (DuplicateRequestMiddleware)

**Request Body**:
```json
{
  "from_supply_id": "234567890",
  "to_supply_id": "345678901",
  "order_ids": [123456, 234567]
}
```

**Response (200 OK)**:
```json
{
  "orders_moved": 2,
  "orders_failed": 0,
  "failed_orders": []
}
```

**Response (200 OK) с ошибками**:
```json
{
  "orders_moved": 1,
  "orders_failed": 1,
  "failed_orders": [
    {
      "order_id": 234567,
      "reason": "Order already shipped"
    }
  ]
}
```

---

### POST /supplies/move-orders-by-qr (НОВОЕ!)

**Назначение**: Перемещение заказов по QR-кодам

**Метод**: POST

**URL**: `/api/v1/supplies/move-orders-by-qr`

**Request Body**:
```json
{
  "to_supply_id": "345678901",
  "qr_codes": [
    {
      "format": "barcode",
      "data": "*CN+tGIpw"
    },
    {
      "format": "part_ab",
      "part_a": "010",
      "part_b": "312"
    }
  ]
}
```

**Response (200 OK)**:
```json
{
  "orders_moved": 2,
  "orders_failed": 0,
  "qr_codes_resolved": 2
}
```

**Поддерживаемые форматы**:
- `barcode`: полный QR из WB (например: `*CN+tGIpw`)
- `part_ab`: разделённый формат (part_a + part_b без разделителя)

---

### POST /supplies/delivery

**Назначение**: РЕАЛЬНАЯ ОТГРУЗКА поставки

**Метод**: POST

**URL**: `/api/v1/supplies/delivery`

**⚠️ КРИТИЧНО**: Невозможно отменить!

**⚠️ Защита от дублей**: 120 сек (DuplicateRequestMiddleware)

**Request Body**:
```json
{
  "supply_id": "234567890"
}
```

**Response (200 OK)**:
```json
{
  "delivery_id": "delivery_123456",
  "status": "delivered",
  "orders_delivered": 10,
  "new_final_supply_id": "234567891"
}
```

**Процесс**:
1. PATCH /api/v3/supplies/{supply_id}/deliver в WB API
2. Отправка в 1C (OneCIntegration)
3. Создание новой финальной поставки
4. Логирование в shipment_of_goods

---

### PATCH /supplies/delivery-fictitious

**Назначение**: Фиктивная отгрузка (для висячих поставок)

**Метод**: PATCH

**URL**: `/api/v1/supplies/delivery-fictitious`

**⚠️ Защита от дублей**: 120 сек

**Request Body**:
```json
{
  "supply_id": "345678901"
}
```

**Response (200 OK)**:
```json
{
  "delivery_id": "fictitious_123456",
  "status": "fictitiously_delivered",
  "orders_marked": 5
}
```

**Что происходит**:
- Заказы маркируются как "фиктивно отгруженные"
- В WB API показывают статус "доставлено"
- Отправляется в 1C
- Продление срока на 120 часов

---

### POST /supplies/delivery-hanging-actual

**Назначение**: Отгрузка из висячей в реальную факт-поставку

**Метод**: POST

**URL**: `/api/v1/supplies/delivery-hanging-actual`

**⚠️ Защита от дублей**: 120 сек

**Request Body**:
```json
{
  "hanging_supply_id": "345678901",
  "orders_to_deliver": [123456, 234567]
}
```

**Response (200 OK)**:
```json
{
  "fact_supply_id": "234567890",
  "status": "delivered",
  "orders_delivered": 2
}
```

---

### POST /supplies/upload_stickers

**Назначение**: Генерация PDF со стикерами (QR-коды)

**Метод**: POST

**URL**: `/api/v1/supplies/upload_stickers`

**Request Body**:
```json
{
  "supply_id": "234567890"
}
```

**Response (200 OK)**:
```json
{
  "pdf_url": "/stickers/234567890.pdf",
  "page_count": 10,
  "orders_in_pdf": 10,
  "file_size_mb": 0.5
}
```

**Скачивание PDF**:
```bash
curl http://localhost:8301/stickers/234567890.pdf > stickers.pdf
```

---

## Orders API

### GET /orders

**Назначение**: Получить все заказы, сгруппированные по wild-кодам

**Метод**: GET

**URL**: `/api/v1/orders`

**Query параметры**:
```
?skip=0&limit=100&from_time=2025-01-20T00:00:00&to_time=2025-01-23T23:59:59
```

| Параметр | Тип | Описание |
|----------|-----|---------|
| skip | int | Сколько пропустить (пагинация) |
| limit | int | Сколько вернуть (макс 100) |
| from_time | datetime | Начальная дата фильтра |
| to_time | datetime | Конечная дата фильтра |

**Response (200 OK)**:
```json
{
  "grouped_orders": {
    "12345": {
      "wild_code": "12345",
      "total_orders": 10,
      "total_price": 10000.00,
      "orders": [
        {
          "order_id": 123456,
          "order_uid": "uuid",
          "article": "SKU123",
          "nm_id": 12345678,
          "price": 1000.00,
          "quantity": 1,
          "status": "awaiting_assembly",
          "delivery_type": "FBS"
        }
      ]
    }
  },
  "total_groups": 5
}
```

**Curl пример**:
```bash
curl -X GET "http://localhost:8301/api/v1/orders?skip=0&limit=100" \
  -H "Authorization: Bearer {token}"
```

---

### POST /orders/with-supply-name

**Назначение**: ГЛАВНЫЙ ЭНДПОИНТ для создания поставок из заказов

**Метод**: POST

**URL**: `/api/v1/orders/with-supply-name`

**⚠️ Защита от дублей**: 120 сек

**Request Body**:
```json
{
  "wild_code": "12345",
  "count": 10
}
```

**Response (201 Created)**:
```json
{
  "supplies": [
    {
      "supply_id": "234567890",
      "supply_name": "ТЕХ_20250123",
      "type": "technical",
      "orders_added": 10,
      "pdf_url": "/stickers/234567890.pdf"
    }
  ],
  "total_supplies_created": 1,
  "total_orders_processed": 10
}
```

**Логика**:
1. Получить доступные заказы по wild коду
2. Определить тип поставки (техкруг или висячая)
3. Создать поставку
4. Добавить заказы
5. Генерировать QR-коды

---

### GET /orders/sticker/{order_id}

**Назначение**: Получить PNG стикер для одного заказа

**Метод**: GET

**URL**: `/api/v1/orders/sticker/123456`

**Response (200 OK)**:
```
Content-Type: image/png
[PNG файл]
```

---

### GET /orders/sessions

**Назначение**: Получить список всех сессий операций

**Метод**: GET

**URL**: `/api/v1/orders/sessions`

**Query параметры**:
```
?skip=0&limit=100
```

**Response (200 OK)**:
```json
{
  "sessions": [
    {
      "operation_id": "uuid-1234",
      "user_id": 1,
      "supply_name": "234567890",
      "status": "success",
      "created_at": "2025-01-23T10:00:00",
      "orders_created": 10
    }
  ],
  "total": 25
}
```

---

### GET /orders/sessions/{operation_id}

**Назначение**: Получить полную информацию о сессии

**Метод**: GET

**URL**: `/api/v1/orders/sessions/uuid-1234`

**Response (200 OK)**:
```json
{
  "operation_id": "uuid-1234",
  "user_id": 1,
  "supply_name": "234567890",
  "status": "success",
  "request_payload": {
    "wild_code": "12345",
    "count": 10
  },
  "response_data": {
    "supply_id": "234567890",
    "orders_added": 10
  },
  "created_at": "2025-01-23T10:00:00",
  "updated_at": "2025-01-23T10:05:00",
  "error_message": null
}
```

---

## QR Parser API

### GET /qr-parser/qr/{qr_code}

**Назначение**: Разобрать QR-код

**Метод**: GET

**URL**: `/api/v1/qr-parser/qr/*CN+tGIpw`

**Response (200 OK)**:
```json
{
  "format": "barcode",
  "order_id": 123456,
  "raw_data": "*CN+tGIpw"
}
```

---

### GET /qr-parser/order/{order_id}

**Назначение**: Получить QR-код по order_id

**Метод**: GET

**URL**: `/api/v1/qr-parser/order/123456`

**Response (200 OK)**:
```json
{
  "order_id": 123456,
  "qr_barcode": "*CN+tGIpw",
  "qr_part_a": "010",
  "qr_part_b": "312",
  "created_at": "2025-01-23T10:00:00"
}
```

---

## Cache API

### POST /cache/warm-up

**Назначение**: Прогреть кэш (переинициализировать)

**Метод**: POST

**URL**: `/api/v1/cache/warm-up`

**⚠️ Требует**: Superuser роль

**Request Body**:
```json
{}
```

**Response (200 OK)**:
```json
{
  "status": "completed",
  "supplies_cached": 150,
  "orders_cached": 50000,
  "cache_size_mb": 45.2,
  "duration_seconds": 12.5
}
```

---

### POST /cache/invalidate

**Назначение**: Инвалидация (удаление) ключей из кэша

**Метод**: POST

**URL**: `/api/v1/cache/invalidate`

**⚠️ Требует**: Superuser роль

**Request Body**:
```json
{
  "keys": ["supplies_all", "orders_all"]
}
```

**Response (200 OK)**:
```json
{
  "status": "success",
  "keys_invalidated": 2
}
```

---

## Archives API

### GET /archives

**Назначение**: Получить список архивов

**Метод**: GET

**URL**: `/api/v1/archives`

**Response (200 OK)**:
```json
{
  "archives": [
    {
      "archive_id": "archive_1",
      "name": "delivered_supplies_20250120.zip",
      "created_at": "2025-01-20T23:59:59",
      "size_mb": 125.4,
      "supplies_count": 500
    }
  ]
}
```

---

### GET /archives/{archive_id}

**Назначение**: Скачать архив

**Метод**: GET

**URL**: `/api/v1/archives/archive_1`

**Response (200 OK)**:
```
Content-Type: application/zip
[ZIP файл]
```

---

## Примеры Использования

### Сценарий 1: Создание Поставки и Отгрузка

```bash
#!/bin/bash

# 1. Login
TOKEN=$(curl -s -X POST http://localhost:8301/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "username": "user@example.com",
    "password": "password123"
  }' | jq -r '.access_token')

echo "✓ Получен токен: $TOKEN"

# 2. Создать поставку с заказами
SUPPLY_ID=$(curl -s -X POST http://localhost:8301/api/v1/orders/with-supply-name \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "wild_code": "12345",
    "count": 10
  }' | jq -r '.supplies[0].supply_id')

echo "✓ Создана поставка: $SUPPLY_ID"

# 3. Скачать стикеры
curl -s -X POST http://localhost:8301/api/v1/supplies/upload_stickers \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"supply_id\": \"$SUPPLY_ID\"}" | jq -r '.pdf_url' | \
  xargs -I {} curl -s http://localhost:8301{} -o stickers.pdf

echo "✓ Стикеры скачаны: stickers.pdf"

# 4. Отгрузить поставку
curl -s -X POST http://localhost:8301/api/v1/supplies/delivery \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"supply_id\": \"$SUPPLY_ID\"}" | jq '.'

echo "✓ Поставка отгружена"
```

### Сценарий 2: Перемещение Заказов по QR-кодам

```bash
#!/bin/bash

TOKEN="YOUR_TOKEN_HERE"

# 1. Получить целевую поставку
TO_SUPPLY_ID="345678901"

# 2. Отсканировать/получить QR-коды
QR_CODES=$(cat <<'EOF'
[
  {
    "format": "barcode",
    "data": "*CN+tGIpw"
  },
  {
    "format": "part_ab",
    "part_a": "010",
    "part_b": "312"
  }
]
EOF
)

# 3. Переместить заказы по QR
curl -s -X POST http://localhost:8301/api/v1/supplies/move-orders-by-qr \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"to_supply_id\": \"$TO_SUPPLY_ID\",
    \"qr_codes\": $QR_CODES
  }" | jq '.'

echo "✓ Заказы перемещены"
```

### Сценарий 3: Обработка Висячей Поставки

```bash
#!/bin/bash

TOKEN="YOUR_TOKEN_HERE"

# 1. Создать висячую поставку
HANGING_ID=$(curl -s -X POST http://localhost:8301/api/v1/supplies/create-hanging-supply \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "account": "account_name",
    "order_ids": [123456, 234567]
  }' | jq -r '.supply_id')

echo "✓ Висячая поставка: $HANGING_ID"

# 2. Позже... сделать фиктивную отгрузку (продлить срок)
curl -s -X PATCH http://localhost:8301/api/v1/supplies/delivery-fictitious \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"supply_id\": \"$HANGING_ID\"}" | jq '.'

echo "✓ Фиктивная отгрузка"

# 3. Когда товар прибыл... реальная отгрузка из висячей
curl -s -X POST http://localhost:8301/api/v1/supplies/delivery-hanging-actual \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"hanging_supply_id\": \"$HANGING_ID\",
    \"orders_to_deliver\": [123456, 234567]
  }" | jq '.'

echo "✓ Отгружено из висячей"
```

---

## Обработка Ошибок

### 401 Unauthorized

```json
{
  "status": "error",
  "error_code": "INVALID_TOKEN",
  "message": "Token expired or invalid",
  "details": {
    "token_type": "JWT",
    "exp": "2025-01-23T12:00:00"
  }
}
```

**Решение**: Переполучить токен через `/auth/login`

### 409 Conflict (дублирующий запрос)

```json
{
  "status": "error",
  "error_code": "DUPLICATE_REQUEST",
  "message": "Operation already in progress",
  "details": {
    "timeout_seconds": 120
  }
}
```

**Решение**: Подождать 120 сек или проверить статус операции через `/orders/sessions/{operation_id}`

### 400 Bad Request

```json
{
  "status": "error",
  "error_code": "VALIDATION_ERROR",
  "message": "Invalid request data",
  "details": {
    "field": "order_ids",
    "error": "List of integers expected"
  }
}
```

**Решение**: Проверить формат данных согласно документации

---

## Rate Limiting

Текущая реализация не имеет явного rate limiting, но есть защита от дублирующих запросов через DuplicateRequestMiddleware.

Рекомендуется:
- Максимум 1 запрос на создание поставки в 2-3 секунды
- Максимум 100 заказов в одном запросе
- Максимум 5 запросов на получение информации в 1 секунду

---

**API документация завершена!**

Этот документ охватывает:
- ✅ 20+ API endpoints
- ✅ Примеры запросов и ответов
- ✅ Обработку ошибок
- ✅ Практические сценарии
- ✅ Bash примеры
