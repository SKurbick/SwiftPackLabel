# Конфигурация, Развёртывание и Быстрый Старт

## Содержание

1. [Переменные Окружения](#переменные-окружения)
2. [Docker Compose](#docker-compose)
3. [Инициализация БД](#инициализация-бд)
4. [Запуск Приложения](#запуск-приложения)
5. [Мониторинг и Логирование](#мониторинг-и-логирование)
6. [Troubleshooting](#troubleshooting)

---

## Переменные Окружения

### .env Файл

```bash
# ============================================
# PostgreSQL Configuration
# ============================================
POSTGRES_HOST=postgres                    # Хост БД
POSTGRES_PORT=5432                        # Порт БД
POSTGRES_USER=swift_pack                  # Пользователь БД
POSTGRES_PASSWORD=secure_password_123     # Пароль БД
POSTGRES_DB=swift_pack_label              # Имя БД

# ============================================
# Redis Configuration
# ============================================
REDIS_HOST=redis                          # Хост Redis
REDIS_PORT=6379                           # Порт Redis
REDIS_DB=0                                # Database для кэша (0)
REDIS_PASSWORD=redis_password             # Пароль Redis (optional)
CACHE_TTL=2400                            # TTL кэша (40 минут)
CACHE_REFRESH_INTERVAL=1800               # Интервал обновления (30 минут)

# ============================================
# Celery Configuration
# ============================================
CELERY_BROKER_URL=redis://:redis_password@redis:6379/1     # Broker (DB 1)
CELERY_RESULT_BACKEND=redis://:redis_password@redis:6379/2 # Backend (DB 2)
CELERY_TIMEZONE=UTC                       # Часовой пояс
CELERY_RESULT_EXPIRES=3600                # Срок жизни результата (1 час)

# ============================================
# FastAPI Configuration
# ============================================
DEBUG=False                               # Debug mode (False в production!)
SECRET_KEY=super_secret_key_change_in_prod # Секретный ключ для JWT
ALGORITHM=HS256                           # Алгоритм подписи JWT
ACCESS_TOKEN_EXPIRE_MINUTES=30            # Время жизни токена (30 мин)

# ============================================
# Default Superuser
# ============================================
INIT_SUPERUSER_USERNAME=admin             # Username суперпользователя
INIT_SUPERUSER_PASSWORD=adminpass123      # Пароль суперпользователя
INIT_SUPERUSER_EMAIL=admin@example.com    # Email суперпользователя

# ============================================
# 1C Integration
# ============================================
ONEC_HOST=http://1c_routing_api:8002      # Хост 1C API
ONEC_USER=1c_user                         # Пользователь 1C
ONEC_PASSWORD=1c_password                 # Пароль 1C

# 1C API URLs
SHIPMENT_API_URL=http://1c_routing_api:8002/api/shipment_of_goods/update
PRODUCT_RESERVATION_API_URL=http://1c_routing_api:8002/api/shipment_of_goods/create_reserve
PRODUCT_RESERVATION_WAREHOUSE_ID=1        # ID склада в 1C
PRODUCT_RESERVATION_DELIVERY_TYPE=ФБС     # Тип доставки
PRODUCT_RESERVATION_EXPIRES_DAYS=10       # Дни экспирации резервации
SHIPPED_GOODS_API_URL=http://1c_routing_api:8002/api/shipment_of_goods/add_shipped_goods

# ============================================
# AsyncPG Configuration
# ============================================
ASYNC_PG_POOL_SIZE=5                      # Min pool size
ASYNC_PG_POOL_MAX_SIZE=15                 # Max pool size
CONNECTION_TIMEOUT=10.0                   # Connection timeout (сек)
STATEMENT_TIMEOUT=30.0                    # Statement timeout (сек)
```

### Безопасность Переменных

⚠️ **Критично в Production**:

```bash
# 🔴 НИКОГДА не коммитить .env в git!
echo ".env" >> .gitignore

# 🟢 Использовать переменные окружения сервера
export POSTGRES_PASSWORD="$(vault read secret/db/password)"
export SECRET_KEY="$(openssl rand -hex 32)"
export REDIS_PASSWORD="$(vault read secret/redis/password)"

# 🟢 Использовать secrets в Docker Compose
# docker-compose.yml:
# services:
#   app:
#     environment:
#       POSTGRES_PASSWORD_FILE: /run/secrets/db_password
# secrets:
#   db_password:
#     file: ./secrets/db_password.txt
```

---

## Docker Compose

### docker-compose.yml

```yaml
version: '3.8'

services:
  # ================================================
  # PostgreSQL Database
  # ================================================
  postgres:
    image: postgres:15-alpine
    container_name: swift_pack_postgres
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_INITDB_ARGS: "--encoding=UTF8 --locale=C"
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./migrations:/docker-entrypoint-initdb.d
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - app_network

  # ================================================
  # Redis (Cache + Broker + Backend)
  # ================================================
  redis:
    image: redis:7-alpine
    container_name: swift_pack_redis
    command: redis-server --requirepass ${REDIS_PASSWORD:-redis_password}
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - app_network

  # ================================================
  # FastAPI Application
  # ================================================
  app:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: swift_pack_app
    ports:
      - "8301:8301"
    environment:
      - POSTGRES_HOST=postgres
      - REDIS_HOST=redis
      - DEBUG=False
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    volumes:
      - ./src:/app/src
      - ./logs:/app/logs
      - ./src/archives/data:/app/src/archives/data
      - ./src/images/uploads:/app/src/images/uploads
    command: uvicorn src.app:app --host 0.0.0.0 --port 8301
    networks:
      - app_network

  # ================================================
  # Celery Worker
  # ================================================
  celery_worker:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: swift_pack_celery_worker
    environment:
      - POSTGRES_HOST=postgres
      - REDIS_HOST=redis
      - CELERY_BROKER_URL=redis://:${REDIS_PASSWORD:-redis_password}@redis:6379/1
      - CELERY_RESULT_BACKEND=redis://:${REDIS_PASSWORD:-redis_password}@redis:6379/2
    depends_on:
      - postgres
      - redis
    volumes:
      - ./src:/app/src
      - ./src/logging/celery:/app/src/logging/celery
    command: celery -A src.celery_app.celery worker --loglevel=info --concurrency=2 --pool=prefork
    networks:
      - app_network

  # ================================================
  # Celery Beat (Task Scheduler)
  # ================================================
  celery_beat:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: swift_pack_celery_beat
    environment:
      - POSTGRES_HOST=postgres
      - REDIS_HOST=redis
      - CELERY_BROKER_URL=redis://:${REDIS_PASSWORD:-redis_password}@redis:6379/1
      - CELERY_RESULT_BACKEND=redis://:${REDIS_PASSWORD:-redis_password}@redis:6379/2
    depends_on:
      - postgres
      - redis
    volumes:
      - ./src:/app/src
      - ./src/logging/celery:/app/src/logging/celery
      - celerybeat_data:/tmp
    command: celery -A src.celery_app.celery beat --loglevel=info
    networks:
      - app_network

  # ================================================
  # Flower (Celery Monitoring)
  # ================================================
  flower:
    image: mher/flower:2.0.1
    container_name: swift_pack_flower
    ports:
      - "5555:5555"
    environment:
      CELERY_BROKER_URL: redis://:${REDIS_PASSWORD:-redis_password}@redis:6379/1
      CELERY_RESULT_BACKEND: redis://:${REDIS_PASSWORD:-redis_password}@redis:6379/2
    depends_on:
      - redis
    command: celery --broker=redis://:${REDIS_PASSWORD:-redis_password}@redis:6379/1 flower --port=5555
    networks:
      - app_network

# ================================================
# Volumes
# ================================================
volumes:
  postgres_data:
    driver: local
  redis_data:
    driver: local
  celerybeat_data:
    driver: local

# ================================================
# Networks
# ================================================
networks:
  app_network:
    driver: bridge
```

### Запуск Docker Compose

```bash
# Инициализировать переменные окружения
cp .env.example .env
# ⚠️ Отредактировать .env с реальными значениями

# Запустить все сервисы
docker-compose up -d

# Проверить статус
docker-compose ps

# Просмотреть логи
docker-compose logs -f app

# Остановить
docker-compose down

# Удалить всё включая volumes
docker-compose down -v
```

---

## Инициализация БД

### Создание Таблиц

```bash
# Способ 1: Алембик (если есть)
alembic upgrade head

# Способ 2: Вручную (SQL скрипты)
psql -h localhost -U swift_pack -d swift_pack_label < migrations/create_final_supplies_table.sql
psql -h localhost -U swift_pack -d swift_pack_label < migrations/create_qr_scans_table.sql
psql -h localhost -U swift_pack -d swift_pack_label < migrations/create_delivered_supplies_table.sql
# ... и т.д.

# Способ 3: Через Python скрипт
python scripts/init_db.py
```

### Создание Суперпользователя

**Автоматически** при startup (src/auth/init_superuser.py):

```python
# При запуске приложения автоматически создаётся суперпользователь
# Параметры из переменных окружения:
# INIT_SUPERUSER_USERNAME=admin
# INIT_SUPERUSER_PASSWORD=adminpass123
# INIT_SUPERUSER_EMAIL=admin@example.com
```

**Вручную**:

```bash
# curl для создания пользователя
curl -X POST http://localhost:8301/api/v1/auth/users \
  -H "Authorization: Bearer {superuser_token}" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "newuser",
    "email": "newuser@example.com",
    "password": "password123",
    "is_superuser": false
  }'
```

### Создание WB Токенов (tokens.json)

```json
{
  "account_name_1": "token_1_from_wildberries",
  "account_name_2": "token_2_from_wildberries",
  "account_name_3": "token_3_from_wildberries"
}
```

⚠️ Эта файл ДОЛЖЕН находиться в корне проекта или в переменной окружения.

---

## Запуск Приложения

### Локальный Запуск (Development)

```bash
# 1. Создать виртуальное окружение
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# 2. Установить зависимости
pip install -r requirements.txt

# 3. Создать .env файл
cp .env.example .env
# Отредактировать .env

# 4. Запустить PostgreSQL и Redis (например, через Docker)
docker-compose up -d postgres redis

# 5. Инициализировать БД
python scripts/init_db.py

# 6. Запустить FastAPI
uvicorn src.app:app --reload --host 0.0.0.0 --port 8301

# 7. В отдельном терминале: запустить Celery Worker
celery -A src.celery_app.celery worker --loglevel=info

# 8. В отдельном терминале: запустить Celery Beat
celery -A src.celery_app.celery beat --loglevel=info

# 9. В отдельном терминале: запустить Flower (опционально)
celery -A src.celery_app.celery flower --port=5555
```

### Production Запуск (Docker)

```bash
# Запустить через Docker Compose
docker-compose up -d

# Проверить логи
docker-compose logs -f app
docker-compose logs -f celery_worker
docker-compose logs -f celery_beat

# Проверить здоровье
curl http://localhost:8301/

# Открыть Flower
open http://localhost:5555
```

---

## Мониторинг и Логирование

### Структура Логирования

```
/logging/
├── logger/
│   └── logger.log              # Основной логгер
├── celery/
│   ├── orders_sync_*.log       # Логирование sync_orders_periodic
│   ├── hanging_supplies_sync_*.log
│   └── available_quantity_sync_*.log
├── orders/
│   └── orders_*.log            # Логирование заказов
├── supplies/
│   └── supplies_*.log          # Логирование поставок
└── notifications/
    └── notifications_*.log     # Уведомления
```

### Просмотр Логов

```bash
# Real-time логирование приложения
docker-compose logs -f app

# Логирование Celery Worker
docker-compose logs -f celery_worker

# Логирование Celery Beat
docker-compose logs -f celery_beat

# Просмотр архивов логов (zipped)
ls -la /logging/celery/
unzip /logging/celery/orders_sync_*.log.zip

# Поиск ошибок в логах
grep ERROR /logging/logger/logger.log | tail -50

# Поиск по timing
grep "duration_seconds" /logging/celery/*.log
```

### Flower Мониторинг

**URL**: http://localhost:5555

**Доступные метрики**:
- Active workers и их статус
- Выполненные и текущие задачи
- История выполнения (success/failure)
- Timing каждой задачи
- Graph мониторинга

### Prometheus Метрики (опционально)

Можно добавить Prometheus monitoring:

```python
# src/app.py
from prometheus_client import generate_latest, CollectorRegistry

@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type="text/plain")
```

---

## Troubleshooting

### Проблема: БД недоступна

```
ERROR: can't connect to postgres:5432
```

**Решение**:
```bash
# 1. Проверить что PostgreSQL запущён
docker-compose ps postgres

# 2. Проверить логи
docker-compose logs postgres

# 3. Перезагрузить
docker-compose restart postgres

# 4. Проверить здоровье
docker-compose exec postgres pg_isready -U swift_pack
```

### Проблема: Redis недоступен

```
ERROR: can't connect to redis:6379
```

**Решение**:
```bash
# 1. Проверить Redis
docker-compose ps redis

# 2. Перезагрузить Redis
docker-compose restart redis

# 3. Проверить с redis-cli
docker-compose exec redis redis-cli ping
```

### Проблема: Celery задачи не выполняются

```
celery_worker | ERROR: Worker shutdown
```

**Решение**:
```bash
# 1. Проверить что worker запущён
docker-compose ps celery_worker

# 2. Перезагрузить
docker-compose restart celery_worker

# 3. Проверить через Flower
# http://localhost:5555

# 4. Очистить очередь
docker-compose exec celery_worker celery -A src.celery_app.celery purge
```

### Проблема: Низкая производительность

**Анализ**:
```bash
# 1. Проверить используемую память
docker stats

# 2. Проверить БД connections
docker-compose exec postgres psql -U swift_pack -d swift_pack_label -c "SELECT count(*) FROM pg_stat_activity;"

# 3. Проверить Redis memory
docker-compose exec redis redis-cli INFO memory

# 4. Проверить slow queries
# Включить в PostgreSQL:
# log_min_duration_statement = 1000  # логировать запросы > 1 сек
```

**Оптимизация**:
```bash
# 1. Увеличить пул соединений БД
# settings.py: async_pg_pool_size = 10, max = 20

# 2. Увеличить кэш TTL
# settings.py: CACHE_TTL = 3600

# 3. Добавить индексы
# migrations/add_indexes.sql

# 4. Увеличить Celery worker concurrency
# docker-compose.yml: --concurrency=4

# 5. Использовать Redis persistence
# redis.conf: save 900 1
```

### Проблема: Памяти утечка в приложении

```bash
# 1. Профилировать использование памяти
pip install memory_profiler

# 2. Запустить с профилировкой
python -m memory_profiler src/app.py

# 3. Проверить нет ли циклических ссылок
python -m objgraph show_most_common_types

# 4. Проверить connection pool
# PostgreSQL pool должен быть ограничен (max_size=15)
```

---

## Производительность и Оптимизация

### Бенчмарки

| Операция | Время | Примечание |
|----------|-------|-----------|
| Получить 100 заказов | 50ms | Из БД (с индексом) |
| Создать поставку с 10 заказами | 5-10 сек | Включает WB API calls |
| Синхронизировать заказы | 30-60 сек | Зависит от количества |
| Отгрузить поставку | 2-3 сек | WB + 1C |

### Масштабирование

**Горизонтальное масштабирование**:

```yaml
# docker-compose.yml
services:
  celery_worker_1:
    # ... worker 1
  celery_worker_2:
    # ... worker 2 (еще один)
  celery_worker_3:
    # ... worker 3 (еще один)

# Все воркеры подключаются к одному Redis broker
# Redis автоматически распределяет задачи
```

**Вертикальное масштабирование**:

```yaml
# docker-compose.yml
services:
  postgres:
    deploy:
      resources:
        limits:
          cpus: '4'
          memory: 8G
        reservations:
          cpus: '2'
          memory: 4G

  redis:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 4G
```

---

## Чек-лист Развёртывания

### Pre-Deployment

- [ ] Все переменные окружения установлены
- [ ] PostgreSQL доступна и инициализирована
- [ ] Redis доступен
- [ ] WB токены добавлены в tokens.json
- [ ] 1C API доступен и протестирован
- [ ] SSL сертификаты готовы (для production)

### Deployment

- [ ] Docker образ собран
- [ ] docker-compose up успешно
- [ ] Все сервисы healthy
- [ ] Суперпользователь создан
- [ ] Flower мониторинг работает

### Post-Deployment

- [ ] API endpoint доступен (GET /)
- [ ] Аутентификация работает (POST /auth/login)
- [ ] Celery задачи выполняются (Flower)
- [ ] Redis кэширование работает
- [ ] Логирование работает
- [ ] Резервная копия БД создана

---

**Документация по конфигурации завершена!**

Этот документ охватывает:
- ✅ Переменные окружения
- ✅ Docker Compose конфигурация
- ✅ Инициализация БД
- ✅ Запуск приложения (dev и prod)
- ✅ Мониторинг и логирование
- ✅ Troubleshooting
- ✅ Масштабирование
- ✅ Чек-листы
