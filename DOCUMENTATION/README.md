# SwiftPackLabel - Полная Документация

## 📋 Обзор Документации

Эта папка содержит **полную архитектурную и техническую документацию** проекта SwiftPackLabel - системы управления поставками товаров на маркетплейс Wildberries.

---

## 📚 Структура Документации

### 1. [01_ARCHITECTURE_OVERVIEW.md](01_ARCHITECTURE_OVERVIEW.md) - НАЧНИТЕ ОТСЮДА! 🚀

**Для кого**: Архитекторы, lead разработчики, все новые члены команды

**Содержит**:
- Обзор системы и бизнес-контекст
- Архитектурные принципы (слойная архитектура, DI, repository pattern)
- Технический стек (FastAPI, PostgreSQL, Redis, Celery)
- Структура проекта с полной иерархией файлов
- Описание слоёв приложения (API → Service → Repository → Infrastructure)
- Жизненный цикл приложения (startup/shutdown)
- Полный поток данных (диаграммы и примеры)
- Паттерны проектирования (8 основных)
- Ключевые компоненты: GlobalCache, DuplicateRequestMiddleware, SuppliesService

**Начните здесь если**:
- Вы новичок в проекте
- Вы архитектор или senior разработчик
- Вы хотите понять общую структуру системы

---

### 2. [02_MODULES_AND_SERVICES.md](02_MODULES_AND_SERVICES.md) - ДЕТАЛЬНЫЙ РАЗБОР МОДУЛЕЙ

**Для кого**: Разработчики, работающие с конкретными модулями

**Содержит**:
- **Supplies Module (ЯДРО СИСТЕМЫ)**
  - router.py - 40+ API эндпоинтов
  - supplies.py - 5067 строк SuppliesService
  - integration_1c.py - интеграция с 1C
  - schema.py - Pydantic модели
  - Детальное описание всех операций: создание, перемещение, отгрузка

- **Orders Module**
  - Получение и группировка заказов
  - Создание поставок (главный эндпоинт)
  - Логирование статусов

- **Wildberries API Module**
  - Интеграция с WB API
  - Retry логика (90 попыток!)
  - Оптимизированное получение данных (БД → WB)

- **Cache Module**
  - GlobalCache с graceful degradation
  - Redis кэширование
  - Фоновое обновление

- **QR Parser Module**
  - Парсинг QR-кодов
  - Поддержка двух форматов (barcode и part_a+part_b)

- **Authentication Module**
  - JWT токены
  - Dependency injection
  - Управление пользователями

- **Database Models**
  - Repository pattern для всех таблиц

**Начните здесь если**:
- Вы разрабатываете функциональность в конкретном модуле
- Вы хотите добавить новый эндпоинт
- Вы работаете с сервисами

---

### 3. [03_DATABASE_SCHEMA.md](03_DATABASE_SCHEMA.md) - ПОЛНАЯ СХЕМА БД

**Для кого**: DBA, разработчики, работающие с данными

**Содержит**:
- **11 основных таблиц**:
  1. assembly_task_status - основная таблица заказов
  2. hanging_supplies - висячие поставки (JSONB!)
  3. final_supplies - финальные поставки
  4. delivered_supplies - архив (неизменяемый)
  5. order_status_log - логирование статусов
  6. shipment_of_goods - журнал отгрузок
  7. supply_operations - сессии операций
  8. qr_scans - QR-коды
  9. onec_delivery_log - логирование 1C
  10. available_quantity - доступное количество
  11. supply_account_mapping - маппинг

- Детальное описание каждой таблицы:
  - Поля и типы данных
  - Индексы и ключи
  - JSONB структуры

- ER диаграмма и связи между таблицами

- Примеры SQL запросов и оптимизация

- Резервная копия и восстановление

**Начните здесь если**:
- Вы работаете с БД
- Вы добавляете новые таблицы
- Вы оптимизируете производительность БД

---

### 4. [04_API_ENDPOINTS.md](04_API_ENDPOINTS.md) - ПОЛНЫЙ API REFERENCE

**Для кого**: Frontend разработчики, интеграторы, тестировщики

**Содержит**:
- **20+ документированных API эндпоинтов**
- Все основные операции:
  - Аутентификация (login, создание пользователей)
  - Supplies: создание, перемещение, отгрузка
  - Orders: получение, группировка, создание поставок
  - QR Parser: парсинг QR-кодов
  - Cache: управление кэшем
  - Archives: работа с архивами

- Для каждого эндпоинта:
  - Полное описание
  - HTTP метод и URL
  - Request body с примерами
  - Response с примерами
  - Возможные ошибки
  - Curl примеры

- **3 полных практических сценария**:
  1. Создание поставки и отгрузка
  2. Перемещение заказов по QR-кодам
  3. Обработка висячих поставок

- Обработка ошибок и retry logic

**Начните здесь если**:
- Вы интегрируетесь с API
- Вы разрабатываете frontend
- Вы тестируете эндпоинты

---

### 5. [05_CELERY_AND_INTEGRATIONS.md](05_CELERY_AND_INTEGRATIONS.md) - ASYNC И ИНТЕГРАЦИИ

**Для кого**: DevOps, backend разработчики, интеграторы

**Содержит**:
- **Celery конфигурация**
  - Структура и параметры
  - Worker и broker конфигурация
  - Beat schedule

- **3 периодические задачи** (детально):
  1. sync_orders_periodic - каждый час
  2. sync_hanging_supplies - каждые 6 часов
  3. sync_update_available_quantity - 23:59 UTC

- **Flower мониторинг**
  - Web интерфейс
  - Команды управления Celery

- **Wildberries API интеграция**
  - Retry логика (90 попыток!)
  - API эндпоинты
  - Примеры запросов

- **1C ERP интеграция**
  - Отправка данных об отгрузке
  - Резервирование товара
  - Race condition риски

- **Redis конфигурация**
  - 3 отдельные БД (кэш, broker, backend)
  - Graceful degradation

- **Error handling**
  - Стратегии обработки
  - Logging

**Начните здесь если**:
- Вы работаете с Celery задачами
- Вы настраиваете интеграции
- Вы мониторите async операции

---

### 6. [06_DEPLOYMENT_AND_CONFIG.md](06_DEPLOYMENT_AND_CONFIG.md) - РАЗВЁРТЫВАНИЕ

**Для кого**: DevOps, системные администраторы, разработчики

**Содержит**:
- **Переменные окружения** - полный список с объяснением
- **Docker Compose** - полная конфигурация всех сервисов
- **Инициализация БД** - миграции и скрипты
- **Запуск приложения** - development и production
- **Мониторинг и логирование** - структура логов, просмотр
- **Troubleshooting** - решение основных проблем
- **Масштабирование** - горизонтальное и вертикальное
- **Чек-листы** - pre/post deployment

**Начните здесь если**:
- Вы развёртываете приложение
- Вы настраиваете production окружение
- Вы решаете проблемы с инфраструктурой

---

## 🎯 Быстрый Старт по Сценариям

### Я новичок в проекте

1. Прочитайте **[01_ARCHITECTURE_OVERVIEW.md](01_ARCHITECTURE_OVERVIEW.md)** - 30 мин
2. Посмотрите структуру проекта в коде
3. Прочитайте **[02_MODULES_AND_SERVICES.md](02_MODULES_AND_SERVICES.md)** - 30 мин
4. Запустите приложение через **[06_DEPLOYMENT_AND_CONFIG.md](06_DEPLOYMENT_AND_CONFIG.md)**
5. Попробуйте несколько API запросов из **[04_API_ENDPOINTS.md](04_API_ENDPOINTS.md)**

### Я добавляю новый API эндпоинт

1. Прочитайте соответствующий раздел **[02_MODULES_AND_SERVICES.md](02_MODULES_AND_SERVICES.md)**
2. Посмотрите примеры похожих эндпоинтов в **[04_API_ENDPOINTS.md](04_API_ENDPOINTS.md)**
3. Посмотрите структуру данных в **[03_DATABASE_SCHEMA.md](03_DATABASE_SCHEMA.md)**
4. Реализуйте и задокументируйте в **[04_API_ENDPOINTS.md](04_API_ENDPOINTS.md)**

### Я работаю с базой данных

1. Посмотрите **[03_DATABASE_SCHEMA.md](03_DATABASE_SCHEMA.md)** - полное описание таблиц
2. Посмотрите индексы и примеры запросов
3. Используйте примеры SQL запросов
4. Проверьте логирование в **[06_DEPLOYMENT_AND_CONFIG.md](06_DEPLOYMENT_AND_CONFIG.md)**

### Я работаю с интеграциями

1. Прочитайте **[05_CELERY_AND_INTEGRATIONS.md](05_CELERY_AND_INTEGRATIONS.md)**
2. Посмотрите конкретную интеграцию (WB API или 1C)
3. Посмотрите примеры в **[04_API_ENDPOINTS.md](04_API_ENDPOINTS.md)**
4. Проверьте мониторинг в **[05_CELERY_AND_INTEGRATIONS.md](05_CELERY_AND_INTEGRATIONS.md)**

### Я развёртываю приложение

1. Прочитайте **[06_DEPLOYMENT_AND_CONFIG.md](06_DEPLOYMENT_AND_CONFIG.md)** целиком
2. Подготовьте переменные окружения
3. Запустите Docker Compose
4. Используйте чек-листы deployment

### Я отлаживаю проблему

1. Посмотрите Troubleshooting в **[06_DEPLOYMENT_AND_CONFIG.md](06_DEPLOYMENT_AND_CONFIG.md)**
2. Проверьте логи в структуре, описанной там же
3. Используйте Flower для Celery задач
4. Посмотрите примеры SQL запросов в **[03_DATABASE_SCHEMA.md](03_DATABASE_SCHEMA.md)**

---

## 📊 Ключевые Метрики Проекта

| Метрика | Значение | Описание |
|---------|----------|---------|
| **Строк кода** | ~40,000 | src/ (исключая логи и кэш) |
| **Таблиц БД** | 11+ | Полная схема |
| **API Endpoints** | 20+ | Документировано |
| **Celery Задач** | 3 | Периодические |
| **Модулей** | 15+ | Основных |
| **Сложность** | Medium-High | Асинхронная, много интеграций |

---

## 🏗️ Архитектурные Слои

```
┌─────────────────────────────────────┐
│ API Layer (FastAPI routes)          │ 📍 Документ 04
├─────────────────────────────────────┤
│ Business Logic (Services)           │ 📍 Документ 02
├─────────────────────────────────────┤
│ Data Access (Repository pattern)    │ 📍 Документ 02, 03
├─────────────────────────────────────┤
│ Infrastructure (Cache, DB, Auth)    │ 📍 Документ 01, 02
├─────────────────────────────────────┤
│ External (WB API, 1C, Redis)       │ 📍 Документ 05
└─────────────────────────────────────┘
```

---

## 🔑 Критичные Компоненты

| Компонент | Файл | Документ | Статус |
|-----------|------|----------|--------|
| **SuppliesService** | supplies.py | 02 | 🔴 ЯДРО |
| **GlobalCache** | global_cache.py | 02 | 🔴 КРИТИЧНО |
| **DuplicateRequestMiddleware** | duplicate_request.py | 01, 02 | 🟡 ВАЖНО |
| **AsyncHttpClient** | response.py | 01 | 🟡 ВАЖНО |
| **DatabaseManager** | db.py | 01 | 🟡 ВАЖНО |
| **Celery Periodic Tasks** | tasks/* | 05 | 🟡 ВАЖНО |

---

## 📖 Соответствие Файлов к Документам

| Файл/Папка | Документ | Раздел |
|-----------|----------|--------|
| src/app.py | 01, 06 | Initialization |
| src/db.py | 01, 02 | Database Management |
| src/settings.py | 06 | Configuration |
| src/response.py | 01 | HTTP Clients |
| src/supplies/ | 02 | Supplies Module |
| src/orders/ | 02 | Orders Module |
| src/wildberries_api/ | 05 | WB Integration |
| src/cache/ | 02 | Cache Module |
| src/models/ | 03 | Database Tables |
| src/celery_app/ | 05 | Celery Tasks |
| migrations/ | 03, 06 | Database Migrations |
| docker-compose.yml | 06 | Deployment |

---

## ⚠️ Критичные Риски

| Риск | Уровень | Документ | Решение |
|------|---------|----------|---------|
| Невозможность отката после delivery | 🔴 | 02, 04 | Мониторинг, бэкап |
| Race condition 1C интеграция | 🟡 | 05 | Retry logic |
| Расхождение кэша и БД | 🟡 | 02 | Invalidation |
| Потеря данных при сбое | 🟡 | 03, 05 | Logging |

---

## 🚀 Рекомендуемый Порядок Чтения

### День 1: Основы
1. **[01_ARCHITECTURE_OVERVIEW.md](01_ARCHITECTURE_OVERVIEW.md)** (1-2 часа)
   - Понимание общей архитектуры
   - Слои приложения
   - Технический стек

### День 2: Модули
2. **[02_MODULES_AND_SERVICES.md](02_MODULES_AND_SERVICES.md)** (2-3 часа)
   - Детальное описание каждого модуля
   - Сервисы и их методы
   - Примеры кода

### День 3: Данные
3. **[03_DATABASE_SCHEMA.md](03_DATABASE_SCHEMA.md)** (1-2 часа)
   - Структура БД
   - Примеры SQL
   - Оптимизация

### День 4: API
4. **[04_API_ENDPOINTS.md](04_API_ENDPOINTS.md)** (1 час)
   - Все API эндпоинты
   - Примеры использования
   - Ошибки

### День 5: Advanced
5. **[05_CELERY_AND_INTEGRATIONS.md](05_CELERY_AND_INTEGRATIONS.md)** (1-2 часа)
   - Async обработка
   - Интеграции
   - Мониторинг

### День 6: DevOps
6. **[06_DEPLOYMENT_AND_CONFIG.md](06_DEPLOYMENT_AND_CONFIG.md)** (1-2 часа)
   - Конфигурация
   - Развёртывание
   - Troubleshooting

---

## 📞 Контакты и Поддержка

- **Issues**: GitHub Issues
- **Pull Requests**: GitHub PRs
- **Обсуждение архитектуры**: Architecture discussions
- **Documentation**: Этот проект

---

## 📝 История Обновлений

| Дата | Версия | Изменения |
|------|--------|----------|
| 2025-01-23 | 1.0 | Полная документация |

---

## ✅ Чек-лист Прочтения

### Для новичков
- [ ] 01_ARCHITECTURE_OVERVIEW.md
- [ ] 02_MODULES_AND_SERVICES.md
- [ ] 03_DATABASE_SCHEMA.md (обзор)
- [ ] 04_API_ENDPOINTS.md (обзор)
- [ ] 06_DEPLOYMENT_AND_CONFIG.md (запуск)

### Для разработчиков
- [ ] 02_MODULES_AND_SERVICES.md (полностью)
- [ ] 03_DATABASE_SCHEMA.md (полностью)
- [ ] 04_API_ENDPOINTS.md (полностью)
- [ ] 05_CELERY_AND_INTEGRATIONS.md

### Для DevOps
- [ ] 01_ARCHITECTURE_OVERVIEW.md (обзор)
- [ ] 05_CELERY_AND_INTEGRATIONS.md (monitoring)
- [ ] 06_DEPLOYMENT_AND_CONFIG.md (полностью)

### Для архитекторов
- [ ] ВСЕ документы полностью

---

## 📌 Важные Ссылки в Коде

**Главные файлы для изучения**:
1. `/src/app.py` - инициализация
2. `/src/supplies/supplies.py` - основная логика (5000+ строк)
3. `/src/supplies/router.py` - API endpoints (40 маршрутов)
4. `/src/cache/global_cache.py` - кэширование
5. `/src/celery_app/celery.py` - Celery конфигурация

**Примеры интеграций**:
- `/src/wildberries_api/` - WB API
- `/src/supplies/integration_1c.py` - 1C ERP

**Модели БД**:
- `/src/models/` - все repository классы (11 файлов)

**Конфигурация**:
- `/src/settings.py` - все переменные окружения
- `/docker-compose.yml` - развёртывание

---

**Спасибо за внимание к документации! Если у вас есть вопросы - обратитесь к команде разработки.** 🚀
