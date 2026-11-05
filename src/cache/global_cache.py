import asyncio
import json
import pickle
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional, Dict, Iterable, List, Awaitable, Callable
from datetime import datetime, timedelta
import redis.asyncio as redis
from redis.exceptions import LockError

from src.concurrency import run_blocking
from src.db import db as main_db
from src.orders.schema import OrderDetail
from src.settings import settings
from src.logger import app_logger as logger
from src.diagnostics import (
    current_refresh_id,
    current_refresh_source,
    diagnostics,
    new_refresh_id,
    safe_error,
)
from src.supplies.schema import SupplyIdResponseSchema

from src.supplies.supplies import SuppliesService
from src.orders.orders import OrdersService
from src.models.shipment_of_goods import ShipmentOfGoods
from src.models.hanging_supplies import HangingSupplies
from src.models.delivered_supplies import DeliveredSupplies

from src.celery_app.tasks.hanging_supplies_sync import sync_hanging_supplies_with_data

from functools import partial
from redis.asyncio.lock import Lock

from src.cache_state import (
    UPDATED_AT,
    OperationCache,
    is_snapshot,
    operation_cache,
    publish_snapshots,
    read_snapshot,
)
from src.supplies.empty_supply_cleaner import EmptySupplyCleaner

ORDERS_CACHE_PATTERN = "cache:orders_all:*"


class LockBusyError(Exception):
    """Замок занят другой операцией дольше"""


class GlobalCache:
    """
    Глобальный кэш на основе Redis для улучшения производительности приложения.
    
    Предоставляет следующие возможности:
    - Кэширование данных с настраиваемым TTL
    - Автоматическое фоновое обновление кэша
    - Прогрев кэша при запуске приложения
    - Асинхронная работа с Redis
    """
    
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.background_tasks: List[asyncio.Task] = []
        self.is_connected = False
        # Флаг для тестирования оптимизированного метода кэширования
        self.use_optimized_cache = True  # Изменить на True для тестирования нового метода
        self.refresh_tasks: Dict[str, asyncio.Task] = {}
        
    async def connect(self) -> None:
        """Подключение к Redis серверу."""
        try:
            redis_url = f"redis://"
            if settings.REDIS_PASSWORD:
                redis_url += f":{settings.REDIS_PASSWORD}@"
            redis_url += f"{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}"
            
            self.redis_client = redis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=False,  # Работаем с bytes для pickle
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            
            # Проверка подключения
            await self.redis_client.ping()
            for pattern in ("cache:orders_all:*", "cache:supplies_all:*"):
                async for key in self.redis_client.scan_iter(match=pattern):
                    await self.redis_client.persist(key)
            self.is_connected = True
            operation_cache.client = self.redis_client
            logger.info(f"Успешное подключение к Redis: {settings.REDIS_HOST}:{settings.REDIS_PORT}")
            
        except Exception as e:
            logger.error(f"Ошибка подключения к Redis: {str(e)}")
            self.is_connected = False
            raise
    
    async def disconnect(self) -> None:
        """Отключение от Redis и остановка фоновых задач."""
        try:
            # Останавливаем фоновые задачи
            for task in self.background_tasks:
                if not task.done():
                    task.cancel()
                    
            if self.background_tasks:
                await asyncio.gather(*self.background_tasks, return_exceptions=True)
                
            # Закрываем соединение с Redis
            if self.redis_client:
                await self.redis_client.close()
                
            self.is_connected = False
            if operation_cache.client is self.redis_client:
                operation_cache.client = None
            logger.info("Отключение от Redis выполнено успешно")
            
        except Exception as e:
            logger.error(f"Ошибка при отключении от Redis: {str(e)}")
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Получение значения из кэша.
        
        Args:
            key: Ключ для получения данных
            
        Returns:
            Кэшированные данные или None если ключ не найден
        """
        if not self.is_connected or not self.redis_client:
            logger.warning("Redis не подключен, пропускаем получение из кэша")
            return None
            
        try:
            cached_data = await self.redis_client.get(key)
            if cached_data:
                # Десериализация данных
                data = await run_blocking(pickle.loads, cached_data)
                if is_snapshot(key):
                    data = await read_snapshot(self.redis_client, key, data)
                logger.debug(f"Кэш HIT для ключа: {key}")
                return data
            else:
                logger.debug(f"Кэш MISS для ключа: {key}")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка при получении данных из кэша для ключа {key}: {str(e)}")
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Сохранение значения в кэш.
        
        Args:
            key: Ключ для сохранения данных
            value: Данные для кэширования
            ttl: Время жизни в секундах (по умолчанию из настроек)
            
        Returns:
            True если данные успешно сохранены, False иначе
        """
        if not self.is_connected or not self.redis_client:
            logger.warning("Redis не подключен, пропускаем сохранение в кэш")
            return False
            
        try:
            if is_snapshot(key):
                await publish_snapshots(self.redis_client, {key: value})
                return True
            
            # Сериализация данных
            serialized_data = await run_blocking(pickle.dumps, value)
            
            # Установка TTL
            cache_ttl = ttl or settings.CACHE_TTL
            
            # Сохранение в Redis
            await self.redis_client.setex(key, cache_ttl, serialized_data)
            logger.debug(f"Данные сохранены в кэш для ключа: {key}, TTL: {cache_ttl}s")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных в кэш для ключа {key}: {str(e)}")
            return False
    
    async def delete(self, key: str) -> bool:
        """
        Удаление значения из кэша.
        
        Args:
            key: Ключ для удаления
            
        Returns:
            True если ключ был удален, False иначе
        """
        if not self.is_connected or not self.redis_client:
            logger.warning("Redis не подключен, пропускаем удаление из кэша")
            return False
            
        try:
            result = await self.redis_client.delete(key)
            logger.debug(f"Ключ {key} удален из кэша: {bool(result)}")
            return bool(result)
            
        except Exception as e:
            logger.error(f"Ошибка при удалении ключа {key} из кэша: {str(e)}")
            return False

    @asynccontextmanager
    async def exclusive(self, name: str, timeout: int, wait: int) -> AsyncIterator[None]:
        """Не даёт двум операциям одновременно выполнять один и тот же участок."""
        if not self.is_connected or not self.redis_client:
            logger.warning(f"Redis недоступен, «{name}» выполняется без лока")
            yield
            return

        lock = self.redis_client.lock(f"lock:{name}", timeout=timeout, blocking_timeout=wait)
        if not await lock.acquire():
            raise LockBusyError(name)
        try:
            yield
        finally:
            try:
                await lock.release()
            except LockError:
                logger.warning(f"лок «{name}» истёк раньше, чем закончилась операция")

    async def remove_orders_from_orders_cache(self, order_ids: Iterable[int]) -> None:
        """Убирает взятые СЗ из всех списков создания круга."""
        if self.is_connected and self.redis_client:
            try:
                await OperationCache(self.redis_client).record_taken_orders(order_ids)
            except Exception:
                logger.exception("Не удалось убрать взятые в круг задания из кеша")

    async def mark_supply_delivered(self, account: str, supply_id: str) -> None:
        """Убирает отправленную поставку из кеша сборки."""
        if self.is_connected and self.redis_client:
            await OperationCache(self.redis_client).mark_supply_delivered(account, supply_id)

    async def is_fresh(self, key: str) -> bool:
        """Проверяет, пора ли обновить снимок."""
        stamp = await self.redis_client.hget(UPDATED_AT, key)
        return bool(stamp and time.time() - float(stamp) < settings.CACHE_TTL)

    async def request_refresh(
        self, key: str = "all", loader: Callable[[], Awaitable[Any]] | None = None,
    ) -> dict[str, bool]:
        """Запускает одно фоновое обновление для всех пользователей."""
        scope = "all" if key == "all" or key.startswith("cache:supplies_all:") else key
        if scope != "all" and "time_delta:1.0|wild:None" in key:
            if await self.redis_client.exists("lock:cache-refresh:all"):
                return {"refreshing": True, "started": False}
        lock = self.redis_client.lock(f"lock:cache-refresh:{scope}", timeout=90, blocking_timeout=0)
        if not await lock.acquire(blocking=False):
            return {"refreshing": True, "started": False}

        task = asyncio.create_task(self._run_refresh(scope, key, loader, lock))
        self.refresh_tasks[scope] = task
        self.background_tasks.append(task)
        task.add_done_callback(partial(self._forget_refresh, scope))
        return {"refreshing": True, "started": True}

    def _forget_refresh(self, scope: str, task: asyncio.Task) -> None:
        """Убирает завершённую задачу из списка активных."""
        if self.refresh_tasks.get(scope) is task:
            self.refresh_tasks.pop(scope)
        if task in self.background_tasks:
            self.background_tasks.remove(task)

    @staticmethod
    async def _renew_refresh_lock(lock: Lock) -> None:
        """Продлевает блокировку, пока идёт обновление."""
        while True:
            await asyncio.sleep(30)
            await lock.extend(90, replace_ttl=True)

    async def _build_refresh(self, key: str, loader: Callable[[], Awaitable[Any]] | None) -> None:
        """Получает данные и сохраняет готовый снимок."""
        if loader is None:
            await self._warm_up_cache()
            return
        result = await loader()
        if not await self.set(key, result):
            raise RuntimeError(f"Не удалось опубликовать {key}")

    async def _save_refresh_status(self, scope: str, status: str, error: Exception | None = None) -> None:
        """Сохраняет результат обновления для проверки состояния."""
        result = {"status": status, "at": time.time()}
        if error is not None:
            result["error"] = safe_error(error)
        await self.redis_client.hset("cache-state:refresh-status", scope, json.dumps(result))

    async def _run_refresh(
        self, scope: str, key: str, loader: Callable[[], Awaitable[Any]] | None, lock: Lock,
    ) -> bool:
        """Выполняет обновление и освобождает блокировку после завершения."""
        source_token = current_refresh_source.set("background")
        heartbeat = asyncio.create_task(self._renew_refresh_lock(lock))
        builder = asyncio.create_task(self._build_refresh(key, loader))
        try:
            done, _ = await asyncio.wait({builder, heartbeat}, return_when=asyncio.FIRST_COMPLETED)
            if heartbeat in done:
                builder.cancel()
                await asyncio.gather(builder, return_exceptions=True)
                await heartbeat
            await builder
            await self._save_refresh_status(scope, "success")
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception(f"Ошибка обновления {scope}; сохранён предыдущий снимок")
            await self._save_refresh_status(scope, "error", exc)
            return False
        finally:
            current_refresh_source.reset(source_token)
            builder.cancel()
            heartbeat.cancel()
            await asyncio.gather(builder, heartbeat, return_exceptions=True)
            try:
                await lock.release()
            except LockError:
                logger.warning(f"Истёк замок обновления {scope}")

    async def clear_all(self) -> bool:
        """
        Очистка всего кэша.
        
        Returns:
            True если кэш очищен успешно, False иначе
        """
        if not self.is_connected or not self.redis_client:
            logger.warning("Redis не подключен, пропускаем очистку кэша")
            return False
            
        try:
            await self.redis_client.flushdb()
            logger.info("Кэш полностью очищен")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при очистке кэша: {str(e)}")
            return False
    
    async def cleanup_expired_keys(self) -> int:
        """
        Очистка истекших ключей кэша для экономии памяти.
        
        Returns:
            Количество удаленных ключей
        """
        if not self.is_connected or not self.redis_client:
            return 0
            
        try:
            # Получаем все ключи кэша
            all_keys = await self.redis_client.keys("cache:*")
            expired_keys = []
            
            for key in all_keys:
                try:
                    ttl = await self.redis_client.ttl(key)
                    # TTL = -2 означает, что ключ истек, но еще не удален
                    # TTL = -1 означает, что ключ существует без TTL
                    if ttl == -2:
                        expired_keys.append(key)
                except Exception:
                    continue
            
            # Удаляем истекшие ключи
            if expired_keys:
                await self.redis_client.delete(*expired_keys)
                logger.info(f"Очищено {len(expired_keys)} истекших ключей кэша")
                return len(expired_keys)
            
            return 0
            
        except Exception as e:
            logger.error(f"Ошибка при очистке истекших ключей: {str(e)}")
            return 0
    
    async def get_cache_info(self) -> Dict[str, Any]:
        """
        Получение информации о состоянии кэша.
        
        Returns:
            Словарь с информацией о кэше
        """
        if not self.is_connected or not self.redis_client:
            return {"connected": False, "keys_count": 0}
            
        try:
            info = await self.redis_client.info()
            keys_count = await self.redis_client.dbsize()
            refresh_status = await self.redis_client.hgetall("cache-state:refresh-status")
            updated_at = await self.redis_client.hgetall(UPDATED_AT)
            active_refreshes = [
                key.decode().removeprefix("lock:cache-refresh:")
                async for key in self.redis_client.scan_iter(match="lock:cache-refresh:*")
            ]
            
            return {
                "connected": True,
                "keys_count": keys_count,
                "used_memory": info.get("used_memory_human", "Unknown"),
                "connected_clients": info.get("connected_clients", 0),
                "uptime": info.get("uptime_in_seconds", 0),
                "snapshots_updated_at": {
                    key.decode(): float(value) for key, value in updated_at.items()
                },
                "refresh_status": {
                    key.decode(): json.loads(value) for key, value in refresh_status.items()
                },
                "refreshing": bool(active_refreshes),
                "active_refreshes": active_refreshes,
            }
            
        except Exception as e:
            logger.error(f"Ошибка при получении информации о кэше: {str(e)}")
            return {"connected": False, "error": str(e)}
    
    async def warm_up_cache(self) -> bool:
        """Запускает общее обновление и ждёт его результата."""
        await self.request_refresh()
        task = self.refresh_tasks.get("all")
        if task:
            return await asyncio.shield(task)
        return False

    async def _warm_up_cache(self) -> None:
        """Обновляет кеш и записывает результат в диагностику."""
        refresh_id = new_refresh_id()
        source = current_refresh_source.get() or "unknown"
        refresh_token = current_refresh_id.set(refresh_id)
        diagnostics.start_wb_refresh(refresh_id, source)
        try:
            if self.use_optimized_cache:
                logger.info("Используется ОПТИМИЗИРОВАННЫЙ метод прогрева кэша")
                result = await self.warm_up_cache_optimized()
            else:
                logger.info("Используется LEGACY метод прогрева кэша")
                result = await self.warm_up_cache_legacy()
            diagnostics.finish_wb_refresh(refresh_id, "success")
            return result
        except Exception as e:
            diagnostics.finish_wb_refresh(refresh_id, "error", safe_error(e))
            raise
        finally:
            current_refresh_id.reset(refresh_token)

    async def warm_up_cache_legacy(self) -> None:
        # Старый способ запуска тоже сохраняет кеш до готовности нового.
        """Запускает безопасное обновление через прежний метод."""
        await self.warm_up_cache_optimized()

    async def warm_up_cache_optimized(self) -> None:
        """Обновляет списки СЗ и поставок независимо друг от друга."""
        if not self.is_connected:
            raise RuntimeError("Redis недоступен")

        results = await asyncio.gather(
            self._refresh_supplies_snapshot(), self._refresh_orders_snapshot(),
            return_exceptions=True,
        )
        errors = [result for result in results if isinstance(result, BaseException)]
        if errors:
            raise RuntimeError("; ".join(str(error) for error in errors))

    async def _refresh_supplies_snapshot(self) -> None:
        """Получает поставки и сохраняет четыре списка одной транзакцией."""
        service = SuppliesService(main_db)
        unified = await self._get_all_supplies_data_ultra_optimized(service)
        combinations = await self._generate_all_combinations_from_unified_data(unified, service)
        values = {
            "cache:supplies_all:hanging_only:False|is_delivery:False": combinations["wb_normal"],
            "cache:supplies_all:hanging_only:True|is_delivery:False": combinations["wb_hanging"],
            "cache:supplies_all:hanging_only:False|is_delivery:True": combinations["delivery_normal"],
            "cache:supplies_all:hanging_only:True|is_delivery:True": combinations["delivery_hanging"],
        }
        await publish_snapshots(self.redis_client, values)
        try:
            await EmptySupplyCleaner(self.redis_client).auto_clean_empty_supplies()
            sync_hanging_supplies_with_data.delay(unified["wb_supplies_grouped"])
        except Exception:
            logger.exception("Ошибка обслуживания поставок после публикации кеша")

    async def _refresh_orders_snapshot(self) -> None:
        """Обновляет списки СЗ с учётом фильтра по остаткам."""
        service = OrdersService(main_db)
        data = await service.get_filtered_orders(time_delta=1.0, article=None)
        grouped = await service.group_orders_by_wild([OrderDetail(**order) for order in data])
        values = {"cache:orders_all:time_delta:1.0|wild:None": grouped}
        for positive_stock in (False, True):
            key = f"cache:orders_all:positive_stock:{positive_stock}|time_delta:1.0|wild:None"
            values[key] = service.filter_orders_by_stock(grouped, positive_stock)
        await publish_snapshots(self.redis_client, values)

    async def _get_all_supplies_data_ultra_optimized(self, supplies_service) -> Dict[str, Any]:
        """
        УЛЬТРА-ОПТИМИЗИРОВАННОЕ получение ВСЕХ данных единым блоком:
        1. WB API вызывается ОДИН РАЗ для всех случаев
        2. БД запросы делаются ОДИН РАЗ
        3. Для delivery используется трехуровневая система: Redis → PostgreSQL (delivered_supplies) → WB API
        4. Все 4 комбинации генерируются из одних данных
        """
        logger.info("Получение ВСЕХ данных поставок ультра-оптимизированным способом...")

        # 1. ЕДИНСТВЕННЫЙ вызов WB API для получения ВСЕХ WB поставок
        logger.info("1/5: Получение ВСЕХ WB поставок...")
        all_active_supplies = await supplies_service.get_information_to_supplies(
            strict=True, include_fbs2=True,
        )
        wb_supplies_ids = supplies_service.exclude_fbs2_assembly_supplies(all_active_supplies)
        logger.info("2/5: Получение ВСЕХ WB заказов...")
        wb_orders_data = await supplies_service.get_information_orders_to_supplies(wb_supplies_ids, strict=True)  # ОДИН РАЗ!
        wb_supplies_grouped = supplies_service.group_result(wb_orders_data)

        # 2. ЕДИНСТВЕННЫЕ запросы к БД для delivery данных
        logger.info("3/5: Получение delivery данных из БД...")
        basic_supplies_ids = await ShipmentOfGoods(supplies_service.db).get_weekly_supply_ids()  # ОДИН РАЗ!
        fictitious_supplies_ids = await HangingSupplies(supplies_service.db).get_weekly_fictitious_supplies_ids(
            is_fictitious_delivered=True)  # ОДИН РАЗ!

        # 3. Подготовка delivery данных (используем уже полученные wb_supplies_ids!)
        logger.info("4/5: Обработка delivery поставок...")
        all_db_supplies_ids = supplies_service._merge_supplies_data(basic_supplies_ids, fictitious_supplies_ids)
        filtered_delivery_supplies_ids = supplies_service._exclude_wb_active_from_db_supplies(
            all_db_supplies_ids, all_active_supplies
        )

        # 4. НОВОЕ: Используем трехуровневую систему кэширования для delivery поставок
        logger.info("5/5: Применение трехуровневой системы для delivery (Redis → PostgreSQL → WB API)...")

        # Формируем список запрашиваемых поставок для проверки в БД
        requested_supplies = []
        for account_data in filtered_delivery_supplies_ids:
            for account, supplies_list in account_data.items():
                for supply in supplies_list:
                    requested_supplies.append((supply['id'], account))

        logger.info(f"Запрошено {len(requested_supplies)} delivery поставок для кэширования")

        # Проверяем БД хранилище delivered_supplies (Tier 2)
        delivered_storage = DeliveredSupplies(supplies_service.db)
        stored_supplies = await delivered_storage.get_supplies_from_storage(requested_supplies)
        missing_supplies = await delivered_storage.get_missing_supplies(requested_supplies)

        # Метрики для логирования
        db_hit_count = len(stored_supplies)
        db_miss_count = len(missing_supplies)
        db_hit_rate = (db_hit_count / len(requested_supplies) * 100) if requested_supplies else 0

        logger.info(
            f"Статистика БД хранилища при прогреве: "
            f"total={len(requested_supplies)}, from_db={db_hit_count}, "
            f"from_api={db_miss_count}, db_hit_rate={db_hit_rate:.1f}%"
        )

        # Получаем недостающие из WB API (Tier 3) только если нужно
        delivery_supplies_for_grouping = {}
        if missing_supplies:
            logger.info(f"Получение {len(missing_supplies)} недостающих delivery поставок из WB API")

            # Формируем структуру для WB API
            missing_by_account = {}
            for supply_id, account in missing_supplies:
                if account not in missing_by_account:
                    missing_by_account[account] = []
                missing_by_account[account].append({'id': supply_id})

            missing_formatted = [{acc: sups} for acc, sups in missing_by_account.items()]

            # Получаем данные из WB API
            enriched_supplies = await supplies_service.get_information_to_supply_details(missing_formatted)
            missing_orders = await supplies_service.get_information_orders_to_supplies(enriched_supplies, strict=True)

            # Формируем result для недостающих
            missing_result = []
            enriched_dict = {key: val for d in enriched_supplies for key, val in d.items()}

            for order_data in missing_orders:
                for account, supply_orders in order_data.items():
                    for supply_id, orders in supply_orders.items():
                        supply_meta = {
                            data["id"]: {"name": data["name"], "createdAt": data['createdAt']}
                            for data in enriched_dict.get(account, [])
                        }

                        if supply_id in supply_meta:
                            supply_obj = supplies_service.create_supply_result(
                                supply_meta,
                                supply_id,
                                account,
                                orders
                            )
                            missing_result.append(supply_obj)

            # Сохраняем недостающие в БД хранилище для будущих прогревов
            if missing_result:
                saved_count = await delivered_storage.save_supplies_to_storage(missing_result)
                logger.info(f"Сохранено {saved_count} новых delivery поставок в БД хранилище при прогреве")

                # Добавляем к stored_supplies
                for supply_obj in missing_result:
                    key = (supply_obj['supply_id'], supply_obj['account'])
                    stored_supplies[key] = supply_obj

        logger.info("ВСЕ данные поставок получены ультра-оптимизированным способом с БД кэшированием!")

        return {
            'wb_supplies_ids': wb_supplies_ids,
            'wb_supplies_grouped': wb_supplies_grouped,
            'delivery_supplies_data': stored_supplies,  # Данные из БД (Tier 2) + недостающие из API (Tier 3)
            'delivery_supplies_ids': filtered_delivery_supplies_ids
        }

    async def _generate_all_combinations_from_unified_data(self, unified_data, supplies_service):
        """
        Генерирует все 4 комбинации из единых данных без дополнительных API вызовов.
        УЛЬТРА-ОПТИМИЗАЦИЯ: вся фильтрация происходит в памяти!
        """
        logger.info("Генерация всех 4 комбинаций из единых данных...")
        
        combinations = {}
        
        try:
            # 1. WB Normal (hanging_only=False, is_delivery=False)
            logger.info("Генерация WB Normal...")
            combinations['wb_normal'] = await self._filter_wb_supplies_ultra_optimized(
                unified_data['wb_supplies_grouped'], 
                unified_data['wb_supplies_ids'], 
                hanging_only=False,
                supplies_service=supplies_service
            )
            
            # 2. WB Hanging (hanging_only=True, is_delivery=False)  
            logger.info("Генерация WB Hanging...")
            combinations['wb_hanging'] = await self._filter_wb_supplies_ultra_optimized(
                unified_data['wb_supplies_grouped'],
                unified_data['wb_supplies_ids'], 
                hanging_only=True,
                supplies_service=supplies_service
            )
            
            # 3. Delivery Normal (hanging_only=False, is_delivery=True)
            logger.info("Генерация Delivery Normal из БД хранилища...")
            combinations['delivery_normal'] = await self._filter_delivery_supplies_ultra_optimized(
                unified_data['delivery_supplies_data'],  # ИЗМЕНЕНО: данные из БД
                unified_data['delivery_supplies_ids'],
                hanging_only=False,
                supplies_service=supplies_service
            )

            # 4. Delivery Hanging (hanging_only=True, is_delivery=True)
            logger.info("Генерация Delivery Hanging из БД хранилища...")
            combinations['delivery_hanging'] = await self._filter_delivery_supplies_ultra_optimized(
                unified_data['delivery_supplies_data'],  # ИЗМЕНЕНО: данные из БД
                unified_data['delivery_supplies_ids'],
                hanging_only=True,
                supplies_service=supplies_service
            )
            
            logger.info("Все 4 комбинации сгенерированы успешно!")
            return combinations
            
        except Exception as e:
            logger.error(f"Ошибка генерации комбинаций: {str(e)}")
            raise

    async def _filter_wb_supplies_ultra_optimized(
        self, 
        wb_supplies_grouped: Dict[str, Dict], 
        wb_supplies_ids: List[Dict],
        hanging_only: bool,
        supplies_service
    ) -> SupplyIdResponseSchema:
        """
        УЛЬТРА-ОПТИМИЗИРОВАННАЯ фильтрация WB поставок - БЕЗ дополнительных API вызовов.
        """
        try:
            result = []
            supplies_ids_dict = {key: value for d in wb_supplies_ids for key, value in d.items()}
            
            for account, value in wb_supplies_grouped.items():
                for supply_id, orders in value.items():
                    # Формируем данные поставки (не delivery)
                    supply = {
                        data["id"]: {"name": data["name"], "createdAt": data['createdAt']}
                        for data in supplies_ids_dict[account] if not data['done']
                    }
                    
                    result.append(supplies_service.create_supply_result(supply, supply_id, account, orders))
            
            # Применяем фильтр по hanging_only
            filtered_result = await supplies_service.filter_supplies_by_hanging(result, hanging_only)
            return SupplyIdResponseSchema(supplies=filtered_result)
            
        except Exception as e:
            logger.error(f"Ошибка ультра-фильтрации WB поставок: {str(e)}")
            raise

    async def _filter_delivery_supplies_ultra_optimized(
        self,
        delivery_supplies_data: Dict[tuple, Dict],  # ИЗМЕНЕНО: теперь принимаем данные из БД
        delivery_supplies_ids: List[Dict],
        hanging_only: bool,
        supplies_service
    ) -> SupplyIdResponseSchema:
        """
        УЛЬТРА-ОПТИМИЗИРОВАННАЯ фильтрация delivery поставок из БД хранилища (delivered_supplies).

        Использует данные, полученные через трехуровневую систему:
        Redis → PostgreSQL (delivered_supplies) → WB API

        Args:
            delivery_supplies_data: Данные из БД в формате {(supply_id, account): {полная структура}}
            delivery_supplies_ids: Метаданные для дополнительной информации
            hanging_only: Фильтр по висячим поставкам
            supplies_service: Сервис поставок для фильтрации

        Returns:
            SupplyIdResponseSchema: Отфильтрованные delivery поставки
        """
        try:
            # Преобразуем данные из БД в список результатов
            result = list(delivery_supplies_data.values())

            logger.info(f"Фильтрация {len(result)} delivery поставок из БД хранилища (hanging_only={hanging_only})")

            # Применяем фильтр по hanging_only (та же логика, что в get_list_supplies)
            filtered_result = await supplies_service.filter_supplies_by_hanging(result, hanging_only)

            logger.info(f"После фильтрации осталось {len(filtered_result)} delivery поставок")

            return SupplyIdResponseSchema(supplies=filtered_result)

        except Exception as e:
            logger.error(f"Ошибка ультра-фильтрации delivery поставок из БД: {str(e)}")
            raise
    
    async def _background_refresh_task(self) -> None:
        """Фоновая задача для обновления кэша."""
        refresh_count = 0
        while True:
            try:
                # Обновляем каждые 8 минут вместо 10, чтобы данные не успевали истечь
                refresh_interval = max(1, int(settings.CACHE_REFRESH_INTERVAL * 0.8))
                await asyncio.sleep(refresh_interval)
                refresh_count += 1
                
                logger.info(f"[Refresh #{refresh_count}] Запуск фонового обновления кэша...")
                logger.info(f"Интервал обновления: {settings.CACHE_REFRESH_INTERVAL} секунд")
                
                # Проверяем состояние Redis перед обновлением
                if not self.is_connected:
                    logger.error("Redis недоступен, пропускаем обновление кэша")
                    continue
                
                # Логируем текущее состояние кэша
                cache_info = await self.get_cache_info()
                logger.info(f"Состояние кэша перед обновлением: {cache_info}")
                
                # Очищаем истекшие ключи перед обновлением
                cleaned_keys = await self.cleanup_expired_keys()
                if cleaned_keys > 0:
                    logger.info(f"Очищено {cleaned_keys} истекших ключей перед обновлением")
                
                # Обновляем кэш
                source_token = current_refresh_source.set("background")
                try:
                    success = await self.warm_up_cache()
                finally:
                    current_refresh_source.reset(source_token)
                
                # Логируем состояние после обновления
                cache_info_after = await self.get_cache_info()
                logger.info(f"[Refresh #{refresh_count}] Обновление завершено, success={success}")
                logger.info(f"Состояние кэша после обновления: {cache_info_after}")
                
            except asyncio.CancelledError:
                logger.info("Фоновая задача обновления кэша отменена")
                break
            except Exception as e:
                logger.error(f"КРИТИЧЕСКАЯ ОШИБКА в фоновой задаче обновления кэша (refresh #{refresh_count}): {str(e)}")
                logger.error(f"Попытаемся продолжить через {settings.CACHE_REFRESH_INTERVAL} секунд...")
                # Продолжаем работу, даже если произошла ошибка
                continue
    
    async def start_background_refresh_all(self) -> None:
        """Запуск фонового обновления кэша."""
        if not self.is_connected:
            logger.warning("Redis не подключен, пропускаем запуск фонового обновления")
            return
            
        logger.info("Запускаем фоновое обновление кэша...")
        logger.info(f"Интервал обновления: {settings.CACHE_REFRESH_INTERVAL} секунд")
        task = asyncio.create_task(self._background_refresh_task())
        self.background_tasks.append(task)
    
    async def diagnose_cache_keys(self) -> dict:
        """Диагностика ключей кэша для отладки."""
        if not self.is_connected or not self.redis_client:
            return {"error": "Redis недоступен"}
        
        try:
            all_keys = await self.redis_client.keys("cache:*")
            keys_info = {}
            
            for key in all_keys:
                key_str = key.decode('utf-8') if isinstance(key, bytes) else str(key)
                try:
                    ttl = await self.redis_client.ttl(key)
                    size = await self.redis_client.memory_usage(key)
                    keys_info[key_str] = {
                        "ttl": ttl,
                        "size_bytes": size or 0
                    }
                except Exception as e:
                    keys_info[key_str] = {"error": str(e)}
            
            return {
                "total_keys": len(all_keys),
                "keys": keys_info,
                "expected_keys": [
                    "cache:supplies_all:hanging_only:False|is_delivery:False",
                    "cache:supplies_all:hanging_only:True|is_delivery:False", 
                    "cache:supplies_all:hanging_only:False|is_delivery:True",
                    "cache:supplies_all:hanging_only:True|is_delivery:True",
                    "cache:orders_all:time_delta:1.0|wild:None"
                ]
            }
            
        except Exception as e:
            return {"error": f"Ошибка диагностики: {str(e)}"}
            
    async def force_refresh_cache(self) -> bool:
        """Принудительное обновление кэша."""
        logger.info("Принудительное обновление кэша...")
        try:
            source_token = current_refresh_source.set("manual")
            try:
                success = await self.warm_up_cache()
            finally:
                current_refresh_source.reset(source_token)
            logger.info("Принудительное обновление кэша завершено успешно")
            return success
        except Exception as e:
            logger.error(f"Ошибка при принудительном обновлении кэша: {str(e)}")
            return False

    async def refresh_specific_cache(self, cache_type: str, hanging_only: bool = None, is_delivery: bool = None) -> bool:
        """
        Принудительное обновление конкретного типа кэша.
        
        Args:
            cache_type: Тип кэша ('supplies' или 'orders')
            hanging_only: Для поставок - только висячие (True) или обычные (False), None - все
            is_delivery: Для поставок - из доставки (True) или WB API (False), None - все
            
        Returns:
            bool: True если обновление успешно
        """
        logger.info(f"Селективное обновление кэша: type={cache_type}, hanging_only={hanging_only}, is_delivery={is_delivery}")
        
        if not self.is_connected:
            logger.warning("Redis не подключен, пропускаем селективное обновление кэша")
            return False
        
        try:
            if cache_type == "supplies":
                return await self._refresh_supplies_cache(hanging_only, is_delivery)
            else:
                logger.error(f"Неизвестный тип кэша: {cache_type}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка при селективном обновлении кэша {cache_type}: {str(e)}")
            return False
    
    async def _refresh_supplies_cache(self, hanging_only=None, is_delivery=None) -> bool:
        # Связанные списки поставок обновляем вместе.
        """Обновляет связанные списки поставок под общей блокировкой."""
        return await self.warm_up_cache()

    def _extract_supply_ids_from_cache(self, cached_data) -> set:
        """
        Извлекает supply_id из кэшированных данных.
        
        Args:
            cached_data: Кэшированные данные типа SupplyIdResponseSchema
            
        Returns:
            set: Множество supply_id из кэша
        """
        if not cached_data or not hasattr(cached_data, 'supplies'):
            return set()
        return {supply.supply_id for supply in cached_data.supplies}

    def _calculate_supply_differences(self, cached_ids: set, current_ids: set) -> Dict[str, Any]:
        """
        Вычисляет различия между множествами supply_id.
        
        Args:
            cached_ids: Множество supply_id из кэша
            current_ids: Множество текущих supply_id из БД
            
        Returns:
            Dict с результатами сравнения
        """
        new_supplies = list(current_ids - cached_ids)
        removed_supplies = list(cached_ids - current_ids)
        
        return {
            "cached_count": len(cached_ids),
            "current_count": len(current_ids),
            "new_supplies": new_supplies,
            "removed_supplies": removed_supplies,
            "has_changes": len(new_supplies) > 0 or len(removed_supplies) > 0
        }

    async def check_delivery_supplies_diff(self) -> Dict[str, Any]:
        """
        Проверяет различия между актуальными и кэшированными поставками доставки.
        
        Сравнивает только номера supply_id для эффективности.
        Проверяет оба типа delivery поставок:
        - Обычные (hanging_only=False, is_delivery=True)  
        - Висячие (hanging_only=True, is_delivery=True)
        
        Returns:
            Dict с результатами сравнения и метриками
        """
        if not self.is_connected:
            return {"error": "Redis недоступен"}
        
        start_time = datetime.utcnow()
        
        try:
            
            # Получаем кэшированные данные
            cached_delivery_normal = await self.get("cache:supplies_all:hanging_only:False|is_delivery:True")
            cached_delivery_hanging = await self.get("cache:supplies_all:hanging_only:True|is_delivery:True")
            
            # Извлекаем supply_id из кэша
            cached_normal_ids = self._extract_supply_ids_from_cache(cached_delivery_normal)
            cached_hanging_ids = self._extract_supply_ids_from_cache(cached_delivery_hanging)
            
            # Получаем текущие supply_id напрямую из БД (только номера)
            supplies_service = SuppliesService(main_db)
                
            current_normal_ids = await supplies_service.get_delivery_supplies_ids_only(hanging_only=False)
            current_hanging_ids = await supplies_service.get_delivery_supplies_ids_only(hanging_only=True)
            
            # Вычисляем различия
            normal_diff = self._calculate_supply_differences(cached_normal_ids, current_normal_ids)
            hanging_diff = self._calculate_supply_differences(cached_hanging_ids, current_hanging_ids)
            
            duration = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            result = {
                "timestamp": start_time.isoformat(),
                "check_duration_ms": round(duration, 2),
                "method": "optimized_supply_ids_only",
                
                "delivery_normal": {
                    **normal_diff,
                    "cache_key": "cache:supplies_all:hanging_only:False|is_delivery:True"
                },
                
                "delivery_hanging": {
                    **hanging_diff,
                    "cache_key": "cache:supplies_all:hanging_only:True|is_delivery:True"
                },
                
                "summary": {
                    "total_changes": normal_diff["has_changes"] or hanging_diff["has_changes"],
                    "total_new": len(normal_diff["new_supplies"]) + len(hanging_diff["new_supplies"]),
                    "total_removed": len(normal_diff["removed_supplies"]) + len(hanging_diff["removed_supplies"]),
                    "affected_cache_keys": sum([normal_diff["has_changes"], hanging_diff["has_changes"]])
                }
            }
            
            if result["summary"]["total_changes"]:
                logger.info(f"Обнаружены изменения в delivery поставках: {result['summary']}")
            else:
                logger.info("Изменений в delivery поставках не обнаружено")
                
            return result
            
        except Exception as e:
            logger.error(f"Ошибка проверки различий delivery поставок: {str(e)}")
            return {"error": str(e)}


# Глобальный экземпляр кэша
global_cache = GlobalCache()
