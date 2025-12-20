"""
Middleware для предотвращения дублирующих запросов (защита от двойного клика).

Использует Redis SETNX для атомарной блокировки идентичных запросов.
При недоступности Redis - graceful degradation (запросы проходят без проверки).

ВАЖНО: Реализован как чистый ASGI middleware (не BaseHTTPMiddleware)
для корректной работы со StreamingResponse.
"""
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send, Message
from src.cache.global_cache import global_cache
from src.logger import app_logger as logger
import hashlib
import json
import time
from typing import Optional, Set, Any, List


class DuplicateRequestMiddleware:
    """
    Чистый ASGI Middleware для предотвращения дублирующих запросов.

    Принцип работы:
    1. Формирует уникальный ключ из: path + user_token + hash(body)
    2. Пытается установить ключ в Redis с SETNX (атомарно)
    3. Если ключ уже существует - запрос дублирующий - 409 Conflict
    4. После обработки запроса - удаляет ключ

    ВАЖНО: Реализован как чистый ASGI middleware для совместимости
    со StreamingResponse (BaseHTTPMiddleware имеет известные проблемы).
    """

    # Эндпоинты для защиты
    PROTECTED_PATHS: Set[str] = {
        "/api/v1/supplies/move-orders",
        "/api/v1/supplies/delivery",
        "/api/v1/supplies/delivery-hanging",
        "/api/v1/supplies/delivery-fictitious",
        "/api/v1/supplies/shipment_of_fictions",
        "/api/v1/supplies/shipment-hanging-actual",
        "/api/v1/orders/with-supply-name",
    }

    # Поля которые ИСКЛЮЧАЕМ из хеширования (меняются между запросами)
    EXCLUDED_FIELDS: Set[str] = {
        "operation_id",  # Уникален для каждой сессии
        "timestamp",     # Метка времени
        "request_id",    # ID запроса
    }

    LOCK_TIMEOUT: int = 120  # 2 минуты (move-orders может быть долгим!)
    LOCK_PREFIX: str = "dup:"
    MAX_BODY_SIZE: int = 1024 * 1024  # 1MB для хеширования

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """ASGI interface."""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Быстрая проверка - нужна ли защита
        path = scope.get("path", "")
        method = scope.get("method", "")

        if not self._should_protect_path(path, method):
            await self.app(scope, receive, send)
            return

        # Проверяем Redis
        redis = self._get_redis_safe()
        if redis is None:
            logger.warning(
                f"[DuplicateMiddleware] Redis недоступен, "
                f"пропускаем проверку для {path}"
            )
            await self.app(scope, receive, send)
            return

        # Читаем body
        body_chunks: List[bytes] = []

        async def receive_wrapper() -> Message:
            message = await receive()
            if message["type"] == "http.request":
                body = message.get("body", b"")
                if body:
                    body_chunks.append(body)
            return message

        # Собираем всё тело запроса
        body = b""
        while True:
            message = await receive_wrapper()
            if message["type"] == "http.request":
                if not message.get("more_body", False):
                    break
            elif message["type"] == "http.disconnect":
                return

        body = b"".join(body_chunks)

        # Ограничиваем размер для хеширования
        body_for_hash = body[:self.MAX_BODY_SIZE] if len(body) > self.MAX_BODY_SIZE else body

        # Генерируем ключ блокировки
        lock_key = self._generate_lock_key(scope, body_for_hash)

        # Пытаемся получить блокировку
        acquired = await self._try_acquire_lock(redis, lock_key)

        if not acquired:
            # Отправляем 409 Conflict
            response = self._create_conflict_response(path, lock_key)
            await response(scope, receive, send)
            return

        # Создаём новый receive который вернёт сохранённый body
        body_sent = False

        async def cached_receive() -> Message:
            nonlocal body_sent
            if not body_sent:
                body_sent = True
                return {
                    "type": "http.request",
                    "body": body,
                    "more_body": False,
                }
            # После отдачи body ждём disconnect
            return await receive()

        # Обрабатываем запрос
        try:
            await self.app(scope, cached_receive, send)
        finally:
            await self._release_lock_safe(redis, lock_key)

    # ==================== ПРОВЕРКИ ====================

    def _should_protect_path(self, path: str, method: str) -> bool:
        """Определяет нужна ли защита для этого запроса."""
        if method not in ("POST", "PATCH"):
            return False

        for protected in self.PROTECTED_PATHS:
            if path == protected or path.startswith(protected + "/"):
                return True

        return False

    def _get_redis_safe(self) -> Optional[Any]:
        """Безопасно получает Redis клиент."""
        try:
            if global_cache.is_connected and global_cache.redis_client:
                return global_cache.redis_client
        except Exception as e:
            logger.error(f"[DuplicateMiddleware] Ошибка доступа к Redis: {e}")
        return None

    # ==================== ГЕНЕРАЦИЯ КЛЮЧА ====================

    def _generate_lock_key(self, scope: Scope, body: bytes) -> str:
        """
        Генерирует уникальный ключ блокировки.

        Формат: dup:{path_hash}:{user_hash}:{body_hash}
        """
        # 1. Path hash
        path = scope.get("path", "")
        path_hash = hashlib.md5(path.encode()).hexdigest()[:8]

        # 2. User hash (из Bearer token)
        user_hash = self._extract_user_identifier(scope)

        # 3. Body hash (с исключением динамических полей)
        body_hash = self._hash_body_excluding_fields(body)

        return f"{self.LOCK_PREFIX}{path_hash}:{user_hash}:{body_hash}"

    def _extract_user_identifier(self, scope: Scope) -> str:
        """
        Извлекает идентификатор пользователя из Authorization header.
        """
        headers = dict(scope.get("headers", []))
        auth_header = headers.get(b"authorization", b"").decode("utf-8", errors="ignore")

        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
            return hashlib.md5(token.encode()).hexdigest()[:12]

        # Fallback: IP + User-Agent
        client = scope.get("client")
        client_ip = client[0] if client else "unknown"
        user_agent = headers.get(b"user-agent", b"").decode("utf-8", errors="ignore")[:50]
        fallback = f"{client_ip}:{user_agent}"
        return hashlib.md5(fallback.encode()).hexdigest()[:12]

    def _hash_body_excluding_fields(self, body: bytes) -> str:
        """
        Хеширует body, исключая динамические поля.
        """
        try:
            data = json.loads(body.decode('utf-8'))
            cleaned_data = self._remove_excluded_fields(data)
            cleaned_body = json.dumps(cleaned_data, sort_keys=True)
            return hashlib.sha256(cleaned_body.encode()).hexdigest()[:16]
        except (json.JSONDecodeError, UnicodeDecodeError):
            return hashlib.sha256(body).hexdigest()[:16]

    def _remove_excluded_fields(self, data: Any) -> Any:
        """Рекурсивно удаляет исключённые поля из структуры данных."""
        if isinstance(data, dict):
            return {
                k: self._remove_excluded_fields(v)
                for k, v in data.items()
                if k not in self.EXCLUDED_FIELDS
            }
        elif isinstance(data, list):
            return [self._remove_excluded_fields(item) for item in data]
        else:
            return data

    # ==================== БЛОКИРОВКА ====================

    async def _try_acquire_lock(self, redis, lock_key: str) -> bool:
        """Атомарно пытается получить блокировку."""
        try:
            result = await redis.set(
                lock_key,
                value=f"{time.time()}",
                nx=True,
                ex=self.LOCK_TIMEOUT
            )

            acquired = result is not None

            if acquired:
                logger.debug(f"[DuplicateMiddleware] Блокировка получена: {lock_key}")
            else:
                logger.info(f"[DuplicateMiddleware] Блокировка занята: {lock_key}")

            return acquired

        except Exception as e:
            logger.error(f"[DuplicateMiddleware] Ошибка получения блокировки: {e}")
            return True

    async def _release_lock_safe(self, redis, lock_key: str) -> None:
        """Безопасно освобождает блокировку."""
        try:
            await redis.delete(lock_key)
            logger.debug(f"[DuplicateMiddleware] Блокировка освобождена: {lock_key}")
        except Exception as e:
            logger.warning(
                f"[DuplicateMiddleware] Не удалось освободить блокировку {lock_key}: {e}. "
                f"Истечёт автоматически через {self.LOCK_TIMEOUT} сек."
            )

    # ==================== ОТВЕТ ОБ ОШИБКЕ ====================

    def _create_conflict_response(self, path: str, lock_key: str) -> JSONResponse:
        """Создаёт ответ о конфликте (дублирующий запрос)."""
        logger.warning(
            f"[DuplicateMiddleware] ЗАБЛОКИРОВАН дублирующий запрос: {path} [key={lock_key}]"
        )

        return JSONResponse(
            status_code=409,
            content={
                "success": False,
                "message": "Запрос уже обрабатывается. Пожалуйста, дождитесь завершения.",
                "error_code": "DUPLICATE_REQUEST",
                "detail": "Идентичный запрос находится в обработке. "
                          "Повторите попытку через несколько секунд.",
                "retry_after_seconds": 5
            },
            headers={
                "Retry-After": "5",
                "X-Duplicate-Blocked": "true"
            }
        )
