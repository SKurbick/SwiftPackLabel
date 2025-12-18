"""
Middleware для предотвращения дублирующих запросов (защита от двойного клика).

Использует Redis SETNX для атомарной блокировки идентичных запросов.
При недоступности Redis - graceful degradation (запросы проходят без проверки).
"""
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from src.cache.global_cache import global_cache
from src.logger import app_logger as logger
import hashlib
import json
import time
from typing import Optional, Set, Any


class DuplicateRequestMiddleware(BaseHTTPMiddleware):
    """
    Middleware для предотвращения дублирующих запросов.

    Принцип работы:
    1. Формирует уникальный ключ из: path + user_token + hash(body)
    2. Пытается установить ключ в Redis с SETNX (атомарно)
    3. Если ключ уже существует - запрос дублирующий - 409 Conflict
    4. После обработки запроса - удаляет ключ

    ВАЖНО: User недоступен на этапе middleware (auth через Depends),
    поэтому используем Bearer token для идентификации.

    Graceful Degradation:
    - Если Redis недоступен - пропускает запрос БЕЗ блокировки
    - Логирует предупреждение для мониторинга
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

    async def dispatch(self, request: Request, call_next) -> Response:
        """Основной обработчик middleware."""

        # 1. Быстрые проверки - пропускаем ненужные запросы
        if not self._should_protect(request):
            return await call_next(request)

        # 2. Проверяем Redis
        redis = self._get_redis_safe()
        if redis is None:
            logger.warning(
                f"[DuplicateMiddleware] Redis недоступен, "
                f"пропускаем проверку для {request.url.path}"
            )
            return await call_next(request)

        # 3. Читаем и сохраняем body
        try:
            body = await self._read_body_safe(request)
        except Exception as e:
            logger.error(f"[DuplicateMiddleware] Ошибка чтения body: {e}")
            return await call_next(request)

        # 4. Генерируем ключ блокировки
        lock_key = self._generate_lock_key(request, body)

        # 5. Пытаемся получить блокировку
        acquired = await self._try_acquire_lock(redis, lock_key)

        if not acquired:
            return self._create_conflict_response(request, lock_key)

        # 6. Обрабатываем запрос
        try:
            response = await self._process_request_with_body(request, call_next, body)
            self._add_protection_headers(response, lock_key)
            return response
        finally:
            await self._release_lock_safe(redis, lock_key)

    # ==================== ПРОВЕРКИ ====================

    def _should_protect(self, request: Request) -> bool:
        """Определяет нужна ли защита для этого запроса."""
        # Только POST и PATCH
        if request.method not in ("POST", "PATCH"):
            return False

        # Проверяем путь
        path = request.url.path
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

    # ==================== ЧТЕНИЕ BODY ====================

    async def _read_body_safe(self, request: Request) -> bytes:
        """Безопасно читает body с ограничением размера."""
        body = await request.body()

        # Ограничиваем размер для хеширования
        if len(body) > self.MAX_BODY_SIZE:
            logger.warning(
                f"[DuplicateMiddleware] Body слишком большой ({len(body)} bytes), "
                f"хешируем первые {self.MAX_BODY_SIZE} bytes"
            )
            return body[:self.MAX_BODY_SIZE]

        return body

    # ==================== ГЕНЕРАЦИЯ КЛЮЧА ====================

    def _generate_lock_key(self, request: Request, body: bytes) -> str:
        """
        Генерирует уникальный ключ блокировки.

        Формат: dup:{path_hash}:{user_hash}:{body_hash}

        ВАЖНО:
        - User берётся из Authorization header (token), НЕ из request.state
        - Исключаем изменяющиеся поля (operation_id) из body hash
        """
        # 1. Path hash
        path = request.url.path
        path_hash = hashlib.md5(path.encode()).hexdigest()[:8]

        # 2. User hash (из Bearer token)
        user_hash = self._extract_user_identifier(request)

        # 3. Body hash (с исключением динамических полей)
        body_hash = self._hash_body_excluding_fields(body)

        return f"{self.LOCK_PREFIX}{path_hash}:{user_hash}:{body_hash}"

    def _extract_user_identifier(self, request: Request) -> str:
        """
        Извлекает идентификатор пользователя из Authorization header.

        ВАЖНО: На этапе middleware user ещё не авторизован через Depends!
        Поэтому используем сам токен (или его хеш) как идентификатор.
        """
        auth_header = request.headers.get("Authorization", "")

        if auth_header.startswith("Bearer "):
            token = auth_header[7:]  # Убираем "Bearer "
            # Хешируем токен (не храним в открытом виде)
            return hashlib.md5(token.encode()).hexdigest()[:12]

        # Fallback: IP + User-Agent
        client_ip = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("User-Agent", "")[:50]
        fallback = f"{client_ip}:{user_agent}"
        return hashlib.md5(fallback.encode()).hexdigest()[:12]

    def _hash_body_excluding_fields(self, body: bytes) -> str:
        """
        Хеширует body, исключая динамические поля.

        Это важно! Например, operation_id меняется между запросами,
        но это НЕ означает что запросы разные по сути.
        """
        try:
            # Пытаемся распарсить как JSON
            data = json.loads(body.decode('utf-8'))

            # Рекурсивно удаляем исключённые поля
            cleaned_data = self._remove_excluded_fields(data)

            # Сериализуем обратно (с сортировкой ключей для стабильности)
            cleaned_body = json.dumps(cleaned_data, sort_keys=True)
            return hashlib.sha256(cleaned_body.encode()).hexdigest()[:16]

        except (json.JSONDecodeError, UnicodeDecodeError):
            # Не JSON - хешируем как есть (multipart, binary, etc.)
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
                nx=True,   # Only if Not eXists
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
            # При ошибке - пропускаем (graceful degradation)
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

    # ==================== ОБРАБОТКА ЗАПРОСА ====================

    async def _process_request_with_body(
        self,
        request: Request,
        call_next,
        body: bytes
    ) -> Response:
        """
        Обрабатывает запрос, восстанавливая body для route handler.

        КРИТИЧНО: Body уже был прочитан в middleware,
        нужно "вернуть" его для Pydantic валидации в endpoint.
        """
        # Создаём новую функцию receive, которая вернёт сохранённый body
        async def receive():
            return {"type": "http.request", "body": body}

        # Заменяем receive в request
        request._receive = receive

        return await call_next(request)

    def _add_protection_headers(self, response: Response, lock_key: str) -> None:
        """Добавляет заголовки для отладки."""
        try:
            response.headers["X-Duplicate-Protected"] = "true"
            response.headers["X-Lock-Key"] = lock_key[:20] + "..."
        except Exception:
            pass  # Streaming responses могут не поддерживать headers

    # ==================== ОТВЕТ ОБ ОШИБКЕ ====================

    def _create_conflict_response(self, request: Request, lock_key: str) -> JSONResponse:
        """Создаёт ответ о конфликте (дублирующий запрос)."""
        logger.warning(
            f"[DuplicateMiddleware] ЗАБЛОКИРОВАН дублирующий запрос: "
            f"{request.method} {request.url.path} [key={lock_key}]"
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
