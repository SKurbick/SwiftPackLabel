import asyncio
import json
import random
import time
import weakref
from dataclasses import dataclass
from typing import Any, Callable, Dict, Generic, Optional, TypeVar
from urllib.parse import urlsplit

import aiohttp
import requests
from requests import Session

from src.diagnostics import diagnostics, get_refresh_id, sanitize_url
from src.logger import app_logger as logger
from src.settings import settings

T = TypeVar("T")

RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})

ERROR_BODY_PREVIEW_LEN = 500


class ExternalApiError(RuntimeError):
    """Запрос к внешнему API не удался: все попытки исчерпаны или ответ неповторяем."""


def ensure_response(response: str | bytes | None, description: str) -> str | bytes:
    """Возвращает тело ответа или падает, если запрос не удался."""
    if response is None:
        raise ExternalApiError(f"{description}: запрос к внешнему API не удался")
    return response


class HttpClient:
    """Синхронный HTTP-клиент (для кода вне event loop: скрипты, Celery)."""

    def __init__(self, timeout: int = 120, retries: int = 3, delay: int = 1):
        self.timeout = timeout
        self.retries = max(1, retries)
        self.delay = delay
        self.session = Session()

    def _make_request(self, method: str, url: str, **kwargs) -> str | None:
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.request(method, url, timeout=self.timeout, **kwargs)

                if response.status_code == 404:
                    logger.warning(f"404 for {method} {sanitize_url(url)}. Stop retry.")
                    return None

                response.raise_for_status()
                return response.text

            except requests.RequestException as e:
                logger.warning(f"Попытка {attempt}/{self.retries}: ошибка {method} {sanitize_url(url)} - {e}")
                if attempt < self.retries:
                    time.sleep(self.delay)

        return None

    def request(self, method: str, url: str, params: Optional[Dict[str, Any]] = None,
                json: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None,
                headers: Optional[Dict[str, str]] = None) -> Optional[str]:
        """Выполняет HTTP-запрос с указанным методом, URL и параметрами.
        Args:
            method: HTTP-метод (например, "GET", "POST").
            url: URL-адрес для запроса.
            params: Параметры запроса.
            json: JSON-данные для отправки в теле запроса.
            data: Данные для отправки в теле запроса.
            headers: HTTP-заголовки для включения в запрос.
        Returns:
            Содержимое ответа, если запрос успешен, иначе None.
        """
        return self._make_request(method, url, params=params, json=json, data=data, headers=headers)

    def get(self, url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> \
            Optional[str]:
        """Выполняет GET-запрос по указанному URL."""
        return self.request("GET", url, params=params, headers=headers)

    def post(self, url: str, json: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None,
             headers: Optional[Dict[str, str]] = None) -> Optional[str]:
        """Выполняет POST-запрос по указанному URL."""
        return self.request("POST", url, json=json, data=data, headers=headers)

    def put(self, url: str, json: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None):
        """Выполняет PUT-запрос по указанному URL."""
        return self.request("PUT", url, json=json, headers=headers)

    def delete(self, url: str, headers: Optional[Dict[str, str]] = None):
        """Выполняет DELETE-запрос по указанному URL."""
        return self.request("DELETE", url, headers=headers)

    def patch(self, url: str, json: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None,
              headers: Optional[Dict[str, str]] = None) -> Optional[str]:
        """Выполняет PATCH-запрос по указанному URL."""
        return self.request("PATCH", url, json=json, data=data, headers=headers)


class _LoopLocal(Generic[T]):
    """Хранилище объектов, привязанных к конкретному event loop."""

    def __init__(self, factory: Callable[[], T]) -> None:
        self._factory = factory
        self._items: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, T]" = weakref.WeakKeyDictionary()

    def get(self) -> T:
        loop = asyncio.get_running_loop()
        item = self._items.get(loop)
        if item is None:
            item = self._factory()
            self._items[loop] = item
        return item

    def pop_current(self) -> Optional[T]:
        """Извлекает объект текущего цикла (нужно для корректного закрытия)."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return None
        return self._items.pop(loop, None)


class HostThrottle:
    """Ограничитель нагрузки на одного получателя запросов."""

    __slots__ = ("key", "_semaphore", "_resume_at", "_max_wait")

    def __init__(self, key: str, limit: int, max_wait: Optional[float] = None) -> None:
        self.key = key
        self._semaphore = asyncio.Semaphore(max(1, limit))
        self._resume_at: float = 0.0
        self._max_wait = max_wait or settings.HTTP_THROTTLE_MAX_WAIT_SEC

    def pause(self, seconds: float) -> None:
        """Останавливает все запросы этого получателя на `seconds` (паузы не суммируются)."""
        loop = asyncio.get_running_loop()
        self._resume_at = max(self._resume_at, loop.time() + seconds)

    async def __aenter__(self) -> "HostThrottle":
        """Занимает слот, дождавшись общей паузы и не превысив потолок ожидания."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._max_wait

        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise ExternalApiError(
                    f"{self.key}: очередь к API не разошлась за {self._max_wait:.0f}с, запрос отменён"
                )

            cooldown = self._resume_at - loop.time()
            if cooldown > 0:
                await asyncio.sleep(min(cooldown + random.uniform(0, 1.5), remaining))
                continue

            try:
                await asyncio.wait_for(self._semaphore.acquire(), remaining)
            except asyncio.TimeoutError:
                raise ExternalApiError(
                    f"{self.key}: не дождались свободного слота за {self._max_wait:.0f}с, запрос отменён"
                )

            if self._resume_at - loop.time() > 0:
                self._semaphore.release()
                continue

            return self

    async def __aexit__(self, *exc_info) -> None:
        self._semaphore.release()


def _new_session() -> aiohttp.ClientSession:
    """Создаёт сессию с общим пулом соединений.

    Раньше сессия создавалась на каждый запрос, то есть каждый вызов WB API
    заново поднимал TCP-соединение и делал TLS-handshake.
    """
    connector = aiohttp.TCPConnector(
        limit=settings.HTTP_CONNECTION_POOL_SIZE,
        # Ограничение по хосту не задаём: параллелизм считает HostThrottle отдельно
        # по каждому кабинету, а общий потолок держит `limit`.
        limit_per_host=0,
        ttl_dns_cache=300,
    )
    return aiohttp.ClientSession(connector=connector)


_sessions: _LoopLocal[aiohttp.ClientSession] = _LoopLocal(_new_session)
_throttles: _LoopLocal[Dict[str, HostThrottle]] = _LoopLocal(dict)


def _throttle_for(url: str, limit: int, scope: Optional[str] = None) -> HostThrottle:
    """Возвращает ограничитель для пары «хост + scope», создавая его при необходимости."""
    host = urlsplit(url).netloc or url
    key = f"{host}|{scope}" if scope else host
    registry = _throttles.get()
    throttle = registry.get(key)
    if throttle is None:
        throttle = HostThrottle(key, limit)
        registry[key] = throttle
    return throttle


async def close_http_sessions() -> None:
    """Закрывает HTTP-сессию текущего event loop (вызывается при остановке приложения)."""
    session = _sessions.pop_current()
    if session is not None and not session.closed:
        await session.close()
    _throttles.pop_current()


@dataclass(slots=True)
class _Attempt:
    """Результат одной попытки запроса."""

    body: str | bytes | None = None
    finished: bool = False  # ответ получен окончательно (успех или неповторяемая ошибка)
    retry_after: float = 0.0
    rate_limited: bool = False  # отказ по лимиту (429), а не сбой — бюджет попыток отдельный

    @classmethod
    def done(cls, body: str | bytes | None) -> "_Attempt":
        return cls(body=body, finished=True)

    @classmethod
    def retry(cls, retry_after: float, rate_limited: bool = False) -> "_Attempt":
        return cls(finished=False, retry_after=retry_after, rate_limited=rate_limited)


class AsyncHttpClient:
    """Асинхронный HTTP-клиент к внешним API (WB, 1C)."""

    def __init__(
            self,
            timeout: Optional[float] = None,
            max_attempts: Optional[int] = None,
            backoff_base: Optional[float] = None,
            backoff_max: Optional[float] = None,
            max_concurrent_requests: Optional[int] = None,
            throttle_scope: Optional[str] = None,
            rate_limit_max_attempts: Optional[int] = None,
    ):
        """
        Args:
            timeout: Общий таймаут запроса в секундах.
            max_attempts: Максимальное число попыток, включая первую.
            backoff_base: Базовая задержка экспоненциального отката в секундах.
            backoff_max: Верхняя граница задержки между попытками.
            max_concurrent_requests: Лимит одновременных запросов к одному получателю.
            throttle_scope: Владелец лимита (кабинет WB). Запросы разных кабинетов
                не мешают друг другу и переживают 429 независимо.
        """
        self.timeout = aiohttp.ClientTimeout(total=timeout or settings.HTTP_TIMEOUT_SEC)
        self.max_attempts = max(1, max_attempts or settings.HTTP_MAX_ATTEMPTS)
        self.backoff_base = backoff_base or settings.HTTP_RETRY_BACKOFF_BASE_SEC
        self.backoff_max = backoff_max or settings.HTTP_RETRY_BACKOFF_MAX_SEC
        self.max_concurrent_requests = (
                max_concurrent_requests or settings.HTTP_MAX_CONCURRENT_REQUESTS_PER_HOST
        )
        self.throttle_scope = throttle_scope
        self.rate_limit_max_attempts = max(1, rate_limit_max_attempts or settings.HTTP_RATE_LIMIT_MAX_ATTEMPTS)

    def _backoff_delay(self, attempt: int) -> float:
        """Экспоненциальная задержка с джиттером ±25%, чтобы попытки не синхронизировались."""
        delay = min(self.backoff_max, self.backoff_base * 2 ** (attempt - 1))
        return delay * random.uniform(0.75, 1.25)

    def _retry_after_delay(self, response: aiohttp.ClientResponse, attempt: int) -> float:
        """Берёт паузу из заголовка Retry-After, иначе откатывается к backoff."""
        header = response.headers.get("Retry-After", "")
        try:
            requested = float(header)
        except ValueError:
            delay = settings.HTTP_RATE_LIMIT_BACKOFF_BASE_SEC * 2 ** (attempt - 1)
            return min(delay * random.uniform(0.75, 1.25), settings.HTTP_RETRY_AFTER_MAX_SEC)
        return min(max(requested, 1.0), settings.HTTP_RETRY_AFTER_MAX_SEC)

    @staticmethod
    async def _read_body(response: aiohttp.ClientResponse) -> str | bytes:
        """Бинарные ответы (стикеры, штрихкоды) возвращаются как есть, остальные — текстом."""
        if response.headers.get("Content-Type", "").startswith("image/"):
            return await response.read()
        return await response.text()

    async def _handle_response(self, response: aiohttp.ClientResponse, throttle: HostThrottle,
                               method: str, url: str, attempt: int, rate_limit_attempt: int,
                               duration_ms: float) -> _Attempt:
        diagnostics.increment_wb_counter("requests_total")
        status_code = response.status

        if status_code < 400:
            diagnostics.increment_wb_counter("success")
            return _Attempt.done(await self._read_body(response))

        diagnostics.increment_wb_counter("errors")
        body_preview = (await response.text())[:ERROR_BODY_PREVIEW_LEN]

        if status_code == 404:
            diagnostics.increment_wb_counter("not_found_404")
            diagnostics.record_event(
                "WB_REQUEST_404",
                level="warning",
                refresh_id=get_refresh_id(),
                method=method,
                url=sanitize_url(url),
                status_code=status_code,
                attempt=attempt,
                retries=self.max_attempts,
                duration_ms=duration_ms,
            )
            logger.warning(f"404 при {method} {sanitize_url(url)} — повтор не выполняется")
            return _Attempt.done(None)

        if status_code == 429:
            # Пауза растёт по числу упоров в лимит, а не по общему счётчику попыток
            delay = self._retry_after_delay(response, rate_limit_attempt)
            throttle.pause(delay)
            diagnostics.increment_wb_counter("rate_limited")
            diagnostics.record_event(
                "WB_REQUEST_RATE_LIMITED",
                level="warning",
                refresh_id=get_refresh_id(),
                method=method,
                url=sanitize_url(url),
                status_code=status_code,
                attempt=rate_limit_attempt,
                retries=self.rate_limit_max_attempts,
                delay=round(delay, 3),
                duration_ms=duration_ms,
            )
            logger.warning(
                f"429 [{throttle.key}]: пауза {delay:.1f}с для всех запросов этого кабинета "
                f"(упор в лимит {rate_limit_attempt}/{self.rate_limit_max_attempts}, "
                f"{method} {sanitize_url(url)})"
            )
            return _Attempt.retry(delay, rate_limited=True)

        if status_code not in RETRYABLE_STATUSES:
            logger.error(
                f"{status_code} при {method} {sanitize_url(url)} — повтор не поможет. Ответ: {body_preview}"
            )
            return _Attempt.done(None)

        delay = self._backoff_delay(attempt)
        self._record_retry(method, url, attempt, delay, duration_ms, status_code)
        logger.warning(
            f"Попытка {attempt}/{self.max_attempts}: {status_code} при {method} {sanitize_url(url)}. "
            f"Ответ: {body_preview}"
        )
        return _Attempt.retry(delay)

    def _record_retry(self, method: str, url: str, attempt: int, delay: float,
                      duration_ms: float, status_code: Optional[int]) -> None:
        diagnostics.increment_wb_counter("retries")
        diagnostics.record_event(
            "WB_REQUEST_RETRY",
            level="warning",
            refresh_id=get_refresh_id(),
            method=method,
            url=sanitize_url(url),
            status_code=status_code,
            attempt=attempt,
            retries=self.max_attempts,
            delay=round(delay, 3),
            duration_ms=duration_ms,
        )

    async def _attempt_request(self, throttle: HostThrottle, method: str, url: str,
                               attempt: int, rate_limit_attempt: int, **kwargs: Any) -> _Attempt:
        started = time.monotonic()
        try:
            async with throttle:
                session = _sessions.get()
                async with session.request(method, url, timeout=self.timeout, **kwargs) as response:
                    duration_ms = round((time.monotonic() - started) * 1000, 3)
                    return await self._handle_response(
                        response, throttle, method, url, attempt, rate_limit_attempt, duration_ms
                    )

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            duration_ms = round((time.monotonic() - started) * 1000, 3)
            diagnostics.increment_wb_counter("requests_total")
            diagnostics.increment_wb_counter("errors")
            delay = self._backoff_delay(attempt)
            self._record_retry(method, url, attempt, delay, duration_ms, getattr(e, "status", None))
            logger.warning(
                f"Попытка {attempt}/{self.max_attempts}: ошибка {method} {sanitize_url(url)} - "
                f"{type(e).__name__}: {e}"
            )
            return _Attempt.retry(delay)

    async def _make_request(self, method: str, url: str, **kwargs: Any) -> str | bytes | None:
        """Выполняет запрос с повторами."""
        throttle = _throttle_for(url, self.max_concurrent_requests, self.throttle_scope)

        attempt = 0
        errors_left = self.max_attempts
        rate_limits_left = self.rate_limit_max_attempts
        rate_limit_hits = 0

        while True:
            attempt += 1
            result = await self._attempt_request(
                throttle, method, url, attempt, rate_limit_hits + 1, **kwargs
            )
            if result.finished:
                return result.body

            if result.rate_limited:
                rate_limit_hits += 1
                rate_limits_left -= 1
                exhausted, reason = rate_limits_left <= 0, (
                    f"лимит WB не разошёлся за {self.rate_limit_max_attempts} попыток"
                )
            else:
                errors_left -= 1
                exhausted, reason = errors_left <= 0, f"исчерпаны все {self.max_attempts} попытки"

            if exhausted:
                logger.error(f"{method} {sanitize_url(url)}: {reason}")
                return None

            await asyncio.sleep(result.retry_after)

    async def request(self, method: str, url: str, params: Optional[Dict[str, Any]] = None,
                      json: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None,
                      headers: Optional[Dict[str, str]] = None) -> str | bytes | None:
        """Выполняет асинхронный HTTP-запрос с указанным методом, URL и параметрами.
        Args:
            method: HTTP-метод (например, "GET", "POST").
            url: URL-адрес для запроса.
            params: Параметры запроса.
            json: JSON-данные для отправки в теле запроса.
            data: Данные для отправки в теле запроса.
            headers: HTTP-заголовки для включения в запрос.
        Returns:
            Тело ответа, если запрос успешен, иначе None.
        """
        return await self._make_request(method, url, params=params, json=json, data=data, headers=headers)

    async def get(self, url: str, params: Optional[Dict[str, Any]] = None,
                  headers: Optional[Dict[str, str]] = None) -> str | bytes | None:
        """Выполняет асинхронный GET-запрос по указанному URL."""
        return await self.request("GET", url, params=params, headers=headers)

    async def post(self, url: str, json: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None,
                   headers: Optional[Dict[str, str]] = None) -> str | bytes | None:
        """Выполняет асинхронный POST-запрос по указанному URL."""
        return await self.request("POST", url, json=json, data=data, headers=headers)

    async def put(self, url: str, json: Optional[Dict[str, Any]] = None,
                  headers: Optional[Dict[str, str]] = None) -> str | bytes | None:
        """Выполняет асинхронный PUT-запрос по указанному URL."""
        return await self.request("PUT", url, json=json, headers=headers)

    async def delete(self, url: str, headers: Optional[Dict[str, str]] = None) -> str | bytes | None:
        """Выполняет асинхронный DELETE-запрос по указанному URL."""
        return await self.request("DELETE", url, headers=headers)

    async def patch(self, url: str, json: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None,
                    headers: Optional[Dict[str, str]] = None) -> str | bytes | None:
        """Выполняет асинхронный PATCH-запрос по указанному URL."""
        return await self.request("PATCH", url, json=json, data=data, headers=headers)


def parse_json(response_text: str | bytes | None) -> dict | None:
    """Преобразует строку ответа в JSON или выбрасывает исключение.
    Args:
        response_text: Строка ответа от сервера.
    Returns:
        Распарсенный JSON в виде словаря.
    Raises:
        ValueError: Если строка не является корректным JSON.
    """
    if response_text is None:
        return {}
    try:
        return json.loads(response_text)
    except json.JSONDecodeError as e:
        raise ValueError(f"Ошибка парсинга JSON: {e}") from e
