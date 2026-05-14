import asyncio
import contextlib
import inspect
import os
import resource
import threading
import time
import uuid
from collections import deque
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, Optional
from urllib.parse import urlsplit, urlunsplit

from src.logger import app_logger as logger


EVENT_MESSAGES = {
    "REQUEST_END": "HTTP-запрос завершен",
    "REQUEST_SLOW": "HTTP-запрос выполнялся дольше порога",
    "LOGIN_START": "Начата обработка входа пользователя",
    "LOGIN_BEFORE_AUTHENTICATE": "Перед проверкой учетных данных",
    "LOGIN_AFTER_AUTHENTICATE": "Проверка учетных данных завершена",
    "LOGIN_BEFORE_CREATE_TOKEN": "Перед созданием токена доступа",
    "LOGIN_AFTER_CREATE_TOKEN": "Создание токена доступа завершено",
    "LOGIN_SUCCESS": "Вход пользователя успешно завершен",
    "LOGIN_FAILED": "Вход пользователя завершился ошибкой",
    "AUTH_DB_FETCH_START": "Начат запрос пользователя из PostgreSQL",
    "AUTH_DB_FETCH_END": "Запрос пользователя из PostgreSQL завершен",
    "AUTH_BCRYPT_START": "Начата проверка пароля через bcrypt",
    "AUTH_BCRYPT_END": "Проверка пароля через bcrypt завершена",
    "DB_ACQUIRE_SLOW": "Долгое ожидание соединения из пула PostgreSQL",
    "DB_CONNECTION_HELD_LONG": "Соединение PostgreSQL удерживалось дольше порога",
    "DB_QUERY_SLOW": "SQL-запрос выполнялся дольше порога",
    "WB_REFRESH_START": "Начато обновление кэша Wildberries",
    "WB_REFRESH_END": "Обновление кэша Wildberries завершено",
    "WB_REFRESH_ERROR": "Обновление кэша Wildberries завершилось ошибкой",
    "WB_REQUEST_RETRY": "Запланирована повторная попытка запроса Wildberries",
    "WB_REQUEST_404": "Запрос Wildberries вернул 404",
    "EVENT_LOOP_LAG_WARNING": "Обнаружена задержка event loop",
    "EVENT_LOOP_LAG_CRITICAL": "Обнаружена критическая задержка event loop",
    "DIAG_STATUS": "Периодическая сводка диагностики процесса",
}


current_request_id: ContextVar[Optional[str]] = ContextVar("current_request_id", default=None)
current_refresh_id: ContextVar[Optional[str]] = ContextVar("current_refresh_id", default=None)
current_refresh_source: ContextVar[Optional[str]] = ContextVar("current_refresh_source", default=None)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def get_request_id() -> Optional[str]:
    return current_request_id.get()


def get_refresh_id() -> Optional[str]:
    return current_refresh_id.get()


def new_refresh_id() -> str:
    return str(uuid.uuid4())


def mask_username(username: Optional[str]) -> str:
    if not username:
        return "none"
    if "@" in username:
        local, domain = username.split("@", 1)
        if len(local) <= 2:
            masked_local = local[:1] + "***"
        else:
            masked_local = local[:2] + "***"
        return f"{masked_local}@{domain}"
    if len(username) <= 2:
        return username[:1] + "***"
    if len(username) <= 5:
        return username[:2] + "***"
    return username[:2] + "***" + username[-2:]


def sanitize_url(url: str) -> str:
    try:
        parts = urlsplit(url)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    except Exception:
        return "<invalid-url>"


def short_query(query: Any, max_len: int = 300) -> str:
    text = " ".join(str(query).split())
    if len(text) > max_len:
        return text[:max_len] + "..."
    return text


def safe_error(exc: BaseException) -> str:
    return f"{exc.__class__.__name__}: {str(exc)[:300]}"


def get_caller_name() -> str:
    try:
        for frame in inspect.stack(context=0)[2:12]:
            module = inspect.getmodule(frame.frame)
            module_name = module.__name__ if module else ""
            if module_name.startswith("src.diagnostics") or module_name.startswith("src.db"):
                continue
            if "site-packages" in frame.filename:
                continue
            return frame.function
    except Exception:
        pass
    return "unknown"


class DiagnosticsCollector:
    def __init__(self) -> None:
        self.started_at = time.time()
        self.recent_events: Deque[Dict[str, Any]] = deque(maxlen=1000)
        self.recent_requests: Deque[Dict[str, Any]] = deque(maxlen=500)
        self.recent_login_events: Deque[Dict[str, Any]] = deque(maxlen=500)
        self.recent_db_acquires: Deque[Dict[str, Any]] = deque(maxlen=500)
        self.recent_db_holds: Deque[Dict[str, Any]] = deque(maxlen=500)
        self.recent_db_queries: Deque[Dict[str, Any]] = deque(maxlen=500)
        self.recent_wb_refreshes: Deque[Dict[str, Any]] = deque(maxlen=500)
        self.recent_event_loop_lags: Deque[Dict[str, Any]] = deque(maxlen=500)
        self.wb_refresh_state: Dict[str, Any] = {
            "active": False,
            "current": None,
            "last": None,
        }
        self.event_loop_state: Dict[str, Any] = {
            "last_lag_ms": 0,
            "max_lag_ms": 0,
        }

    def _buffer_for(self, event_name: str) -> Optional[Deque[Dict[str, Any]]]:
        if event_name.startswith("REQUEST_"):
            return self.recent_requests
        if event_name.startswith("LOGIN_") or event_name.startswith("AUTH_"):
            return self.recent_login_events
        if event_name == "DB_ACQUIRE_SLOW":
            return self.recent_db_acquires
        if event_name == "DB_CONNECTION_HELD_LONG":
            return self.recent_db_holds
        if event_name == "DB_QUERY_SLOW":
            return self.recent_db_queries
        if event_name.startswith("WB_REFRESH") or event_name.startswith("WB_REQUEST"):
            return self.recent_wb_refreshes
        if event_name.startswith("EVENT_LOOP_LAG"):
            return self.recent_event_loop_lags
        return None

    def record_event(self, event_name: str, level: str = "info", **fields: Any) -> Dict[str, Any]:
        try:
            event = {
                "ts": utc_now_iso(),
                "event": event_name,
                "message": fields.pop("message", EVENT_MESSAGES.get(event_name, "Диагностическое событие")),
                "request_id": fields.pop("request_id", get_request_id()),
                **fields,
            }
            self.recent_events.append(event)
            buffer = self._buffer_for(event_name)
            if buffer is not None:
                buffer.append(event)
            self._log_event(event, level)
            return event
        except Exception as exc:
            with contextlib.suppress(Exception):
                logger.warning(f"DIAGNOSTICS_COLLECTOR_ERROR error={safe_error(exc)}")
            return {"event": event_name, "message": "Не удалось записать диагностическое событие", "error": "collector_failed"}

    def _log_event(self, event: Dict[str, Any], level: str) -> None:
        parts = []
        for key, value in event.items():
            if key in {"ts", "event"}:
                continue
            parts.append(f"{key}={self._format_value(value)}")
        message = f"{event['event']} " + " ".join(parts)
        log_method = getattr(logger, level, logger.info)
        log_method(message)

    @staticmethod
    def _format_value(value: Any) -> str:
        if isinstance(value, dict):
            return "{" + ",".join(f"{k}:{DiagnosticsCollector._format_value(v)}" for k, v in value.items()) + "}"
        if isinstance(value, (list, tuple)):
            return "[" + ",".join(DiagnosticsCollector._format_value(v) for v in value[:10]) + "]"
        text = str(value)
        if any(ch.isspace() for ch in text):
            return repr(text)
        return text

    def start_wb_refresh(self, refresh_id: str, source: str) -> None:
        try:
            started_at = utc_now_iso()
            state = {
                "refresh_id": refresh_id,
                "source": source,
                "started_at": started_at,
                "started_monotonic": time.monotonic(),
                "duration_sec": 0,
                "counters": {
                    "requests_total": 0,
                    "success": 0,
                    "errors": 0,
                    "not_found_404": 0,
                    "retries": 0,
                },
            }
            self.wb_refresh_state = {"active": True, "current": state, "last": self.wb_refresh_state.get("last")}
            self.record_event("WB_REFRESH_START", refresh_id=refresh_id, source=source)
        except Exception as exc:
            logger.warning(f"DIAGNOSTICS_WB_REFRESH_START_ERROR error={safe_error(exc)}")

    def finish_wb_refresh(self, refresh_id: str, status: str, error: Optional[str] = None) -> None:
        try:
            current = self.wb_refresh_state.get("current") or {}
            duration_sec = round(time.monotonic() - current.get("started_monotonic", time.monotonic()), 3)
            finished = {**current, "duration_sec": duration_sec, "finished_at": utc_now_iso(), "status": status}
            finished.pop("started_monotonic", None)
            self.wb_refresh_state = {"active": False, "current": None, "last": finished}
            fields = {"refresh_id": refresh_id, "duration_sec": duration_sec, "status": status}
            if error:
                fields["error"] = error
            self.record_event("WB_REFRESH_END" if status == "success" else "WB_REFRESH_ERROR", **fields)
        except Exception as exc:
            logger.warning(f"DIAGNOSTICS_WB_REFRESH_FINISH_ERROR error={safe_error(exc)}")

    def increment_wb_counter(self, counter: str, amount: int = 1) -> None:
        try:
            current = self.wb_refresh_state.get("current")
            if not current:
                return
            counters = current.setdefault("counters", {})
            counters[counter] = int(counters.get(counter, 0)) + amount
            current["duration_sec"] = round(time.monotonic() - current.get("started_monotonic", time.monotonic()), 3)
        except Exception:
            pass

    def record_event_loop_lag(self, lag_ms: float) -> None:
        try:
            self.event_loop_state["last_lag_ms"] = round(lag_ms, 3)
            self.event_loop_state["max_lag_ms"] = round(max(float(self.event_loop_state.get("max_lag_ms") or 0), lag_ms), 3)
            sample = {"ts": utc_now_iso(), "event": "EVENT_LOOP_LAG_SAMPLE", "lag_ms": round(lag_ms, 3)}
            self.recent_event_loop_lags.append(sample)
        except Exception:
            pass

    def recent(self, limit: int = 100, event_type: Optional[str] = None) -> list[Dict[str, Any]]:
        try:
            limit = max(1, min(limit, 500))
            events: Iterable[Dict[str, Any]] = reversed(self.recent_events)
            if event_type:
                events = (event for event in events if event.get("event") == event_type)
            return list(events)[:limit]
        except Exception:
            return []

    def snapshot(self) -> Dict[str, Any]:
        try:
            return {
                "process": get_process_metrics(self.started_at),
                "event_loop": {
                    **self.event_loop_state,
                    "recent": list(self.recent_event_loop_lags)[-20:],
                },
                "wb_refresh": self.get_wb_refresh_state(),
                "recent_counts": {
                    "events": len(self.recent_events),
                    "requests": len(self.recent_requests),
                    "login_events": len(self.recent_login_events),
                    "db_acquires": len(self.recent_db_acquires),
                    "db_holds": len(self.recent_db_holds),
                    "db_queries": len(self.recent_db_queries),
                    "wb_refreshes": len(self.recent_wb_refreshes),
                    "event_loop_lags": len(self.recent_event_loop_lags),
                },
            }
        except Exception as exc:
            return {"error": safe_error(exc)}

    def get_wb_refresh_state(self) -> Dict[str, Any]:
        try:
            state = dict(self.wb_refresh_state)
            current = state.get("current")
            if current:
                current = dict(current)
                current["duration_sec"] = round(time.monotonic() - current.get("started_monotonic", time.monotonic()), 3)
                current.pop("started_monotonic", None)
                state["current"] = current
            return state
        except Exception:
            return {"active": False, "current": None, "last": None}


def get_process_metrics(started_at: float) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {
        "pid": os.getpid(),
        "rss_mb": None,
        "open_fds": None,
        "asyncio_tasks": None,
        "threads": threading.active_count(),
        "uptime_sec": round(time.time() - started_at, 3),
    }
    try:
        rss_kb = None
        status_path = Path("/proc/self/status")
        if status_path.exists():
            for line in status_path.read_text(encoding="utf-8", errors="ignore").splitlines():
                if line.startswith("VmRSS:"):
                    rss_kb = int(line.split()[1])
                    break
        if rss_kb is None:
            usage = resource.getrusage(resource.RUSAGE_SELF)
            rss_kb = usage.ru_maxrss
        metrics["rss_mb"] = round(rss_kb / 1024, 2)
    except Exception:
        pass
    try:
        fd_path = Path("/proc/self/fd")
        if fd_path.exists():
            metrics["open_fds"] = len(list(fd_path.iterdir()))
    except Exception:
        pass
    try:
        metrics["asyncio_tasks"] = len(asyncio.all_tasks())
    except Exception:
        pass
    return metrics


async def event_loop_lag_monitor(settings: Any) -> None:
    interval = 1.0
    expected = time.monotonic() + interval
    while True:
        try:
            await asyncio.sleep(interval)
            now = time.monotonic()
            lag_ms = max(0.0, (now - expected) * 1000)
            diagnostics.record_event_loop_lag(lag_ms)
            if lag_ms >= settings.DIAGNOSTICS_EVENT_LOOP_LAG_CRITICAL_MS:
                diagnostics.record_event("EVENT_LOOP_LAG_CRITICAL", level="error", lag_ms=round(lag_ms, 3))
            elif lag_ms >= settings.DIAGNOSTICS_EVENT_LOOP_LAG_WARN_MS:
                diagnostics.record_event("EVENT_LOOP_LAG_WARNING", level="warning", lag_ms=round(lag_ms, 3))
            expected = now + interval
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(f"EVENT_LOOP_LAG_MONITOR_ERROR error={safe_error(exc)}")
            expected = time.monotonic() + interval


async def diagnostics_summary_logger(settings: Any, db_manager: Any) -> None:
    while True:
        try:
            await asyncio.sleep(max(10, int(settings.DIAGNOSTICS_LOG_INTERVAL_SEC)))
            process = get_process_metrics(diagnostics.started_at)
            pool = db_manager.get_pool_stats() if db_manager else {}
            wb = diagnostics.get_wb_refresh_state()
            loop = diagnostics.event_loop_state
            diagnostics.record_event(
                "DIAG_STATUS",
                rss_mb=process.get("rss_mb"),
                fds=process.get("open_fds"),
                tasks=process.get("asyncio_tasks"),
                loop_lag_ms=loop.get("last_lag_ms"),
                db_pool_size=pool.get("size"),
                db_pool_idle=pool.get("idle"),
                db_pool_used=pool.get("used"),
                wb_refresh_active=wb.get("active"),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(f"DIAG_STATUS_ERROR error={safe_error(exc)}")


diagnostics = DiagnosticsCollector()
