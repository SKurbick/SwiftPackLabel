import time
import uuid
from typing import Optional

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from src.diagnostics import current_request_id, diagnostics
from src.settings import settings


class RequestIdMiddleware:
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = self._extract_request_id(scope) or str(uuid.uuid4())
        token = current_request_id.set(request_id)
        start = time.monotonic()
        status_code: Optional[int] = None
        method = scope.get("method", "")
        path = scope.get("path", "")

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message.get("status")
                headers = list(message.get("headers", []))
                headers.append((b"x-request-id", request_id.encode("utf-8")))
                message["headers"] = headers
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration_ms = round((time.monotonic() - start) * 1000, 3)
            fields = {
                "request_id": request_id,
                "method": method,
                "path": path,
                "status_code": status_code or 0,
                "duration_ms": duration_ms,
            }
            diagnostics.record_event("REQUEST_END", **fields)
            if duration_ms >= settings.DIAGNOSTICS_REQUEST_SLOW_MS:
                diagnostics.record_event("REQUEST_SLOW", level="warning", **fields)
            current_request_id.reset(token)

    @staticmethod
    def _extract_request_id(scope: Scope) -> Optional[str]:
        for key, value in scope.get("headers", []):
            if key.lower() == b"x-request-id":
                request_id = value.decode("utf-8", errors="ignore").strip()
                return request_id[:128] if request_id else None
        return None
