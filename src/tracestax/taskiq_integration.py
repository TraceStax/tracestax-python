"""TraceStax auto-instrumentation for TaskIQ.

Provides ``TraceStaxMiddleware``, a TaskIQ middleware that captures task lifecycle
events and ships them to the TraceStax ingest API.

Usage::

    from taskiq import InMemoryBroker
    from tracestax.taskiq_integration import configure

    middleware = configure(api_key="ts_live_xxx")
    broker = InMemoryBroker().with_middlewares(middleware)
"""

from __future__ import annotations

import logging
import os
import platform
import time
import traceback as tb_module
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tracestax.client import TraceStaxClient

logger = logging.getLogger("tracestax")

_SDK_VERSION = "0.1.0"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class TraceStaxMiddleware:
    """TaskIQ middleware that captures task execution events and ships them
    to the TraceStax ingest API.

    Events captured:

    * ``pre_execute``   -- records the start time and sends a *started* event.
    * ``post_execute``  -- sends a *succeeded* or *failed* event with duration.

    Usage::

        from tracestax.taskiq_integration import configure

        middleware = configure(api_key="ts_live_xxx")
        broker = InMemoryBroker().with_middlewares(middleware)
    """

    def __init__(self, client: TraceStaxClient) -> None:
        self._client = client
        self._start_times: dict[str, float] = {}

    async def pre_execute(self, message: Any) -> Any:
        """Record the start time and send a started event."""
        try:
            task_id = _get_task_id(message)
            # Prevent unbounded memory growth
            if len(self._start_times) > 10_000:
                sorted_keys = sorted(self._start_times, key=self._start_times.get)
                for k in sorted_keys[: len(sorted_keys) // 2]:
                    self._start_times.pop(k, None)
            self._start_times[task_id] = time.monotonic()

            self._client.send_event(
                _build_task_payload(
                    message=message,
                    status="started",
                    duration_ms=0.0,
                )
            )
        except Exception:
            logger.debug("TraceStax: failed to handle pre_execute", exc_info=True)
        return message

    async def post_execute(self, message: Any, result: Any) -> None:
        """Send a succeeded or failed event with duration."""
        try:
            task_id = _get_task_id(message)
            start = self._start_times.pop(task_id, None)
            duration_ms = round((time.monotonic() - start) * 1000.0, 2) if start else 0.0

            # TaskIQ result objects have is_err and error attributes
            is_error = getattr(result, "is_err", False)
            error_val = getattr(result, "error", None)

            if is_error:
                error_info = _extract_error(error_val)
                self._client.send_event(
                    _build_task_payload(
                        message=message,
                        status="failed",
                        duration_ms=duration_ms,
                        error=error_info,
                    )
                )
            else:
                self._client.send_event(
                    _build_task_payload(
                        message=message,
                        status="succeeded",
                        duration_ms=duration_ms,
                    )
                )
        except Exception:
            logger.debug("TraceStax: failed to handle post_execute", exc_info=True)

    async def on_error(
        self,
        message: Any,
        result: Any,
        exception: BaseException,
    ) -> None:
        """Handle task errors that bypass post_execute."""
        try:
            task_id = _get_task_id(message)
            start = self._start_times.pop(task_id, None)
            duration_ms = round((time.monotonic() - start) * 1000.0, 2) if start else 0.0

            error_info = _extract_error(exception)
            self._client.send_event(
                _build_task_payload(
                    message=message,
                    status="failed",
                    duration_ms=duration_ms,
                    error=error_info,
                )
            )
        except Exception:
            logger.debug("TraceStax: failed to handle on_error", exc_info=True)


# ── Payload builders ──────────────────────────────────────────────────


def _get_task_id(message: Any) -> str:
    """Extract the task ID from a TaskIQ message."""
    return getattr(message, "task_id", None) or getattr(message, "message_id", "unknown")


def _get_task_name(message: Any) -> str:
    """Extract the task name from a TaskIQ message."""
    return getattr(message, "task_name", None) or getattr(message, "name", "unknown")


def _build_task_payload(
    *,
    message: Any,
    status: str,
    duration_ms: float,
    error: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Construct an IngestPayload dict matching @tracestax/types."""
    task_name = _get_task_name(message)
    task_id = _get_task_id(message)

    # TaskIQ labels may contain queue and retry info
    labels = getattr(message, "labels", {}) or {}
    queue = labels.get("queue", "default")
    attempt = int(labels.get("retry_count", 0)) + 1

    hostname = platform.node()
    pid = os.getpid()

    payload: dict[str, Any] = {
        "framework": "taskiq",
        "language": "python",
        "sdk_version": _SDK_VERSION,
        "type": "task_event",
        "worker": {
            "key": f"{hostname}:{pid}",
            "hostname": hostname,
            "pid": pid,
            "concurrency": 1,
            "queues": [queue],
        },
        "task": {
            "name": task_name,
            "id": task_id,
            "queue": queue,
            "attempt": attempt,
        },
        "status": status,
        "metrics": {
            "duration_ms": round(duration_ms, 2),
        },
    }

    if error:
        payload["error"] = error

    return payload


def _extract_error(exception) -> dict[str, str] | None:
    """Build a TaskError dict from a Python exception."""
    if exception is None:
        return None
    info: dict[str, str] = {
        "type": type(exception).__qualname__ if isinstance(exception, BaseException) else "Error",
        "message": str(exception),
    }
    if isinstance(exception, BaseException) and hasattr(exception, "__traceback__") and exception.__traceback__ is not None:
        lines = tb_module.format_exception(
            type(exception), exception, exception.__traceback__
        )
        full = "".join(lines).split("\n")
        info["stack_trace"] = "\n".join(full[:60])
    return info


# ── Public configure helper ───────────────────────────────────────────


def configure(
    *,
    api_key: str,
    endpoint: str = "https://ingest.tracestax.com",
    flush_interval: float = 5.0,
    max_batch_size: int = 100,
) -> TraceStaxMiddleware:
    """Initialise the TraceStax SDK and return a TaskIQ middleware instance.

    Usage::

        from tracestax.taskiq_integration import configure

        middleware = configure(api_key="ts_live_xxx")
        broker = InMemoryBroker().with_middlewares(middleware)
    """
    from tracestax.client import TraceStaxClient

    client = TraceStaxClient(
        api_key=api_key,
        endpoint=endpoint,
        flush_interval=flush_interval,
        max_batch_size=max_batch_size,
    )
    return TraceStaxMiddleware(client=client)
