"""TraceStax auto-instrumentation for Huey task queue.

Connects to a Huey instance's signal system to automatically capture task
lifecycle events without requiring any changes to task code.

Usage::

    from huey import RedisHuey
    from tracestax.huey_integration import connect

    huey = RedisHuey("my-app")
    client = connect(huey, api_key="ts_live_xxx")
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

# Store per-task start times keyed by task id
_task_start_times: dict[str, float] = {}


def connect(
    huey_instance,
    *,
    api_key: str,
    endpoint: str = "https://ingest.tracestax.com",
    flush_interval: float = 5.0,
    max_batch_size: int = 100,
) -> TraceStaxClient:
    """Connect TraceStax tracking to a Huey instance.

    Hooks into Huey's signal system to capture task started, succeeded,
    failed, and retried events.

    Parameters
    ----------
    huey_instance:
        The Huey application instance to instrument.
    api_key:
        TraceStax project API key.
    endpoint:
        Base URL of the ingest service.
    flush_interval:
        Seconds between automatic flushes.
    max_batch_size:
        Maximum number of events per HTTP request.

    Returns
    -------
    TraceStaxClient
        The initialised client (for manual shutdown via ``client.close()``).
    """
    from tracestax.client import TraceStaxClient

    client = TraceStaxClient(
        api_key=api_key,
        endpoint=endpoint,
        flush_interval=flush_interval,
        max_batch_size=max_batch_size,
    )

    try:
        from huey.signals import (
            SIGNAL_COMPLETE,
            SIGNAL_ERROR,
            SIGNAL_EXECUTING,
            SIGNAL_RETRYING,
        )
    except ImportError as exc:
        raise ImportError(
            "huey is required for TraceStax Huey integration. "
            "Install it with: pip install tracestax[huey]"
        ) from exc

    def _on_executing(signal, task, exc=None):
        try:
            # Prevent unbounded memory growth
            if len(_task_start_times) > 10_000:
                sorted_keys = sorted(_task_start_times, key=_task_start_times.get)
                for k in sorted_keys[: len(sorted_keys) // 2]:
                    _task_start_times.pop(k, None)
            _task_start_times[task.id] = time.monotonic()

            client.send_event(
                _build_task_payload(
                    task=task,
                    status="started",
                    duration_ms=0.0,
                )
            )
        except Exception:
            logger.debug("TraceStax: failed to handle executing signal", exc_info=True)

    def _on_complete(signal, task, exc=None):
        try:
            duration_ms = _pop_duration(task.id)
            client.send_event(
                _build_task_payload(
                    task=task,
                    status="succeeded",
                    duration_ms=duration_ms,
                )
            )
        except Exception:
            logger.debug("TraceStax: failed to handle complete signal", exc_info=True)

    def _on_error(signal, task, exc=None):
        try:
            duration_ms = _pop_duration(task.id)
            error_info = _extract_error(exc)
            client.send_event(
                _build_task_payload(
                    task=task,
                    status="failed",
                    duration_ms=duration_ms,
                    error=error_info,
                )
            )
        except Exception:
            logger.debug("TraceStax: failed to handle error signal", exc_info=True)

    def _on_retrying(signal, task, exc=None):
        try:
            duration_ms = _pop_duration(task.id)
            error_info = _extract_error(exc)
            client.send_event(
                _build_task_payload(
                    task=task,
                    status="retried",
                    duration_ms=duration_ms,
                    error=error_info,
                )
            )
        except Exception:
            logger.debug("TraceStax: failed to handle retrying signal", exc_info=True)

    huey_instance.signal(SIGNAL_EXECUTING)(_on_executing)
    huey_instance.signal(SIGNAL_COMPLETE)(_on_complete)
    huey_instance.signal(SIGNAL_ERROR)(_on_error)
    huey_instance.signal(SIGNAL_RETRYING)(_on_retrying)

    logger.info("TraceStax Huey hooks installed")
    return client


# ── Payload builders ──────────────────────────────────────────────────


def _build_task_payload(
    *,
    task,
    status: str,
    duration_ms: float,
    error: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Construct an IngestPayload dict matching @tracestax/types."""
    task_name = getattr(task, "name", None) or type(task).__name__
    task_id = getattr(task, "id", "unknown")
    retries = getattr(task, "retries", 0) or 0

    payload: dict[str, Any] = {
        "framework": "huey",
        "language": "python",
        "sdk_version": _SDK_VERSION,
        "type": "task_event",
        "worker": _fallback_worker_info(),
        "task": {
            "name": task_name,
            "id": task_id,
            "queue": "default",
            "attempt": retries + 1,
        },
        "status": status,
        "metrics": {
            "duration_ms": round(duration_ms, 2),
        },
    }

    if error:
        payload["error"] = error

    return payload


def _fallback_worker_info() -> dict[str, Any]:
    """Minimal worker info."""
    hostname = platform.node()
    pid = os.getpid()
    return {
        "key": f"{hostname}:{pid}",
        "hostname": hostname,
        "pid": pid,
        "concurrency": 1,
        "queues": ["default"],
    }


# ── Utilities ─────────────────────────────────────────────────────────


def _pop_duration(task_id: str | None) -> float:
    """Pop the start time and compute elapsed ms; returns 0.0 if not found."""
    if task_id and task_id in _task_start_times:
        start = _task_start_times.pop(task_id)
        return (time.monotonic() - start) * 1000.0
    return 0.0


def _extract_error(exception) -> dict[str, str] | None:
    """Build a TaskError dict from a Python exception."""
    if exception is None:
        return None
    info: dict[str, str] = {
        "type": type(exception).__qualname__,
        "message": str(exception),
    }
    if hasattr(exception, "__traceback__") and exception.__traceback__ is not None:
        lines = tb_module.format_exception(
            type(exception), exception, exception.__traceback__
        )
        full = "".join(lines).split("\n")
        info["stack_trace"] = "\n".join(full[:60])
    return info
