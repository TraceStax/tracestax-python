"""Celery signal-hook integration for TraceStax.

Hooks into Celery's built-in signals to automatically capture task lifecycle
events without requiring any changes to task code.
"""

from __future__ import annotations

import logging
import os
import platform
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from celery import Celery

    from tracestax.client import TraceStaxClient

logger = logging.getLogger("tracestax")

SDK_VERSION = "0.1.0"

# Store per-task-run start times keyed by task_id
_task_start_times: dict[str, float] = {}

# Worker metadata, populated on worker_ready
_worker_info: dict[str, Any] | None = None


def install_hooks(app: Celery, client: TraceStaxClient) -> None:
    """Connect all Celery signals to TraceStax event handlers.

    Handles Celery's prefork model: the ``worker_process_init`` signal
    re-initialises the client's background flush thread in each forked
    child so events are delivered correctly.
    """
    from celery.signals import (
        task_failure,
        task_postrun,
        task_prerun,
        task_retry,
        task_revoked,
        worker_process_init,
        worker_ready,
        worker_shutdown,
    )

    # Bind the client into each handler via closures.
    # Using weak=False so Celery retains references to our handlers.

    task_prerun.connect(_on_task_prerun, weak=False)
    task_postrun.connect(_make_postrun_handler(client), weak=False)
    task_failure.connect(_make_failure_handler(client), weak=False)
    task_retry.connect(_make_retry_handler(client), weak=False)
    task_revoked.connect(_make_revoked_handler(client), weak=False)
    worker_ready.connect(_make_worker_ready_handler(client, app), weak=False)
    worker_shutdown.connect(_make_worker_shutdown_handler(client), weak=False)

    # In prefork mode, the parent process forks child workers. The daemon
    # thread from the parent dies in the child. Re-start it here so each
    # child process can flush events independently.
    def _on_worker_process_init(**kwargs: Any) -> None:
        try:
            client._daemon = client._start_daemon()
            logger.debug("TraceStax flush daemon restarted in forked worker (pid=%s)", os.getpid())
        except Exception:
            logger.exception("Failed to restart TraceStax flush daemon after fork")

    worker_process_init.connect(_on_worker_process_init, weak=False)

    logger.info("TraceStax Celery hooks installed")


# ── Signal handlers ───────────────────────────────────────────────────


def _on_task_prerun(sender=None, task_id=None, **kwargs) -> None:
    """Record the wall-clock start time for duration calculation."""
    # Prevent unbounded memory growth: if dict exceeds 10,000 entries,
    # clear the oldest half.
    if len(_task_start_times) > 10_000:
        sorted_keys = sorted(_task_start_times, key=_task_start_times.get)
        for k in sorted_keys[: len(sorted_keys) // 2]:
            _task_start_times.pop(k, None)
    _task_start_times[task_id] = time.monotonic()


def _make_postrun_handler(client: TraceStaxClient):
    def _on_task_postrun(sender=None, task_id=None, state=None, retval=None, **kwargs) -> None:
        try:
            duration_ms = _pop_duration(task_id)
            status = "succeeded" if state == "SUCCESS" else "failed"

            client.send_event(
                _build_task_payload(
                    sender=sender,
                    task_id=task_id,
                    status=status,
                    duration_ms=duration_ms,
                )
            )
        finally:
            _task_start_times.pop(task_id, None)

    return _on_task_postrun


def _make_failure_handler(client: TraceStaxClient):
    def _on_task_failure(
        sender=None,
        task_id=None,
        exception=None,
        traceback=None,
        **kwargs,
    ) -> None:
        try:
            duration_ms = _pop_duration(task_id)
            error_info = _extract_error(exception, traceback)

            client.send_event(
                _build_task_payload(
                    sender=sender,
                    task_id=task_id,
                    status="failed",
                    duration_ms=duration_ms,
                    error=error_info,
                )
            )
        finally:
            _task_start_times.pop(task_id, None)

    return _on_task_failure


def _make_retry_handler(client: TraceStaxClient):
    def _on_task_retry(sender=None, request=None, reason=None, **kwargs) -> None:
        task_id = request.id if request else None
        try:
            duration_ms = _pop_duration(task_id)

            error_info = None
            if reason is not None:
                error_info = {
                    "type": type(reason).__name__ if isinstance(reason, BaseException) else "Retry",
                    "message": str(reason),
                }

            client.send_event(
                _build_task_payload(
                    sender=sender,
                    task_id=task_id,
                    status="retried",
                    duration_ms=duration_ms,
                    error=error_info,
                )
            )
        finally:
            _task_start_times.pop(task_id, None)

    return _on_task_retry


def _make_revoked_handler(client: TraceStaxClient):
    def _on_task_revoked(sender=None, request=None, terminated=None, **kwargs) -> None:
        task_id = request.id if request else None
        try:
            duration_ms = _pop_duration(task_id)

            client.send_event(
                _build_task_payload(
                    sender=sender,
                    task_id=task_id,
                    status="failed",
                    duration_ms=duration_ms,
                    error={
                        "type": "TaskRevoked",
                        "message": f"Task revoked (terminated={terminated})",
                    },
                )
            )
        finally:
            _task_start_times.pop(task_id, None)

    return _on_task_revoked


def _make_worker_ready_handler(client: TraceStaxClient, app: Celery):
    def _on_worker_ready(sender=None, **kwargs) -> None:
        global _worker_info
        _worker_info = _build_worker_info(sender, app)
        # Expose worker_key to the client for thread dump identification
        client._worker_key = _worker_info.get("key")
        directives = client.send_heartbeat(
            {
                "framework": "celery",
                "worker": _worker_info,
                "timestamp": _iso_now(),
            }
        )
        if directives:
            if directives.get("pause_ingest"):
                pause_until_ms = directives.get("pause_until_ms")
                if pause_until_ms:
                    client.set_pause_until(float(pause_until_ms))
            for cmd in directives.get("commands") or []:
                try:
                    client.execute_command(cmd)
                except Exception:
                    logger.warning("Failed to execute server command %r", cmd, exc_info=True)
        logger.info("TraceStax heartbeat sent for worker %s", _worker_info.get("key"))

    return _on_worker_ready


def _make_worker_shutdown_handler(client: TraceStaxClient):
    def _on_worker_shutdown(sender=None, **kwargs) -> None:
        if _worker_info:
            client.send_heartbeat(
                {
                    "framework": "celery",
                    "worker": _worker_info,
                    "timestamp": _iso_now(),
                    "shutdown": True,
                }
            )
        client.close()

    return _on_worker_shutdown


# ── Payload builders ──────────────────────────────────────────────────


def _build_task_payload(
    *,
    sender,
    task_id: str | None,
    status: str,
    duration_ms: float,
    error: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Construct an IngestPayload dict matching @tracestax/types."""
    task_name = sender.name if sender else "unknown"
    queue = _resolve_queue(sender)
    attempt = _resolve_attempt(sender, task_id)

    # Try to retrieve lineage context from task headers
    parent_id = None
    chain_id = None
    if sender and hasattr(sender, "request"):
        headers = getattr(sender.request, "headers", None) or {}
        parent_id = headers.get("tracestax_parent_id")
        chain_id = headers.get("tracestax_chain_id")

    payload: dict[str, Any] = {
        "framework": "celery",
        "language": "python",
        "sdk_version": SDK_VERSION,
        "type": "task_event",
        "worker": _worker_info or _fallback_worker_info(),
        "task": {
            "name": task_name,
            "id": task_id or "unknown",
            "queue": queue,
            "attempt": attempt,
        },
        "status": status,
        "metrics": {
            "duration_ms": round(duration_ms, 2),
        },
    }

    if parent_id:
        payload["task"]["parent_id"] = parent_id
    if chain_id:
        payload["task"]["chain_id"] = chain_id
    if error:
        payload["error"] = error

    return payload


def _build_worker_info(sender, app: Celery) -> dict[str, Any]:
    """Build WorkerInfo from the Celery worker consumer instance."""
    hostname = platform.node()
    pid = os.getpid()

    # sender is the worker consumer; try to extract concurrency & queues
    concurrency = 1
    queues: list[str] = []

    if sender is not None:
        pool = getattr(sender, "pool", None)
        if pool is not None:
            concurrency = getattr(pool, "limit", 1) or 1

        consumer = getattr(sender, "consumer", sender)
        task_consumer = getattr(consumer, "task_consumer", None)
        if task_consumer:
            queues = [q.name for q in getattr(task_consumer, "queues", [])]

    if not queues and app is not None:
        try:
            queues = [q.name for q in app.amqp.queues.values()]
        except Exception:
            queues = ["celery"]

    return {
        "key": f"{hostname}:{pid}",
        "hostname": hostname,
        "pid": pid,
        "concurrency": concurrency,
        "queues": queues,
    }


def _fallback_worker_info() -> dict[str, Any]:
    """Minimal worker info when we don't yet have a worker_ready event."""
    hostname = platform.node()
    pid = os.getpid()
    return {
        "key": f"{hostname}:{pid}",
        "hostname": hostname,
        "pid": pid,
        "concurrency": 1,
        "queues": [],
    }


# ── Utilities ─────────────────────────────────────────────────────────


def _pop_duration(task_id: str | None) -> float:
    """Pop the start time and compute elapsed ms; returns 0.0 if not found."""
    if task_id and task_id in _task_start_times:
        start = _task_start_times.pop(task_id)
        return (time.monotonic() - start) * 1000.0
    return 0.0


def _resolve_queue(sender) -> str:
    """Best-effort extraction of the queue name from the task sender."""
    if sender is None:
        return "unknown"
    req = getattr(sender, "request", None)
    if req:
        delivery_info = getattr(req, "delivery_info", None) or {}
        queue = delivery_info.get("routing_key") or delivery_info.get("exchange", "")
        if queue:
            return queue
    return getattr(sender, "queue", "celery") or "celery"


def _resolve_attempt(sender, task_id: str | None) -> int:
    """Return the attempt/retry number for the current task execution."""
    if sender is None:
        return 1
    req = getattr(sender, "request", None)
    if req:
        retries = getattr(req, "retries", 0) or 0
        return retries + 1
    return 1


def _extract_error(exception, traceback) -> dict[str, str]:
    """Build a TaskError dict from a Python exception."""
    info: dict[str, str] = {
        "type": type(exception).__name__ if exception else "UnknownError",
        "message": str(exception) if exception else "Unknown error",
    }
    if traceback is not None:
        import traceback as tb_module

        if hasattr(traceback, "tb_frame"):
            info["stack_trace"] = "".join(tb_module.format_tb(traceback))
        else:
            info["stack_trace"] = str(traceback)
    return info


def _iso_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()
