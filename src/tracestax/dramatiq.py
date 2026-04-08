"""Dramatiq middleware that reports actor lifecycle events to TraceStax."""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
import traceback
from datetime import datetime, timezone
from typing import Any

import dramatiq

from tracestax.client import TraceStaxClient

logger = logging.getLogger("tracestax")

_SDK_VERSION = "0.1.0"

_HEARTBEAT_INTERVAL = 30


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class TraceStaxMiddleware(dramatiq.Middleware):
    """Dramatiq middleware that captures actor execution events and ships them
    to the TraceStax ingest API.

    Events captured:

    * ``before_worker_boot``      -- sends an initial heartbeat and starts a
      background thread that re-sends the heartbeat every 30 seconds.
    * ``before_process_message``  -- records the monotonic start time and sends
      a *started* event.
    * ``after_process_message``   -- sends a *succeeded* event with duration.
    * ``after_skip_message``      -- sends a *failed* (skipped) event.

    Usage::

        import dramatiq
        from tracestax.dramatiq import configure

        middleware = configure(api_key="ts_live_xxx")
        broker = dramatiq.get_broker()
        broker.add_middleware(middleware)
    """

    def __init__(self, client: TraceStaxClient) -> None:
        self._client = client
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._worker_queues: list[str] = []
        self._start_times: dict[str, int] = {}

    # ── Worker lifecycle hooks ──────────────────────────────────────────

    def before_worker_boot(
        self, broker: dramatiq.Broker, worker: Any
    ) -> None:
        try:
            self._worker_queues = [q for q in broker.queues]
        except Exception:
            self._worker_queues = []

        self._send_heartbeat()

        self._heartbeat_stop.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name="tracestax-heartbeat",
        )
        self._heartbeat_thread.start()
        logger.debug("TraceStax heartbeat thread started (interval=%ss)", _HEARTBEAT_INTERVAL)

    def after_worker_shutdown(
        self, broker: dramatiq.Broker, worker: Any
    ) -> None:
        self._heartbeat_stop.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=5.0)
        logger.debug("TraceStax heartbeat thread stopped")

    # ── Heartbeat helpers ───────────────────────────────────────────────

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.wait(timeout=_HEARTBEAT_INTERVAL):
            self._send_heartbeat()

    def _send_heartbeat(self) -> None:
        try:
            hostname = socket.gethostname()
            pid = os.getpid()
            self._client.send_heartbeat(
                {
                    "framework": "dramatiq",
                    "language": "python",
                    "sdk_version": _SDK_VERSION,
                    "timestamp": _iso_now(),
                    "worker": {
                        "key": f"{hostname}:{pid}",
                        "hostname": hostname,
                        "pid": pid,
                        "queues": self._worker_queues,
                        "concurrency": 1,
                    },
                }
            )
        except Exception:
            logger.debug("TraceStax heartbeat send failed", exc_info=True)

    # ── Middleware hooks ────────────────────────────────────────────────

    def before_process_message(
        self, broker: dramatiq.Broker, message: dramatiq.Message
    ) -> None:
        start_ns = time.monotonic_ns()
        self._start_times[message.message_id] = start_ns

        self._client.send_event(
            {
                "framework": "dramatiq",
                "language": "python",
                "sdk_version": _SDK_VERSION,
                "type": "task_event",
                "worker": self._build_worker_payload(message),
                "task": self._build_task_payload(message),
                "status": "started",
                "metrics": {},
            }
        )

    def after_process_message(
        self,
        broker: dramatiq.Broker,
        message: dramatiq.Message,
        *,
        result: Any = None,
        exception: BaseException | None = None,
    ) -> None:
        try:
            duration_ms = self._calculate_duration(message)

            if exception is not None:
                payload = {
                    "framework": "dramatiq",
                    "language": "python",
                    "sdk_version": _SDK_VERSION,
                    "type": "task_event",
                    "worker": self._build_worker_payload(message),
                    "task": self._build_task_payload(message),
                    "status": "failed",
                    "metrics": {"duration_ms": duration_ms},
                    "error": {
                        "type": type(exception).__qualname__,
                        "message": str(exception),
                        "stack_trace": self._format_traceback(exception),
                    },
                }
            else:
                payload = {
                    "framework": "dramatiq",
                    "language": "python",
                    "sdk_version": _SDK_VERSION,
                    "type": "task_event",
                    "worker": self._build_worker_payload(message),
                    "task": self._build_task_payload(message),
                    "status": "succeeded",
                    "metrics": {"duration_ms": duration_ms},
                }

            self._client.send_event(payload)
        finally:
            self._start_times.pop(message.message_id, None)

    def after_skip_message(
        self, broker: dramatiq.Broker, message: dramatiq.Message
    ) -> None:
        try:
            self._client.send_event(
                {
                    "framework": "dramatiq",
                    "language": "python",
                    "sdk_version": _SDK_VERSION,
                    "type": "task_event",
                    "worker": self._build_worker_payload(message),
                    "task": self._build_task_payload(message),
                    "status": "failed",
                    "metrics": {},
                    "error": {
                        "type": "MessageSkipped",
                        "message": "Message was skipped (e.g. max retries exceeded or age limit reached)",
                        "stack_trace": "",
                    },
                }
            )
        finally:
            self._start_times.pop(message.message_id, None)

    # ── Payload builders ───────────────────────────────────────────────

    def _build_worker_payload(self, message: dramatiq.Message) -> dict[str, Any]:
        hostname = socket.gethostname()
        pid = os.getpid()
        return {
            "key": f"{hostname}:{pid}",
            "hostname": hostname,
            "pid": pid,
            "queues": [message.queue_name],
        }

    def _build_task_payload(self, message: dramatiq.Message) -> dict[str, Any]:
        return {
            "name": message.actor_name,
            "id": message.message_id,
            "queue": message.queue_name,
            "attempt": message.options.get("retries", 0) + 1,
        }

    # ── Helpers ────────────────────────────────────────────────────────

    def _calculate_duration(self, message: dramatiq.Message) -> float | None:
        start_ns = self._start_times.get(message.message_id)
        if start_ns is None:
            return None
        elapsed_ns = time.monotonic_ns() - start_ns
        return round(elapsed_ns / 1_000_000, 2)

    @staticmethod
    def _format_traceback(exception: BaseException) -> str:
        lines = traceback.format_exception(type(exception), exception, exception.__traceback__)
        full = "".join(lines)
        split = full.split("\n")
        return "\n".join(split[:60])  # ~3 lines per frame * 20 frames


def configure(
    *,
    api_key: str,
    endpoint: str = "https://ingest.tracestax.com",
    flush_interval: float = 5.0,
    max_batch_size: int = 100,
) -> TraceStaxMiddleware:
    """Initialise the TraceStax SDK and return a Dramatiq middleware instance.

    Usage::

        import dramatiq
        from tracestax.dramatiq import configure

        middleware = configure(api_key="ts_live_xxx")
        broker = dramatiq.get_broker()
        broker.add_middleware(middleware)
    """
    client = TraceStaxClient(
        api_key=api_key,
        endpoint=endpoint,
        flush_interval=flush_interval,
        max_batch_size=max_batch_size,
    )
    return TraceStaxMiddleware(client=client)
