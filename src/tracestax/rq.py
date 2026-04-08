"""RQ (Redis Queue) integration for TraceStax.

Provides ``TraceStaxWorker``, a drop-in replacement for ``rq.Worker`` that
automatically captures task timing, status, and error information.
"""

from __future__ import annotations

import logging
import os
import platform
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tracestax.client import TraceStaxClient

logger = logging.getLogger("tracestax")

SDK_VERSION = "0.1.0"

_HEARTBEAT_INTERVAL = 60.0  # seconds between heartbeats


class TraceStaxWorker:
    """A wrapper around ``rq.Worker`` that instruments job execution.

    Usage::

        from tracestax import configure
        from tracestax.rq import TraceStaxWorker

        client = configure(api_key="ts_live_xxx")
        worker = TraceStaxWorker(client=client, queues=["default"])
        worker.work()

    The wrapper extends the real RQ Worker class at runtime so that
    ``isinstance(worker, rq.Worker)`` is ``True``.
    """

    def __new__(cls, *, client: TraceStaxClient, queues: list[str] | None = None, **kwargs):
        """Dynamically subclass rq.Worker so callers get a real Worker instance."""
        try:
            import rq
        except ImportError as exc:
            raise ImportError(
                "rq is required for TraceStaxWorker. Install it with: "
                "pip install tracestax-celery[rq]"
            ) from exc

        # Build a dynamic subclass that mixes in our instrumentation
        worker_cls = type(
            "TraceStaxRQWorker",
            (rq.Worker,),
            {
                "_tracestax_client": client,
                "perform_job": _perform_job,
                "work": _work_with_heartbeat,
                "_tracestax_send_heartbeat": _send_heartbeat,
            },
        )

        if queues is None:
            queues = ["default"]

        # rq.Worker expects Queue objects
        queue_objects = []
        connection = kwargs.pop("connection", None)
        for q in queues:
            if isinstance(q, str):
                queue_objects.append(rq.Queue(q, connection=connection))
            else:
                queue_objects.append(q)

        instance = worker_cls.__new__(worker_cls)
        worker_cls.__init__(instance, queue_objects, connection=connection, **kwargs)
        return instance


def _perform_job(self, job, queue):
    """Override of Worker.perform_job that wraps execution with timing."""
    import rq

    client: TraceStaxClient = self._tracestax_client
    start = time.monotonic()
    error_info = None
    status = "succeeded"

    try:
        result = rq.Worker.perform_job(self, job, queue)
        # rq returns True/False for success
        if not result:
            status = "failed"
        return result
    except Exception as exc:
        status = "failed"
        error_info = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
        raise
    finally:
        duration_ms = (time.monotonic() - start) * 1000.0
        payload = _build_ingest_payload(
            worker=self,
            job=job,
            queue=queue,
            status=status,
            duration_ms=duration_ms,
            error=error_info,
        )
        client.send_event(payload)


def _work_with_heartbeat(self, *, burst: bool = False, **kwargs):
    """Override of Worker.work that adds periodic heartbeats."""
    import threading

    import rq

    client: TraceStaxClient = self._tracestax_client
    stop_event = threading.Event()

    def _heartbeat_loop():
        while not stop_event.is_set():
            stop_event.wait(timeout=_HEARTBEAT_INTERVAL)
            if not stop_event.is_set():
                self._tracestax_send_heartbeat()

    # Send initial heartbeat
    self._tracestax_send_heartbeat()

    hb_thread = threading.Thread(target=_heartbeat_loop, daemon=True, name="tracestax-rq-heartbeat")
    hb_thread.start()

    try:
        return rq.Worker.work(self, burst=burst, **kwargs)
    finally:
        stop_event.set()
        # Final flush
        client.close()


def _send_heartbeat(self):
    """Send a heartbeat payload to TraceStax."""
    client: TraceStaxClient = self._tracestax_client
    worker_info = _build_worker_info(self)
    client.send_heartbeat(
        {
            "framework": "rq",
            "worker": worker_info,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )


def _build_ingest_payload(
    *,
    worker,
    job,
    queue,
    status: str,
    duration_ms: float,
    error: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build an IngestPayload dict for an RQ job execution."""
    worker_info = _build_worker_info(worker)

    queue_name = "unknown"
    if hasattr(queue, "name"):
        queue_name = queue.name
    elif isinstance(queue, str):
        queue_name = queue

    task_name = job.func_name if hasattr(job, "func_name") else str(job)
    task_id = job.id if hasattr(job, "id") else "unknown"

    # Attempt tracking: rq stores meta on the job
    attempt = 1
    meta = getattr(job, "meta", {}) or {}
    if "tracestax_attempt" in meta:
        attempt = meta["tracestax_attempt"]
    elif hasattr(job, "retries_left") and hasattr(job, "retry_intervals"):
        total_retries = len(job.retry_intervals) if job.retry_intervals else 0
        retries_left = job.retries_left or 0
        attempt = max(1, total_retries - retries_left + 1)

    payload: dict[str, Any] = {
        "framework": "rq",
        "language": "python",
        "sdk_version": SDK_VERSION,
        "type": "task_event",
        "worker": worker_info,
        "task": {
            "name": task_name,
            "id": task_id,
            "queue": queue_name,
            "attempt": attempt,
        },
        "status": status,
        "metrics": {
            "duration_ms": round(duration_ms, 2),
        },
    }

    # RQ queued time
    enqueued_at = getattr(job, "enqueued_at", None)
    started_at = getattr(job, "started_at", None)
    if enqueued_at and started_at:
        queued_ms = (started_at - enqueued_at).total_seconds() * 1000.0
        payload["metrics"]["queued_ms"] = round(queued_ms, 2)

    if error:
        payload["error"] = error

    return payload


def _build_worker_info(worker) -> dict[str, Any]:
    """Extract WorkerInfo from an rq.Worker instance."""
    hostname = platform.node()
    pid = os.getpid()
    queues = [q.name for q in getattr(worker, "queues", [])]

    return {
        "key": f"{hostname}:{pid}",
        "hostname": hostname,
        "pid": pid,
        "concurrency": 1,  # RQ workers are single-threaded
        "queues": queues,
    }
