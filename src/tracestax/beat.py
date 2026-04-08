"""Celery Beat scheduler integration for TraceStax.

Reports scheduled task dispatches so you can monitor whether your periodic
tasks are actually firing on schedule.

Usage::

    from celery.beat import PersistentScheduler
    from tracestax.beat import TraceStaxBeatScheduler

    class MyScheduler(TraceStaxBeatScheduler, PersistentScheduler):
        pass

    # In celery config:
    # beat_scheduler = "myapp.schedulers:MyScheduler"

Or configure standalone::

    from tracestax.beat import TraceStaxBeatScheduler

    scheduler = TraceStaxBeatScheduler(api_key="ts_live_xxx")
    # Call scheduler.on_task_dispatched(task_name, task_id) from your Beat
    # scheduler subclass's apply_async override.
"""

from __future__ import annotations

import os
from typing import Optional

from tracestax.client import TraceStaxClient

_SDK_VERSION = "0.1.0"


class TraceStaxBeatScheduler:
    """Mixin / standalone class that reports Celery Beat dispatches to TraceStax.

    Subclass alongside a real Celery Beat scheduler (e.g. ``PersistentScheduler``)
    and call ``on_task_dispatched`` from an ``apply_async`` override, or use it
    standalone to manually report dispatches.
    """

    def __init__(
        self,
        api_key: str,
        endpoint: str = "https://ingest.tracestax.com",
        enabled: Optional[bool] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[call-arg]
        if enabled is False or os.environ.get("TRACESTAX_ENABLED") == "false":
            self._tracestax_client: TraceStaxClient | None = None
            return
        self._tracestax_client = TraceStaxClient(api_key=api_key, endpoint=endpoint)

    def on_task_dispatched(self, task_name: str, task_id: str) -> None:
        """Report a scheduled task dispatch event to TraceStax.

        Call this from your Beat scheduler subclass's ``apply_async`` override::

            def apply_async(self, entry, producer=None, advance=True, **kwargs):
                result = super().apply_async(entry, producer, advance, **kwargs)
                self.on_task_dispatched(entry.name, result.id)
                return result
        """
        if not self._tracestax_client:
            return

        import socket
        import time

        self._tracestax_client.send_event(
            {
                "type": "task_event",
                "framework": "celery",
                "language": "python",
                "sdk_version": _SDK_VERSION,
                "task": {
                    "name": task_name,
                    "id": task_id,
                    "queue": "celery",
                    "attempt": 0,
                    "scheduled": True,
                },
                "status": "started",
                "metrics": {"duration_ms": 0},
                "worker": {
                    "key": f"celery-beat-{os.getpid()}",
                    "hostname": socket.gethostname(),
                    "pid": os.getpid(),
                    "concurrency": 1,
                    "queues": ["celery"],
                },
            }
        )

    def close(self) -> None:
        """Flush pending events and close the TraceStax client."""
        if self._tracestax_client:
            self._tracestax_client.close()
        try:
            super().close()  # type: ignore[misc]
        except AttributeError:
            pass  # Celery scheduler versions prior to 5.x do not implement close()
