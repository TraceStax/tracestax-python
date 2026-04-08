"""Periodic queue-depth snapshot collection for TraceStax.

Installs a Celery beat task (or standalone thread) that periodically reads
queue depths from the broker and sends SnapshotPayload to TraceStax.
"""

from __future__ import annotations

import logging
import os
import platform
import threading
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from celery import Celery

    from tracestax.client import TraceStaxClient

logger = logging.getLogger("tracestax")


def install_snapshot_task(
    app: Celery,
    client: TraceStaxClient,
    *,
    interval: float = 60.0,
) -> None:
    """Start a background thread that collects queue snapshots.

    We use a background thread instead of Celery beat to avoid requiring
    beat configuration. The thread reads queue stats from the broker
    connection every ``interval`` seconds.
    """
    collector = SnapshotCollector(app, client, interval=interval)
    collector.start()
    logger.info("TraceStax snapshot collector started (interval=%.0fs)", interval)


class SnapshotCollector:
    """Background thread that periodically polls queue depths."""

    def __init__(self, app: Celery, client: TraceStaxClient, *, interval: float) -> None:
        self._app = app
        self._client = client
        self._interval = interval
        self._stopped = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="tracestax-snapshot",
        )
        self._thread.start()

    def stop(self) -> None:
        self._stopped.set()

    def _run(self) -> None:
        # Wait one interval before the first snapshot (let the worker warm up)
        self._stopped.wait(timeout=self._interval)

        while not self._stopped.is_set():
            try:
                snapshot = self._collect()
                if snapshot:
                    self._client.send_snapshot(snapshot)
            except Exception:
                logger.warning("Failed to collect queue snapshot", exc_info=True)
            self._stopped.wait(timeout=self._interval)

    def _collect(self) -> dict[str, Any] | None:
        """Read queue depths from the Celery broker and build a SnapshotPayload."""
        queues = self._get_queue_stats()
        if not queues:
            return None

        hostname = platform.node()
        pid = os.getpid()

        return {
            "framework": "celery",
            "worker_key": f"{hostname}:{pid}",
            "queues": queues,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def _get_queue_stats(self) -> list[dict[str, Any]]:
        """Extract per-queue stats from the Celery app's broker connection.

        This works with Redis and AMQP brokers. For Redis we use llen to
        get the queue depth; for AMQP we fall back to the management API
        if available.
        """
        stats: list[dict[str, Any]] = []

        try:
            # Determine which queues this app knows about
            queue_names = self._get_queue_names()

            connection = self._app.connection_for_read()
            with connection as conn:
                transport_name = conn.transport.driver_type if hasattr(conn.transport, "driver_type") else ""

                if transport_name == "redis":
                    stats = self._read_redis_queues(conn, queue_names)
                else:
                    stats = self._read_amqp_queues(conn, queue_names)

        except Exception:
            logger.debug("Could not read queue stats from broker", exc_info=True)

        return stats

    def _get_queue_names(self) -> list[str]:
        """Get the list of configured queue names."""
        try:
            return [q.name for q in self._app.amqp.queues.values()]
        except Exception:
            return ["celery"]

    def _read_redis_queues(self, conn, queue_names: list[str]) -> list[dict[str, Any]]:
        """Read queue depths directly from Redis."""
        stats = []
        channel = conn.channel()

        for name in queue_names:
            try:
                # Redis stores Celery queues as lists
                client = channel.client
                depth = client.llen(name)
                active = client.llen(f"{name}:active") if client.exists(f"{name}:active") else 0

                stats.append(
                    {
                        "name": name,
                        "depth": depth,
                        "active": active,
                        "failed": 0,
                        "throughput_per_min": 0,  # computed server-side
                    }
                )
            except Exception:
                logger.debug("Could not read queue %s", name, exc_info=True)

        return stats

    def _read_amqp_queues(self, conn, queue_names: list[str]) -> list[dict[str, Any]]:
        """Best-effort queue depth reading for AMQP brokers."""
        stats = []
        channel = conn.channel()

        for name in queue_names:
            try:
                # queue_declare with passive=True returns message count
                _, message_count, consumer_count = channel.queue_declare(
                    queue=name,
                    passive=True,
                )
                stats.append(
                    {
                        "name": name,
                        "depth": message_count,
                        "active": consumer_count,
                        "failed": 0,
                        "throughput_per_min": 0,
                    }
                )
            except Exception:
                logger.debug("Could not read queue %s", name, exc_info=True)

        return stats
