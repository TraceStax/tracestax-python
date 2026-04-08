"""TraceStax SDK for Python task queues and serverless functions.

Supports Celery, Dramatiq, RQ, Huey, TaskIQ, AWS Lambda, and Google Cloud
Functions.
"""

from __future__ import annotations

import logging
import warnings

logger = logging.getLogger(__name__)

__version__ = "0.1.0"

from tracestax.client import TraceStaxClient

# Module-level singleton set by configure()
_client: TraceStaxClient | None = None


def configure(
    app=None,
    *,
    api_key: str,
    endpoint: str = "https://ingest.tracestax.com",
    flush_interval: float = 5.0,
    max_batch_size: int = 100,
    enable_lineage: bool = True,
    enable_snapshots: bool = True,
    snapshot_interval: float = 60.0,
) -> TraceStaxClient:
    """Initialise the TraceStax SDK.

    For Celery:
        from tracestax import configure
        configure(celery_app, api_key="ts_live_xxx")

    For RQ:
        from tracestax import configure
        client = configure(api_key="ts_live_xxx")
        # Then use TraceStaxWorker(client=client, ...)

    For Dramatiq, use tracestax.dramatiq.configure() instead.
    """
    global _client

    if _client is not None:
        warnings.warn(
            "tracestax.configure() called more than once. The previous client will be "
            "replaced. Call client.close() before re-configuring to avoid a leaked "
            "background thread.",
            stacklevel=2,
            category=RuntimeWarning,
        )
        try:
            _client.close()
        except Exception:
            logger.debug("Failed to close previous TraceStax client during reconfigure", exc_info=True)

    client = TraceStaxClient(
        api_key=api_key,
        endpoint=endpoint,
        flush_interval=flush_interval,
        max_batch_size=max_batch_size,
    )
    _client = client

    # If a Celery app is provided, auto-instrument it
    if app is not None:
        _configure_celery(app, client, enable_lineage, enable_snapshots, snapshot_interval)

    return client


def _configure_celery(
    app,
    client: TraceStaxClient,
    enable_lineage: bool,
    enable_snapshots: bool,
    snapshot_interval: float,
) -> None:
    """Wire Celery signal hooks and optional lineage/snapshot collectors."""
    from tracestax.celery import install_hooks

    install_hooks(app, client)

    if enable_lineage:
        from tracestax.lineage import install_lineage_hooks

        install_lineage_hooks(app, client)

    if enable_snapshots:
        from tracestax.snapshots import install_snapshot_task

        install_snapshot_task(app, client, interval=snapshot_interval)


def get_client() -> TraceStaxClient | None:
    """Return the module-level client (set by configure())."""
    return _client


__all__ = [
    "TraceStaxClient",
    "configure",
    "get_client",
    "__version__",
    # Framework-specific modules (imported on demand to avoid hard deps):
    # from tracestax.dramatiq import TraceStaxMiddleware, configure as configure_dramatiq
    # from tracestax.beat import TraceStaxBeatScheduler
    # from tracestax.rq import TraceStaxWorker
    # from tracestax.huey_integration import connect as connect_huey
    # from tracestax.taskiq_integration import configure as configure_taskiq
    # from tracestax.aws_lambda import tracestax_handler
    # from tracestax.cloud_functions import tracestax_cloud_function
]
