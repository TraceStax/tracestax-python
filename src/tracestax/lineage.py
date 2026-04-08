"""Celery lineage tracking for TraceStax.

Monkey-patches ``Task.apply_async`` to automatically inject parent/chain
context into child task headers and report parent-child relationships.
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from celery import Celery

    from tracestax.client import TraceStaxClient

logger = logging.getLogger("tracestax")

_original_apply_async = None
_lineage_client: TraceStaxClient | None = None
_displacement_warned = False  # rate-limit the "hook superseded" warning to one log per process


def install_lineage_hooks(app: Celery, client: TraceStaxClient) -> None:
    """Monkey-patch Task.apply_async to capture lineage context.

    When a task spawns child tasks (e.g. via ``some_task.delay()`` inside
    another task), this patch:

    1. Injects ``tracestax_parent_id`` and ``tracestax_chain_id`` into the
       child's headers.
    2. Sends a ``LineagePayload`` to TraceStax reporting the parent-child
       relationship.
    """
    global _original_apply_async, _lineage_client

    from celery import Task

    if _original_apply_async is not None:
        logger.warning("Lineage hooks already installed — skipping")
        return

    _original_apply_async = Task.apply_async
    _lineage_client = client

    def patched_apply_async(self, args=None, kwargs=None, **options):
        """Wrap apply_async to inject lineage headers."""
        global _displacement_warned
        # Detect if another patch has replaced Task.apply_async after TraceStax
        # installed its hook. The last patch wins in a monkey-patch chain, so if
        # this function is no longer the live implementation, lineage tracking is
        # silently inactive. Warn once so the issue is visible in logs.
        if not _displacement_warned and Task.apply_async is not patched_apply_async:
            logger.warning(
                "TraceStax lineage hook has been superseded by another patch on "
                "Task.apply_async. Lineage tracking may be inactive."
            )
            _displacement_warned = True
        _inject_lineage_context(self, options)
        return _original_apply_async(self, args=args, kwargs=kwargs, **options)

    Task.apply_async = patched_apply_async
    logger.info("TraceStax lineage hooks installed")


def _inject_lineage_context(task, options: dict[str, Any]) -> None:
    """Inject parent_id and chain_id into the task's headers."""
    from celery import current_task

    if current_task is None or current_task.request is None:
        return  # Not called from within a task; nothing to track

    parent_id = current_task.request.id
    if parent_id is None:
        return

    # Determine the chain_id: inherit from parent or create a new one
    parent_headers = getattr(current_task.request, "headers", None) or {}
    chain_id = parent_headers.get("tracestax_chain_id") or str(uuid.uuid4())

    # Inject into child headers
    headers = options.get("headers") or {}
    headers["tracestax_parent_id"] = parent_id
    headers["tracestax_chain_id"] = chain_id
    options["headers"] = headers

    # Determine child task info for the lineage payload
    child_task_id = options.get("task_id") or str(uuid.uuid4())
    # Ensure the child gets this ID
    if "task_id" not in options:
        options["task_id"] = child_task_id

    child_task_name = task.name if hasattr(task, "name") else "unknown"

    # Buffer children spawned from this parent in the same flush cycle
    _report_lineage(parent_id, chain_id, child_task_id, child_task_name)


def _report_lineage(
    parent_id: str,
    chain_id: str,
    child_id: str,
    child_task_name: str,
) -> None:
    """Send a LineagePayload to TraceStax."""
    if _lineage_client is None:
        return

    payload = {
        "parent_id": parent_id,
        "children": [
            {
                "id": child_id,
                "task_name": child_task_name,
            }
        ],
        "chain_id": chain_id,
    }

    _lineage_client.send_lineage(payload)
