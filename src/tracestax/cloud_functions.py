"""TraceStax auto-instrumentation for Google Cloud Functions.

Provides a decorator that wraps Cloud Function handlers to automatically
capture invocation timing, status, and error information.

Usage (HTTP function)::

    from tracestax.cloud_functions import tracestax_cloud_function

    @tracestax_cloud_function(api_key="ts_live_xxx")
    def hello_http(request):
        return "Hello World!"

Usage (CloudEvent function)::

    from tracestax.cloud_functions import tracestax_cloud_function

    @tracestax_cloud_function(api_key="ts_live_xxx")
    def hello_event(cloud_event):
        print(cloud_event.data)
"""

from __future__ import annotations

import functools
import logging
import os
import platform
import time
import traceback as tb_module
import uuid
from typing import Any, Callable

logger = logging.getLogger("tracestax")

_SDK_VERSION = "0.1.0"


def tracestax_cloud_function(
    _func: Callable | None = None,
    *,
    api_key: str | None = None,
    endpoint: str = "https://ingest.tracestax.com",
    flush_interval: float = 1.0,
    max_batch_size: int = 50,
):
    """Decorator that wraps a Google Cloud Function with TraceStax instrumentation.

    Can be used with or without arguments::

        @tracestax_cloud_function(api_key="ts_live_xxx")
        def my_function(request):
            ...

        # Or with env-var-based config:
        @tracestax_cloud_function
        def my_function(request):
            ...

    The decorator sends a *started* event before execution, and either a
    *succeeded* or *failed* event after. All exceptions are re-raised so
    Cloud Functions error handling is unaffected.

    Parameters
    ----------
    api_key:
        TraceStax project API key. Falls back to ``TRACESTAX_API_KEY`` env var.
    endpoint:
        Base URL of the ingest service.
    flush_interval:
        Seconds between automatic flushes. Lower than default since Cloud
        Functions are short-lived.
    max_batch_size:
        Maximum number of events per HTTP request.
    """

    def decorator(func: Callable) -> Callable:
        # Lazily initialise the client on first invocation to avoid import-time
        # side effects in cold starts.
        _client_holder: list = []

        def _get_client():
            if not _client_holder:
                from tracestax.client import TraceStaxClient

                resolved_key = api_key or os.environ.get("TRACESTAX_API_KEY", "")
                client = TraceStaxClient(
                    api_key=resolved_key,
                    endpoint=endpoint,
                    flush_interval=flush_interval,
                    max_batch_size=max_batch_size,
                )
                _client_holder.append(client)
            return _client_holder[0]

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                client = _get_client()
            except Exception:
                logger.debug("TraceStax: failed to initialise client", exc_info=True)
                return func(*args, **kwargs)

            function_name = _resolve_function_name(func)
            invocation_id = _resolve_invocation_id(args)

            worker_info = _build_worker_info(function_name)

            # Send started event
            try:
                client.send_event(
                    _build_payload(
                        function_name=function_name,
                        invocation_id=invocation_id,
                        worker=worker_info,
                        status="started",
                        duration_ms=0.0,
                    )
                )
            except Exception:
                logger.debug("TraceStax: failed to send started event", exc_info=True)

            start = time.monotonic()
            error_info = None
            status = "succeeded"

            try:
                result = func(*args, **kwargs)
                return result
            except Exception as exc:
                status = "failed"
                error_info = _extract_error(exc)
                raise
            finally:
                duration_ms = round((time.monotonic() - start) * 1000.0, 2)

                try:
                    payload = _build_payload(
                        function_name=function_name,
                        invocation_id=invocation_id,
                        worker=worker_info,
                        status=status,
                        duration_ms=duration_ms,
                        error=error_info,
                    )
                    client.send_event(payload)
                    # Flush immediately — function may be frozen after return
                    client.flush()
                except Exception:
                    logger.debug("TraceStax: failed to send completion event", exc_info=True)

        return wrapper

    # Support both @tracestax_cloud_function and @tracestax_cloud_function(...)
    if _func is not None:
        return decorator(_func)
    return decorator


# ── Payload builders ──────────────────────────────────────────────────


def _resolve_function_name(func: Callable) -> str:
    """Determine the Cloud Function name from environment or function object."""
    # GCF sets these environment variables
    name = os.environ.get("FUNCTION_NAME") or os.environ.get("K_SERVICE")
    if name:
        return name
    return getattr(func, "__name__", "unknown")


def _resolve_invocation_id(args: tuple) -> str:
    """Try to extract an invocation/trace ID from the request or event."""
    if args:
        first_arg = args[0]
        # HTTP functions receive a Flask request with trace header
        headers = getattr(first_arg, "headers", None)
        if headers:
            trace = headers.get("X-Cloud-Trace-Context")
            if trace:
                return trace.split("/")[0]
        # CloudEvent functions receive an event with an id
        event_id = getattr(first_arg, "id", None)
        if event_id:
            return str(event_id)
    return uuid.uuid4().hex


def _build_payload(
    *,
    function_name: str,
    invocation_id: str,
    worker: dict[str, Any],
    status: str,
    duration_ms: float,
    error: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Construct an IngestPayload dict matching @tracestax/types."""
    payload: dict[str, Any] = {
        "framework": "cloud_functions",
        "language": "python",
        "sdk_version": _SDK_VERSION,
        "type": "task_event",
        "worker": worker,
        "task": {
            "name": function_name,
            "id": invocation_id,
            "queue": "cloud_functions",
            "attempt": 1,
        },
        "status": status,
        "metrics": {
            "duration_ms": duration_ms,
        },
    }

    if error:
        payload["error"] = error

    return payload


def _build_worker_info(function_name: str) -> dict[str, Any]:
    """Build worker info for a Cloud Function invocation."""
    hostname = platform.node() or os.environ.get("K_REVISION", "gcf")
    pid = os.getpid()
    region = os.environ.get("FUNCTION_REGION", os.environ.get("GOOGLE_CLOUD_REGION", "unknown"))

    return {
        "key": f"gcf:{function_name}:{region}",
        "hostname": hostname,
        "pid": pid,
        "concurrency": 1,
        "queues": ["cloud_functions"],
    }


def _extract_error(exception: BaseException) -> dict[str, str]:
    """Build a TaskError dict from a Python exception."""
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
