"""TraceStax auto-instrumentation for AWS Lambda.

Provides a decorator that wraps Lambda handler functions to automatically
capture invocation timing, status, and error information.

Usage::

    from tracestax.aws_lambda import tracestax_handler

    @tracestax_handler(api_key="ts_live_xxx")
    def handler(event, context):
        return process(event)
"""

from __future__ import annotations

import functools
import logging
import os
import platform
import time
import traceback as tb_module
from typing import Any, Callable

logger = logging.getLogger("tracestax")

_SDK_VERSION = "0.1.0"


def tracestax_handler(
    _func: Callable | None = None,
    *,
    api_key: str | None = None,
    endpoint: str = "https://ingest.tracestax.com",
    flush_interval: float = 1.0,
    max_batch_size: int = 50,
):
    """Decorator that wraps an AWS Lambda handler with TraceStax instrumentation.

    Can be used with or without arguments::

        @tracestax_handler(api_key="ts_live_xxx")
        def handler(event, context):
            ...

        # Or with env-var-based config:
        @tracestax_handler
        def handler(event, context):
            ...

    The decorator sends a *started* event before execution, and either a
    *succeeded* or *failed* event after. All exceptions are re-raised so
    Lambda error handling is unaffected.

    Parameters
    ----------
    api_key:
        TraceStax project API key. Falls back to ``TRACESTAX_API_KEY`` env var.
    endpoint:
        Base URL of the ingest service.
    flush_interval:
        Seconds between automatic flushes. Lower than default since Lambda
        functions are short-lived.
    max_batch_size:
        Maximum number of events per HTTP request.
    """

    def decorator(func: Callable) -> Callable:
        # Lazily initialise the client on first invocation to avoid import-time
        # side effects in Lambda cold starts.
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
        def wrapper(event: Any, context: Any) -> Any:
            try:
                client = _get_client()
            except Exception:
                logger.debug("TraceStax: failed to initialise client", exc_info=True)
                return func(event, context)

            function_name = getattr(context, "function_name", "unknown")
            request_id = getattr(context, "aws_request_id", "unknown")
            memory_mb = getattr(context, "memory_limit_in_mb", None)

            worker_info = _build_worker_info(function_name)

            # Send started event
            try:
                client.send_event(
                    _build_payload(
                        function_name=function_name,
                        request_id=request_id,
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
                result = func(event, context)
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
                        request_id=request_id,
                        worker=worker_info,
                        status=status,
                        duration_ms=duration_ms,
                        error=error_info,
                        memory_mb=memory_mb,
                    )
                    client.send_event(payload)
                    # Flush immediately — Lambda may freeze after return
                    client.flush()
                except Exception:
                    logger.debug("TraceStax: failed to send completion event", exc_info=True)

        return wrapper

    # Support both @tracestax_handler and @tracestax_handler(...)
    if _func is not None:
        return decorator(_func)
    return decorator


# ── Payload builders ──────────────────────────────────────────────────


def _build_payload(
    *,
    function_name: str,
    request_id: str,
    worker: dict[str, Any],
    status: str,
    duration_ms: float,
    error: dict[str, str] | None = None,
    memory_mb: int | None = None,
) -> dict[str, Any]:
    """Construct an IngestPayload dict matching @tracestax/types."""
    payload: dict[str, Any] = {
        "framework": "aws_lambda",
        "language": "python",
        "sdk_version": _SDK_VERSION,
        "type": "task_event",
        "worker": worker,
        "task": {
            "name": function_name,
            "id": request_id,
            "queue": "lambda",
            "attempt": 1,
        },
        "status": status,
        "metrics": {
            "duration_ms": duration_ms,
        },
    }

    if memory_mb is not None:
        payload["metrics"]["memory_limit_mb"] = int(memory_mb)

    if error:
        payload["error"] = error

    return payload


def _build_worker_info(function_name: str) -> dict[str, Any]:
    """Build worker info for a Lambda invocation."""
    hostname = platform.node() or os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME", "lambda")
    pid = os.getpid()
    region = os.environ.get("AWS_REGION", "unknown")

    return {
        "key": f"lambda:{function_name}:{region}",
        "hostname": hostname,
        "pid": pid,
        "concurrency": 1,
        "queues": ["lambda"],
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
