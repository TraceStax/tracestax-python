"""Thread-safe batching client that ships events to the TraceStax ingest API.

Resilience features:
- Exponential backoff on flush interval when HTTP calls fail
- Circuit breaker: 3 consecutive failures → OPEN (silent drop) → 30 s cooldown
  → HALF_OPEN (probe) → CLOSED on success
- X-Retry-After header support
- Backpressure directive from heartbeat response
"""

from __future__ import annotations

import atexit
import collections
import io
import json
import logging
import os
import threading
import time
import traceback as _traceback
from typing import Any, Optional

import requests

from tracestax.watchdog import Watchdog

logger = logging.getLogger("tracestax")

_INGEST_PATH = "/v1/ingest"
_SNAPSHOT_PATH = "/v1/snapshot"
_LINEAGE_PATH = "/v1/lineage"
_HEARTBEAT_PATH = "/v1/heartbeat"
_DUMP_PATH = "/v1/dump"

_SDK_VERSION = "0.1.0"
_CIRCUIT_OPEN_THRESHOLD = 3
_CIRCUIT_COOLDOWN_S = 30.0
_MAX_FLUSH_INTERVAL_S = 60.0


class TraceStaxClient:
    """Buffers events in a thread-safe deque and flushes them in batches via a
    background daemon thread.

    Parameters
    ----------
    api_key:
        TraceStax project API key (``ts_live_…`` or ``ts_test_…``).
    endpoint:
        Base URL of the ingest service.
    flush_interval:
        Seconds between automatic flushes.
    max_batch_size:
        Maximum number of events per HTTP request.
    """

    def __init__(
        self,
        api_key: str,
        endpoint: str = "https://ingest.tracestax.com",
        flush_interval: float = 5.0,
        max_batch_size: int = 100,
        enabled: Optional[bool] = None,
        dry_run: Optional[bool] = None,
    ) -> None:
        # RUN-140: Check enabled/dry_run from params or env vars
        self._enabled = enabled if enabled is not None else os.environ.get("TRACESTAX_ENABLED") != "false"
        self._dry_run = dry_run if dry_run is not None else os.environ.get("TRACESTAX_DRY_RUN") == "true"

        # Set by the framework monitor layer; used in thread dump payloads
        self._worker_key: Optional[str] = None

        if not self._enabled:
            self._api_key = ""
            self._endpoint = ""
            self._flush_interval = 0.0
            self._max_batch_size = 0
            self._current_flush_interval = 0.0
            self._queue: collections.deque[dict[str, Any]] = collections.deque()
            self._lock = threading.Lock()
            self._closed = threading.Event()
            self._flush_now = threading.Event()
            self._session = None  # type: ignore[assignment]
            self._daemon = threading.Thread()
            self._watchdog = None  # type: ignore[assignment]
            # Resilience state (unused when disabled)
            self._consecutive_failures = 0
            self._circuit_state = "CLOSED"
            self._circuit_opened_at: Optional[float] = None
            self._pause_until: Optional[float] = None
            self._resilience_lock = threading.Lock()
            self._dropped_events = 0
            return

        if not api_key and not self._dry_run:
            raise ValueError("api_key is required")

        self._api_key = api_key or ""
        self._endpoint = endpoint.rstrip("/")
        self._flush_interval = flush_interval
        self._max_batch_size = max_batch_size
        self._current_flush_interval = flush_interval

        # Resilience state
        self._consecutive_failures = 0
        self._circuit_state = "CLOSED"  # "CLOSED" | "OPEN" | "HALF_OPEN"
        self._circuit_opened_at: Optional[float] = None
        self._pause_until: Optional[float] = None
        self._resilience_lock = threading.Lock()
        self._dropped_events = 0

        # Thread-safe unbounded queue
        self._queue = collections.deque()
        self._lock = threading.Lock()

        # Shutdown flag
        self._closed = threading.Event()

        if self._dry_run:
            self._flush_now = threading.Event()
            self._session = None  # type: ignore[assignment]
            self._daemon = threading.Thread()
            self._watchdog = None  # type: ignore[assignment]
            return

        # Session for connection pooling
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
                "User-Agent": f"tracestax-python/{_SDK_VERSION}",
            }
        )

        # Background flush thread
        self._daemon = self._start_daemon()

        # Watchdog monitors daemon health
        self._watchdog = Watchdog(self)

        # Best-effort flush on interpreter exit
        atexit.register(self._atexit_flush)

        # Fork safety: Celery / Gunicorn prefork workers call os.fork().
        # If a fork happens while the daemon thread holds _lock or
        # _resilience_lock, the child inherits the locked mutex without the
        # thread that owns it — causing a permanent deadlock.
        # register_at_fork reinitialises all state in the child process.
        if hasattr(os, "register_at_fork"):
            os.register_at_fork(after_in_child=self._reinitialize_after_fork)

    # ── Public API ─────────────────────────────────────────────────────

    def send_event(self, payload: dict[str, Any]) -> None:
        """Enqueue an event for batched delivery."""
        if not self._enabled:
            return

        # Guard against huge payloads that would OOM during serialization at
        # flush time. Serialize here so non-serializable values are caught early
        # rather than poisoning an entire batch of otherwise-valid events.
        try:
            serialized = json.dumps(payload)
        except (TypeError, ValueError):
            logger.warning("TraceStax send_event: payload not serializable, dropping")
            return
        if len(serialized) > 512 * 1024:
            logger.warning("TraceStax send_event: payload exceeds 512 KB, dropping")
            return

        if self._dry_run:
            preview = serialized[:512] + "…" if len(serialized) > 512 else serialized
            logger.info("[tracestax dry-run] send_event: %s", preview)
            return
        if self._closed.is_set():
            logger.warning("Client is closed; dropping event")
            return

        # Queue memory cap: prevent unbounded growth when the server is down.
        # Drop the oldest half when we exceed 10K events (same policy as Node/Ruby).
        if len(self._queue) >= 10_000:
            drop = len(self._queue) - 5_000
            for _ in range(drop):
                try:
                    self._queue.popleft()
                except IndexError:
                    break
            with self._resilience_lock:
                self._dropped_events += drop
            logger.warning("TraceStax queue overflow: dropped %d oldest events", drop)

        self._queue.append(payload)

        # If the queue has grown beyond batch size, wake the daemon early
        if len(self._queue) >= self._max_batch_size:
            self._flush_now.set()

    def send_snapshot(self, payload: dict[str, Any]) -> None:
        """Send a snapshot payload immediately (not batched)."""
        if not self._enabled:
            return
        if self._dry_run:
            import json
            try:
                raw = json.dumps(payload)
                preview = raw[:512] + "…" if len(raw) > 512 else raw
                logger.info("[tracestax dry-run] send_snapshot: %s", preview)
            except (TypeError, ValueError):
                logger.info("[tracestax dry-run] send_snapshot: [payload not serializable: %s]", repr(payload))
            return
        self._post_for_json(_SNAPSHOT_PATH, payload)

    def send_lineage(self, payload: dict[str, Any]) -> None:
        """Send a lineage payload immediately (not batched)."""
        if not self._enabled:
            return
        if self._dry_run:
            import json
            try:
                raw = json.dumps(payload)
                preview = raw[:512] + "…" if len(raw) > 512 else raw
                logger.info("[tracestax dry-run] send_lineage: %s", preview)
            except (TypeError, ValueError):
                logger.info("[tracestax dry-run] send_lineage: [payload not serializable: %s]", repr(payload))
            return
        self._post_for_json(_LINEAGE_PATH, payload)

    def send_heartbeat(self, payload: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Send a heartbeat payload and return the server directives dict, or None."""
        if not self._enabled:
            return None
        if self._dry_run:
            import json
            try:
                raw = json.dumps(payload)
                preview = raw[:512] + "…" if len(raw) > 512 else raw
                logger.info("[tracestax dry-run] send_heartbeat: %s", preview)
            except (TypeError, ValueError):
                logger.info("[tracestax dry-run] send_heartbeat: [payload not serializable: %s]", repr(payload))
            return None
        return self._post_for_json(_HEARTBEAT_PATH, payload)

    def stats(self) -> dict[str, Any]:
        """Return a snapshot of the client's internal health metrics.

        Keys: ``queue_size``, ``dropped_events``, ``circuit_state``,
        ``consecutive_failures``.  Safe to call from any thread.
        """
        with self._resilience_lock:
            return {
                "queue_size": len(self._queue),
                "dropped_events": self._dropped_events,
                "circuit_state": self._circuit_state.lower(),
                "consecutive_failures": self._consecutive_failures,
            }

    def set_pause_until(self, epoch_ms: float) -> None:
        """Pause ingest flushing until the given epoch millisecond timestamp.
        Called by the framework monitor when the heartbeat signals backpressure."""
        with self._resilience_lock:
            self._pause_until = epoch_ms / 1000.0  # convert ms → seconds

    def execute_command(self, cmd: dict[str, Any]) -> None:
        """Execute a server-issued command. Currently supports 'thread_dump'."""
        if cmd.get("type") != "thread_dump":
            return
        dump = self._capture_thread_dump()
        self._post_for_json(
            _DUMP_PATH,
            {
                "cmd_id": cmd["id"],
                "worker_key": self._worker_key or f"python:{os.getpid()}",
                "dump_text": dump,
                "language": "python",
                "sdk_version": _SDK_VERSION,
                "captured_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            },
        )

    def flush(self) -> None:
        """Immediately flush all queued events."""
        self._drain_queue()

    def close(self) -> None:
        """Flush remaining events and shut down the background thread."""
        if not self._enabled or self._dry_run:
            return
        if self._closed.is_set():
            return
        self._closed.set()
        self._flush_now.set()  # wake the daemon so it exits
        deadline = time.time() + 5.0
        self._daemon.join(timeout=max(0.0, deadline - time.time()))
        self._drain_queue(deadline=deadline)  # final drain in calling thread, bounded
        if self._watchdog:
            self._watchdog.stop()
        if self._session:
            self._session.close()
        logger.debug("TraceStaxClient closed")

    # ── Daemon thread ──────────────────────────────────────────────────

    def _start_daemon(self) -> threading.Thread:
        self._flush_now = threading.Event()
        t = threading.Thread(target=self._run_daemon, daemon=True, name="tracestax-flush")
        t.start()
        return t

    def _run_daemon(self) -> None:
        """Loop: wait for flush_interval (adaptive) or a wake signal, then flush."""
        while not self._closed.is_set():
            with self._resilience_lock:
                wait_secs = self._current_flush_interval
            self._flush_now.wait(timeout=wait_secs)
            self._flush_now.clear()
            try:
                self._drain_queue()
            except Exception:
                logger.exception("Error during flush")

    def _drain_queue(self, deadline: "float | None" = None) -> None:
        """Drain the deque in batches, respecting the circuit breaker.

        Args:
            deadline: Optional ``time.time()`` epoch value. If provided the drain
                stops when the deadline is reached so callers can bound total time.
        """
        if not self._check_circuit():
            return

        while self._queue:
            if deadline is not None and time.time() >= deadline:
                break
            batch: list[dict[str, Any]] = []
            while self._queue and len(batch) < self._max_batch_size:
                try:
                    batch.append(self._queue.popleft())
                except IndexError:
                    break
            if batch:
                req_timeout = max(0.5, deadline - time.time()) if deadline is not None else 10
                ok = self._post_for_json(_INGEST_PATH, {"events": batch}, timeout=req_timeout) is not None
                if not ok:
                    # Restore the batch — prepend back to queue
                    for item in reversed(batch):
                        self._queue.appendleft(item)
                    break

    # ── Circuit breaker ────────────────────────────────────────────────

    def _check_circuit(self) -> bool:
        """Return True if a flush attempt should proceed."""
        with self._resilience_lock:
            now = time.time()
            # Honour backpressure pause
            if self._pause_until is not None and now < self._pause_until:
                return False
            self._pause_until = None

            if self._circuit_state == "OPEN":
                elapsed = max(0, now - (self._circuit_opened_at or 0))
                if elapsed < _CIRCUIT_COOLDOWN_S:
                    return False
                self._circuit_state = "HALF_OPEN"
            return True

    def _record_success(self) -> None:
        with self._resilience_lock:
            self._consecutive_failures = 0
            self._circuit_state = "CLOSED"
            self._circuit_opened_at = None
            self._current_flush_interval = max(
                self._flush_interval,
                self._current_flush_interval / 2.0,
            )

    def _record_failure(self, retry_after_s: Optional[float] = None) -> None:
        with self._resilience_lock:
            if retry_after_s and retry_after_s > 0:
                self._pause_until = time.time() + retry_after_s
            self._consecutive_failures += 1
            self._current_flush_interval = min(
                _MAX_FLUSH_INTERVAL_S,
                self._current_flush_interval * 2.0,
            )
            if (
                self._consecutive_failures >= _CIRCUIT_OPEN_THRESHOLD
                and self._circuit_state == "CLOSED"
            ):
                self._circuit_state = "OPEN"
                self._circuit_opened_at = time.time()
                logger.warning(
                    "TraceStax unreachable, circuit open, events dropped"
                )
            elif self._circuit_state == "HALF_OPEN":
                self._circuit_state = "OPEN"
                self._circuit_opened_at = time.time()

    # ── HTTP ───────────────────────────────────────────────────────────

    def _post_for_json(self, path: str, body: Any, timeout: float = 10) -> Optional[dict[str, Any]]:
        """POST body to path. Returns parsed JSON dict on success, None on error."""
        url = f"{self._endpoint}{path}"
        try:
            resp = self._session.post(url, json=body, timeout=timeout, stream=True)

            # Honor X-Retry-After
            retry_after_s: Optional[float] = None
            if "X-Retry-After" in resp.headers:
                try:
                    retry_after_s = float(resp.headers["X-Retry-After"])
                except ValueError:
                    logger.debug("Ignoring malformed X-Retry-After header: %r", resp.headers["X-Retry-After"])

            if resp.status_code == 401:
                # Auth failures are NOT counted as circuit-breaker failures — the circuit
                # would open and silently drop all events, masking the real problem.
                # Return False (not None) so _drain_queue discards the batch rather than
                # re-queuing it; 401 is a permanent misconfiguration, not a transient error.
                logger.error(
                    "TraceStax auth failed (401) – check your API key; events will continue to queue"
                )
                resp.close()
                return False  # type: ignore[return-value]

            if resp.status_code >= 400:
                self._record_failure(retry_after_s)
                error_preview = resp.raw.read(200).decode("utf-8", errors="replace")
                resp.close()
                logger.warning(
                    "TraceStax ingest responded %s: %s",
                    resp.status_code,
                    error_preview,
                )
                return None

            self._record_success()
            try:
                # Cap response body at 1 MB to prevent OOM from large error pages.
                raw = resp.raw.read(1_048_576)
                resp.close()
                return json.loads(raw)
            except Exception:
                return {}

        except (ValueError, TypeError):
            # requests.Session.post(json=body) calls json.dumps internally;
            # non-serializable payloads raise ValueError/TypeError which are NOT
            # subclasses of RequestException — catch them here so they never
            # propagate into customer code via send_snapshot / send_lineage / etc.
            logger.warning("Failed to serialize payload for TraceStax ingest", exc_info=True)
            return None
        except requests.RequestException:
            self._record_failure()
            logger.warning("Failed to send to TraceStax ingest", exc_info=True)
            return None

    # ── Thread dump ────────────────────────────────────────────────────

    def _capture_thread_dump(self) -> str:
        """Capture stack traces for all live threads."""
        import sys

        buf = io.StringIO()
        buf.write("=== TraceStax Python Thread Dump ===\n")
        buf.write(f"PID: {os.getpid()}\n")
        buf.write(f"Timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n")

        frames = sys._current_frames()  # type: ignore[attr-defined]
        for thread in threading.enumerate():
            buf.write(f"\nThread: {thread.name} (ident={thread.ident}, daemon={thread.daemon})\n")
            frame = frames.get(thread.ident or 0)
            if frame:
                lines = _traceback.format_stack(frame)
                buf.write("".join(lines))

        result = buf.getvalue()
        # 500 KB client-side cap to avoid a wasted HTTP round-trip
        return result[:500_000]

    # ── Helpers ────────────────────────────────────────────────────────

    def _atexit_flush(self) -> None:
        try:
            self._drain_queue()
        except Exception:
            logger.debug("Error during atexit flush", exc_info=True)

    def _reinitialize_after_fork(self) -> None:
        """Reinitialise client state in a child process after os.fork().

        Called by os.register_at_fork(after_in_child=...). The parent's daemon
        thread does NOT exist in the child, so any lock it held at fork time
        would be permanently locked. We reset all locks and restart the daemon
        thread so the child process can flush events independently.
        """
        if not self._enabled or self._dry_run:
            return
        # Reinitialise all threading primitives — they may be in a locked state
        # inherited from the parent at the moment of the fork.
        self._lock = threading.Lock()
        self._resilience_lock = threading.Lock()
        self._closed = threading.Event()
        self._flush_now = threading.Event()
        self._queue = collections.deque(self._queue)  # copy; original is shared with parent
        # Start a fresh daemon thread in the child
        try:
            self._daemon = self._start_daemon()
        except Exception:
            logger.warning(
                "Failed to restart flush daemon after fork; events in this child process may be lost",
                exc_info=True,
            )

    @property
    def is_alive(self) -> bool:
        return self._daemon is not None and self._daemon.is_alive()

    def __del__(self) -> None:
        """Best-effort cleanup on garbage collection."""
        try:
            self.close()
        except Exception:
            logger.debug("Error during __del__ cleanup", exc_info=True)

    def __repr__(self) -> str:
        return (
            f"TraceStaxClient(endpoint={self._endpoint!r}, "
            f"queue_size={len(self._queue)}, alive={self.is_alive})"
        )
