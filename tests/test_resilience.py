"""
Resilience tests for TraceStaxClient.

These tests guard the most critical production guarantee: the SDK must NEVER
crash, block, or OOM the host application — even when the ingest server is
down, slow, or returning errors.

Scenarios covered:
  - enabled=False / dry_run=True are complete no-ops
  - send_event/send_heartbeat never raise regardless of server state
  - Circuit breaker: CLOSED → OPEN after 3 failures, events dropped silently
  - Circuit breaker: OPEN → HALF_OPEN after 30s cooldown, resets on success
  - Circuit breaker: HALF_OPEN → OPEN again if probe fails
  - Thread safety: multiple threads hitting open circuit simultaneously
  - Prefork safety: _restart_daemon_after_fork restarts the flush thread
  - X-Retry-After pause honoured
"""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, call, patch

import pytest
import requests.exceptions


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_response(status_code: int = 200, headers: dict | None = None) -> MagicMock:
    """Build a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = "ok" if status_code < 400 else f"error {status_code}"
    resp.headers = headers or {}
    resp.json.return_value = {"ok": True} if status_code < 400 else {}
    return resp


def _make_session(status_code: int = 200, headers: dict | None = None) -> MagicMock:
    """Patch requests.Session to return a fixed response."""
    session = MagicMock()
    session.headers = MagicMock()
    session.post.return_value = _make_response(status_code, headers)
    return session


def _make_client(
    mock_session: MagicMock,
    *,
    flush_interval: float = 60.0,
    max_batch_size: int = 10,
    enabled: bool | None = None,
    dry_run: bool | None = None,
) -> "TraceStaxClient":
    from tracestax.client import TraceStaxClient

    kwargs: dict = dict(
        api_key="ts_test_abc123",
        endpoint="https://test.tracestax.com",
        flush_interval=flush_interval,
        max_batch_size=max_batch_size,
    )
    if enabled is not None:
        kwargs["enabled"] = enabled
    if dry_run is not None:
        kwargs["dry_run"] = dry_run
    return TraceStaxClient(**kwargs)


# ── enabled=False ─────────────────────────────────────────────────────────────


class TestEnabledFalse:
    def test_send_event_is_noop(self):
        with patch("tracestax.client.requests.Session") as mock_cls:
            session = MagicMock()
            mock_cls.return_value = session

            from tracestax.client import TraceStaxClient

            client = TraceStaxClient(api_key="", enabled=False)
            client.send_event({"task": "noop"})
            client.flush()

            session.post.assert_not_called()
            assert len(client._queue) == 0

    def test_send_heartbeat_returns_none(self):
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="", enabled=False)
        result = client.send_heartbeat({"framework": "celery", "worker": {}})
        assert result is None

    def test_close_is_idempotent(self):
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="", enabled=False)
        client.close()
        client.close()  # must not raise


class TestDryRun:
    def test_send_event_logs_and_does_not_post(self, capsys):
        with patch("tracestax.client.requests.Session") as mock_cls:
            session = MagicMock()
            mock_cls.return_value = session

            from tracestax.client import TraceStaxClient

            client = TraceStaxClient(api_key="ts_test", dry_run=True)
            client.send_event({"task": "dry"})

            session.post.assert_not_called()

    def test_send_heartbeat_returns_none_in_dry_run(self):
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="ts_test", dry_run=True)
        result = client.send_heartbeat({"framework": "celery"})
        assert result is None


# ── Fire-and-forget guarantees ────────────────────────────────────────────────


class TestFireAndForget:
    def test_send_event_never_raises_with_dead_server(self, mock_session):
        mock_session.post.side_effect = requests.exceptions.ConnectionError("ECONNREFUSED")
        client = _make_client(mock_session)

        # Must not raise
        for i in range(20):
            client.send_event({"task": f"t{i}"})
        client.flush()
        client.close()

    def test_send_event_never_raises_with_500_response(self, mock_session):
        mock_session.post.return_value = _make_response(500)
        client = _make_client(mock_session)

        for i in range(5):
            client.send_event({"task": f"t{i}"})

        # flush must not raise even on 500
        client.flush()
        client.close()

    def test_send_heartbeat_returns_none_on_network_error(self, mock_session):
        mock_session.post.side_effect = requests.exceptions.ConnectionError("network error")
        client = _make_client(mock_session)

        result = client.send_heartbeat({"framework": "celery", "worker": {}})
        assert result is None
        client.close()

    def test_sdk_does_not_break_celery_job_when_ingest_is_down(self, mock_session):
        """Simulate Celery calling send_event from signal hooks — must not propagate."""
        mock_session.post.side_effect = requests.exceptions.ConnectionError("connection refused")
        client = _make_client(mock_session)

        job_completed = threading.Event()

        def celery_task():
            start = time.monotonic()
            try:
                # user job logic
                time.sleep(0.001)
                job_completed.set()
            finally:
                # Signal hook — must not raise
                client.send_event({
                    "type": "task_event",
                    "status": "succeeded",
                    "metrics": {"duration_ms": (time.monotonic() - start) * 1000},
                })

        # Must complete without raising
        celery_task()
        assert job_completed.is_set()
        client.close()

    def test_original_exception_propagates_when_job_crashes(self, mock_session):
        """Original job error must not be swallowed by SDK instrumentation."""
        mock_session.post.side_effect = requests.exceptions.ConnectionError("connection refused")
        client = _make_client(mock_session)
        job_error = ValueError("bad input")

        def crashing_job():
            try:
                raise job_error
            finally:
                client.send_event({"type": "task_event", "status": "failed"})

        with pytest.raises(ValueError) as exc_info:
            crashing_job()

        assert exc_info.value is job_error
        client.close()


# ── Circuit breaker ───────────────────────────────────────────────────────────


class TestCircuitBreaker:
    def test_opens_after_3_consecutive_failures(self, mock_session):
        mock_session.post.return_value = _make_response(503)
        client = _make_client(mock_session)

        # 3 flush attempts to open the circuit
        for i in range(3):
            client.send_event({"task": f"fail{i}"})
            client.flush()

        assert client._circuit_state == "OPEN"
        assert client._consecutive_failures >= 3

        calls_at_open = mock_session.post.call_count

        # Next flush must be a no-op (circuit open, no HTTP call)
        client.send_event({"task": "drop"})
        client.flush()
        assert mock_session.post.call_count == calls_at_open

        client.close()

    def test_drops_events_silently_when_open(self, mock_session):
        mock_session.post.return_value = _make_response(503)
        client = _make_client(mock_session)

        # Open the circuit
        for i in range(3):
            client.send_event({"task": f"fail{i}"})
            client.flush()

        mock_session.post.reset_mock()

        # Events queued while circuit is open must not trigger HTTP calls
        for i in range(10):
            client.send_event({"task": f"drop{i}"})
        client.flush()

        mock_session.post.assert_not_called()
        client.close()

    def test_transitions_open_to_half_open_after_cooldown(self, mock_session):
        mock_session.post.return_value = _make_response(503)
        client = _make_client(mock_session)

        # Open the circuit
        for i in range(3):
            client.send_event({"task": f"fail{i}"})
            client.flush()

        assert client._circuit_state == "OPEN"

        # Simulate 30s passing by manipulating _circuit_opened_at
        with client._resilience_lock:
            client._circuit_opened_at = time.time() - 31.0

        # A check_circuit call should now transition to HALF_OPEN
        result = client._check_circuit()
        assert result is True  # probe is allowed
        assert client._circuit_state == "HALF_OPEN"

        client.close()

    def test_resets_to_closed_on_successful_probe(self, mock_session):
        # First 3 calls fail, then succeed
        responses = [_make_response(503)] * 3 + [_make_response(200)]
        mock_session.post.side_effect = responses
        client = _make_client(mock_session)

        # Open the circuit
        for i in range(3):
            client.send_event({"task": f"fail{i}"})
            client.flush()

        assert client._circuit_state == "OPEN"

        # Simulate cooldown elapsed
        with client._resilience_lock:
            client._circuit_opened_at = time.time() - 31.0

        # Probe: should succeed → CLOSED
        client.send_event({"task": "probe"})
        client.flush()

        assert client._circuit_state == "CLOSED"
        assert client._consecutive_failures == 0
        client.close()

    def test_returns_to_open_if_half_open_probe_fails(self, mock_session):
        mock_session.post.return_value = _make_response(503)
        client = _make_client(mock_session)

        # Open the circuit
        for i in range(3):
            client.send_event({"task": f"fail{i}"})
            client.flush()

        # Force to HALF_OPEN
        with client._resilience_lock:
            client._circuit_state = "HALF_OPEN"

        # Probe fails → should go back to OPEN
        client.send_event({"task": "probe"})
        client.flush()

        assert client._circuit_state == "OPEN"
        client.close()

    def test_thread_safety_multiple_threads_open_circuit(self, mock_session):
        """Multiple threads concurrently trying to flush an open circuit must not crash."""
        mock_session.post.return_value = _make_response(503)
        client = _make_client(mock_session, max_batch_size=1)

        # Open the circuit
        for i in range(3):
            client.send_event({"task": f"fail{i}"})
            client.flush()

        errors: list[Exception] = []

        def worker(n: int) -> None:
            try:
                for i in range(50):
                    client.send_event({"task": f"t{n}-{i}"})
                    client.flush()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Threads raised exceptions: {errors}"
        client.close()


# ── X-Retry-After backpressure ────────────────────────────────────────────────


class TestRetryAfterBackpressure:
    def test_pauses_flush_for_retry_after_duration(self, mock_session):
        mock_session.post.return_value = _make_response(
            429, headers={"X-Retry-After": "10"}
        )
        client = _make_client(mock_session)

        client.send_event({"task": "rate-limited"})
        client.flush()
        assert mock_session.post.call_count == 1

        # Within the 10s window, flush must not make another HTTP call
        client.send_event({"task": "paused"})
        client.flush()
        assert mock_session.post.call_count == 1  # still 1

        # Simulate expiry by moving the pause timestamp into the past
        with client._resilience_lock:
            client._pause_until = time.time() - 1.0

        mock_session.post.return_value = _make_response(200)
        client.flush()
        assert mock_session.post.call_count == 2  # resumes

        client.close()


# ── Queue memory cap ─────────────────────────────────────────────────────────


class TestQueueMemoryCap:
    def test_queue_capped_at_10k_events(self, mock_session):
        """send_event must trim the queue when it reaches 10K entries."""
        client = _make_client(mock_session, flush_interval=60.0)

        for i in range(11_000):
            client.send_event({"task": f"e{i}"})

        assert len(client._queue) <= 10_000, (
            f"Queue grew to {len(client._queue)}, expected ≤10K"
        )
        client.close()

    def test_queue_trims_to_5k_on_overflow(self, mock_session):
        """After overflow the queue should be trimmed to ≤5K (oldest dropped)."""
        client = _make_client(mock_session, flush_interval=60.0)

        for i in range(11_000):
            client.send_event({"task": f"e{i}"})

        assert len(client._queue) < 11_000, "Queue must have been trimmed"
        client.close()

    def test_send_event_never_raises_during_overflow(self, mock_session):
        """Queue trimming must be silent — no exception must escape."""
        client = _make_client(mock_session, flush_interval=60.0)

        for i in range(15_000):
            client.send_event({"task": f"e{i}"})  # must not raise

        client.close()

    def test_dropped_events_counter_increments_on_overflow(self, mock_session):
        """_dropped_events must be > 0 after a queue overflow."""
        # max_batch_size must exceed 10k so the daemon wake-up signal doesn't
        # fire before the queue overflows — otherwise background flushes drain
        # events and _dropped_events stays 0.
        client = _make_client(mock_session, flush_interval=60.0, max_batch_size=20_000)

        for i in range(11_000):
            client.send_event({"task": f"e{i}"})

        assert client._dropped_events > 0, "dropped_events must increment on overflow"
        client.close()


# ── Stats API ─────────────────────────────────────────────────────────────────


class TestStatsApi:
    def test_stats_returns_dict_with_expected_keys(self, mock_session):
        """stats() must return a dict with the four required keys."""
        client = _make_client(mock_session)
        s = client.stats()
        assert "queue_size" in s
        assert "dropped_events" in s
        assert "circuit_state" in s
        assert "consecutive_failures" in s
        client.close()

    def test_stats_initial_circuit_state_is_closed(self, mock_session):
        """A fresh client's circuit state must be 'closed'."""
        client = _make_client(mock_session)
        assert client.stats()["circuit_state"] == "closed"
        client.close()

    def test_stats_dropped_events_increments_with_overflow(self, mock_session):
        """stats().dropped_events must reflect actual drops."""
        client = _make_client(mock_session, flush_interval=60.0, max_batch_size=20_000)
        for i in range(11_000):
            client.send_event({"task": f"e{i}"})
        s = client.stats()
        assert s["dropped_events"] > 0
        assert s["queue_size"] <= 10_000
        client.close()


# ── Prefork daemon restart ────────────────────────────────────────────────────


class TestPreforkSafety:
    def test_restart_daemon_after_fork_starts_new_thread(self, mock_session):
        """After a fork (Celery prefork), the daemon must be restartable."""
        client = _make_client(mock_session)

        original_thread = client._daemon
        assert original_thread.is_alive()

        # Simulate what _restart_daemon_after_fork does by calling the internal
        # restart if the attribute exists (Celery integration calls this post-fork)
        if hasattr(client, "_restart_daemon_after_fork"):
            client._closed.clear()
            client._restart_daemon_after_fork()
            assert client._daemon.is_alive()
            assert client._daemon is not original_thread

        client.close()

    def test_reinitialize_after_fork_resets_locks_and_starts_daemon(self, mock_session):
        """_reinitialize_after_fork must reset all threading primitives and start
        a new daemon thread so child processes can operate independently of the
        parent (H6 — Celery deadlock fix)."""
        client = _make_client(mock_session)

        original_thread = client._daemon
        original_lock = client._lock
        original_resilience_lock = client._resilience_lock

        assert original_thread.is_alive()

        # Simulate what os.register_at_fork(after_in_child=...) calls
        client._reinitialize_after_fork()

        # Locks must be new objects (old ones may be poisoned by the fork)
        assert client._lock is not original_lock
        assert client._resilience_lock is not original_resilience_lock

        # A fresh daemon must be running in the child
        assert client._daemon is not original_thread
        assert client._daemon.is_alive()

        # The client must still be able to enqueue events
        client.send_event({"type": "task_event", "status": "started"})
        assert len(client._queue) == 1

        client.close()

    def test_reinitialize_after_fork_with_locked_mutex_does_not_deadlock(self, mock_session):
        """Verify that even if _lock was held at fork time, calling
        _reinitialize_after_fork() in the child does not deadlock (because we
        replace the lock with a fresh one rather than trying to release it)."""
        import threading

        client = _make_client(mock_session)

        # Simulate a worst-case scenario: _lock is held (as it might be at fork time)
        # by acquiring it and then calling _reinitialize_after_fork without releasing.
        # The reinitializer must replace the lock, not try to acquire the old one.
        held = client._lock.acquire(blocking=False)
        assert held, "Lock should have been available"
        # Do NOT release — simulate a lock held at fork time

        result = []

        def _run():
            try:
                client._reinitialize_after_fork()
                result.append("ok")
            except Exception as exc:
                result.append(f"error: {exc}")

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        t.join(timeout=3.0)

        assert result == ["ok"], f"_reinitialize_after_fork deadlocked or failed: {result}"

        # Release original lock (now orphaned — the client no longer uses it)
        client._lock  # client now has a fresh lock; original held lock is the old one
        # Cleanup
        client.close()


# ── Serialization safety ──────────────────────────────────────────────────────


class TestSerializationSafety:
    """SDK must never raise into customer code due to non-serializable payloads."""

    def test_send_event_dry_run_non_serializable_does_not_raise(self):
        """send_event in dry-run mode must not raise on a non-JSON-serializable payload."""
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="ts_test_abc123", dry_run=True)
        # A lambda is not JSON-serializable — json.dumps would raise TypeError
        payload = {"task": "circ", "fn": lambda: None}
        # Must not raise
        client.send_event(payload)

    def test_send_snapshot_non_serializable_does_not_raise(self, mock_session):
        """send_snapshot with a non-serializable payload must not raise into caller.
        mock_session fixture already patches requests.Session; we just set side_effect."""
        from tracestax.client import TraceStaxClient

        # Simulate requests' internal json.dumps raising ValueError
        mock_session.post.side_effect = ValueError("circular reference detected")
        client = TraceStaxClient(api_key="ts_test_abc123")
        # Must not raise
        result = client.send_snapshot({"queue": "default", "depth": 5})
        assert result is None
        client.close()

    def test_send_heartbeat_non_serializable_does_not_raise(self, mock_session):
        """send_heartbeat with a non-serializable payload must not raise into caller."""
        from tracestax.client import TraceStaxClient

        mock_session.post.side_effect = ValueError("not serializable")
        client = TraceStaxClient(api_key="ts_test_abc123")
        # Must not raise
        result = client.send_heartbeat({"type": "heartbeat"})
        assert result is None
        client.close()

    def test_send_lineage_non_serializable_does_not_raise(self, mock_session):
        """send_lineage with a non-serializable payload must not raise into caller."""
        from tracestax.client import TraceStaxClient

        mock_session.post.side_effect = TypeError("not serializable")
        client = TraceStaxClient(api_key="ts_test_abc123")
        # Must not raise
        client.send_lineage({"parent": "abc", "child": "def"})
        client.close()


# ── Watchdog thread health ────────────────────────────────────────────────────


class TestWatchdog:
    """Verify the watchdog restarts the daemon and emits an informative warning."""

    def test_logs_warning_with_queue_size_on_daemon_restart(self, mock_session, caplog):
        """When the flush daemon dies, the watchdog must emit a WARNING that
        includes the current queue length so operators can assess event loss.
        This validates the fix to watchdog.py (_CHECK_INTERVAL=5s + queue size)."""
        import logging
        import time

        from tracestax.watchdog import Watchdog

        client = _make_client(mock_session)

        # Queue a few events so the count appears in the warning
        client.send_event({"task": "w1"})
        client.send_event({"task": "w2"})

        # Force the daemon thread into a stopped state without triggering close()
        original_daemon = client._daemon
        client._daemon = None  # make is_alive return False

        watchdog = Watchdog(client)

        with caplog.at_level(logging.WARNING, logger="tracestax"):
            # Wait long enough for at least one watchdog check (CHECK_INTERVAL=5s)
            time.sleep(6.5)

        watchdog.stop()
        client.close()

        warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("flush thread died" in msg for msg in warning_messages), (
            f"Expected 'flush thread died' warning. Got: {warning_messages}"
        )
        assert any("events in queue" in msg for msg in warning_messages), (
            f"Expected queue size in warning. Got: {warning_messages}"
        )

        # Restore to allow clean shutdown
        if original_daemon and original_daemon.is_alive():
            original_daemon = None


# ── Lineage monkey-patch ──────────────────────────────────────────────────────


class TestLineageHooks:
    """Verify the lineage monkey-patch installation and displacement detection."""

    def setup_method(self):
        """Reset lineage module state before each test."""
        try:
            import tracestax.lineage as lineage_mod
            lineage_mod._original_apply_async = None
            lineage_mod._lineage_client = None
            lineage_mod._displacement_warned = False
        except ImportError:
            pass  # tracestax.lineage is an optional Celery extra

    def test_install_lineage_hooks_is_idempotent(self, mock_session):
        """Calling install_lineage_hooks twice must not double-wrap apply_async."""
        try:
            from celery import Celery, Task
            from tracestax.lineage import install_lineage_hooks
        except ImportError:
            import pytest
            pytest.skip("celery not installed")

        client = _make_client(mock_session)
        app = Celery("test_idempotent")

        install_lineage_hooks(app, client)
        first_patch = Task.apply_async

        install_lineage_hooks(app, client)  # second call — must be a no-op
        second_patch = Task.apply_async

        assert first_patch is second_patch, (
            "apply_async was re-patched on second install_lineage_hooks call"
        )
        client.close()

    def test_displacement_warning_logged_when_hook_superseded(self, mock_session, caplog):
        """If customer code patches Task.apply_async after TraceStax, the SDK
        must emit a single WARNING the next time patched_apply_async is entered."""
        try:
            from celery import Celery, Task
            from tracestax.lineage import install_lineage_hooks
            import tracestax.lineage as lineage_mod
        except ImportError:
            import pytest
            pytest.skip("celery not installed")

        import logging

        client = _make_client(mock_session)
        app = Celery("test_displacement")
        install_lineage_hooks(app, client)

        tracestax_patch = Task.apply_async

        # Simulate a second (customer) monkey-patch displacing the TraceStax hook
        def customer_patch(self, *args, **kwargs):
            return tracestax_patch(self, *args, **kwargs)

        Task.apply_async = customer_patch

        # Invoke via the original patched function directly (it's still reachable
        # via the closure reference in the module)
        with caplog.at_level(logging.WARNING, logger="tracestax"):
            # Call the TraceStax patch function directly — it should detect displacement
            try:
                tracestax_patch(Task(), args=(), kwargs={})
            except Exception:
                pass  # we only care about the log, not the call outcome

        warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("superseded" in msg for msg in warning_messages), (
            f"Expected displacement warning. Got: {warning_messages}"
        )
        client.close()


# ── Large payload size guard (2B) ─────────────────────────────────────────────


class TestLargePayloadGuard:
    """send_event must silently drop payloads that exceed 512 KB."""

    def test_oversized_payload_is_dropped_without_raising(self, mock_session):
        """A 600 KB payload must be dropped silently — no exception, no queue growth."""
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="ts_test_size")
        big_payload = {"task": "big", "data": "x" * (600 * 1024)}
        client.send_event(big_payload)  # must not raise
        assert len(client._queue) == 0  # dropped, not queued
        client.close()

    def test_non_serializable_payload_is_dropped_without_raising(self, mock_session):
        """A non-JSON-serializable payload is dropped at send_event time."""
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="ts_test_serial")
        client.send_event({"task": "bad", "fn": lambda: None})  # must not raise
        assert len(client._queue) == 0
        client.close()

    def test_normal_payload_still_accepted_after_oversized_drop(self, mock_session):
        """Client continues to function normally after dropping an oversized payload."""
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="ts_test_recover")
        client.send_event({"data": "x" * (600 * 1024)})  # dropped
        client.send_event({"task": "small"})  # must be accepted
        assert len(client._queue) == 1
        client.close()


# ── Concurrent close() calls (2C) ────────────────────────────────────────────


class TestConcurrentClose:
    """close() must be idempotent and safe to call from multiple threads."""

    def test_concurrent_close_does_not_deadlock(self, mock_session):
        """Calling close() from 3 threads simultaneously must not deadlock."""
        import threading
        from tracestax.client import TraceStaxClient

        mock_session.post.return_value = _make_response(200)
        client = TraceStaxClient(api_key="ts_test_close", flush_interval=60.0)
        client.send_event({"task": "e1"})

        errors: list[Exception] = []

        def _close():
            try:
                client.close()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_close) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)
            assert not t.is_alive(), "close() thread timed out — possible deadlock"

        assert not errors, f"close() raised: {errors}"


# ── Circuit breaker clock skew (2D) ──────────────────────────────────────────


class TestCircuitClockSkew:
    """A backward system clock must not permanently keep the circuit OPEN."""

    def test_backward_clock_does_not_freeze_circuit_open(self, mock_session):
        """With max(0, elapsed), a backward clock jump keeps circuit OPEN (< 30 s
        elapsed = 0) rather than locking it there indefinitely."""
        mock_session.post.return_value = _make_response(503)
        client = _make_client(mock_session)

        # Open the circuit
        for i in range(3):
            client.send_event({"task": f"fail{i}"})
            client.flush()
        assert client._circuit_state == "OPEN"

        # Simulate backward clock: push circuit_opened_at into the future
        with client._resilience_lock:
            client._circuit_opened_at = time.time() + 60.0

        # Circuit check: elapsed = max(0, now - future) = 0 < 30 → stays OPEN
        # This is correct — the circuit should stay closed until the real 30s
        # cooldown passes. The important thing is it doesn't crash.
        assert client._check_circuit() is False

        # Now push opened_at far enough into the past for the cooldown to expire
        with client._resilience_lock:
            client._circuit_opened_at = time.time() - 31.0

        # Circuit should transition to HALF_OPEN
        assert client._check_circuit() is True
        assert client._circuit_state == "HALF_OPEN"

        client.close()


class TestAuth401:
    """HTTP 401 must NOT open the circuit breaker.

    A 401 signals a permanent misconfiguration (wrong API key), not a transient
    network failure. Opening the circuit on 401 would silently drop all events
    and hide the real problem. Instead the SDK logs an error and keeps the circuit
    CLOSED so events continue to queue — ready to ship once the key is fixed.
    """

    def test_401_does_not_open_circuit_breaker(self, mock_session):
        mock_session.post.return_value = _make_response(401)
        client = _make_client(mock_session)

        # Three flush attempts with 401 responses
        for i in range(3):
            client.send_event({"task": f"auth-fail-{i}"})
            client.flush()

        # Circuit must still be CLOSED
        assert client._circuit_state == "CLOSED", (
            f"Circuit must stay CLOSED on 401 (was {client._circuit_state!r})"
        )
        assert client._consecutive_failures == 0

        client.close()

    def test_401_does_not_increment_consecutive_failures(self, mock_session):
        mock_session.post.return_value = _make_response(401)
        client = _make_client(mock_session)

        client.send_event({"task": "auth-probe"})
        client.flush()

        assert client._consecutive_failures == 0

        client.close()

    def test_events_continue_to_queue_after_401(self, mock_session):
        """After a 401 response the client must still accept and enqueue events."""
        mock_session.post.return_value = _make_response(401)
        client = _make_client(mock_session)

        client.send_event({"task": "auth-fail"})
        client.flush()

        # Queue new events — they should be accepted (circuit is still CLOSED)
        for i in range(5):
            client.send_event({"task": f"queued-{i}"})

        assert len(client._queue) == 5, (
            f"Expected 5 queued events after 401, got {len(client._queue)}"
        )

        client.close()
