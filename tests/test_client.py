"""Unit tests for TraceStaxClient: queue batching, flush on close, thread safety."""

from __future__ import annotations

import threading

import pytest


class TestTraceStaxClientInit:
    def test_requires_api_key(self, mock_session):
        from tracestax.client import TraceStaxClient

        with pytest.raises(ValueError, match="api_key is required"):
            TraceStaxClient(api_key="", endpoint="https://test.tracestax.com")

    def test_sets_auth_header(self, mock_session):
        from tracestax.client import TraceStaxClient

        c = TraceStaxClient(api_key="ts_test_xyz", endpoint="https://test.tracestax.com")
        mock_session.headers.update.assert_called_once()
        headers = mock_session.headers.update.call_args[0][0]
        assert headers["Authorization"] == "Bearer ts_test_xyz"
        c.close()

    def test_strips_trailing_slash(self, mock_session):
        from tracestax.client import TraceStaxClient

        c = TraceStaxClient(api_key="ts_test_xyz", endpoint="https://test.tracestax.com/")
        assert c._endpoint == "https://test.tracestax.com"
        c.close()


class TestQueueBatching:
    def test_send_event_queues(self, client, mock_session):
        client.send_event({"task": "add", "status": "succeeded"})
        client.send_event({"task": "mul", "status": "failed"})

        assert len(client._queue) == 2
        # No HTTP call yet — flush interval is 60s
        mock_session.post.assert_not_called()

    def test_flush_sends_batch(self, client, mock_session):
        for i in range(5):
            client.send_event({"task": f"t{i}"})

        client.flush()

        mock_session.post.assert_called_once()
        url, kwargs = mock_session.post.call_args[0][0], mock_session.post.call_args[1]
        assert url == "https://test.tracestax.com/v1/ingest"
        body = kwargs["json"]
        assert len(body["events"]) == 5

    def test_flush_respects_max_batch_size(self, client, mock_session):
        # max_batch_size is 10
        for i in range(25):
            client.send_event({"task": f"t{i}"})

        client.flush()

        # Should have been sent in 3 batches: 10, 10, 5
        assert mock_session.post.call_count == 3
        calls = mock_session.post.call_args_list
        assert len(calls[0][1]["json"]["events"]) == 10
        assert len(calls[1][1]["json"]["events"]) == 10
        assert len(calls[2][1]["json"]["events"]) == 5

    def test_flush_empties_queue(self, client, mock_session):
        client.send_event({"task": "a"})
        client.flush()
        assert len(client._queue) == 0


class TestCloseAndShutdown:
    def test_close_flushes_remaining(self, client, mock_session):
        client.send_event({"task": "final"})
        client.close()

        mock_session.post.assert_called()
        body = mock_session.post.call_args[1]["json"]
        assert body["events"][0]["task"] == "final"

    def test_close_is_idempotent(self, client, mock_session):
        client.close()
        client.close()  # should not raise

    def test_send_after_close_is_dropped(self, client, mock_session):
        client.close()
        client.send_event({"task": "dropped"})
        assert len(client._queue) == 0


class TestThreadSafety:
    def test_concurrent_sends(self, client, mock_session):
        errors = []

        def _send(n):
            try:
                for i in range(100):
                    client.send_event({"task": f"thread{n}-{i}"})
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_send, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        # All 400 events should be in the queue (since flush interval is long)
        # Some may have been flushed by batch-size trigger — check total
        client.flush()
        total = sum(
            len(c[1]["json"]["events"])
            for c in mock_session.post.call_args_list
        )
        assert total == 400


class TestDirectSendMethods:
    def test_send_heartbeat(self, client, mock_session):
        client.send_heartbeat({"framework": "celery", "worker": {}})
        mock_session.post.assert_called_once()
        url = mock_session.post.call_args[0][0]
        assert url == "https://test.tracestax.com/v1/heartbeat"

    def test_send_snapshot(self, client, mock_session):
        client.send_snapshot({"queues": []})
        url = mock_session.post.call_args[0][0]
        assert url == "https://test.tracestax.com/v1/snapshot"

    def test_send_lineage(self, client, mock_session):
        client.send_lineage({"parent_id": "abc", "children": []})
        url = mock_session.post.call_args[0][0]
        assert url == "https://test.tracestax.com/v1/lineage"


class TestDaemonThread:
    def test_daemon_is_alive(self, client):
        assert client.is_alive

    def test_repr(self, client):
        r = repr(client)
        assert "TraceStaxClient" in r
        assert "test.tracestax.com" in r


class TestCircuitBreaker:
    """Basic circuit breaker contract: open after 3 failures, drop silently."""

    def test_opens_after_3_failures(self, client, mock_session):
        mock_session.post.return_value = type(
            "R",
            (),
            {
                "status_code": 503,
                "text": "down",
                "headers": {},
                "raw": type("Raw", (), {"read": lambda self, n: b"service unavailable"})(),
                "close": lambda self: None,
            },
        )()

        for i in range(3):
            client.send_event({"task": f"fail{i}"})
            client.flush()

        assert client._circuit_state == "OPEN"
        assert client._consecutive_failures >= 3

    def test_drops_events_silently_when_open(self, client, mock_session):
        mock_session.post.return_value = type(
            "R",
            (),
            {
                "status_code": 503,
                "text": "down",
                "headers": {},
                "raw": type("Raw", (), {"read": lambda self, n: b"service unavailable"})(),
                "close": lambda self: None,
            },
        )()

        for i in range(3):
            client.send_event({"task": f"fail{i}"})
            client.flush()

        mock_session.post.reset_mock()

        for i in range(5):
            client.send_event({"task": f"drop{i}"})
        client.flush()

        mock_session.post.assert_not_called()

    def test_does_not_raise_when_open(self, client, mock_session):
        mock_session.post.return_value = type(
            "R",
            (),
            {
                "status_code": 503,
                "text": "down",
                "headers": {},
                "raw": type("Raw", (), {"read": lambda self, n: b"service unavailable"})(),
                "close": lambda self: None,
            },
        )()

        for i in range(3):
            client.send_event({"task": f"fail{i}"})
            client.flush()

        # Must not raise
        for i in range(5):
            client.send_event({"task": f"drop{i}"})
        client.flush()
