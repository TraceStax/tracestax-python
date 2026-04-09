"""Unit tests for Celery signal hooks: signal capture, payload format."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest


@pytest.fixture()
def mock_client():
    client = MagicMock()
    client.send_event = MagicMock()
    client.send_heartbeat = MagicMock()
    client.close = MagicMock()
    return client


class TestTaskPrerun:
    def test_records_start_time(self):
        from tracestax.celery import _on_task_prerun, _task_start_times

        _on_task_prerun(sender=None, task_id="task-001")

        assert "task-001" in _task_start_times
        assert isinstance(_task_start_times["task-001"], float)

        # cleanup
        _task_start_times.pop("task-001", None)


class TestTaskPostrun:
    def test_success_payload(self, mock_client):
        from tracestax.celery import _make_postrun_handler, _on_task_prerun

        handler = _make_postrun_handler(mock_client)

        sender = MagicMock()
        sender.name = "myapp.tasks.add"
        sender.request.delivery_info = {"routing_key": "default"}
        sender.request.retries = 0
        sender.request.headers = {}

        _on_task_prerun(sender=sender, task_id="task-002")
        time.sleep(0.01)  # simulate some work
        handler(sender=sender, task_id="task-002", state="SUCCESS")

        mock_client.send_event.assert_called_once()
        payload = mock_client.send_event.call_args[0][0]

        assert payload["framework"] == "celery"
        assert payload["language"] == "python"
        assert payload["type"] == "task_event"
        assert payload["status"] == "succeeded"
        assert payload["task"]["name"] == "myapp.tasks.add"
        assert payload["task"]["id"] == "task-002"
        assert payload["task"]["queue"] == "default"
        assert payload["task"]["attempt"] == 1
        assert payload["metrics"]["duration_ms"] > 0

    def test_failure_payload(self, mock_client):
        from tracestax.celery import _make_postrun_handler, _on_task_prerun

        handler = _make_postrun_handler(mock_client)
        sender = MagicMock()
        sender.name = "myapp.tasks.divide"
        sender.request.delivery_info = {"routing_key": "math"}
        sender.request.retries = 0
        sender.request.headers = {}

        _on_task_prerun(sender=sender, task_id="task-003")
        handler(sender=sender, task_id="task-003", state="FAILURE")

        payload = mock_client.send_event.call_args[0][0]
        assert payload["status"] == "failed"


class TestTaskFailure:
    def test_includes_error_info(self, mock_client):
        from tracestax.celery import _make_failure_handler, _on_task_prerun

        handler = _make_failure_handler(mock_client)
        sender = MagicMock()
        sender.name = "myapp.tasks.process"
        sender.request.delivery_info = {"routing_key": "default"}
        sender.request.retries = 0
        sender.request.headers = {}

        _on_task_prerun(sender=sender, task_id="task-004")
        exc = ValueError("division by zero")
        handler(sender=sender, task_id="task-004", exception=exc, traceback=None)

        payload = mock_client.send_event.call_args[0][0]
        assert payload["status"] == "failed"
        assert payload["error"]["type"] == "ValueError"
        assert "division by zero" in payload["error"]["message"]


class TestTaskRetry:
    def test_retry_payload(self, mock_client):
        from tracestax.celery import _make_retry_handler, _on_task_prerun

        handler = _make_retry_handler(mock_client)
        sender = MagicMock()
        sender.name = "myapp.tasks.flaky"
        sender.request.delivery_info = {"routing_key": "default"}
        sender.request.retries = 2
        sender.request.headers = {}

        request = MagicMock()
        request.id = "task-005"

        _on_task_prerun(sender=sender, task_id="task-005")
        handler(sender=sender, request=request, reason=ConnectionError("timeout"))

        payload = mock_client.send_event.call_args[0][0]
        assert payload["status"] == "retried"
        assert payload["error"]["type"] == "ConnectionError"


class TestTaskRevoked:
    def test_revoked_payload(self, mock_client):
        from tracestax.celery import _make_revoked_handler

        handler = _make_revoked_handler(mock_client)
        sender = MagicMock()
        sender.name = "myapp.tasks.slow"
        sender.request.delivery_info = {"routing_key": "default"}
        sender.request.retries = 0
        sender.request.headers = {}

        request = MagicMock()
        request.id = "task-006"

        handler(sender=sender, request=request, terminated=True)

        payload = mock_client.send_event.call_args[0][0]
        assert payload["status"] == "failed"
        assert payload["error"]["type"] == "TaskRevoked"


class TestWorkerReady:
    def test_sends_heartbeat(self, mock_client):
        from tracestax.celery import _make_worker_ready_handler

        try:
            from celery import Celery
        except ImportError:
            pytest.skip("celery not installed")

        app = Celery("test")
        handler = _make_worker_ready_handler(mock_client, app)
        handler(sender=None)

        mock_client.send_heartbeat.assert_called_once()
        payload = mock_client.send_heartbeat.call_args[0][0]
        assert payload["framework"] == "celery"
        assert "worker" in payload
        assert "timestamp" in payload


class TestPayloadFormat:
    """Verify payloads conform to the IngestPayload type schema."""

    REQUIRED_TOP_LEVEL = {
        "framework",
        "language",
        "sdk_version",
        "type",
        "worker",
        "task",
        "status",
        "metrics",
    }
    REQUIRED_TASK = {"name", "id", "queue", "attempt"}
    REQUIRED_WORKER = {"key", "hostname", "pid", "concurrency", "queues"}
    REQUIRED_METRICS = {"duration_ms"}

    def test_payload_has_required_fields(self, mock_client):
        from tracestax.celery import _make_postrun_handler, _on_task_prerun

        handler = _make_postrun_handler(mock_client)
        sender = MagicMock()
        sender.name = "check.format"
        sender.request.delivery_info = {"routing_key": "q"}
        sender.request.retries = 0
        sender.request.headers = {}

        _on_task_prerun(sender=sender, task_id="fmt-001")
        handler(sender=sender, task_id="fmt-001", state="SUCCESS")

        payload = mock_client.send_event.call_args[0][0]
        assert self.REQUIRED_TOP_LEVEL.issubset(payload.keys())
        assert self.REQUIRED_TASK.issubset(payload["task"].keys())
        assert self.REQUIRED_WORKER.issubset(payload["worker"].keys())
        assert self.REQUIRED_METRICS.issubset(payload["metrics"].keys())

    def test_valid_status_values(self, mock_client):
        from tracestax.celery import _make_postrun_handler, _on_task_prerun

        handler = _make_postrun_handler(mock_client)
        sender = MagicMock()
        sender.name = "check.status"
        sender.request.delivery_info = {"routing_key": "q"}
        sender.request.retries = 0
        sender.request.headers = {}

        VALID_STATUSES = {"started", "succeeded", "failed", "stalled", "timeout", "retried"}

        _on_task_prerun(sender=sender, task_id="st-001")
        handler(sender=sender, task_id="st-001", state="SUCCESS")
        payload = mock_client.send_event.call_args[0][0]
        assert payload["status"] in VALID_STATUSES
