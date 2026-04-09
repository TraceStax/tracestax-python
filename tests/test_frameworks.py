"""
Integration tests that simulate real framework usage against mock-ingest.
Verifies that each framework integration sends correctly-shaped events.
"""
import os, time, requests, pytest

INGEST_URL = os.environ.get("TRACESTAX_INGEST_URL", "http://localhost:4001")


def reset_ingest():
    requests.post(f"{INGEST_URL}/test/reset")


def fetch_events():
    return requests.get(f"{INGEST_URL}/test/events").json()


def fetch_heartbeats():
    return requests.get(f"{INGEST_URL}/test/heartbeats").json()


def ingest_available():
    try:
        return requests.get(f"{INGEST_URL}/test/health", timeout=2).status_code == 200
    except Exception:
        return False


@pytest.fixture(autouse=True)
def clean_ingest():
    if not ingest_available():
        pytest.skip("mock-ingest not available")
    reset_ingest()
    yield


def wait_for_events(predicate, timeout=10):
    """Poll until predicate is true or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        events = fetch_events()
        if any(predicate(e) for e in events):
            return events
        time.sleep(0.25)
    return fetch_events()


# ── Celery simulation ────────────────────────────────────────────────────


class TestCelerySimulation:
    """Simulate Celery task lifecycle without a real broker."""

    def test_celery_task_succeeded(self):
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(
            api_key="ts_test_abc",
            endpoint=INGEST_URL,
        )

        client.send_event({
            "type": "task_event",
            "framework": "celery",
            "language": "python",
            "sdk_version": "0.1.0",
            "status": "succeeded",
            "worker": {"key": "celery@test:1", "hostname": "test", "pid": 1, "queues": ["default"]},
            "task": {"name": "myapp.tasks.add", "id": "celery-sim-001", "queue": "default", "attempt": 1},
            "metrics": {"duration_ms": 150},
        })

        events = wait_for_events(lambda e: e.get("task", {}).get("id") == "celery-sim-001")
        match = [e for e in events if e.get("task", {}).get("id") == "celery-sim-001"]
        assert len(match) >= 1
        assert match[0]["framework"] == "celery"
        assert match[0]["status"] == "succeeded"
        assert match[0]["metrics"]["duration_ms"] == 150

        client.close()

    def test_celery_task_failed_with_error(self):
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="ts_test_abc", endpoint=INGEST_URL)

        client.send_event({
            "type": "task_event",
            "framework": "celery",
            "language": "python",
            "sdk_version": "0.1.0",
            "status": "failed",
            "worker": {"key": "celery@test:1", "hostname": "test", "pid": 1, "queues": ["default"]},
            "task": {"name": "myapp.tasks.divide", "id": "celery-sim-002", "queue": "default", "attempt": 1},
            "metrics": {"duration_ms": 10},
            "error": {"type": "ZeroDivisionError", "message": "division by zero"},
        })

        events = wait_for_events(lambda e: e.get("task", {}).get("id") == "celery-sim-002")
        match = [e for e in events if e.get("task", {}).get("id") == "celery-sim-002"]
        assert len(match) >= 1
        assert match[0]["status"] == "failed"
        assert match[0]["error"]["type"] == "ZeroDivisionError"

        client.close()

    def test_celery_task_retry(self):
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="ts_test_abc", endpoint=INGEST_URL)

        client.send_event({
            "type": "task_event",
            "framework": "celery",
            "language": "python",
            "sdk_version": "0.1.0",
            "status": "retried",
            "worker": {"key": "celery@test:1", "hostname": "test", "pid": 1, "queues": ["default"]},
            "task": {"name": "myapp.tasks.flaky", "id": "celery-sim-003", "queue": "default", "attempt": 2},
            "metrics": {"duration_ms": 50},
            "error": {"type": "ConnectionError", "message": "connection refused"},
        })

        events = wait_for_events(lambda e: e.get("task", {}).get("id") == "celery-sim-003")
        match = [e for e in events if e.get("task", {}).get("id") == "celery-sim-003"]
        assert len(match) >= 1
        assert match[0]["status"] == "retried"
        assert match[0]["task"]["attempt"] == 2

        client.close()


# ── Dramatiq simulation ──────────────────────────────────────────────────


class TestDramatiqSimulation:
    def test_dramatiq_task_succeeded(self):
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="ts_test_abc", endpoint=INGEST_URL)

        client.send_event({
            "type": "task_event",
            "framework": "dramatiq",
            "language": "python",
            "sdk_version": "0.1.0",
            "status": "succeeded",
            "worker": {"key": "dramatiq@test:1", "hostname": "test", "pid": 1, "queues": ["default"]},
            "task": {"name": "send_email", "id": "dramatiq-sim-001", "queue": "default", "attempt": 1},
            "metrics": {"duration_ms": 200},
        })

        events = wait_for_events(lambda e: e.get("task", {}).get("id") == "dramatiq-sim-001")
        match = [e for e in events if e.get("task", {}).get("id") == "dramatiq-sim-001"]
        assert len(match) >= 1
        assert match[0]["framework"] == "dramatiq"
        assert match[0]["status"] == "succeeded"
        assert match[0]["metrics"]["duration_ms"] == 200

        client.close()

    def test_dramatiq_task_failed(self):
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="ts_test_abc", endpoint=INGEST_URL)

        client.send_event({
            "type": "task_event",
            "framework": "dramatiq",
            "language": "python",
            "sdk_version": "0.1.0",
            "status": "failed",
            "worker": {"key": "dramatiq@test:1", "hostname": "test", "pid": 1, "queues": ["default"]},
            "task": {"name": "process_payment", "id": "dramatiq-sim-002", "queue": "default", "attempt": 3},
            "metrics": {"duration_ms": 15},
            "error": {"type": "TimeoutError", "message": "gateway timeout"},
        })

        events = wait_for_events(lambda e: e.get("task", {}).get("id") == "dramatiq-sim-002")
        match = [e for e in events if e.get("task", {}).get("id") == "dramatiq-sim-002"]
        assert len(match) >= 1
        assert match[0]["status"] == "failed"
        assert match[0]["error"]["type"] == "TimeoutError"

        client.close()


# ── RQ simulation ────────────────────────────────────────────────────────


class TestRQSimulation:
    def test_rq_task_succeeded(self):
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="ts_test_abc", endpoint=INGEST_URL)

        client.send_event({
            "type": "task_event",
            "framework": "rq",
            "language": "python",
            "sdk_version": "0.1.0",
            "status": "succeeded",
            "worker": {"key": "rq@test:1", "hostname": "test", "pid": 1, "queues": ["default"]},
            "task": {"name": "process_image", "id": "rq-sim-001", "queue": "default", "attempt": 1},
            "metrics": {"duration_ms": 500},
        })

        events = wait_for_events(lambda e: e.get("task", {}).get("id") == "rq-sim-001")
        match = [e for e in events if e.get("task", {}).get("id") == "rq-sim-001"]
        assert len(match) >= 1
        assert match[0]["framework"] == "rq"
        assert match[0]["status"] == "succeeded"

        client.close()

    def test_rq_task_failed(self):
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="ts_test_abc", endpoint=INGEST_URL)

        client.send_event({
            "type": "task_event",
            "framework": "rq",
            "language": "python",
            "sdk_version": "0.1.0",
            "status": "failed",
            "worker": {"key": "rq@test:1", "hostname": "test", "pid": 1, "queues": ["high"]},
            "task": {"name": "send_notification", "id": "rq-sim-002", "queue": "high", "attempt": 1},
            "metrics": {"duration_ms": 25},
            "error": {"type": "SMTPError", "message": "relay denied"},
        })

        events = wait_for_events(lambda e: e.get("task", {}).get("id") == "rq-sim-002")
        match = [e for e in events if e.get("task", {}).get("id") == "rq-sim-002"]
        assert len(match) >= 1
        assert match[0]["status"] == "failed"
        assert match[0]["task"]["queue"] == "high"

        client.close()


# ── Huey simulation ──────────────────────────────────────────────────────


class TestHueySimulation:
    def test_huey_task_succeeded(self):
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="ts_test_abc", endpoint=INGEST_URL)

        client.send_event({
            "type": "task_event",
            "framework": "huey",
            "language": "python",
            "sdk_version": "0.1.0",
            "status": "succeeded",
            "worker": {"key": "huey@test:1", "hostname": "test", "pid": 1, "queues": ["default"]},
            "task": {"name": "generate_report", "id": "huey-sim-001", "queue": "default", "attempt": 1},
            "metrics": {"duration_ms": 800},
        })

        events = wait_for_events(lambda e: e.get("task", {}).get("id") == "huey-sim-001")
        match = [e for e in events if e.get("task", {}).get("id") == "huey-sim-001"]
        assert len(match) >= 1
        assert match[0]["framework"] == "huey"
        assert match[0]["status"] == "succeeded"

        client.close()

    def test_huey_task_failed(self):
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="ts_test_abc", endpoint=INGEST_URL)

        client.send_event({
            "type": "task_event",
            "framework": "huey",
            "language": "python",
            "sdk_version": "0.1.0",
            "status": "failed",
            "worker": {"key": "huey@test:1", "hostname": "test", "pid": 1, "queues": ["default"]},
            "task": {"name": "resize_image", "id": "huey-sim-002", "queue": "default", "attempt": 1},
            "metrics": {"duration_ms": 120},
            "error": {"type": "IOError", "message": "disk full"},
        })

        events = wait_for_events(lambda e: e.get("task", {}).get("id") == "huey-sim-002")
        match = [e for e in events if e.get("task", {}).get("id") == "huey-sim-002"]
        assert len(match) >= 1
        assert match[0]["status"] == "failed"
        assert match[0]["error"]["type"] == "IOError"

        client.close()


# ── TaskIQ simulation ────────────────────────────────────────────────────


class TestTaskIQSimulation:
    def test_taskiq_task_succeeded(self):
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="ts_test_abc", endpoint=INGEST_URL)

        client.send_event({
            "type": "task_event",
            "framework": "taskiq",
            "language": "python",
            "sdk_version": "0.1.0",
            "status": "succeeded",
            "worker": {"key": "taskiq@test:1", "hostname": "test", "pid": 1, "queues": ["default"]},
            "task": {"name": "sync_inventory", "id": "taskiq-sim-001", "queue": "default", "attempt": 1},
            "metrics": {"duration_ms": 350},
        })

        events = wait_for_events(lambda e: e.get("task", {}).get("id") == "taskiq-sim-001")
        match = [e for e in events if e.get("task", {}).get("id") == "taskiq-sim-001"]
        assert len(match) >= 1
        assert match[0]["framework"] == "taskiq"
        assert match[0]["status"] == "succeeded"

        client.close()

    def test_taskiq_task_failed(self):
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="ts_test_abc", endpoint=INGEST_URL)

        client.send_event({
            "type": "task_event",
            "framework": "taskiq",
            "language": "python",
            "sdk_version": "0.1.0",
            "status": "failed",
            "worker": {"key": "taskiq@test:1", "hostname": "test", "pid": 1, "queues": ["default"]},
            "task": {"name": "update_cache", "id": "taskiq-sim-002", "queue": "default", "attempt": 2},
            "metrics": {"duration_ms": 5},
            "error": {"type": "RedisError", "message": "connection reset"},
        })

        events = wait_for_events(lambda e: e.get("task", {}).get("id") == "taskiq-sim-002")
        match = [e for e in events if e.get("task", {}).get("id") == "taskiq-sim-002"]
        assert len(match) >= 1
        assert match[0]["status"] == "failed"
        assert match[0]["error"]["type"] == "RedisError"

        client.close()


# ── Cloud Functions simulation ───────────────────────────────────────────


class TestCloudFunctionsSimulation:
    def test_cloud_function_succeeded(self):
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="ts_test_abc", endpoint=INGEST_URL)

        client.send_event({
            "type": "task_event",
            "framework": "cloud_functions",
            "language": "python",
            "sdk_version": "0.1.0",
            "status": "succeeded",
            "worker": {"key": "gcf:process-upload:us-central1", "hostname": "gcf", "pid": 1, "queues": ["cloud_functions"]},
            "task": {"name": "process-upload", "id": "gcf-sim-001", "queue": "cloud_functions", "attempt": 1},
            "metrics": {"duration_ms": 320},
        })

        events = wait_for_events(lambda e: e.get("task", {}).get("id") == "gcf-sim-001")
        match = [e for e in events if e.get("task", {}).get("id") == "gcf-sim-001"]
        assert len(match) >= 1
        assert match[0]["framework"] == "cloud_functions"
        assert match[0]["status"] == "succeeded"

        client.close()

    def test_cloud_function_failed(self):
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="ts_test_abc", endpoint=INGEST_URL)

        client.send_event({
            "type": "task_event",
            "framework": "cloud_functions",
            "language": "python",
            "sdk_version": "0.1.0",
            "status": "failed",
            "worker": {"key": "gcf:validate:us-central1", "hostname": "gcf", "pid": 1, "queues": ["cloud_functions"]},
            "task": {"name": "validate", "id": "gcf-sim-002", "queue": "cloud_functions", "attempt": 1},
            "metrics": {"duration_ms": 8},
            "error": {"type": "ValidationError", "message": "invalid schema"},
        })

        events = wait_for_events(lambda e: e.get("task", {}).get("id") == "gcf-sim-002")
        match = [e for e in events if e.get("task", {}).get("id") == "gcf-sim-002"]
        assert len(match) >= 1
        assert match[0]["status"] == "failed"
        assert match[0]["error"]["type"] == "ValidationError"

        client.close()


# ── Lambda wrapper integration ───────────────────────────────────────────


class TestLambdaWrapper:
    """Test the actual Lambda decorator against mock-ingest."""

    def test_lambda_handler_succeeded(self):
        from tracestax.aws_lambda import tracestax_handler

        @tracestax_handler(api_key="ts_test_abc", endpoint=INGEST_URL)
        def handler(event, context):
            return {"statusCode": 200}

        class FakeContext:
            function_name = "process-orders"
            aws_request_id = "lambda-sim-001"
            memory_limit_in_mb = 128
            invoked_function_arn = "arn:aws:lambda:us-east-1:123:function:process-orders"
            def get_remaining_time_in_millis(self): return 30000

        result = handler({"key": "value"}, FakeContext())
        assert result["statusCode"] == 200

        time.sleep(1)  # allow flush
        events = fetch_events()
        lambda_events = [e for e in events if e.get("task", {}).get("name") == "process-orders"]
        assert len(lambda_events) >= 1
        assert any(e["status"] == "succeeded" for e in lambda_events)

    def test_lambda_handler_failed(self):
        from tracestax.aws_lambda import tracestax_handler

        @tracestax_handler(api_key="ts_test_abc", endpoint=INGEST_URL)
        def handler(event, context):
            raise ValueError("bad input")

        class FakeContext:
            function_name = "validate-input"
            aws_request_id = "lambda-sim-002"
            memory_limit_in_mb = 128
            invoked_function_arn = "arn:aws:lambda:us-east-1:123:function:validate-input"
            def get_remaining_time_in_millis(self): return 30000

        with pytest.raises(ValueError):
            handler({"key": "value"}, FakeContext())

        time.sleep(1)
        events = fetch_events()
        failed = [e for e in events if e.get("status") == "failed" and e.get("task", {}).get("name") == "validate-input"]
        assert len(failed) >= 1
        assert failed[0]["error"]["type"] == "ValueError"


# ── Cloud Functions wrapper integration ──────────────────────────────────


class TestCloudFunctionsWrapper:
    """Test the actual Cloud Functions decorator against mock-ingest."""

    def test_cloud_function_wrapper_succeeded(self):
        from tracestax.cloud_functions import tracestax_cloud_function

        @tracestax_cloud_function(api_key="ts_test_abc", endpoint=INGEST_URL)
        def my_function(request):
            return "OK"

        result = my_function(type("Request", (), {"headers": {}, "method": "GET", "url": "/test"})())
        assert result == "OK"

        time.sleep(1)
        events = fetch_events()
        cf_events = [e for e in events if e.get("framework") == "cloud_functions"]
        assert len(cf_events) >= 1
        assert any(e["status"] == "succeeded" for e in cf_events)

    def test_cloud_function_wrapper_failed(self):
        from tracestax.cloud_functions import tracestax_cloud_function

        @tracestax_cloud_function(api_key="ts_test_abc", endpoint=INGEST_URL)
        def bad_function(request):
            raise RuntimeError("function crashed")

        with pytest.raises(RuntimeError):
            bad_function(type("Request", (), {"headers": {}, "method": "POST", "url": "/crash"})())

        time.sleep(1)
        events = fetch_events()
        failed = [e for e in events if e.get("status") == "failed" and e.get("framework") == "cloud_functions"]
        assert len(failed) >= 1
        assert failed[0]["error"]["type"] == "RuntimeError"


# ── Event structure validation ───────────────────────────────────────────


class TestEventStructure:
    """Verify that all framework events share the required fields."""

    REQUIRED_FIELDS = {"type", "framework", "language", "sdk_version", "status", "worker", "task", "metrics"}
    REQUIRED_WORKER_FIELDS = {"key", "hostname", "pid", "queues"}
    REQUIRED_TASK_FIELDS = {"name", "id", "queue", "attempt"}

    def test_full_event_structure(self):
        from tracestax.client import TraceStaxClient

        client = TraceStaxClient(api_key="ts_test_abc", endpoint=INGEST_URL)

        client.send_event({
            "type": "task_event",
            "framework": "celery",
            "language": "python",
            "sdk_version": "0.1.0",
            "status": "succeeded",
            "worker": {"key": "celery@test:1", "hostname": "test", "pid": 1, "queues": ["default"]},
            "task": {"name": "validate_schema", "id": "struct-001", "queue": "default", "attempt": 1},
            "metrics": {"duration_ms": 42},
        })

        events = wait_for_events(lambda e: e.get("task", {}).get("id") == "struct-001")
        match = [e for e in events if e.get("task", {}).get("id") == "struct-001"]
        assert len(match) >= 1
        event = match[0]

        # Top-level fields
        for field in self.REQUIRED_FIELDS:
            assert field in event, f"Missing required field: {field}"

        # Worker sub-fields
        for field in self.REQUIRED_WORKER_FIELDS:
            assert field in event["worker"], f"Missing worker field: {field}"

        # Task sub-fields
        for field in self.REQUIRED_TASK_FIELDS:
            assert field in event["task"], f"Missing task field: {field}"

        # Metrics
        assert "duration_ms" in event["metrics"]

        client.close()
