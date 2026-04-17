"""Shared pytest fixtures for TraceStax SDK tests."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

# When running inside Docker (docker-compose.test.yml), the mock ingest is
# available at TRACESTAX_INGEST_URL. Unit tests use a mock session instead.
INGEST_URL = os.environ.get("TRACESTAX_INGEST_URL", "http://localhost:4001")


@pytest.fixture()
def mock_session():
    """Patch requests.Session so no real HTTP calls are made."""
    with patch("tracestax.client.requests.Session") as mock_cls:
        session = MagicMock()
        mock_cls.return_value = session
        response = MagicMock()
        response.status_code = 200
        response.text = '{"ok":true}'
        session.post.return_value = response
        yield session


@pytest.fixture()
def client(mock_session):
    """A TraceStaxClient wired to a mock HTTP session."""
    from tracestax.client import TraceStaxClient

    c = TraceStaxClient(
        api_key="ts_test_abc123",
        endpoint="https://test.tracestax.com",
        flush_interval=60.0,  # long interval — we flush manually in tests
        max_batch_size=10,
    )
    yield c
    c.close()


@pytest.fixture()
def celery_app():
    """A minimal Celery app for testing signal hooks."""
    try:
        from celery import Celery

        app = Celery("test")
        app.config_from_object(
            {
                "broker_url": "memory://",
                "result_backend": "cache+memory://",
                "task_always_eager": True,
            }
        )
        return app
    except ImportError:
        pytest.skip("celery not installed")
        return None
