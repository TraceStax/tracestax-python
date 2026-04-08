# tracestax

TraceStax SDK for Python task queues. Automatically captures task lifecycle events, queue depth snapshots, and task lineage from **Celery**, **RQ**, and **Dramatiq** workers.

## Installation

```bash
pip install tracestax
```

With optional extras:

```bash
pip install tracestax[celery]    # Celery support
pip install tracestax[rq]        # RQ support
pip install tracestax[dramatiq]  # Dramatiq support
```

## Quickstart - Celery

Add two lines to your Celery app:

```python
from celery import Celery
from tracestax import configure

app = Celery("myapp", broker="redis://localhost:6379/0")
configure(app, api_key="ts_live_xxx")
```

That's it. The SDK hooks into Celery's signal system and automatically reports:

- Task started / succeeded / failed / retried / revoked events
- Worker heartbeats on startup and shutdown
- Queue depth snapshots every 60 seconds
- Parent-child task lineage when tasks spawn other tasks

## Quickstart - RQ

```python
from tracestax import configure
from tracestax.rq import TraceStaxWorker

client = configure(api_key="ts_live_xxx")
worker = TraceStaxWorker(client=client, queues=["default"], connection=redis_conn)
worker.work()
```

## Quickstart - Dramatiq

```python
import dramatiq
from tracestax.dramatiq import TraceStaxMiddleware

dramatiq.set_middleware([TraceStaxMiddleware(api_key="ts_live_xxx")])
```

## Configuration

```python
configure(
    app,                                         # Celery app (optional for RQ/Dramatiq)
    api_key="ts_live_xxx",                       # Required
    endpoint="https://ingest.tracestax.com",     # Override ingest endpoint
    flush_interval=5.0,                          # Seconds between batch flushes
    max_batch_size=100,                          # Max events per HTTP request
    enable_lineage=True,                         # Track parent-child task relationships
    enable_snapshots=True,                       # Collect queue depth snapshots
    snapshot_interval=60.0,                      # Seconds between snapshots
)
```

## How It Works

The SDK runs a lightweight background daemon thread that batches events and sends them to `https://ingest.tracestax.com/v1/ingest` with your API key in the `Authorization: Bearer` header. Events are flushed:

- Every `flush_interval` seconds (default 5s)
- When the batch buffer reaches `max_batch_size` (default 100)
- On worker shutdown or process exit (`atexit`)

A watchdog thread monitors the flush daemon and restarts it if it dies unexpectedly.

## Authentication

All requests use your API key as a Bearer token:

```
Authorization: Bearer ts_live_xxx
```

Get your project API key from the TraceStax dashboard under **Project → API Key**.

## License

MIT
