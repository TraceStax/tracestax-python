"""Watchdog that monitors the daemon flush thread and restarts it if it dies.

Also installs signal handlers for graceful shutdown.
"""

from __future__ import annotations

import logging
import signal
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tracestax.client import TraceStaxClient

logger = logging.getLogger("tracestax")

_CHECK_INTERVAL = 5.0  # seconds between health checks


class Watchdog:
    """Periodically checks that the ``TraceStaxClient`` daemon thread is alive.

    If the thread has died unexpectedly (e.g. unhandled exception bubble-up),
    the watchdog restarts it.  The watchdog itself runs as a daemon thread so
    it won't prevent interpreter shutdown.

    Signal handling
    ---------------
    On construction the watchdog installs SIGTERM / SIGINT handlers (on the
    main thread only) that call ``client.close()`` for a clean flush before
    the process exits.  Previous handlers are chained so user-installed
    handlers still fire.
    """

    def __init__(self, client: TraceStaxClient) -> None:
        self._client = client
        self._stopped = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="tracestax-watchdog",
        )
        self._thread.start()
        self._install_signal_handlers()

    # ── Public ─────────────────────────────────────────────────────────

    def stop(self) -> None:
        self._stopped.set()

    # ── Internals ──────────────────────────────────────────────────────

    def _run(self) -> None:
        while not self._stopped.is_set():
            self._stopped.wait(timeout=_CHECK_INTERVAL)
            if self._stopped.is_set():
                break
            if not self._client.is_alive and not self._client._closed.is_set():
                queue_size = len(self._client._queue)
                logger.warning(
                    "TraceStax flush thread died — restarting (events in queue: %d)",
                    queue_size,
                )
                self._client._daemon = self._client._start_daemon()

    def _install_signal_handlers(self) -> None:
        """Install graceful-shutdown signal handlers on the main thread."""
        if threading.current_thread() is not threading.main_thread():
            return  # signal handlers can only be set from the main thread

        for sig in (signal.SIGTERM, signal.SIGINT):
            prev = signal.getsignal(sig)
            self._chain_handler(sig, prev)

    def _chain_handler(self, sig: signal.Signals, prev_handler) -> None:
        def _handler(signum, frame):
            logger.debug("Received signal %s — flushing TraceStax events", signum)
            try:
                self._client.close()
            except Exception:
                logger.debug("Failed to close TraceStax client in signal handler", exc_info=True)
            # Chain to previous handler
            if callable(prev_handler) and prev_handler not in (
                signal.SIG_DFL,
                signal.SIG_IGN,
            ):
                prev_handler(signum, frame)
            elif prev_handler == signal.SIG_DFL:
                # Re-raise with default behaviour
                signal.signal(sig, signal.SIG_DFL)
                signal.raise_signal(sig)

        signal.signal(sig, _handler)
