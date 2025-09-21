"""
Centralized Prometheus metrics used by the orchestrator process.

All metrics are defined here so every module in the process can import
and update the same metric instances. This module also provides a
convenience function to start a background thread that updates the
uptime gauge at regular intervals.

Docstrings and function descriptions are in English (per request).
"""

import time
import threading
from typing import Optional
from prometheus_client import Gauge, Counter

# --- Metric definitions (global singletons) ---
# Gauge storing seconds since orchestrator process start (monotonic)
UPTIME = Gauge('orchestrator_uptime_seconds', 'Orchestrator uptime in seconds')

# Counters to observe event flow
READY_COUNTER = Counter('orchestrator_ready_published_total', 'Total number of READY events published by the orchestrator')
LOADED_COUNTER = Counter('orchestrator_loaded_received_total', 'Total number of LOADED events received by the orchestrator')
FAILED_COUNTER = Counter('orchestrator_failed_received_total', 'Total number of FAILED events handled by the orchestrator')
PRECHECK_FAILED_COUNTER = Counter('orchestrator_precheck_failed_total', 'Total number of precheck failures detected by the orchestrator')

# Internal handle for uptime thread (kept here for convenience)
_UPTIME_THREAD = None
_UPTIME_STOP_EVENT = None


def start_uptime_collector(start_ts: Optional[float] = None, interval: float = 1.0, daemon: bool = True) -> threading.Event:
    """
    Start a background thread that periodically updates the UPTIME gauge.

    Args:
        start_ts: optional epoch timestamp (seconds) to use as the process start time.
                  If None, time.time() is used when the function is called.
        interval: update interval in seconds (default 1.0).
        daemon: whether the background thread should be a daemon (default True).

    Returns:
        threading.Event that can be set() to stop the background thread.
        The event is initially cleared.
    """
    global _UPTIME_THREAD, _UPTIME_STOP_EVENT

    if _UPTIME_THREAD is not None and _UPTIME_THREAD.is_alive():
        # Already running — return existing stop event
        return _UPTIME_STOP_EVENT

    if start_ts is None:
        start_ts = time.time()

    stop_event = threading.Event()
    stop_event.clear()

    def _loop():
        while not stop_event.is_set():
            try:
                UPTIME.set(time.time() - start_ts)
            except Exception:
                # Keep loop resilient; metrics update must not crash the process
                pass
            # use wait so we exit quickly when stop_event is set
            stop_event.wait(interval)

    t = threading.Thread(target=_loop, name="metrics-uptime-collector", daemon=daemon)
    _UPTIME_THREAD = t
    _UPTIME_STOP_EVENT = stop_event
    t.start()
    return stop_event


def stop_uptime_collector():
    """
    Stop the uptime collector background thread if it is running.
    This function is best-effort and will return quickly.
    """
    global _UPTIME_THREAD, _UPTIME_STOP_EVENT
    try:
        if _UPTIME_STOP_EVENT is not None:
            _UPTIME_STOP_EVENT.set()
        if _UPTIME_THREAD is not None:
            _UPTIME_THREAD.join(timeout=2.0)
    finally:
        _UPTIME_THREAD = None
        _UPTIME_STOP_EVENT = None
