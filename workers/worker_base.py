import logging
import threading
import time
import random
import concurrent.futures
import signal
from typing import Callable, Dict, Any

import redis

from orchestrator.kafka_client import get_kafka_client
from orchestrator.config import (
    REDIS_HOST,
    REDIS_PORT,
    RETRY_TTL_SECONDS,
    TOPIC_READY,
    TOPIC_LOADED,
    TOPIC_FAILED,
    TOPIC_DLQ,
    MAX_WORKERS,
    REDIS_PREFIX,
    WORKER_PRECHECK_SUFFIXES
)
from orchestrator.logger import get_logger

log = get_logger("worker_base")

try:
    rclient = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    rclient.ping()
except Exception as e:
    log.warning("Redis not available at startup: %s", e)
    rclient = None

client = get_kafka_client()
shutdown_event = threading.Event()

def _on_signal(signum, frame):
    log.info("Signal %s received, setting shutdown flag", signum)
    shutdown_event.set()

try:
    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)
except Exception:
    pass

RETRY_PREFIX = "retry"

def retry_key(obj_id: str) -> str:
    """Return the Redis key used to store retry count for an object."""
    return f"{RETRY_PREFIX}:{obj_id}"

def safe_incr(key: str) -> int:
    """
    Atomically increment a Redis key and refresh its TTL.

    Returns:
        The integer value after increment, or 1 if Redis is unavailable.
    """
    try:
        if rclient is None:
            return 1
        val = rclient.incr(key)
        rclient.expire(key, RETRY_TTL_SECONDS)
        return int(val)
    except Exception as e:
        log.warning("Redis incr failed for %s: %s", key, e)
        return 1

def safe_del(key: str) -> None:
    """Delete a Redis key if Redis is available (best-effort)."""
    try:
        if rclient:
            rclient.delete(key)
    except Exception:
        pass

def publish_result(obj_id: str, zone: str, ok: bool, info: str, run_id: str = None) -> None:
    """
    Publish the processing result for an object.

    Success path:
      - publish an ObjectLoaded message to TOPIC_LOADED

    Failure path:
      - deduplicate failures using Redis set (REDIS_PREFIX:failed)
      - publish to TOPIC_FAILED and TOPIC_DLQ only once per object

    This function is idempotent in the failure path (best-effort via Redis).
    """
    payload: Dict[str, Any] = {
        "event": "ObjectLoaded",
        "id": obj_id,
        "status": "loaded" if ok else "failed",
        "zone": zone,
        "info": info,
    }
    if run_id:
        payload["run_id"] = run_id

    if ok:
        try:
            client.send(TOPIC_LOADED, payload, key=obj_id, sync=False)
            log.info("Published LOADED %s zone=%s", obj_id, zone)
        except Exception as e:
            log.exception("Failed to publish to %s: %s", TOPIC_LOADED, e)
        return

    failed_set_key = f"{REDIS_PREFIX}:failed"
    should_publish = True
    try:
        if rclient is not None:
            added = rclient.sadd(failed_set_key, obj_id)
            if added:
                try:
                    rclient.expire(failed_set_key, RETRY_TTL_SECONDS * 100)
                except Exception:
                    pass
                should_publish = True
            else:
                should_publish = False
    except Exception as e:
        log.warning("Redis failed-set operation failed for %s: %s", obj_id, e)
        should_publish = True

    if not should_publish:
        log.info("Failure for %s already published previously — skipping duplicate failed/dlq", obj_id)
        return

    try:
        client.send(TOPIC_FAILED, payload, key=obj_id, sync=False)
        client.send(TOPIC_DLQ, payload, key=obj_id, sync=False)
        log.info("Published FAILED and DLQ for %s zone=%s", obj_id, zone)
    except Exception as e:
        log.exception("Failed to publish failure/DLQ for %s: %s", obj_id, e)

def _republish_ready_nonblocking(obj_id: str, zone: str, run_id: str = None, attempt: int = 1) -> None:
    """
    Re-publish a READY message in background after a backoff.

    This helper is used to retry READY publishes without blocking worker threads.
    """
    def _job():
        delay = min(30, (2 ** attempt) + random.random())
        log.info("Retrying %s after %.1fs (attempt=%d)", obj_id, delay, attempt)
        time.sleep(delay)
        msg = {
            "event": "ObjectReady",
            "id": obj_id,
            "zone": zone,
            "info": f"retry {attempt}",
            "run_id": run_id,
        }
        try:
            client.send(TOPIC_READY, msg, key=obj_id, sync=False)
        except Exception as e:
            log.exception("Failed to re-publish READY for %s: %s", obj_id, e)

    t = threading.Thread(target=_job, daemon=True)
    t.start()

def handle_message(payload: Dict[str, Any], zone: str, process_fn: Callable[[str], tuple]) -> None:
    """
    Handle a single READY message: call the provided process_fn(obj_id) and publish results.

    The provided process_fn must return a tuple (ok: bool, info: str).
    Exceptions inside process_fn are caught and treated as failures.
    """
    obj_id = str(payload.get("id"))
    run_id = payload.get("run_id")
    try:
        ok, info = process_fn(obj_id)
    except Exception as e:
        ok = False
        info = f"exception: {e}"

    if not ok:
        log.warning("Processing failed for %s: %s", obj_id, info)
        try:
            safe_del(retry_key(obj_id))
        except Exception:
            pass
        publish_result(obj_id, zone, False, info, run_id=run_id)
        return

    try:
        safe_del(retry_key(obj_id))
    except Exception:
        pass
    publish_result(obj_id, zone, True, info, run_id=run_id)

def start_worker_loop(zone: str, process_fn: Callable[[str], tuple]) -> None:
    """
    Start the worker loop for a specific zone.

    This function:
      - creates a Kafka consumer for TOPIC_READY (with retry loop)
      - polls for messages and dispatches work to a ThreadPoolExecutor
      - honors the shutdown_event for graceful stopping
    """
    log.info("Worker loop start (zone=%s, max_workers=%d)", zone, MAX_WORKERS)

    consumer = None
    while not shutdown_event.is_set() and consumer is None:
        try:
            consumer = client.consumer(TOPIC_READY, group_id=f"worker-{zone}")
        except Exception as e:
            log.warning("Kafka consumer not ready for worker %s: %s — retrying in 3s", zone, e)
            time.sleep(3)

    if shutdown_event.is_set():
        log.info("Shutdown requested before consumer ready (zone=%s)", zone)
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        try:
            while not shutdown_event.is_set():
                try:
                    records = consumer.poll(timeout_ms=1000)
                except Exception as e:
                    log.exception("Kafka poll failed: %s", e)
                    time.sleep(1)
                    continue

                if not records:
                    continue

                for tp, msgs in records.items():
                    for msg in msgs:
                        payload = msg.value or {}
                        if payload.get("zone") != zone:
                            continue
                        executor.submit(handle_message, payload, zone, process_fn)
        except Exception as e:
            log.exception("Worker loop unexpected error: %s", e)
        finally:
            log.info("Shutting down worker loop (zone=%s)", zone)
            try:
                consumer.close()
            except Exception:
                pass
            executor.shutdown(wait=True)
            log.info("Worker stopped (zone=%s)", zone)
