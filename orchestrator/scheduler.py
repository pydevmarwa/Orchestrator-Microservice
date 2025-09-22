"""
Orchestrator scheduler module.

This file contains the Orchestrator class that:
 - loads the object metadata
 - builds a canonical dependency graph
 - computes a topological order for initial publishing
 - publishes READY events to Kafka and listens for LOADED/FAILED events
 - updates Prometheus counters (imported from orchestrator.metrics)

This module does NOT start an HTTP server for metrics; metrics exposure
is handled by the HTTP server in main.py (which calls prometheus_client.generate_latest()).
"""

import time
import threading
import redis
from pydantic import BaseModel, ValidationError
from typing import Set

from orchestrator.graph_utils import load_objects, build_full_graph, collect_subgraph_for_terminals, topological_sort
from orchestrator.config import (
    TOPIC_READY,
    TOPIC_LOADED,
    TOPIC_FAILED,
    TOPIC_DLQ,
    OBJECTS_PATH,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_PREFIX,
    WORKER_PROCESS_SECONDS,
    WORKER_PRECHECK_SUFFIXES,
    RETRY_TTL_SECONDS,
)
from orchestrator.kafka_client import wait_for_kafka, get_kafka_client
from orchestrator.logger import get_logger
from orchestrator import metrics  # centralized metrics module

log = get_logger("scheduler")


class LoadedEvent(BaseModel):
    """
    Pydantic model for incoming loaded events from workers.

    Fields:
      event: string event name (e.g. 'ObjectLoaded')
      id: canonical object id (string)
      status: 'loaded' or 'failed'
      info: optional free-text info or error message
      run_id: orchestrator run id that produced the READY (optional)
      ts: timestamp from worker (optional)
    """
    event: str
    id: str
    status: str
    info: str = None
    run_id: str = None
    ts: int = None


class Orchestrator:
    """
    Orchestrator that computes dependency order and publishes READY events.

    Responsibilities:
      - Load metadata and construct canonical parents graph.
      - Extract terminal subgraph for application-layer targets.
      - Compute a topological order and publish initial READY messages for roots.
      - Listen for LOADED/FAILED messages from workers and trigger children when ready.
      - Update Prometheus metrics through the centralized metrics module.

    This class is designed to be instantiated and run inside a long-lived process.
    """
    def __init__(self, objects_path: str = OBJECTS_PATH):
        self.run_id = str(int(time.time()))
        log.info("Orchestrator run=%s", self.run_id)

        # Load objects and build canonical graph
        objs = load_objects(objects_path)
        full_graph, self.meta = build_full_graph(objs)
        self.parents_map, self.terminals = collect_subgraph_for_terminals(full_graph, self.meta)
        self.order = topological_sort(self.parents_map)
        log.info("Subgraph nodes=%d terminals=%s", len(self.parents_map), self.terminals)

        # Build children map for quick lookup when parent completed
        self.children_map = {}
        for node, parents in self.parents_map.items():
            for p in parents:
                self.children_map.setdefault(str(p), set()).add(str(node))

        # Wait for Kafka to be reachable before proceeding (raises on failure)
        wait_for_kafka()
        self.client = get_kafka_client()

        # Connect to Redis (required for dedup & state)
        try:
            self.redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            self.redis.ping()
        except Exception as e:
            log.error("Redis unavailable: %s", e)
            raise

        # Redis keys for this run
        self.key_loaded = f"{REDIS_PREFIX}:loaded"
        self.key_inflight = f"{REDIS_PREFIX}:inflight:{self.run_id}"

        # internal control flags
        self._stop = threading.Event()
        self._metrics_stop_event = None

        # start uptime collector (updates metrics.UPTIME)
        self._metrics_stop_event = metrics.start_uptime_collector(start_ts=time.time(), interval=1.0)

    def stop(self):
        """
        Request graceful stop of the orchestrator.
        This will signal the main loop to end and stop the uptime collector.
        """
        log.info("Orchestrator stop requested")
        self._stop.set()
        try:
            if self._metrics_stop_event:
                self._metrics_stop_event.set()
        except Exception:
            pass

    # --- Helper methods ---

    def parents_of(self, node: str) -> Set[str]:
        """Return the set of parent node ids for the given node (empty set if none)."""
        return set(self.parents_map.get(node, set()))

    def all_parents_loaded(self, node: str) -> bool:
        """
        Return True if all parents of a node are present in the Redis loaded set.
        A node with no parents is considered ready.
        """
        parents = self.parents_of(node)
        if not parents:
            return True
        for p in parents:
            if not self.redis.sismember(self.key_loaded, p):
                return False
        return True

    def _estimate_proc_time(self, node_id: str) -> int:
        """Estimate processing time (seconds) for a node based on Zone metadata."""
        meta = self.meta.get(node_id, {}) or {}
        zone = meta.get("Zone")
        if zone and zone in WORKER_PROCESS_SECONDS:
            return int(WORKER_PROCESS_SECONDS.get(zone, 0))
        return 0

    def _should_skip_by_frequency(self, parent_id: str) -> bool:
        """
        Heuristic optimization: skip republishing parent if it was recently refreshed.
        Uses Lastrun timestamp or Fréquence cron expression; best-effort evaluation.
        """
        meta = self.meta.get(parent_id, {}) or {}
        lastrun = meta.get("Lastrun")
        freq = meta.get("Fréquence")
        est = self._estimate_proc_time(parent_id)
        now = time.time()

        try:
            if lastrun is not None:
                lastrun_ts = int(lastrun)
                if est > 0 and (now - lastrun_ts) < (2 * est):
                    log.debug("Freq-opt skip parent %s by last run (lastrun=%s est=%s)", parent_id, lastrun_ts, est)
                    return True
        except Exception:
            pass

        try:
            if freq:
                from croniter import croniter
                it = croniter(freq, now)
                prev_run = it.get_prev(float)
                if est > 0 and (now - prev_run) < (2 * est):
                    log.debug("Freq-opt skip parent %s by cron prev_run=%s est=%s", parent_id, prev_run, est)
                    return True
        except Exception:
            pass

        return False

    def _preflight_check(self, node: str, zone: str) -> bool:
        """
        Run orchestrator-level preflight checks and return True if the node should be rejected.

        Example: simple suffix-based rejection configured via WORKER_PRECHECK_SUFFIXES.
        """
        if not zone:
            return False
        suffixes = WORKER_PRECHECK_SUFFIXES.get(zone) or []
        if not suffixes:
            return False
        for s in suffixes:
            if str(node).endswith(str(s)):
                log.info("Preflight: node %s rejected for zone %s by suffix '%s'", node, zone, s)
                metrics.PRECHECK_FAILED_COUNTER.inc()
                return True
        return False

    def _publish_failed_by_orch(self, node: str, reason: str):
        """
        Publish orchestrator-initiated FAILED + DLQ events and mark failure in Redis set to dedupe.
        """
        payload = {"id": node, "info": reason, "run_id": self.run_id}
        failed_set_key = f"{REDIS_PREFIX}:failed"
        try:
            if self.redis is not None:
                self.redis.sadd(failed_set_key, node)
                try:
                    self.redis.expire(failed_set_key, RETRY_TTL_SECONDS)
                except Exception:
                    pass
        except Exception as e:
            log.warning("Redis failed-set operation failed for precheck of %s: %s", node, e)

        try:
            self.client.send(TOPIC_FAILED, payload, key=node, sync=False)
            self.client.send(TOPIC_DLQ, payload, key=node, sync=False)
            metrics.FAILED_COUNTER.inc()
            log.info("Orchestrator published FAILED+DLQ for %s (reason=%s)", node, reason)
        except Exception as e:
            log.exception("Failed to publish orchestrator-side failure for %s: %s", node, e)

    # --- Publishing logic ---

    def publish_ready(self, node: str):
        """
        Publish a READY event for the given node.

        Logic:
          - Skip if already loaded or inflight (dedupe via Redis)
          - Perform preflight checks
              - Include a preflight flag in the payload so worker knows it will fail
          - Mark node as inflight in Redis
          - Metrics and logging
        """
        node = str(node)
        # dedupe checks
        try:
            if self.redis.sismember(self.key_loaded, node) or self.redis.sismember(self.key_inflight, node):
                log.debug("Skip publish_ready %s loaded/inflight", node)
                return
        except Exception:
            pass

        meta = self.meta.get(node, {})
        zone = meta.get("Zone")
        if not zone:
            log.debug("No zone for %s, skip", node)
            return

        # preflight
        preflight_fail = self._preflight_check(node, zone)
        payload = {
            "event": "ObjectReady",
            "id": node,
            "zone": zone,
            "name": meta.get("NomObjet"),
            "run_id": self.run_id,
            "ts": int(time.time()),
            "preflight_fail": preflight_fail,  # flag pour le worker
        }

        try:
            # synchronous send to get immediate backpressure if needed
            self.client.send(TOPIC_READY, payload, key=node, sync=True, timeout=5)
            try:
                self.redis.sadd(self.key_inflight, node)
            except Exception as e:
                log.warning("Failed to mark inflight %s: %s", node, e)
            metrics.READY_COUNTER.inc()
            log.info("Published READY %s zone=%s preflight_fail=%s", node, zone, preflight_fail)
        except Exception as e:
            log.exception("Failed to publish READY for %s: %s", node, e)

    def initial_publish(self):
        """Publish initial root nodes (nodes with no parents) in topological order."""
        for n in self.order:
            if not self.parents_of(n):
                self.publish_ready(n)

    # --- Main loop ---

    def run(self) -> None:
        # Publish initial roots
        self.initial_publish()
        log.info("Listening for loaded events on %s", TOPIC_LOADED)

        consumer = None
        while not self._stop.is_set() and consumer is None:
            try:
                consumer = self.client.consumer(TOPIC_LOADED, group_id=f"orch-{self.run_id}")
            except Exception as e:
                log.warning("Kafka consumer not ready yet: %s — retry in 3s", e)
                time.sleep(3)

        if self._stop.is_set():
            return

        try:
            while not self._stop.is_set():
                for msg in consumer:
                    payload = msg.value or {}
                    try:
                        ev = LoadedEvent(**payload)
                    except ValidationError:
                        log.warning("Invalid loaded event: %s", payload)
                        continue

                    nid = str(ev.id)
                    status = ev.status

                    if status == "loaded":
                        metrics.LOADED_COUNTER.inc()
                        self.redis.sadd(self.key_loaded, nid)
                        self.redis.srem(self.key_inflight, nid)

                        # Publish a READY event for children whose all parents are LOADED.
                        for child in self.children_map.get(nid, set()):
                            if self.all_parents_loaded(child):
                                self.publish_ready(child)

                    else:
                        metrics.FAILED_COUNTER.inc()
                        try:
                            self.client.send(TOPIC_FAILED, {"id": nid, "info": ev.info, "run_id": self.run_id}, key=nid,
                                             sync=False)
                            self.client.send(TOPIC_DLQ, {"id": nid, "info": ev.info, "run_id": self.run_id}, key=nid,
                                             sync=False)
                        except Exception as e:
                            log.exception("Failed to forward FAILED/DLQ for %s: %s", nid, e)

                if self._stop.is_set():
                    break

        finally:
            try:
                consumer.close()
            except Exception:
                pass
            if self._metrics_stop_event:
                self._metrics_stop_event.set()
            log.info("Orchestrator stopped (run_id=%s)", self.run_id)
