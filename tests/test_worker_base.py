import time
from unittest.mock import MagicMock

import pytest

# import the module under test - adjust if your layout differs
import workers.worker_base as wb
from orchestrator.config import TOPIC_LOADED, TOPIC_FAILED, TOPIC_DLQ, REDIS_PREFIX


def setup_function(fn):
    """
    Ensure module-level singletons are reset/mocked before each test.
    """
    # mock redis client used in module (best-effort)
    wb.rclient = MagicMock()
    # mock kafka client used in module
    wb.client = MagicMock()
    # ensure shutdown event doesn't interfere
    wb.shutdown_event.clear()


def test_publish_result_success_publishes_loaded():
    wb.client.reset_mock()
    wb.rclient.reset_mock()

    wb.publish_result("obj-1", "1-Raw", True, "ingested ok", run_id="r1")

    # should publish only to TOPIC_LOADED
    assert wb.client.send.call_count == 1
    called_topic = wb.client.send.call_args_list[0][0][0]
    assert called_topic == TOPIC_LOADED or "event.load.loaded" in called_topic


def test_publish_result_failure_publishes_failed_and_dlq_and_dedupe():
    wb.client.reset_mock()
    wb.rclient.reset_mock()

    # First call: Redis.sadd returns 1 => newly added => should publish
    wb.rclient.sadd.return_value = 1
    wb.publish_result("obj-2", "2-Standardized", False, "error happened", run_id="r2")

    # expect two sends: FAILED and DLQ
    assert wb.client.send.call_count >= 2
    topics = [cargs[0][0] for cargs in wb.client.send.call_args_list]
    assert any(t == TOPIC_FAILED or "event.load.failed" in t for t in topics)
    assert any(t == TOPIC_DLQ or "event.load.dlq" in t for t in topics)

    wb.client.reset_mock()

    # Second call: Redis.sadd returns 0 => duplicate => should skip publishing
    wb.rclient.sadd.return_value = 0
    wb.publish_result("obj-2", "2-Standardized", False, "error happened again", run_id="r2")
    # client.send should not be called this time
    assert wb.client.send.call_count == 0


def test_handle_message_success_calls_publish_result(monkeypatch):
    wb.client.reset_mock()
    wb.rclient.reset_mock()

    # simulate process function returning success
    def proc_ok(obj_id):
        return True, f"processed {obj_id}"

    # spy publish_result
    called = {}
    def spy_publish(obj_id, zone, ok, info, run_id=None):
        called['args'] = (obj_id, zone, ok, info, run_id)

    monkeypatch.setattr(wb, "publish_result", spy_publish)

    payload = {"id": "obj-3"}
    wb.handle_message(payload, "1-Raw", proc_ok)

    assert 'args' in called
    assert called['args'][0] == "obj-3"
    assert called['args'][2] is True


def test_handle_message_exception_results_in_failure(monkeypatch):
    wb.client.reset_mock()
    wb.rclient.reset_mock()

    # process fn that raises
    def proc_raises(obj_id):
        raise RuntimeError("boom")

    # spy publish_result
    calls = []
    def spy_publish(obj_id, zone, ok, info, run_id=None):
        calls.append((obj_id, zone, ok, info, run_id))

    monkeypatch.setattr(wb, "publish_result", spy_publish)

    payload = {"id": "obj-4"}
    wb.handle_message(payload, "1-Raw", proc_raises)

    assert len(calls) == 1
    assert calls[0][0] == "obj-4"
    assert calls[0][2] is False
    assert "exception" in calls[0][3]
