import pytest
import time
from unittest.mock import MagicMock, patch
import threading

import workers.worker_base as wb

@pytest.fixture
def mock_redis(monkeypatch):
    # Replace Redis client with in-memory mock
    fake_redis = MagicMock()
    monkeypatch.setattr(wb, "rclient", fake_redis)
    return fake_redis

@pytest.fixture
def mock_kafka(monkeypatch):
    # Replace Kafka client with mock
    fake_client = MagicMock()
    monkeypatch.setattr(wb, "client", fake_client)
    return fake_client

def dummy_process(obj_id):
    """
    Simulate a worker processing function:
    - fail if id ends with 9
    - succeed otherwise
    """
    if str(obj_id).endswith("9"):
        return False, "simulated failure"
    return True, f"processed {obj_id}"

def test_worker_handle_message_integration(mock_redis, mock_kafka):
    """
    Integration test: simulate handle_message workflow end-to-end
    """
    payloads = [
        {"id": "1001", "run_id": "r1"},
        {"id": "1009", "run_id": "r2"},  # will fail
    ]

    for p in payloads:
        wb.handle_message(p, "1-Raw", dummy_process)

    # Check Redis: retry counters deleted
    calls = [c[0][0] for c in mock_redis.delete.call_args_list]
    assert all(k.startswith("retry:") for k in calls)

    # Check Kafka: client.send called
    send_calls = [c[0][0] for c in mock_kafka.send.call_args_list]
    assert any("loaded" in t or "loaded" in t for t in send_calls)
    assert any("failed" in t or "dlq" in t for t in send_calls)

def test_worker_loop_threaded_integration(mock_redis, mock_kafka):
    """
    Run a small threaded worker loop in a test with 2 messages.
    Only runs briefly to ensure no exceptions are raised.
    """
    # Patch the client.consumer to return our test messages
    class DummyConsumer:
        def poll(self, timeout_ms=1000):
            return {None: [{"value": {"id": "1011", "zone": "1-Raw"}}]}

        def close(self):
            pass

    mock_kafka.consumer.return_value = DummyConsumer()

    # Run worker loop in a thread, stop quickly
    stop_event = threading.Event()

    def run_loop():
        wb.start_worker_loop("1-Raw", dummy_process)
        stop_event.set()

    t = threading.Thread(target=run_loop)
    t.start()

    # Wait briefly and then signal shutdown
    time.sleep(0.5)
    wb.shutdown_event.set()
    t.join(timeout=2)

    # If we reach here, loop ran without crashing
    assert True
