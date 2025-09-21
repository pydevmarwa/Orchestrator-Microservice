from unittest.mock import MagicMock, patch
import pytest

from orchestrator import kafka_client as kc
from orchestrator.config import TOPIC_READY

def test_send_without_producer_returns_none(monkeypatch):
    # ensure client has no producer and _create_producer will not be called (simulate failure)
    client = kc.KafkaClient()
    client._producer = None

    # monkeypatch _ensure_producer to return None (simulate broker down)
    monkeypatch.setattr(client, "_ensure_producer", lambda raise_on_fail=False: None)

    # sending async should return None and not raise
    res = client.send(TOPIC_READY, {"event": "ObjectReady", "id": "1"}, key="1", sync=False)
    assert res is None

    # sync send should raise because we force creation when sync=True. Simulate creation raising
    def ensure_raise(raise_on_fail=False):
        if raise_on_fail:
            raise RuntimeError("no brokers")
        return None
    monkeypatch.setattr(client, "_ensure_producer", ensure_raise)
    with pytest.raises(RuntimeError):
        client.send(TOPIC_READY, {"event": "ObjectReady", "id": "1"}, key="1", sync=True)
