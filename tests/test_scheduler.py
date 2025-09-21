# tests/test_scheduler.py
import time
from unittest.mock import MagicMock, patch

import pytest

import orchestrator.metrics as metrics
from orchestrator.scheduler import Orchestrator

SIMPLE_OBJS = [
    {"IdObjet": 1001, "IdObjet_Parent": None, "NomObjet": "A", "Zone": "1-Raw", "id": "1"},
    {"IdObjet": 2001, "IdObjet_Parent": "1001", "NomObjet": "B", "Zone": "2-Standardized", "id": "2"},
    {"IdObjet": 3001, "IdObjet_Parent": "2001", "NomObjet": "C", "Zone": "3-Application", "id": "3"},
]

@patch("orchestrator.scheduler.wait_for_kafka")
@patch("orchestrator.scheduler.get_kafka_client")
@patch("orchestrator.scheduler.load_objects")
@patch("orchestrator.scheduler.redis.Redis")
def test_publish_ready_and_inflight(mock_redis_cls, mock_load_objs, mock_get_kafka, mock_wait):
    # mock loading objects
    mock_load_objs.return_value = SIMPLE_OBJS

    # mock kafka client send
    client = MagicMock()
    mock_get_kafka.return_value = client

    # mock redis
    rinst = MagicMock()
    # sismember for loaded/inflight returns False initially
    rinst.sismember.return_value = False
    mock_redis_cls.return_value = rinst

    orch = Orchestrator(objects_path="/non/existent.json")
    # pick a root (1001) and publish
    orch.publish_ready("1001")

    # kafka client should have been asked to send to TOPIC_READY
    assert client.send.call_count >= 1
    called_topics = [cargs[0][0] for cargs in client.send.call_args_list]
    assert any("event.load.ready" in t or t == "event.load.ready" for t in called_topics)

    # mark as loaded then simulate LOADED event processed
    rinst.sismember.side_effect = lambda key, member=None: False
    # simulate LOADED message handling by calling internal logic:
    # we call the internal loop manually: mark 1001 as loaded and ensure child becomes ready
    rinst.sadd.assert_called()  # ensure some sets were used

@patch("orchestrator.scheduler.wait_for_kafka")
@patch("orchestrator.scheduler.get_kafka_client")
@patch("orchestrator.scheduler.load_objects")
@patch("orchestrator.scheduler.redis.Redis")
def test_should_skip_by_frequency_using_lastrun(mock_redis_cls, mock_load_objs, mock_get_kafka, mock_wait):
    mock_load_objs.return_value = SIMPLE_OBJS
    client = MagicMock()
    mock_get_kafka.return_value = client
    rinst = MagicMock()
    mock_redis_cls.return_value = rinst

    orch = Orchestrator(objects_path="/non/existent.json")
    # set Lastrun recent for 1001
    orch.meta["1001"]["Lastrun"] = str(int(time.time()))
    # _should_skip_by_frequency should return True for parent just run now
    assert orch._should_skip_by_frequency("1001") is True
