import json
from unittest.mock import MagicMock

import pytest

from pipeline.rabbitmq_worker import process_job


@pytest.fixture
def mock_redis():
    return MagicMock()


@pytest.fixture
def mock_channel():
    return MagicMock()


@pytest.fixture
def mock_method():
    method = MagicMock()
    method.delivery_tag = 42
    return method


@pytest.mark.parametrize(
    "event_id",
    [
        "abc-123",
        "00000000-0000-0000-0000-000000000000",
        "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    ],
)
def test_process_job_marks_as_processed(
    mock_redis, mock_channel, mock_method, event_id
):
    body = json.dumps({"event_id": event_id}).encode()

    process_job(mock_redis, mock_channel, mock_method, None, body)

    mock_redis.set.assert_called_once_with(f"job:{event_id}", "processed")


def test_process_job_acknowledges_message(mock_redis, mock_channel, mock_method):
    body = json.dumps({"event_id": "test-123"}).encode()

    process_job(mock_redis, mock_channel, mock_method, None, body)

    mock_channel.basic_ack.assert_called_once_with(delivery_tag=42)


def test_process_job_acks_after_redis_write(mock_redis, mock_channel, mock_method):
    """Redis write should happen before the message is acknowledged."""
    call_order = []
    mock_redis.set.side_effect = lambda *a: call_order.append("redis_set")
    mock_channel.basic_ack.side_effect = lambda **kw: call_order.append("ack")

    body = json.dumps({"event_id": "test-123"}).encode()
    process_job(mock_redis, mock_channel, mock_method, None, body)

    assert call_order == ["redis_set", "ack"]


def test_process_job_invalid_json(mock_redis, mock_channel, mock_method):
    with pytest.raises(json.JSONDecodeError):
        process_job(mock_redis, mock_channel, mock_method, None, b"not json")


def test_process_job_missing_event_id(mock_redis, mock_channel, mock_method):
    body = json.dumps({"other_field": "value"}).encode()

    with pytest.raises(KeyError):
        process_job(mock_redis, mock_channel, mock_method, None, body)
