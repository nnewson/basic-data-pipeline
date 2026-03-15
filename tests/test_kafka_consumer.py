import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from pipeline.kafka_consumer import process_messages, update_cassandra, update_redis


@pytest.fixture
def sample_event():
    return {
        "event_id": "abc-123",
        "user_id": "jane_doe",
        "page": "/pricing",
        "timestamp": 1700000000.0,
    }


# --- update_redis ---


@pytest.mark.parametrize(
    "page, user_id",
    [
        ("/pricing", "jane_doe"),
        ("/", "admin"),
        ("/docs", "test_user"),
        ("/checkout", "user-with-dashes"),
    ],
)
def test_update_redis_sets_correct_keys(page, user_id):
    redis_client = MagicMock()
    event = {"page": page, "user_id": user_id, "event_id": "x", "timestamp": 0}

    update_redis(redis_client, event)

    redis_client.incr.assert_called_once_with(f"pageviews:{page}")
    redis_client.set.assert_called_once_with(f"user:last_page:{user_id}", page)


def test_update_redis_increments_before_setting(sample_event):
    """Verify incr is called before set (order matters for consistency)."""
    redis_client = MagicMock()
    call_order = []
    redis_client.incr.side_effect = lambda *a: call_order.append("incr")
    redis_client.set.side_effect = lambda *a: call_order.append("set")

    update_redis(redis_client, sample_event)

    assert call_order == ["incr", "set"]


# --- update_cassandra ---


@pytest.mark.parametrize(
    "user_id, event_id, page",
    [
        ("jane_doe", "abc-123", "/pricing"),
        ("admin", "def-456", "/"),
        ("test_user", "ghi-789", "/docs"),
    ],
)
def test_update_cassandra_executes_insert(user_id, event_id, page):
    session = MagicMock()
    event = {"user_id": user_id, "event_id": event_id, "page": page, "timestamp": 0}

    update_cassandra(session, event)

    session.execute.assert_called_once()
    args = session.execute.call_args
    query = args[0][0]
    params = args[0][1]

    assert "INSERT INTO pageviews" in query
    assert params == (user_id, event_id, page)


# --- process_messages ---


def test_process_messages_fans_out_to_all_destinations(sample_event):
    """Each consumed message should go to Redis, Cassandra, and RabbitMQ."""
    msg = SimpleNamespace(value=sample_event)
    consumer = [msg]
    redis_client = MagicMock()
    session = MagicMock()
    channel = MagicMock()

    process_messages(consumer, redis_client, session, channel)

    redis_client.incr.assert_called_once()
    redis_client.set.assert_called_once()
    session.execute.assert_called_once()
    channel.basic_publish.assert_called_once()


def test_process_messages_publishes_event_as_json(sample_event):
    msg = SimpleNamespace(value=sample_event)
    consumer = [msg]
    channel = MagicMock()

    process_messages(consumer, MagicMock(), MagicMock(), channel)

    published_body = channel.basic_publish.call_args[1]["body"]
    assert json.loads(published_body) == sample_event


def test_process_messages_handles_multiple_messages():
    events = [
        {"event_id": "1", "user_id": "a", "page": "/", "timestamp": 0},
        {"event_id": "2", "user_id": "b", "page": "/docs", "timestamp": 1},
        {"event_id": "3", "user_id": "c", "page": "/pricing", "timestamp": 2},
    ]
    consumer = [SimpleNamespace(value=e) for e in events]
    redis_client = MagicMock()
    session = MagicMock()
    channel = MagicMock()

    process_messages(consumer, redis_client, session, channel)

    assert redis_client.incr.call_count == 3
    assert session.execute.call_count == 3
    assert channel.basic_publish.call_count == 3


def test_process_messages_empty_consumer():
    """An empty consumer should complete without calling anything."""
    channel = MagicMock()
    redis_client = MagicMock()
    session = MagicMock()

    process_messages([], redis_client, session, channel)

    redis_client.incr.assert_not_called()
    session.execute.assert_not_called()
    channel.basic_publish.assert_not_called()
