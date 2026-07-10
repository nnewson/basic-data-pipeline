import asyncio
from collections import namedtuple
from datetime import datetime
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from pipeline.api import WebSocketConnectionManager, app, get_redis, get_session
from pipeline.flink_stats_consumer import (
    LATEST_WINDOW_KEY,
    page_count_key,
    page_window_end_key,
    page_window_start_key,
)
from pipeline.realtime_events import (
    FLINK_WINDOWS_WS_PATH,
    PAGEVIEWS_WS_PATH,
    REALTIME_PAGE_PATH,
)


@pytest.fixture
def mock_redis():
    return MagicMock()


@pytest.fixture
def mock_session():
    return MagicMock()


@pytest.fixture
def client(mock_redis, mock_session):
    app.dependency_overrides[get_redis] = lambda: mock_redis
    app.dependency_overrides[get_session] = lambda: mock_session
    yield TestClient(app)
    app.dependency_overrides.clear()


# --- page_count ---


def test_root_returns_ok(client):
    response = client.get("/")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_returns_ok(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_realtime_returns_websocket_test_page(client):
    response = client.get(REALTIME_PAGE_PATH)

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert PAGEVIEWS_WS_PATH in response.text
    assert FLINK_WINDOWS_WS_PATH in response.text


def test_zookeeper_status_returns_snapshot(client, monkeypatch):
    expected = {
        "connected": True,
        "zookeeper_root": "/pipeline",
        "leader": {"coordinator_id": "coordinator-1"},
        "coordinators": ["coordinator-1"],
        "workers": [],
        "consumers": [],
        "flink": {"active_job": None},
        "control": {"paused": False},
        "summary": {
            "has_leader": True,
            "coordinator_count": 1,
            "worker_count": 0,
            "consumer_count": 0,
            "active_flink_job_id": None,
        },
    }

    monkeypatch.setattr("pipeline.api.read_zookeeper_status", lambda: expected)

    response = client.get("/zookeeper/status")

    assert response.status_code == 200
    assert response.json() == expected


@pytest.mark.parametrize(
    "page, redis_value, expected_count",
    [
        ("pricing", "42", 42),
        ("docs", "1", 1),
        ("checkout", "999", 999),
        ("pricing", None, 0),
    ],
)
def test_page_count(client, mock_redis, page, redis_value, expected_count):
    mock_redis.get.return_value = redis_value

    response = client.get(f"/counts/page/{page}")

    assert response.status_code == 200
    data = response.json()
    assert data["page"] == f"/{page}"
    assert data["count"] == expected_count
    mock_redis.get.assert_called_once_with(f"pageviews:/{page}")


def test_page_count_zero_value(client, mock_redis):
    mock_redis.get.return_value = "0"

    response = client.get("/counts/page/home")

    assert response.json()["page"] == "/home"
    assert response.json()["count"] == 0


# --- last_page ---


@pytest.mark.parametrize(
    "user_id, redis_value, expected_page",
    [
        ("jane_doe", "/pricing", "/pricing"),
        ("admin", "/", "/"),
        ("new_user", None, None),
    ],
)
def test_last_page(client, mock_redis, user_id, redis_value, expected_page):
    mock_redis.get.return_value = redis_value

    response = client.get(f"/users/{user_id}/last-page")

    assert response.status_code == 200
    data = response.json()
    assert data["user"] == user_id
    assert data["last_page"] == expected_page
    mock_redis.get.assert_called_once_with(f"user:last_page:{user_id}")


# --- user_events ---


PageviewRow = namedtuple("PageviewRow", ["user_id", "event_id", "event_time", "page"])


@pytest.mark.parametrize(
    "num_events",
    [0, 1, 3],
)
def test_user_events_returns_correct_count(client, mock_session, num_events):
    rows = [
        PageviewRow(
            user_id="jane",
            event_id=UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890"),
            event_time=datetime(2026, 1, 1, 12, 0, i),
            page="/docs",
        )
        for i in range(num_events)
    ]
    mock_session.execute.return_value = rows

    response = client.get("/events/jane")

    assert response.status_code == 200
    assert len(response.json()) == num_events


def test_user_events_response_shape(client, mock_session):
    rows = [
        PageviewRow(
            user_id="jane",
            event_id=UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890"),
            event_time=datetime(2026, 1, 1, 12, 0, 0),
            page="/pricing",
        ),
    ]
    mock_session.execute.return_value = rows

    response = client.get("/events/jane")

    data = response.json()
    event = data[0]
    assert event["user_id"] == "jane"
    assert event["event_id"] == "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    assert event["page"] == "/pricing"
    assert "event_time" in event


def test_user_events_passes_user_id_to_query(client, mock_session):
    mock_session.execute.return_value = []

    client.get("/events/some_user")

    args = mock_session.execute.call_args[0]
    assert "user_id=%s" in args[0]
    assert args[1] == ("some_user",)


# --- Flink stats ---


def test_flink_page_count_returns_latest_window(client, mock_redis):
    mock_redis.get.side_effect = [
        "42",
        "2026-07-09T12:00:00.000Z",
        "2026-07-09T12:00:10.000Z",
    ]

    response = client.get("/flink/counts/page/pricing")

    assert response.status_code == 200
    assert response.json() == {
        "page": "/pricing",
        "count": 42,
        "window_start": "2026-07-09T12:00:00.000Z",
        "window_end": "2026-07-09T12:00:10.000Z",
    }
    assert [call.args[0] for call in mock_redis.get.call_args_list] == [
        page_count_key("/pricing"),
        page_window_start_key("/pricing"),
        page_window_end_key("/pricing"),
    ]


def test_flink_page_count_defaults_to_zero(client, mock_redis):
    mock_redis.get.side_effect = [None, None, None]

    response = client.get("/flink/counts/page/pricing")

    assert response.status_code == 200
    assert response.json() == {
        "page": "/pricing",
        "count": 0,
        "window_start": None,
        "window_end": None,
    }


def test_flink_latest_window_returns_latest_record(client, mock_redis):
    mock_redis.get.return_value = (
        '{"page": "/docs", "count": 7, '
        '"window_start": "2026-07-09T12:00:00.000Z", '
        '"window_end": "2026-07-09T12:00:10.000Z"}'
    )

    response = client.get("/flink/windows/latest")

    assert response.status_code == 200
    assert response.json() == {
        "page": "/docs",
        "count": 7,
        "window_start": "2026-07-09T12:00:00.000Z",
        "window_end": "2026-07-09T12:00:10.000Z",
    }
    mock_redis.get.assert_called_once_with(LATEST_WINDOW_KEY)


def test_flink_latest_window_returns_null_when_empty(client, mock_redis):
    mock_redis.get.return_value = None

    response = client.get("/flink/windows/latest")

    assert response.status_code == 200
    assert response.json() is None


# --- WebSockets ---


class FakeWebSocket:
    def __init__(self):
        self.accepted = False
        self.messages = []

    async def accept(self):
        self.accepted = True

    async def send_text(self, message):
        self.messages.append(message)


class FailingWebSocket(FakeWebSocket):
    async def send_text(self, message):
        raise RuntimeError("send failed")


def test_websocket_connection_manager_broadcasts_by_channel():
    async def run():
        manager = WebSocketConnectionManager()
        pageviews = FakeWebSocket()
        flink_windows = FakeWebSocket()

        await manager.connect("events:pageviews", pageviews)
        await manager.connect("events:flink:windows", flink_windows)
        await manager.broadcast("events:pageviews", '{"type": "pageview"}')

        assert pageviews.accepted is True
        assert flink_windows.accepted is True
        assert pageviews.messages == ['{"type": "pageview"}']
        assert flink_windows.messages == []

    asyncio.run(run())


def test_websocket_connection_manager_drops_failed_connections():
    async def run():
        manager = WebSocketConnectionManager()
        websocket = FailingWebSocket()

        await manager.connect("events:pageviews", websocket)
        await manager.broadcast("events:pageviews", '{"type": "pageview"}')

        assert "events:pageviews" not in manager.active_connections

    asyncio.run(run())
