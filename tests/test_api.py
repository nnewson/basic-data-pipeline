from collections import namedtuple
from datetime import datetime
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from pipeline.api import app, get_redis, get_session


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
    assert data["page"] == page
    assert data["count"] == expected_count
    mock_redis.get.assert_called_once_with(f"pageviews:{page}")


def test_page_count_zero_value(client, mock_redis):
    mock_redis.get.return_value = "0"

    response = client.get("/counts/page/home")

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
