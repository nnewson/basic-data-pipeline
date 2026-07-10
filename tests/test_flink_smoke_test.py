import pytest

from pipeline.flink_smoke_test import (
    result_from_message,
    websocket_result_from_message,
    websocket_url,
)


def test_result_from_message_returns_matching_page():
    result = result_from_message(
        {
            "page": "/pricing-smoke",
            "count": 40,
            "window_start": "2026-07-09T12:00:00.000Z",
            "window_end": "2026-07-09T12:00:10.000Z",
        },
        "/pricing-smoke",
    )

    assert result is not None
    assert result.page == "/pricing-smoke"
    assert result.count == 40
    assert result.window_start == "2026-07-09T12:00:00.000Z"
    assert result.window_end == "2026-07-09T12:00:10.000Z"


def test_result_from_message_ignores_other_pages():
    result = result_from_message(
        {
            "page": "/docs",
            "count": 40,
            "window_start": "2026-07-09T12:00:00.000Z",
            "window_end": "2026-07-09T12:00:10.000Z",
        },
        "/pricing-smoke",
    )

    assert result is None


@pytest.mark.parametrize(
    "api_url, path, expected",
    [
        (
            "http://localhost:8000",
            "/ws/pageviews",
            "ws://localhost:8000/ws/pageviews",
        ),
        (
            "https://pipeline.example.com/api",
            "/ws/flink/windows",
            "wss://pipeline.example.com/ws/flink/windows",
        ),
    ],
)
def test_websocket_url(api_url, path, expected):
    assert websocket_url(api_url, path) == expected


def test_websocket_result_from_message_accepts_pageview():
    page = websocket_result_from_message(
        {
            "type": "pageview",
            "page": "/pricing-smoke",
            "event_id": "abc",
        },
        "pageview",
        1,
    )

    assert page == "/pricing-smoke"


def test_websocket_result_from_message_requires_flink_count():
    page = websocket_result_from_message(
        {
            "type": "flink_window",
            "page": "/pricing-smoke",
            "count": 3,
        },
        "flink_window",
        4,
    )

    assert page is None
