import json
from types import SimpleNamespace

from pipeline.realtime_events import (
    FLINK_WINDOWS_CHANNEL,
    FLINK_WINDOWS_WS_PATH,
    PAGEVIEWS_CHANNEL,
    PAGEVIEWS_WS_PATH,
    REALTIME_PAGE_PATH,
    event_json,
    flink_window_event,
    pageview_event,
)
from pipeline.realtime_page import realtime_page


def test_channels_are_stable():
    assert PAGEVIEWS_CHANNEL == "events:pageviews"
    assert FLINK_WINDOWS_CHANNEL == "events:flink:windows"


def test_realtime_paths_are_stable():
    assert REALTIME_PAGE_PATH == "/realtime"
    assert PAGEVIEWS_WS_PATH == "/ws/pageviews"
    assert FLINK_WINDOWS_WS_PATH == "/ws/flink/windows"


def test_realtime_page_references_websocket_paths():
    page = realtime_page()

    assert PAGEVIEWS_WS_PATH in page
    assert FLINK_WINDOWS_WS_PATH in page


def test_pageview_event_payload():
    event = {
        "event_id": "abc-123",
        "user_id": "jane_doe",
        "page": "/pricing",
        "timestamp": 1700000000.0,
    }

    assert pageview_event(event) == {
        "type": "pageview",
        "event_id": "abc-123",
        "user_id": "jane_doe",
        "page": "/pricing",
        "timestamp": 1700000000.0,
    }


def test_flink_window_event_payload():
    stats = SimpleNamespace(
        page="/docs",
        count="7",
        window_start="2026-07-09T12:00:00.000Z",
        window_end="2026-07-09T12:00:10.000Z",
    )

    assert flink_window_event(stats) == {
        "type": "flink_window",
        "page": "/docs",
        "count": 7,
        "window_start": "2026-07-09T12:00:00.000Z",
        "window_end": "2026-07-09T12:00:10.000Z",
    }


def test_event_json_serializes_payload():
    payload = {"type": "pageview", "page": "/pricing"}

    assert json.loads(event_json(payload)) == payload
