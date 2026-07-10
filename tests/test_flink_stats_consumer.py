import json

from pipeline.flink_stats_consumer import (
    LATEST_WINDOW_KEY,
    FlinkPageViewStats,
    page_count_key,
    page_window_end_key,
    page_window_start_key,
    stats_from_message,
    update_redis,
)
from pipeline.realtime_events import FLINK_WINDOWS_CHANNEL


def test_stats_from_message_normalizes_values():
    stats = stats_from_message(
        {
            "page": "/pricing",
            "count": "42",
            "window_start": "2026-07-09T12:00:00.000Z",
            "window_end": "2026-07-09T12:00:10.000Z",
        }
    )

    assert stats == FlinkPageViewStats(
        page="/pricing",
        count=42,
        window_start="2026-07-09T12:00:00.000Z",
        window_end="2026-07-09T12:00:10.000Z",
    )


def test_update_redis_writes_latest_page_and_window():
    redis_client = {}
    stats = FlinkPageViewStats(
        page="/docs",
        count=7,
        window_start="2026-07-09T12:00:00.000Z",
        window_end="2026-07-09T12:00:10.000Z",
    )

    class FakeRedis:
        def set(self, key, value):
            redis_client[key] = value

        def publish(self, channel, payload):
            redis_client[channel] = payload

    update_redis(FakeRedis(), stats)

    assert redis_client[page_count_key("/docs")] == 7
    assert redis_client[page_window_start_key("/docs")] == "2026-07-09T12:00:00.000Z"
    assert redis_client[page_window_end_key("/docs")] == "2026-07-09T12:00:10.000Z"
    assert json.loads(redis_client[LATEST_WINDOW_KEY]) == {
        "page": "/docs",
        "count": 7,
        "window_start": "2026-07-09T12:00:00.000Z",
        "window_end": "2026-07-09T12:00:10.000Z",
    }
    assert json.loads(redis_client[FLINK_WINDOWS_CHANNEL]) == {
        "type": "flink_window",
        "page": "/docs",
        "count": 7,
        "window_start": "2026-07-09T12:00:00.000Z",
        "window_end": "2026-07-09T12:00:10.000Z",
    }
