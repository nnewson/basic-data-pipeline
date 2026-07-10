import json
from typing import Any

PAGEVIEWS_CHANNEL = "events:pageviews"
FLINK_WINDOWS_CHANNEL = "events:flink:windows"


def pageview_event(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "pageview",
        "event_id": str(event["event_id"]),
        "user_id": str(event["user_id"]),
        "page": str(event["page"]),
        "timestamp": event.get("timestamp"),
    }


def flink_window_event(stats: Any) -> dict[str, Any]:
    return {
        "type": "flink_window",
        "page": str(stats.page),
        "count": int(stats.count),
        "window_start": str(stats.window_start),
        "window_end": str(stats.window_end),
    }


def event_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload)
