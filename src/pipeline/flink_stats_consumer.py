import json
import logging
from dataclasses import dataclass
from typing import Any

import redis
from kafka import KafkaConsumer

from pipeline import wait_for_connection
from pipeline.config import KAFKA_SERVER, KAFKA_STATS_TOPIC, REDIS_HOST, REDIS_PORT
from pipeline.realtime_events import (
    FLINK_WINDOWS_CHANNEL,
    event_json,
    flink_window_event,
)

logger = logging.getLogger("flink-stats-consumer")

LATEST_WINDOW_KEY = "flink:windows:latest"


@dataclass
class FlinkPageViewStats:
    page: str
    count: int
    window_start: str
    window_end: str


def stats_from_message(message: dict[str, Any]) -> FlinkPageViewStats:
    return FlinkPageViewStats(
        page=str(message["page"]),
        count=int(message["count"]),
        window_start=str(message["window_start"]),
        window_end=str(message["window_end"]),
    )


def page_count_key(page: str) -> str:
    return f"flink:pageviews:{page}:latest"


def page_window_start_key(page: str) -> str:
    return f"flink:pageviews:{page}:window_start"


def page_window_end_key(page: str) -> str:
    return f"flink:pageviews:{page}:window_end"


def update_redis(redis_client: redis.Redis, stats: FlinkPageViewStats) -> None:
    payload = {
        "page": stats.page,
        "count": stats.count,
        "window_start": stats.window_start,
        "window_end": stats.window_end,
    }
    # Redis is a latest-value cache for the API. The append-only history remains
    # in Kafka's pageview_stats topic.
    redis_client.set(page_count_key(stats.page), stats.count)
    redis_client.set(page_window_start_key(stats.page), stats.window_start)
    redis_client.set(page_window_end_key(stats.page), stats.window_end)
    redis_client.set(LATEST_WINDOW_KEY, json.dumps(payload))
    redis_client.publish(FLINK_WINDOWS_CHANNEL, event_json(flink_window_event(stats)))


def process_messages(consumer: KafkaConsumer, redis_client: redis.Redis) -> None:
    for msg in consumer:
        stats = stats_from_message(msg.value)
        update_redis(redis_client, stats)
        logger.info("Stored Flink stats: %s", stats)


def main() -> None:
    consumer = wait_for_connection(
        "Kafka",
        lambda: KafkaConsumer(
            KAFKA_STATS_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="flink-stats-consumer",
            auto_offset_reset="earliest",
        ),
    )
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

    try:
        process_messages(consumer, redis_client)
    except KeyboardInterrupt:
        logger.info("Shutting down Flink stats consumer")
    finally:
        consumer.close()
        redis_client.close()


if __name__ == "__main__":
    main()
