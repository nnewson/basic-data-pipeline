import argparse
import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any
from urllib.error import URLError
from urllib.parse import urlparse, urlunparse
from urllib.request import urlopen

import redis
import websockets
from kafka import KafkaConsumer, KafkaProducer

from pipeline import get_partition
from pipeline.config import (
    FLINK_WINDOW_SECONDS,
    KAFKA_PARTITIONS,
    KAFKA_SERVER,
    KAFKA_STATS_TOPIC,
    KAFKA_TOPIC,
    REDIS_HOST,
    REDIS_PORT,
)
from pipeline.flink_stats_consumer import LATEST_WINDOW_KEY, page_count_key


@dataclass(frozen=True)
class SmokeResult:
    page: str
    count: int
    window_start: str
    window_end: str


def make_partition_pages() -> dict[str, str]:
    suffix = uuid.uuid4().hex[:8]
    return {
        "alice": f"/pricing-smoke-{suffix}-p0",
        "harry": f"/pricing-smoke-{suffix}-p1",
        "nancy": f"/pricing-smoke-{suffix}-p2",
        "zara": f"/pricing-smoke-{suffix}-p3",
    }


def make_stats_consumer(timeout_ms: int) -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_STATS_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        group_id=f"flink-smoke-test-{uuid.uuid4()}",
        auto_offset_reset="earliest",
        consumer_timeout_ms=timeout_ms,
    )


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def send_test_events(
    producer: KafkaProducer, user_pages: dict[str, str], events_per_partition: int
) -> None:
    base_ts = time.time()

    for user_id, page in user_pages.items():
        for _ in range(events_per_partition):
            producer.send(
                KAFKA_TOPIC,
                {
                    "event_id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "page": page,
                    "timestamp": base_ts,
                },
                partition=get_partition(user_id, KAFKA_PARTITIONS),
            )

    # Flink only closes an event-time window when the watermark moves past it.
    # These later events advance each partition far enough for the smoke pages
    # above to appear in the windowed output topic.
    for user_id in user_pages:
        producer.send(
            KAFKA_TOPIC,
            {
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "page": "/",
                "timestamp": base_ts + (FLINK_WINDOW_SECONDS * 2),
            },
            partition=get_partition(user_id, KAFKA_PARTITIONS),
        )

    producer.flush()


def result_from_message(message: dict[str, Any], page: str) -> SmokeResult | None:
    if message.get("page") != page:
        return None
    return SmokeResult(
        page=str(message["page"]),
        count=int(message["count"]),
        window_start=str(message["window_start"]),
        window_end=str(message["window_end"]),
    )


def wait_for_flink_results(
    consumer: KafkaConsumer,
    pages: set[str],
    expected_count: int,
    timeout_seconds: int,
) -> dict[str, SmokeResult]:
    deadline = time.monotonic() + timeout_seconds
    results: dict[str, SmokeResult] = {}

    while time.monotonic() < deadline:
        for record in consumer.poll(timeout_ms=1000).values():
            for message in record:
                page = str(message.value.get("page"))
                if page not in pages:
                    continue
                result = result_from_message(message.value, page)
                if result and result.count >= expected_count:
                    results[page] = result
                    if set(results) == pages:
                        return results

    raise TimeoutError(
        f"Timed out waiting for {sorted(pages - set(results))} "
        f"with count >= {expected_count} "
        f"on Kafka topic {KAFKA_STATS_TOPIC}. Is the Flink job running?"
    )


def wait_for_redis_results(
    pages: set[str], expected_count: int, timeout_seconds: int
) -> None:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    deadline = time.monotonic() + timeout_seconds
    try:
        while time.monotonic() < deadline:
            latest_window = redis_client.get(LATEST_WINDOW_KEY)
            seen_pages = set()
            for page in pages:
                count_value = redis_client.get(page_count_key(page))
                if count_value and int(count_value) >= expected_count:
                    seen_pages.add(page)
            if seen_pages == pages and latest_window:
                return
            time.sleep(1)
    finally:
        redis_client.close()

    raise TimeoutError(
        f"Timed out waiting for Redis keys for {sorted(pages)}. "
        "Is uv run flink-stats-consumer running?"
    )


def wait_for_api_result(
    api_url: str, page: str, expected_count: int, timeout_seconds: int
) -> None:
    page_route = page.lstrip("/")
    endpoint = f"{api_url.rstrip('/')}/flink/counts/page/{page_route}"
    deadline = time.monotonic() + timeout_seconds
    last_error: str | None = None

    while time.monotonic() < deadline:
        try:
            with urlopen(endpoint, timeout=3) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if int(payload.get("count", 0)) >= expected_count:
                return
            last_error = f"latest response was {payload}"
        except (OSError, URLError, ValueError) as exc:
            last_error = str(exc)
        time.sleep(1)

    raise TimeoutError(
        f"Timed out waiting for FastAPI endpoint {endpoint}. {last_error or ''}".strip()
    )


def wait_for_api_results(
    api_url: str, pages: set[str], expected_count: int, timeout_seconds: int
) -> None:
    for page in pages:
        wait_for_api_result(api_url, page, expected_count, timeout_seconds)


def websocket_url(api_url: str, path: str) -> str:
    parsed = urlparse(api_url)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return urlunparse((scheme, parsed.netloc, path, "", "", ""))


def websocket_result_from_message(
    message: dict[str, Any], expected_type: str, expected_count: int
) -> str | None:
    if message.get("type") != expected_type:
        return None
    page = message.get("page")
    if not isinstance(page, str):
        return None
    if expected_type == "flink_window" and int(message.get("count", 0)) < expected_count:
        return None
    return page


async def wait_for_websocket_results(
    ws_url: str,
    pages: set[str],
    expected_type: str,
    expected_count: int,
    timeout_seconds: int,
    ready: asyncio.Event,
) -> set[str]:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    seen_pages: set[str] = set()

    async with websockets.connect(ws_url) as websocket:
        ready.set()
        while asyncio.get_running_loop().time() < deadline:
            remaining = deadline - asyncio.get_running_loop().time()
            raw_message = await asyncio.wait_for(websocket.recv(), timeout=remaining)
            try:
                message = json.loads(raw_message)
            except json.JSONDecodeError:
                continue

            page = websocket_result_from_message(
                message, expected_type, expected_count
            )
            if page in pages:
                seen_pages.add(page)
                if seen_pages == pages:
                    return seen_pages

    return seen_pages


async def run_websocket_checked_smoke(
    api_url: str,
    producer: KafkaProducer,
    consumer: KafkaConsumer,
    user_pages: dict[str, str],
    pages: set[str],
    events_per_partition: int,
    timeout_seconds: int,
    check_redis: bool,
) -> None:
    pageview_ready = asyncio.Event()
    flink_ready = asyncio.Event()
    pageview_task = asyncio.create_task(
        wait_for_websocket_results(
            websocket_url(api_url, "/ws/pageviews"),
            pages,
            "pageview",
            1,
            timeout_seconds,
            pageview_ready,
        )
    )
    flink_task = asyncio.create_task(
        wait_for_websocket_results(
            websocket_url(api_url, "/ws/flink/windows"),
            pages,
            "flink_window",
            events_per_partition,
            timeout_seconds,
            flink_ready,
        )
    )

    await asyncio.wait_for(pageview_ready.wait(), timeout=timeout_seconds)
    await asyncio.wait_for(flink_ready.wait(), timeout=timeout_seconds)
    send_test_events(producer, user_pages, events_per_partition)
    print(
        f"Sent {events_per_partition} events to each of "
        f"{KAFKA_PARTITIONS} Kafka partitions."
    )

    results = await asyncio.to_thread(
        wait_for_flink_results, consumer, pages, events_per_partition, timeout_seconds
    )
    print_flink_results(results)

    if check_redis:
        await asyncio.to_thread(
            wait_for_redis_results, pages, events_per_partition, timeout_seconds
        )
        print("Redis bridge output ok.")

    await asyncio.to_thread(
        wait_for_api_results, api_url, pages, events_per_partition, timeout_seconds
    )
    print(f"FastAPI output ok: {api_url}")

    try:
        pageview_pages, flink_pages = await asyncio.gather(pageview_task, flink_task)
    finally:
        pageview_task.cancel()
        flink_task.cancel()

    if pageview_pages != pages:
        raise TimeoutError(
            f"Timed out waiting for pageview WebSocket pages "
            f"{sorted(pages - pageview_pages)}."
        )
    if flink_pages != pages:
        raise TimeoutError(
            f"Timed out waiting for Flink WebSocket pages {sorted(pages - flink_pages)}."
        )

    print("WebSocket output ok:")
    print(f"  {websocket_url(api_url, '/ws/pageviews')}")
    print(f"  {websocket_url(api_url, '/ws/flink/windows')}")


def print_flink_results(results: dict[str, SmokeResult]) -> None:
    print("Flink output ok:")
    for result in sorted(results.values(), key=lambda item: item.page):
        print(
            f"  {result.page} count={result.count} "
            f"window={result.window_start}..{result.window_end}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test the Flink sidecar path.")
    parser.add_argument("--count", type=int, default=40)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--check-redis", action="store_true")
    parser.add_argument("--api-url", default=None)
    parser.add_argument(
        "--check-websocket",
        action="store_true",
        help=(
            "Verify /ws/pageviews and /ws/flink/windows. Requires --api-url and "
            "the API, kafka consumers, and flink-stats-consumer to be running."
        ),
    )
    return parser.parse_args()


def main() -> None:
    logging.getLogger("kafka").setLevel(logging.CRITICAL)
    args = parse_args()

    if KAFKA_PARTITIONS != 4:
        raise SystemExit(
            f"Expected KAFKA_PARTITIONS=4 for this smoke test, got {KAFKA_PARTITIONS}."
        )
    if args.check_websocket and not args.api_url:
        raise SystemExit("--check-websocket requires --api-url.")

    user_pages = make_partition_pages()
    pages = set(user_pages.values())
    events_per_partition = max(args.count // KAFKA_PARTITIONS, 1)
    print("Using smoke-test pages:")
    for user_id, page in user_pages.items():
        partition = get_partition(user_id, KAFKA_PARTITIONS)
        print(f"  partition {partition}: {page}")

    producer = make_producer()
    consumer = make_stats_consumer(args.timeout * 1000)
    try:
        if args.check_websocket:
            asyncio.run(
                run_websocket_checked_smoke(
                    args.api_url,
                    producer,
                    consumer,
                    user_pages,
                    pages,
                    events_per_partition,
                    args.timeout,
                    args.check_redis,
                )
            )
            return

        send_test_events(producer, user_pages, events_per_partition)
        print(
            f"Sent {events_per_partition} events to each of "
            f"{KAFKA_PARTITIONS} Kafka partitions."
        )

        results = wait_for_flink_results(
            consumer, pages, events_per_partition, args.timeout
        )
        print_flink_results(results)

        if args.check_redis:
            wait_for_redis_results(pages, events_per_partition, args.timeout)
            print("Redis bridge output ok.")

        if args.api_url:
            wait_for_api_results(args.api_url, pages, events_per_partition, args.timeout)
            print(f"FastAPI output ok: {args.api_url}")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
