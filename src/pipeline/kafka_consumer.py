import json
import logging
import os

from cassandra.cluster import Cluster
from kafka import KafkaConsumer
import pika
import redis

from pipeline import get_partition, wait_for_connection
from pipeline.config import (
    CASSANDRA_HOST,
    KEYSPACE,
    KAFKA_SERVER,
    KAFKA_TOPIC,
    RABBITMQ_HOST,
    RABBITMQ_PARTITIONS,
    RABBITMQ_QUEUE,
    REDIS_HOST,
    REDIS_PORT,
)
from pipeline.realtime_events import PAGEVIEWS_CHANNEL, event_json, pageview_event
from pipeline.zookeeper import process_identity, register_ephemeral, runtime_metadata

logger = logging.getLogger("consumer")


def update_redis(redis_client: redis.Redis, event: dict) -> None:
    page = event["page"]
    user = event["user_id"]
    redis_client.incr(f"pageviews:{page}")
    redis_client.set(f"user:last_page:{user}", page)
    redis_client.publish(PAGEVIEWS_CHANNEL, event_json(pageview_event(event)))


def update_cassandra(session, event: dict) -> None:
    session.execute(
        """
        INSERT INTO pageviews (user_id, event_time, event_id, page)
        VALUES (%s, toTimestamp(now()), %s, %s)
        """,
        (event["user_id"], event["event_id"], event["page"]),
    )


def get_queue_name(base: str, partition: int) -> str:
    return f"{base}_{partition}"


def process_messages(
    consumer: KafkaConsumer, redis_client: redis.Redis, session, channel
) -> None:
    for msg in consumer:
        event = msg.value
        logger.info(f"Consumed: {event}")

        update_redis(redis_client, event)
        update_cassandra(session, event)

        # Keep the downstream RabbitMQ queue aligned with the Kafka partitioning
        # scheme so the same username range stays on the same worker lane.
        partition = get_partition(event["user_id"], RABBITMQ_PARTITIONS)
        queue = get_queue_name(RABBITMQ_QUEUE, partition)
        channel.basic_publish(exchange="", routing_key=queue, body=json.dumps(event))


def main() -> None:
    consumer_id = os.environ.get("CONSUMER_ID", process_identity("consumer"))
    consumer = wait_for_connection(
        "Kafka",
        lambda: KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="pipeline-consumer",
            auto_offset_reset="earliest",
        ),
    )

    cluster = Cluster([CASSANDRA_HOST])
    session = wait_for_connection("Cassandra", lambda: cluster.connect(KEYSPACE))

    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    connection = wait_for_connection(
        "RabbitMQ",
        lambda: pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST)),
    )

    channel = connection.channel()
    for i in range(RABBITMQ_PARTITIONS):
        channel.queue_declare(queue=get_queue_name(RABBITMQ_QUEUE, i))

    registration = register_ephemeral(
        "consumers",
        consumer_id,
        runtime_metadata("consumer", consumer_id, {"kafka_topic": KAFKA_TOPIC}),
    )

    try:
        process_messages(consumer, redis_client, session, channel)
    except KeyboardInterrupt:
        logger.info("Shutting down consumer")
    finally:
        consumer.close()
        connection.close()
        redis_client.close()
        cluster.shutdown()
        registration.close()


if __name__ == "__main__":
    main()
