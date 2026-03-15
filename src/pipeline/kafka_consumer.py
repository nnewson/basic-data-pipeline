import json
import redis
import pika

from kafka import KafkaConsumer
from cassandra.cluster import Cluster

import logging

from pipeline import wait_for_connection
from pipeline.config import (
    KAFKA_TOPIC,
    KAFKA_SERVER,
    REDIS_HOST,
    REDIS_PORT,
    CASSANDRA_HOST,
    KEYSPACE,
    RABBITMQ_HOST,
    RABBITMQ_QUEUE,
)

logger = logging.getLogger("consumer")


def update_redis(redis_client: redis.Redis, event: dict) -> None:
    page = event["page"]
    user = event["user_id"]
    redis_client.incr(f"pageviews:{page}")
    redis_client.set(f"user:last_page:{user}", page)


def update_cassandra(session, event: dict) -> None:
    session.execute(
        """
        INSERT INTO pageviews (user_id, event_time, event_id, page)
        VALUES (%s, toTimestamp(now()), %s, %s)
        """,
        (event["user_id"], event["event_id"], event["page"]),
    )


def process_messages(
    consumer: KafkaConsumer, redis_client: redis.Redis, session, channel
) -> None:
    for msg in consumer:
        event = msg.value
        logger.info(f"Consumed: {event}")

        update_redis(redis_client, event)
        update_cassandra(session, event)

        channel.basic_publish(
            exchange="", routing_key=RABBITMQ_QUEUE, body=json.dumps(event)
        )


def main() -> None:
    # Set up Kafka consumer
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

    # Setup Cassandra session
    cluster = Cluster([CASSANDRA_HOST])
    session = wait_for_connection("Cassandra", lambda: cluster.connect(KEYSPACE))

    # Setup Redis client
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    connection = wait_for_connection(
        "RabbitMQ",
        lambda: pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST)),
    )

    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE)

    try:
        # Process messages from Kafka to Cassandra and Redis, then publish to RabbitMQ
        process_messages(consumer, redis_client, session, channel)
    except KeyboardInterrupt:
        logger.info("Shutting down consumer")
    finally:
        consumer.close()
        connection.close()
        redis_client.close()
        cluster.shutdown()


if __name__ == "__main__":
    main()
