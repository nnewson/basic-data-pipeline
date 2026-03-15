import json
import time
import uuid

from faker import Faker
from kafka import KafkaProducer

from pipeline import get_partition, wait_for_connection
from pipeline.config import KAFKA_PARTITIONS, KAFKA_SERVER, KAFKA_TOPIC

import logging

logger = logging.getLogger("producer")


def create_event(fake: Faker, pages: list[str]) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": fake.user_name(),
        "page": fake.random_element(pages),
        "timestamp": time.time(),
    }


def process_messages(producer: KafkaProducer, fake: Faker, pages: list[str]) -> None:
    while True:
        event = create_event(fake, pages)
        partition = get_partition(event["user_id"], KAFKA_PARTITIONS)

        producer.send(KAFKA_TOPIC, event, partition=partition)

        logger.info(f"Produced (partition {partition}): {event}")

        time.sleep(1)


def main() -> None:
    # Setup Kafka producer
    producer = wait_for_connection(
        "Kafka",
        lambda: KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        ),
    )

    # Setup fake data generator and endpoints
    fake = Faker()
    pages = ["/", "/pricing", "/docs", "/checkout"]

    try:
        process_messages(producer, fake, pages)
    except KeyboardInterrupt:
        logger.info("Shutting down producer")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
