import json
import logging
import os
from functools import partial

import pika
import redis

from pipeline import wait_for_connection
from pipeline.config import REDIS_HOST, REDIS_PORT, RABBITMQ_HOST, RABBITMQ_QUEUE
from pipeline.zookeeper import process_identity, register_ephemeral, runtime_metadata

logger = logging.getLogger("worker")


def process_job(redis_client: redis.Redis, ch, method, properties, body) -> None:
    job = json.loads(body)
    event_id = job["event_id"]
    logger.info(f"Processing job: {event_id}")
    redis_client.set(f"job:{event_id}", "processed")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    worker_id = os.environ.get("WORKER_ID", process_identity("worker"))
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

    connection = wait_for_connection(
        "RabbitMQ",
        lambda: pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST)),
    )

    channel = connection.channel()
    # Each worker process is pointed at one queue with RABBITMQ_QUEUE, for
    # example analytics_jobs_0, analytics_jobs_1, and so on.
    channel.queue_declare(queue=RABBITMQ_QUEUE)

    channel.basic_consume(
        queue=RABBITMQ_QUEUE,
        on_message_callback=partial(process_job, redis_client),
    )

    registration = register_ephemeral(
        "workers",
        worker_id,
        runtime_metadata("worker", worker_id, {"rabbitmq_queue": RABBITMQ_QUEUE}),
    )

    logger.info("Worker started")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Shutting down worker")
    finally:
        connection.close()
        redis_client.close()
        registration.close()


if __name__ == "__main__":
    main()
