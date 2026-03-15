import json
import logging
from functools import partial

import pika
import redis

from pipeline.config import REDIS_HOST, REDIS_PORT, RABBITMQ_HOST, RABBITMQ_QUEUE

logger = logging.getLogger("worker")


def process_job(redis_client: redis.Redis, ch, method, properties, body) -> None:
    job = json.loads(body)
    event_id = job["event_id"]
    logger.info(f"Processing job: {event_id}")
    redis_client.set(f"job:{event_id}", "processed")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

        connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))

        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE)

        channel.basic_consume(
            queue=RABBITMQ_QUEUE,
            on_message_callback=partial(process_job, redis_client),
        )

        logger.info("Worker started")
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Shutting down worker")
        connection.close()
        redis_client.close()


if __name__ == "__main__":
    main()
