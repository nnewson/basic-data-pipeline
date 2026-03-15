import os

KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "pageviews")
KAFKA_SERVER = os.environ.get("KAFKA_SERVER", "localhost:9092")
KAFKA_PARTITIONS = int(os.environ.get("KAFKA_PARTITIONS", "4"))

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_QUEUE = os.environ.get("RABBITMQ_QUEUE", "analytics_jobs")
RABBITMQ_PARTITIONS = int(os.environ.get("RABBITMQ_PARTITIONS", "4"))

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "localhost")
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "pipeline")
