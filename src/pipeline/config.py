import os

KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "pageviews")
KAFKA_STATS_TOPIC = os.environ.get("KAFKA_STATS_TOPIC", "pageview_stats")
KAFKA_SERVER = os.environ.get("KAFKA_SERVER", "localhost:9092")
KAFKA_PARTITIONS = int(os.environ.get("KAFKA_PARTITIONS", "4"))
FLINK_WINDOW_SECONDS = int(os.environ.get("FLINK_WINDOW_SECONDS", "10"))
FLINK_JOB_SUBMIT_INTERVAL_SECONDS = int(
    os.environ.get("FLINK_JOB_SUBMIT_INTERVAL_SECONDS", "30")
)
FLINK_PAGEVIEW_STATS_JOB_NAME = os.environ.get(
    "FLINK_PAGEVIEW_STATS_JOB_NAME", "pageview-stats"
)
FLINK_PAGEVIEW_STATS_JOB_PATH = os.environ.get(
    "FLINK_PAGEVIEW_STATS_JOB_PATH",
    "/opt/pipeline/src/pipeline/flink_pageview_stats.py",
)
FLINK_JOBMANAGER_SERVICE = os.environ.get(
    "FLINK_JOBMANAGER_SERVICE", "flink-jobmanager"
)

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_QUEUE = os.environ.get("RABBITMQ_QUEUE", "analytics_jobs")
RABBITMQ_PARTITIONS = int(os.environ.get("RABBITMQ_PARTITIONS", "4"))

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "localhost")
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "pipeline")

ZOOKEEPER_HOSTS = os.environ.get("ZOOKEEPER_HOSTS", "localhost:2181")
ZOOKEEPER_ROOT = os.environ.get("ZOOKEEPER_ROOT", "/pipeline")
ZOOKEEPER_TIMEOUT_SECONDS = float(os.environ.get("ZOOKEEPER_TIMEOUT_SECONDS", "3"))
