import importlib

import pytest


@pytest.mark.parametrize(
    "env_var, env_value, config_attr, expected",
    [
        ("KAFKA_TOPIC", "custom-topic", "KAFKA_TOPIC", "custom-topic"),
        ("KAFKA_STATS_TOPIC", "custom-stats", "KAFKA_STATS_TOPIC", "custom-stats"),
        ("KAFKA_SERVER", "broker:9093", "KAFKA_SERVER", "broker:9093"),
        ("FLINK_WINDOW_SECONDS", "30", "FLINK_WINDOW_SECONDS", 30),
        ("REDIS_HOST", "redis-server", "REDIS_HOST", "redis-server"),
        ("REDIS_PORT", "6380", "REDIS_PORT", 6380),
        ("RABBITMQ_HOST", "rabbit-server", "RABBITMQ_HOST", "rabbit-server"),
        ("RABBITMQ_QUEUE", "custom_queue", "RABBITMQ_QUEUE", "custom_queue"),
        ("CASSANDRA_HOST", "cassandra-server", "CASSANDRA_HOST", "cassandra-server"),
        ("CASSANDRA_KEYSPACE", "custom_ks", "KEYSPACE", "custom_ks"),
        ("ZOOKEEPER_HOSTS", "zk:2181", "ZOOKEEPER_HOSTS", "zk:2181"),
        ("ZOOKEEPER_ROOT", "/custom", "ZOOKEEPER_ROOT", "/custom"),
        ("ZOOKEEPER_TIMEOUT_SECONDS", "5.5", "ZOOKEEPER_TIMEOUT_SECONDS", 5.5),
        ("GRPC_HOST", "127.0.0.1", "GRPC_HOST", "127.0.0.1"),
        ("GRPC_PORT", "50052", "GRPC_PORT", 50052),
        ("GRPC_TARGET", "127.0.0.1:50052", "GRPC_TARGET", "127.0.0.1:50052"),
    ],
)
def test_config_reads_env_vars(monkeypatch, env_var, env_value, config_attr, expected):
    monkeypatch.setenv(env_var, env_value)
    import pipeline.config as config_module

    importlib.reload(config_module)

    assert getattr(config_module, config_attr) == expected


@pytest.mark.parametrize(
    "config_attr, expected",
    [
        ("KAFKA_TOPIC", "pageviews"),
        ("KAFKA_STATS_TOPIC", "pageview_stats"),
        ("KAFKA_SERVER", "localhost:9092"),
        ("FLINK_WINDOW_SECONDS", 10),
        ("REDIS_HOST", "localhost"),
        ("REDIS_PORT", 6379),
        ("RABBITMQ_HOST", "localhost"),
        ("RABBITMQ_QUEUE", "analytics_jobs"),
        ("CASSANDRA_HOST", "localhost"),
        ("KEYSPACE", "pipeline"),
        ("ZOOKEEPER_HOSTS", "localhost:2181"),
        ("ZOOKEEPER_ROOT", "/pipeline"),
        ("ZOOKEEPER_TIMEOUT_SECONDS", 3.0),
        ("GRPC_HOST", "[::]"),
        ("GRPC_PORT", 50051),
        ("GRPC_TARGET", "localhost:50051"),
    ],
)
def test_config_defaults(monkeypatch, config_attr, expected):
    # Clear any env vars that might be set
    env_vars = [
        "KAFKA_TOPIC",
        "KAFKA_STATS_TOPIC",
        "KAFKA_SERVER",
        "FLINK_WINDOW_SECONDS",
        "REDIS_HOST",
        "REDIS_PORT",
        "RABBITMQ_HOST",
        "RABBITMQ_QUEUE",
        "CASSANDRA_HOST",
        "CASSANDRA_KEYSPACE",
        "ZOOKEEPER_HOSTS",
        "ZOOKEEPER_ROOT",
        "ZOOKEEPER_TIMEOUT_SECONDS",
        "GRPC_HOST",
        "GRPC_PORT",
        "GRPC_TARGET",
    ]
    for var in env_vars:
        monkeypatch.delenv(var, raising=False)

    import pipeline.config as config_module

    importlib.reload(config_module)

    assert getattr(config_module, config_attr) == expected


def test_redis_port_is_int(monkeypatch):
    monkeypatch.setenv("REDIS_PORT", "6380")
    import pipeline.config as config_module

    importlib.reload(config_module)

    assert isinstance(config_module.REDIS_PORT, int)


def test_redis_port_invalid_raises(monkeypatch):
    monkeypatch.setenv("REDIS_PORT", "not_a_number")
    import pipeline.config as config_module

    with pytest.raises(ValueError):
        importlib.reload(config_module)
