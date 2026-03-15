import importlib

import pytest


@pytest.mark.parametrize(
    "env_var, env_value, config_attr, expected",
    [
        ("KAFKA_TOPIC", "custom-topic", "KAFKA_TOPIC", "custom-topic"),
        ("KAFKA_SERVER", "broker:9093", "KAFKA_SERVER", "broker:9093"),
        ("REDIS_HOST", "redis-server", "REDIS_HOST", "redis-server"),
        ("REDIS_PORT", "6380", "REDIS_PORT", 6380),
        ("RABBITMQ_HOST", "rabbit-server", "RABBITMQ_HOST", "rabbit-server"),
        ("RABBITMQ_QUEUE", "custom_queue", "RABBITMQ_QUEUE", "custom_queue"),
        ("CASSANDRA_HOST", "cassandra-server", "CASSANDRA_HOST", "cassandra-server"),
        ("CASSANDRA_KEYSPACE", "custom_ks", "KEYSPACE", "custom_ks"),
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
        ("KAFKA_SERVER", "localhost:9092"),
        ("REDIS_HOST", "localhost"),
        ("REDIS_PORT", 6379),
        ("RABBITMQ_HOST", "localhost"),
        ("RABBITMQ_QUEUE", "analytics_jobs"),
        ("CASSANDRA_HOST", "localhost"),
        ("KEYSPACE", "pipeline"),
    ],
)
def test_config_defaults(monkeypatch, config_attr, expected):
    # Clear any env vars that might be set
    env_vars = [
        "KAFKA_TOPIC",
        "KAFKA_SERVER",
        "REDIS_HOST",
        "REDIS_PORT",
        "RABBITMQ_HOST",
        "RABBITMQ_QUEUE",
        "CASSANDRA_HOST",
        "CASSANDRA_KEYSPACE",
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
