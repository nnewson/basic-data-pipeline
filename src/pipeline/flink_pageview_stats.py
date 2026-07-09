import logging
import os
from pathlib import Path

from pyflink.table import EnvironmentSettings, TableEnvironment

from pipeline.config import (
    FLINK_WINDOW_SECONDS,
    KAFKA_SERVER,
    KAFKA_STATS_TOPIC,
    KAFKA_TOPIC,
)

logger = logging.getLogger("flink-pageview-stats")


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def file_uri(path: str) -> str:
    return Path(path).expanduser().resolve().as_uri()


def configure_table_environment(table_env: TableEnvironment) -> None:
    table_env.get_config().set("pipeline.name", "pageview-stats")
    table_env.get_config().set("table.local-time-zone", "UTC")
    # Small demos often leave some Kafka partitions quiet. Without an idle
    # timeout, those partitions can hold back the global event-time watermark.
    table_env.get_config().set("table.exec.source.idle-timeout", "5 s")

    # Docker puts the Kafka connector JAR in /opt/flink/lib. This override is
    # only for host-side experiments where the connector lives elsewhere.
    connector_jar = os.environ.get("FLINK_KAFKA_CONNECTOR_JAR")
    if connector_jar:
        table_env.get_config().set("pipeline.jars", file_uri(connector_jar))


def create_source_table(table_env: TableEnvironment) -> None:
    table_env.execute_sql(
        f"""
        CREATE TABLE pageviews (
            event_id STRING,
            user_id STRING,
            page STRING,
            `timestamp` DOUBLE,
            -- Producer timestamps are epoch seconds; Flink event time expects
            -- timestamp-with-local-time-zone values at millisecond precision.
            event_time AS TO_TIMESTAMP_LTZ(CAST(`timestamp` * 1000 AS BIGINT), 3),
            WATERMARK FOR event_time AS event_time
        ) WITH (
            'connector' = 'kafka',
            'topic' = {sql_string(KAFKA_TOPIC)},
            'properties.bootstrap.servers' = {sql_string(KAFKA_SERVER)},
            'properties.group.id' = 'flink-pageview-stats',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
        """
    )


def create_sink_table(table_env: TableEnvironment) -> None:
    table_env.execute_sql(
        f"""
        CREATE TABLE pageview_stats (
            page STRING,
            window_start STRING,
            window_end STRING,
            `count` BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = {sql_string(KAFKA_STATS_TOPIC)},
            'properties.bootstrap.servers' = {sql_string(KAFKA_SERVER)},
            'format' = 'json'
        )
        """
    )


def execute_stats_job(table_env: TableEnvironment):
    return table_env.execute_sql(
        f"""
        INSERT INTO pageview_stats
        SELECT
            page,
            DATE_FORMAT(
                TUMBLE_START(event_time, INTERVAL '{FLINK_WINDOW_SECONDS}' SECOND),
                'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''
            ) AS window_start,
            DATE_FORMAT(
                TUMBLE_END(event_time, INTERVAL '{FLINK_WINDOW_SECONDS}' SECOND),
                'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''
            ) AS window_end,
            COUNT(*) AS `count`
        FROM pageviews
        GROUP BY page, TUMBLE(event_time, INTERVAL '{FLINK_WINDOW_SECONDS}' SECOND)
        """
    )


def main() -> None:
    logger.info(
        "Starting Flink pageview stats job: %s -> %s via %s",
        KAFKA_TOPIC,
        KAFKA_STATS_TOPIC,
        KAFKA_SERVER,
    )
    settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(settings)
    configure_table_environment(table_env)
    create_source_table(table_env)
    create_sink_table(table_env)
    execute_stats_job(table_env).wait()


if __name__ == "__main__":
    main()
