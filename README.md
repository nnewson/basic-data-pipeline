[![CI](https://github.com/nnewson/basic-data-pipeline/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/nnewson/basic-data-pipeline/actions/workflows/ci.yml)

# Basic Data Pipeline

This started as a simple sandbox to play with system-level components and how
to glue them together with Python. It now serves as a local reference project
for Kafka, RabbitMQ, Redis, Cassandra, Flink, FastAPI WebSockets, gRPC, and
ZooKeeper coordination.

---

A demo project that wires together Kafka, RabbitMQ, Redis, and Cassandra into a
simple event-processing pipeline. Built as a reference example for learning how
these technologies connect.

## Architecture

```text
                                      +--> Python consumers (x4) --> Redis (counters)
                                      |            |            +--> Cassandra (events)
                                      |            |            +--> RabbitMQ queues (x4)
                                      |            |            |            |
Producer --> Kafka: pageviews (4p) ---+            |            |            v
                                      |            |            |   Workers (x4) --> Redis (jobs)
                                      |            |            |
                                      |            |            +--> Redis pub/sub: events:pageviews
                                      |            |
                                      +--> PyFlink pageview-stats job
                                                   |
                                                   v
                                          Kafka: pageview_stats
                                                   |
                                                   v
                                          flink-stats-consumer
                                                   |
                                                   +--> Redis (flink:* stats)
                                                   +--> Redis pub/sub: events:flink:windows

Serving/read side:
  Redis (counters, jobs, flink:* stats) ----------+
  Cassandra (events) -----------------------------+--> FastAPI
  Redis pub/sub: events:pageviews ----------------+      |
  Redis pub/sub: events:flink:windows ------------+      +--> HTTP JSON endpoints
                                                         +--> WebSockets: /ws/pageviews, /ws/flink/windows
                                                         +--> Browser viewer: /realtime

  Cassandra (events) --> grpc-events-server --> grpc-events-client

ZooKeeper coordination demo:
  coordinator --> leader election: /pipeline/election
  coordinator --> leader status:   /pipeline/leader
  consumers   --> ephemeral nodes: /pipeline/consumers/*
  workers     --> ephemeral nodes: /pipeline/workers/*
  submitter   --> Flink job node:  /pipeline/flink/active-job

Flink dashboard: http://localhost:8081
FastAPI:         http://localhost:8000
gRPC demo:       localhost:50051
```

**Producer** generates fake pageview events and routes them to one of 4 Kafka
partitions based on the first letter of the username.

**Consumers** read from Kafka and fan out to Redis counters, Cassandra event
storage, RabbitMQ jobs, and Redis pub/sub pageview notifications.

**Workers** consume RabbitMQ jobs and mark them processed in Redis.

**Flink pageview stats** reads the same `pageviews` Kafka topic, computes
10-second tumbling page-count windows, and writes records to `pageview_stats`.

**Flink stats consumer** mirrors the latest Flink windows into Redis and
publishes Redis pub/sub notifications for WebSocket clients.

**ZooKeeper coordinator** is optional. It elects one coordinator leader, tracks
active consumers/workers through ephemeral znodes, and exposes status through
FastAPI. The pipeline continues to run if ZooKeeper is not started.

**FastAPI** exposes HTTP snapshots, WebSocket streams, and the `/realtime`
browser viewer.

**gRPC event insights demo** exposes a protobuf service that reads Cassandra
pageview events, lists users, and returns per-user page counts.

## Architecture Legend

| Architecture part | Code / config | What it does |
|-------------------|---------------|--------------|
| Producer | `src/pipeline/producer.py` | Creates fake pageview events and writes them to Kafka partitioned by username. |
| Kafka routing helper | `src/pipeline/__init__.py` | Provides `get_partition`, the shared username-to-partition routing rule. |
| Kafka topic `pageviews` | `docker-compose.yml` | Main event stream with 4 partitions. Host apps use `localhost:9092`; containers use `kafka:29092`. |
| Python consumers | `src/pipeline/kafka_consumer.py` | Consume `pageviews`, update Redis, insert Cassandra events, publish RabbitMQ jobs, and publish pageview events. |
| RabbitMQ workers | `src/pipeline/rabbitmq_worker.py` | Consume partitioned RabbitMQ queues and mark jobs as processed in Redis. |
| PyFlink pageview stats job | `src/pipeline/flink_pageview_stats.py` | Reads `pageviews`, computes event-time tumbling page counts, and writes `pageview_stats`. |
| Flink Redis bridge | `src/pipeline/flink_stats_consumer.py` | Copies latest Flink window counts from `pageview_stats` into Redis `flink:*` keys and pub/sub. |
| Flink job submitter | `src/pipeline/flink_job_submitter.py` | Keeps the Flink pageview stats job running and records the active job in ZooKeeper when available. |
| ZooKeeper coordinator | `src/pipeline/zookeeper_coordinator.py` | Registers coordinator presence and participates in leader election. |
| ZooKeeper helpers | `src/pipeline/zookeeper.py` | Defines znode paths, JSON helpers, optional registration, status reads, and active Flink job tracking. |
| ZooKeeper status CLI | `src/pipeline/zookeeper_status.py` | Prints leader, registration counts, and active Flink job details for failover demos. |
| Realtime helpers | `src/pipeline/realtime_events.py`, `src/pipeline/realtime_page.py` | Define Redis pub/sub channel names, WebSocket payloads, and the `/realtime` page. |
| FastAPI | `src/pipeline/api.py` | Serves Redis/Cassandra data over HTTP and Redis pub/sub messages over WebSockets. |
| gRPC event insights demo | `src/pipeline/protos/event_insights.proto`, `src/pipeline/grpc_event_insights_server.py`, `src/pipeline/grpc_event_insights_client.py` | Demonstrates a protobuf-backed gRPC service over Cassandra event data. |
| Flink smoke test | `src/pipeline/flink_smoke_test.py` | Sends deterministic events through all 4 Kafka partitions and verifies Flink output, optionally Redis, FastAPI, and WebSockets. |
| Local orchestration | `docker-compose.yml`, `Procfile` | Starts infrastructure containers and host Python processes for local development. |

## Prerequisites

- Python 3.12
- [uv](https://docs.astral.sh/uv/)
- Docker and Docker Compose

## Setup

Clone and install dependencies:

```bash
git clone <repo-url>
cd basic-data-pipeline
uv sync --all-extras
```

Start infrastructure services:

```bash
docker compose up -d
docker compose ps
```

Load the Cassandra schema once Cassandra is healthy:

```bash
docker compose cp cassandra_schema.cql cassandra:/tmp/schema.cql
docker compose exec cassandra cqlsh -f /tmp/schema.cql
```

Start the full local pipeline:

```bash
uv run honcho start
```

This starts the producer, 4 consumers, 4 workers, the ZooKeeper coordinator,
the Flink job submitter, the Flink stats consumer, FastAPI, and the gRPC event
insights demo server.

## Useful URLs and Commands

```text
FastAPI:         http://localhost:8000
Realtime page:   http://localhost:8000/realtime
gRPC demo:       localhost:50051
Flink dashboard: http://localhost:8081
RabbitMQ UI:     http://localhost:15672
```

Common checks:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/counts/page/pricing
curl http://localhost:8000/flink/windows/latest
curl http://localhost:8000/zookeeper/status
uv run zookeeper-status
uv run grpc-events-client inspect-first-user --limit 5
```

Run individual host processes:

```bash
uv run producer
uv run consumer
RABBITMQ_QUEUE=analytics_jobs_0 uv run worker
uv run coordinator
uv run flink-job-submitter
uv run flink-stats-consumer
uv run api
uv run grpc-events-server
```

## API Surface

- `GET /health`
- `GET /counts/page/{page}`
- `GET /users/{user_id}/last-page`
- `GET /events/{user_id}`
- `GET /flink/counts/page/{page}`
- `GET /flink/windows/latest`
- `GET /zookeeper/status`
- `GET /realtime`
- `WS /ws/pageviews`
- `WS /ws/flink/windows`

FastAPI also generates interactive API docs at http://localhost:8000/docs.

The gRPC demo exposes `pipeline.grpc_example.EventInsights` on
`localhost:50051` by default:

- `ListUsers(ListUsersRequest) returns (ListUsersResponse)`
- `GetUserStats(UserStatsRequest) returns (UserStatsResponse)`

## Configuration

All settings default to `localhost` for local development. Override via
environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_SERVER` | `localhost:9092` | Kafka bootstrap server |
| `KAFKA_TOPIC` | `pageviews` | Kafka topic name |
| `KAFKA_STATS_TOPIC` | `pageview_stats` | Flink stats Kafka topic |
| `KAFKA_PARTITIONS` | `4` | Number of Kafka partitions |
| `FLINK_WINDOW_SECONDS` | `10` | Flink stats window length |
| `FLINK_JOB_SUBMIT_INTERVAL_SECONDS` | `30` | Seconds between Flink job checks |
| `FLINK_PAGEVIEW_STATS_JOB_NAME` | `pageview-stats` | Running Flink job name to look for |
| `FLINK_PAGEVIEW_STATS_JOB_PATH` | `/opt/pipeline/src/pipeline/flink_pageview_stats.py` | Job path inside the Flink container |
| `FLINK_JOBMANAGER_SERVICE` | `flink-jobmanager` | Docker Compose service used for Flink CLI commands |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `RABBITMQ_HOST` | `localhost` | RabbitMQ host |
| `RABBITMQ_QUEUE` | `analytics_jobs` | RabbitMQ queue base name |
| `RABBITMQ_PARTITIONS` | `4` | Number of RabbitMQ queues |
| `CASSANDRA_HOST` | `localhost` | Cassandra host |
| `CASSANDRA_KEYSPACE` | `pipeline` | Cassandra keyspace |
| `ZOOKEEPER_HOSTS` | `localhost:2181` | ZooKeeper connection string |
| `ZOOKEEPER_ROOT` | `/pipeline` | Root znode for the demo coordination tree |
| `ZOOKEEPER_TIMEOUT_SECONDS` | `3` | ZooKeeper client connection timeout |
| `GRPC_HOST` | `[::]` | gRPC event insights server bind host |
| `GRPC_PORT` | `50051` | gRPC event insights server bind port |
| `GRPC_TARGET` | `localhost:50051` | gRPC event insights client target |

## Detailed Docs

- [Flink](docs/flink.md): Flink architecture notes, job submission, smoke
  testing, manual event generation, and remaining Flink work.
- [ZooKeeper](docs/zookeeper.md): leader election, safe failover demos, znode
  inspection, and ZooKeeper-specific configuration.
- [Troubleshooting](docs/troubleshooting.md): API examples, Flink output
  debugging, Kafka/Cassandra/Redis inspection commands, and management UIs.
- [gRPC](docs/grpc.md): protobuf contract, server/client commands, and the
  Cassandra-backed event insights example.

## Testing

Tests use pytest with mocked external services, so Docker is not required. CI
runs the same checks with GitHub Actions.

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

## Project Structure

```text
src/pipeline/
    config.py
    producer.py
    kafka_consumer.py
    rabbitmq_worker.py
    flink_pageview_stats.py
    flink_job_submitter.py
    flink_stats_consumer.py
    flink_smoke_test.py
    api.py
    realtime_events.py
    realtime_page.py
    grpc_event_insights_server.py
    grpc_event_insights_client.py
    protos/
    zookeeper.py
    zookeeper_coordinator.py
    zookeeper_status.py

tests/
    test_api.py
    test_config.py
    test_flink_job_submitter.py
    test_flink_smoke_test.py
    test_flink_stats_consumer.py
    test_grpc_event_insights.py
    test_kafka_consumer.py
    test_producer.py
    test_rabbitmq_worker.py
    test_realtime_events.py
    test_zookeeper.py
```
