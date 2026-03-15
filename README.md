# Basic Data Pipeline

A demo project that wires together Kafka, RabbitMQ, Redis, and Cassandra into a simple event-processing pipeline. Built as a reference example for learning how these technologies connect.

## Architecture

```
Producer --> Kafka --> Consumer --> Redis (counters + last page)
                          |------> Cassandra (event log)
                          |------> RabbitMQ --> Worker --> Redis (job status)

API (FastAPI) reads from Redis and Cassandra
```

**Producer** generates fake pageview events and publishes them to a Kafka topic.

**Consumer** reads from Kafka and fans out to three destinations:
- **Redis** — increments page view counters and tracks each user's last visited page
- **Cassandra** — stores a persistent log of all pageview events
- **RabbitMQ** — publishes events to a queue for downstream processing

**Worker** consumes jobs from RabbitMQ and marks them as processed in Redis.

**API** exposes the pipeline data via HTTP endpoints:
- `GET /counts/page/{page}` — page view count
- `GET /users/{user_id}/last-page` — last page a user visited
- `GET /events/{user_id}` — recent pageview events for a user

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (Python package manager)
- [Docker](https://www.docker.com/) and Docker Compose

## Setup

### 1. Clone and install dependencies

```bash
git clone <repo-url>
cd basic-data-pipeline
uv sync --all-extras
```

### 2. Start infrastructure services

```bash
docker compose up -d
```

This starts Zookeeper, Kafka, RabbitMQ, Redis, and Cassandra. All services have healthchecks — you can monitor their status with:

```bash
docker compose ps
```

Wait until all services show as `healthy` before proceeding. Cassandra is the slowest and can take up to 2 minutes.

### 3. Create the Cassandra schema

Once Cassandra is healthy, load the schema:

```bash
docker compose exec cassandra cqlsh -f /dev/stdin < cassandra_schema.cql
```

### 4. Start the pipeline

Use [honcho](https://honcho.readthedocs.io/) to run all four processes at once via the `Procfile`:

```bash
uv run honcho start
```

This starts the producer, consumer, worker, and API server simultaneously. Each process is labelled in the log output.

To run individual components instead:

```bash
uv run producer
uv run consumer
uv run worker
uv run api
```

## Configuration

All settings default to `localhost` for local development. Override via environment variables:

| Variable             | Default              | Description               |
|----------------------|----------------------|---------------------------|
| `KAFKA_SERVER`       | `localhost:9092`     | Kafka bootstrap server    |
| `KAFKA_TOPIC`        | `pageviews`          | Kafka topic name          |
| `REDIS_HOST`         | `localhost`          | Redis host                |
| `REDIS_PORT`         | `6379`               | Redis port                |
| `RABBITMQ_HOST`      | `localhost`          | RabbitMQ host             |
| `RABBITMQ_QUEUE`     | `analytics_jobs`     | RabbitMQ queue name       |
| `CASSANDRA_HOST`     | `localhost`          | Cassandra host            |
| `CASSANDRA_KEYSPACE` | `pipeline`           | Cassandra keyspace        |

## Management UIs

With Docker running, you can access:

- **RabbitMQ Management** — http://localhost:15672 (guest/guest)

## Project Structure

```
src/pipeline/
    config.py           # Configuration (env vars with defaults)
    producer.py         # Generates fake events, publishes to Kafka
    kafka_consumer.py   # Reads Kafka, writes to Redis/Cassandra/RabbitMQ
    rabbitmq_worker.py  # Processes RabbitMQ jobs
    api.py              # FastAPI endpoints for querying pipeline data
```
