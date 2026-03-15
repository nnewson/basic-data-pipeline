# Basic Data Pipeline

This started a while ago as a simple sandbox to play around with system level components, and how to use Python to glue them together.
Then I found myself using those files as my reference for new projects, at which point I realise I need to have a local repo for proper change management.
And then I thought, I might as well put it in my repo and clean things up (Claude added test and gave me a code review before hand).

Finally, I decide to use `uv` and `uv_build` - as opposed to pip and hatchling - as all the cool-kids are nowadays.  So that was a nice change.

---

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
docker compose cp cassandra_schema.cql cassandra:/tmp/schema.cql
docker compose exec cassandra cqlsh -f /tmp/schema.cql
```

### 4. Start the pipeline

Activate the virtual enviroment for Python:
```bash
source .venv/bin/activate
```

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

## Querying the API

The API runs on http://localhost:8000 by default. Example queries using `curl`:

```bash
# Get the view count for the /pricing page
curl http://localhost:8000/counts/page/pricing

# Get the last page visited by a user
curl http://localhost:8000/users/john_doe/last-page

# Get recent pageview events for a user
curl http://localhost:8000/events/john_doe
```

Example responses:

```json
// GET /counts/page/pricing
{"page": "pricing", "count": 42}

// GET /users/john_doe/last-page
{"user": "john_doe", "last_page": "/docs"}

// GET /events/john_doe
[
  {
    "user_id": "john_doe",
    "event_id": "a1b2c3d4-...",
    "event_time": "2026-03-15T10:30:00",
    "page": "/pricing"
  }
]
```

FastAPI also generates interactive API docs at http://localhost:8000/docs.

## Inspecting Kafka

To list topics:

```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

To read messages from the `pageviews` topic (from the beginning):

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic pageviews --from-beginning
```

To see consumer group offsets (how far behind the consumer is):

```bash
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group pipeline-consumer --describe
```

## Inspecting Cassandra

To open a CQL shell:

```bash
docker compose exec cassandra cqlsh
```

Useful queries once inside the shell:

```sql
-- Switch to the pipeline keyspace
USE pipeline;

-- Describe the schema
DESCRIBE TABLES;
DESCRIBE TABLE pageviews;

-- View recent events (most recent first)
SELECT * FROM pageviews LIMIT 10;

-- View events for a specific user
SELECT * FROM pageviews WHERE user_id = 'some_username' LIMIT 10;

-- Count total rows (slow on large tables)
SELECT COUNT(*) FROM pageviews;
```

## Inspecting Redis

To open a Redis CLI session:

```bash
docker compose exec redis redis-cli
```

Useful commands once inside the CLI:

```
# List all keys matching a pattern
KEYS pageviews:*
KEYS user:last_page:*
KEYS job:*

# Get a page view count
GET pageviews:/pricing

# Get a user's last visited page
GET user:last_page:some_username

# Check if a job was processed
GET job:some-event-uuid
```

## Management UIs

With Docker running, you can access:

- **RabbitMQ Management** — http://localhost:15672 (guest/guest)

## Testing

Tests use pytest with mocked external services — no Docker required.

```bash
# Activate the virtual env
source .venv/bin/activate

# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run a specific test file
uv run pytest tests/test_api.py

# Run a specific test by name
uv run pytest -k "test_page_count"
```

## Project Structure

```
src/pipeline/
    config.py            # Configuration (env vars with defaults)
    producer.py          # Generates fake events, publishes to Kafka
    kafka_consumer.py    # Reads Kafka, writes to Redis/Cassandra/RabbitMQ
    rabbitmq_worker.py   # Processes RabbitMQ jobs
    api.py               # FastAPI endpoints for querying pipeline data

tests/
    test_config.py           # Env var overrides and defaults
    test_producer.py         # Event creation and validation
    test_kafka_consumer.py   # Redis/Cassandra updates and message fan-out
    test_rabbitmq_worker.py  # Job processing and acknowledgement
    test_api.py              # API endpoint responses
```
