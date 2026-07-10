# Basic Data Pipeline

This started a while ago as a simple sandbox to play around with system level components, and how to use Python to glue them together.
Then I found myself using those files as my reference for new projects, at which point I realise I need to have a local repo for proper change management.
And then I thought, I might as well put it in my repo and clean things up (Claude added test and gave me a code review before hand).

Finally, I decide to use `uv` and `uv_build` - as opposed to pip and hatchling - as all the cool-kids are nowadays.  So that was a nice change.

---

A demo project that wires together Kafka, RabbitMQ, Redis, and Cassandra into a simple event-processing pipeline. Built as a reference example for learning how these technologies connect.

## Architecture

```
                                      +--> Python consumers (x4) --> Redis (counters)
                                      |             |           +--> Cassandra (events)
                                      |             |           +--> RabbitMQ queues (x4)
                                      |             |                        |
Producer --> Kafka: pageviews (4p) ---+             |                        v
                                      |             |           Workers (x4) --> Redis (job status)
                                      |             |
                                      |             +--> Redis pub/sub: events:pageviews
                                      |                                      |
                                      +--> PyFlink pageview-stats job        |
                                                   |                         |
                                                   v                         |
                                          Kafka: pageview_stats              |
                                                   |                         |
                                                   v                         |
                                          flink-stats-consumer               |
                                                   |                         |
                                                   +--> Redis (flink:* stats)
                                                   +--> Redis pub/sub: events:flink:windows
                                                                             |
FastAPI reads Redis and Cassandra and subscribes to Redis pub/sub -----------+
   |THat 
   +--> HTTP JSON endpoints
   +--> WebSockets: /ws/pageviews, /ws/flink/windows
   +--> Browser viewer: /realtime

Flink dashboard: http://localhost:8081
FastAPI:         http://localhost:8000
```

**Producer** generates fake pageview events and routes them to one of 4 Kafka partitions based on the first letter of the username (a-g, h-m, n-t, u-z).

**Consumers** (x4) each read from one partition and fan out to three destinations:
- **Redis** — increments page view counters and tracks each user's last visited page
- **Cassandra** — stores a persistent log of all pageview events
- **RabbitMQ** — publishes events to one of 4 partitioned queues using the same username routing (a-g, h-m, n-t, u-z)

**Workers** (x4) each consume from one RabbitMQ queue and mark jobs as processed in Redis.

**Flink pageview stats job** reads the same `pageviews` Kafka topic as a sidecar
consumer, computes 10-second tumbling page-count windows, and writes those
derived records to the `pageview_stats` Kafka topic.

**Flink stats consumer** reads `pageview_stats` and mirrors the latest Flink
window per page into Redis under `flink:*` keys.

**API** exposes the pipeline data via HTTP endpoints:
- `GET /health` — API health check
- `GET /counts/page/{page}` — page view count
- `GET /users/{user_id}/last-page` — last page a user visited
- `GET /events/{user_id}` — recent pageview events for a user
- `GET /flink/counts/page/{page}` — latest Flink-derived page count
- `GET /flink/windows/latest` — latest Flink window mirrored into Redis
- `GET /realtime` — browser page for watching live WebSocket messages
- `WS /ws/pageviews` — live pageview notifications from Redis pub/sub
- `WS /ws/flink/windows` — live Flink window notifications from Redis pub/sub

The WebSocket endpoints are a presentation layer only. The Kafka consumer and
Flink stats consumer publish small notifications to Redis channels after their
existing Redis writes:

- `events:pageviews`
- `events:flink:windows`

FastAPI subscribes to those channels and broadcasts messages to connected
WebSocket clients. It does not consume Kafka directly.

### Architecture Legend

| Architecture part | Code / config | What it does |
|-------------------|---------------|--------------|
| Producer | `src/pipeline/producer.py` | Creates fake pageview events and writes them to Kafka partitioned by username. |
| Kafka routing helper | `src/pipeline/__init__.py` | Provides `get_partition`, the shared username-to-partition routing rule. |
| Kafka topic `pageviews` | `docker-compose.yml` | Main event stream with 4 partitions. Host apps use `localhost:9092`; containers use `kafka:29092`. |
| Python consumers | `src/pipeline/kafka_consumer.py` | Consume `pageviews`, update Redis counters, insert Cassandra events, and publish RabbitMQ jobs. |
| RabbitMQ workers | `src/pipeline/rabbitmq_worker.py` | Consume partitioned RabbitMQ queues and mark jobs as processed in Redis. |
| PyFlink pageview stats job | `src/pipeline/flink_pageview_stats.py` | Reads `pageviews`, computes event-time tumbling page counts, and writes `pageview_stats`. |
| Kafka topic `pageview_stats` | `src/pipeline/flink_pageview_stats.py` | Derived Flink output stream used for inspection and API visibility. |
| Flink Redis bridge | `src/pipeline/flink_stats_consumer.py` | Copies latest Flink window counts from `pageview_stats` into Redis `flink:*` keys. |
| Realtime event helpers | `src/pipeline/realtime_events.py` | Defines Redis pub/sub channel names and WebSocket payload shapes. |
| FastAPI | `src/pipeline/api.py` | Serves Redis/Cassandra data over HTTP, Redis pub/sub messages over WebSockets, and the `/realtime` browser viewer. |
| Flink smoke test | `src/pipeline/flink_smoke_test.py` | Sends deterministic events through all 4 Kafka partitions and verifies Flink output, optionally Redis, FastAPI, and WebSockets. |
| Flink image | `flink.Dockerfile` | Extends the official Flink image with Python runtime packages and the SQL Kafka connector JAR. |
| Local orchestration | `docker-compose.yml`, `Procfile` | Starts infrastructure containers and host Python processes for local development. |

## Prerequisites

- Python 3.12
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

This starts Zookeeper, Kafka, RabbitMQ, Redis, Cassandra, and the Flink session
cluster. All services have healthchecks — you can monitor their status with:

```bash
docker compose ps
```

Wait until all services show as `healthy` before proceeding. Cassandra is the
slowest and can take up to 2 minutes. The Flink JobManager exposes its
dashboard at http://localhost:8081 once it is running.

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

Use [honcho](https://honcho.readthedocs.io/) to run all processes at once via the `Procfile`:

```bash
uv run honcho start
```

This starts the producer, 4 consumers, 4 workers, the Flink job submitter,
the Flink stats consumer, and API server simultaneously. Each process is
labelled in the log output. The Flink job submitter periodically checks the
Flink session cluster and submits the pageview stats job if it is not already
running. The Flink stats consumer waits for the Flink output topic and mirrors
the latest Flink-derived counts into Redis.

To run individual components instead:

```bash
uv run producer

# 4 consumers — each joins the same consumer group, so Kafka assigns one partition to each
uv run consumer   # run in 4 separate terminals
uv run consumer
uv run consumer
uv run consumer

# 4 workers — each reads from a specific RabbitMQ queue
RABBITMQ_QUEUE=analytics_jobs_0 uv run worker
RABBITMQ_QUEUE=analytics_jobs_1 uv run worker
RABBITMQ_QUEUE=analytics_jobs_2 uv run worker
RABBITMQ_QUEUE=analytics_jobs_3 uv run worker

uv run flink-job-submitter
uv run flink-stats-consumer
uv run api
```

Each consumer instance joins the same Kafka consumer group (`pipeline-consumer`), so Kafka automatically assigns one partition to each. Each worker instance reads from a dedicated RabbitMQ queue, matching the same username partitioning scheme used by Kafka.

To inspect live WebSocket messages, open this page while the API and consumer
are running:

```text
http://localhost:8000/realtime
```

The page connects to `ws://localhost:8000/ws/pageviews` and
`ws://localhost:8000/ws/flink/windows`. The Flink stream shows messages after
the Flink job and `flink-stats-consumer` are running.

## Configuration

All settings default to `localhost` for local development. Override via environment variables:

| Variable             | Default              | Description               |
|----------------------|----------------------|---------------------------|
| `KAFKA_SERVER`       | `localhost:9092`     | Kafka bootstrap server    |
| `KAFKA_TOPIC`        | `pageviews`          | Kafka topic name          |
| `KAFKA_STATS_TOPIC`  | `pageview_stats`     | Flink stats Kafka topic   |
| `KAFKA_PARTITIONS`   | `4`                  | Number of Kafka partitions|
| `FLINK_WINDOW_SECONDS`| `10`                | Flink stats window length |
| `FLINK_JOB_SUBMIT_INTERVAL_SECONDS`| `30`  | Seconds between Flink job checks |
| `FLINK_PAGEVIEW_STATS_JOB_NAME`| `pageview-stats`| Running Flink job name to look for |
| `FLINK_PAGEVIEW_STATS_JOB_PATH`| `/opt/pipeline/src/pipeline/flink_pageview_stats.py`| Job path inside the Flink container |
| `FLINK_JOBMANAGER_SERVICE`| `flink-jobmanager`| Docker Compose service used for Flink CLI commands |
| `REDIS_HOST`         | `localhost`          | Redis host                |
| `REDIS_PORT`         | `6379`               | Redis port                |
| `RABBITMQ_HOST`      | `localhost`          | RabbitMQ host             |
| `RABBITMQ_QUEUE`     | `analytics_jobs`     | RabbitMQ queue base name  |
| `RABBITMQ_PARTITIONS`| `4`                  | Number of RabbitMQ queues |
| `CASSANDRA_HOST`     | `localhost`          | Cassandra host            |
| `CASSANDRA_KEYSPACE` | `pipeline`           | Cassandra keyspace        |

## Flink Stats

The project includes an optional Apache Flink sidecar job. It reads pageview
events from Kafka, computes page counts over 10-second tumbling windows, and
writes JSON records to the `pageview_stats` Kafka topic.

The local Flink cluster is configured with default parallelism `4` and one
TaskManager with `4` task slots. That matches the `pageviews` topic's 4 Kafka
partitions, so Flink can consume the topic with 4-way parallelism without
requiring more Flink containers. For a more distributed local setup, you can
also run multiple TaskManagers as long as the total task slots stay at least 4.

The job uses event-time windows. It sets a source idle timeout so an idle Kafka
partition does not hold back the global watermark and prevent small demo windows
from closing.

The job uses the Flink Table API rather than the DataStream API. That keeps the
first Flink example compact: the source and sink are declared as Kafka tables,
and the windowed count is a SQL `INSERT INTO ... SELECT ... GROUP BY TUMBLE(...)`.

Flink notes to keep in mind:

- PyFlink support can lag behind new CPython releases. This project targets
  Python 3.12 until the package metadata and docs confirm a newer supported
  runtime.
- Flink connector versions are tied to the Flink runtime version. Avoid mixing
  a random Kafka connector JAR with a different Flink runtime.
- Kafka advertised listeners are the most common Docker networking trap. Host
  Python processes use `localhost:9092`; Flink containers use `kafka:29092`.
- The current Cassandra schema is useful for this sandbox, but it is not a
  general-purpose Flink analytics sink. The first Flink path writes derived
  output back to Kafka, then mirrors the latest values into Redis for the API.

The Docker Compose setup builds a local Flink image from `flink.Dockerfile`:

```bash
docker compose build flink-jobmanager flink-taskmanager
docker compose up -d
```

That Dockerfile starts from `flink:2.3.0-scala_2.12`, then adds the Python
runtime packages needed by PyFlink inside the container and downloads the Flink
SQL Kafka connector JAR into `/opt/flink/lib`. Both `flink-jobmanager` and
`flink-taskmanager` use this same image through the `build:` block in
`docker-compose.yml`; it is part of the normal Compose flow, not a separate
manual image.

Optional multi-TaskManager run:

```bash
docker compose up -d --scale flink-taskmanager=2
```

The default `honcho start` process includes `flink-job-submitter`, which checks
the session cluster every 30 seconds and submits the pageview stats job when it
is missing. To run the submitter by itself:

```bash
uv run flink-job-submitter
```

To submit the Flink job manually instead:

```bash
docker compose exec flink-jobmanager \
  flink run -d -py /opt/pipeline/src/pipeline/flink_pageview_stats.py
```

Open the Flink dashboard at http://localhost:8081.

To inspect the Flink output topic directly:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic pageview_stats \
  --from-beginning
```

The `flink-stats-consumer` process copies those records to Redis so the API can
serve them:

```bash
uv run flink-stats-consumer
```

The default `honcho start` process already includes `flink-job-submitter` and
`flink-stats-consumer`.

## Querying the API

Docker Compose starts the infrastructure services, but the FastAPI app runs as
a host Python process. Start it with either:

```bash
uv run api
```

or with the rest of the pipeline:

```bash
uv run honcho start
```

The API runs on http://localhost:8000 by default once that process is running.
Example queries using `curl`:

```bash
# Check the API is reachable
curl http://localhost:8000/health

# Get the view count for the /pricing page
curl http://localhost:8000/counts/page/pricing

# Get the last page visited by a user
curl http://localhost:8000/users/john_doe/last-page

# Get recent pageview events for a user
curl http://localhost:8000/events/john_doe

# Get the latest Flink-derived count for the /pricing page
curl http://localhost:8000/flink/counts/page/pricing

# Get the latest Flink-derived window seen by the bridge consumer
curl http://localhost:8000/flink/windows/latest
```

Example responses:

```json
// GET /counts/page/pricing
{"page": "/pricing", "count": 42}

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

// GET /flink/counts/page/pricing
{
  "page": "/pricing",
  "count": 42,
  "window_start": "2026-07-09T12:00:00.000Z",
  "window_end": "2026-07-09T12:00:10.000Z"
}
```

FastAPI also generates interactive API docs at http://localhost:8000/docs.

## Testing Flink

Check that the Flink containers are running:

```bash
docker compose ps flink-jobmanager flink-taskmanager
```

Check the Flink REST API:

```bash
curl http://localhost:8081/overview
curl http://localhost:8081/taskmanagers
curl http://localhost:8081/jobs/overview
```

Check jobs through the Flink CLI:

```bash
docker compose exec flink-jobmanager flink list
```

Submit the pageview stats job manually:

```bash
docker compose exec flink-jobmanager \
  flink run -d -py /opt/pipeline/src/pipeline/flink_pageview_stats.py
```

Or let the local submitter process keep it running:

```bash
uv run flink-job-submitter
```

Run the automated smoke test:

```bash
uv run flink-smoke-test
```

That sends a unique test page for each of the 4 Kafka partitions, advances the
event-time watermark, and waits for matching records on `pageview_stats`.

To verify the Redis bridge and FastAPI path too, run the bridge and API in
separate terminals before starting the smoke test:

```bash
# terminal 1
uv run flink-stats-consumer

# terminal 2
uv run api

# terminal 3
uv run flink-smoke-test --check-redis --api-url http://localhost:8000
```

To include WebSocket delivery, run the normal `honcho` stack so the API,
Kafka consumers, Flink submitter, and Flink stats consumer are all active, then
run:

```bash
uv run flink-smoke-test \
  --check-redis \
  --api-url http://localhost:8000 \
  --check-websocket
```

The WebSocket check connects to `/ws/pageviews` and `/ws/flink/windows` before
sending smoke events, then waits for messages containing the unique smoke-test
pages.

Send deterministic test events across all 4 Kafka partitions. This sends 40
`/pricing` events, then 4 later events to advance the event-time watermark so
the `/pricing` window can close:

```bash
uv run python - <<'PY'
import json
import time
import uuid

from kafka import KafkaProducer

from pipeline import get_partition
from pipeline.config import KAFKA_PARTITIONS, KAFKA_SERVER, KAFKA_TOPIC

users = ["alice", "harry", "nancy", "zara"]
base_ts = time.time()
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

for index in range(40):
    user_id = users[index % len(users)]
    producer.send(
        KAFKA_TOPIC,
        {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "page": "/pricing",
            "timestamp": base_ts,
        },
        partition=get_partition(user_id, KAFKA_PARTITIONS),
    )

for user_id in users:
    producer.send(
        KAFKA_TOPIC,
        {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "page": "/",
            "timestamp": base_ts + 20,
        },
        partition=get_partition(user_id, KAFKA_PARTITIONS),
    )

producer.flush()
producer.close()
print("sent 40 /pricing events and 4 watermark-advance events")
PY
```

Manual alternative: inspect the output topic. You should see JSON records for
`/pricing` after the Flink window closes:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic pageview_stats \
  --from-beginning \
  --timeout-ms 20000
```

To see the Flink output through FastAPI, run the bridge and API in separate
terminals:

```bash
# terminal 1
uv run flink-stats-consumer

# terminal 2
uv run api

# terminal 3
curl http://localhost:8000/flink/counts/page/pricing
curl http://localhost:8000/flink/windows/latest
```

If `pageview_stats` has `/pricing` records but the API still returns `0`, the
Flink job is working and the Redis bridge is the part to check. Start or restart
`uv run flink-stats-consumer`, then inspect Redis:

```bash
docker compose exec redis redis-cli KEYS 'flink:*'
docker compose exec redis redis-cli GET 'flink:pageviews:/pricing:latest'
```

If `pageview_stats` is empty, check that the Flink job is running:

```bash
docker compose exec flink-jobmanager flink list
```

Then send another batch of test events and wait longer than
`FLINK_WINDOW_SECONDS`.

If repeated smoke-test runs behave inconsistently after a long local session,
cancel the old Flink job and submit a fresh one:

```bash
docker compose exec flink-jobmanager flink list
docker compose exec flink-jobmanager flink cancel <job-id>
docker compose exec flink-jobmanager \
  flink run -d -py /opt/pipeline/src/pipeline/flink_pageview_stats.py
```

## Remaining Flink Work

The current Flink integration proves the sidecar path works: Kafka input,
parallel Flink processing, Kafka output, Redis bridge, and FastAPI visibility.
The remaining work is about making it more operational and more useful:

1. Add checkpointing and persisted Flink state volumes so the job can restart
   without losing stream position.
2. Document savepoint operations: stop with savepoint, restore from savepoint,
   and cancel a failed local job cleanly.
3. Add a malformed-event path, likely a Kafka dead-letter topic plus tests for
   missing fields and invalid JSON.
4. Decide whether the Kafka `pageview_stats` topic plus Redis bridge should
   remain the teaching architecture, or whether a direct Redis/Cassandra sink is
   worth adding as a second example.
5. Add a second Flink analytics job, such as per-user activity windows or
   top-pages-per-window, so Flink is doing more than a page-count demo.
6. Extend `flink-smoke-test` so it can optionally start and stop the bridge/API
   helper processes itself.

## Inspecting Kafka

To list topics:

```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

To see topic details including partition count:

```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic pageviews
```

To read messages from all partitions (from the beginning):

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic pageviews --from-beginning
```

To read messages from a specific partition (e.g. partition 0, usernames a-g):

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic pageviews --partition 0 --from-beginning
```

To see consumer group offsets per partition (which consumer owns which partition, and how far behind each is):

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

-- View events for a user within a time range
SELECT * FROM pageviews WHERE user_id = 'some_username'
  AND event_time > '2026-03-15 00:00:00' LIMIT 10;

-- Count total rows (slow on large tables)
SELECT COUNT(*) FROM pageviews;

-- Count events per user (requires ALLOW FILTERING)
SELECT user_id, COUNT(*) FROM pageviews GROUP BY user_id;
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
KEYS flink:pageviews:*
GET flink:windows:latest
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
    flink_pageview_stats.py  # PyFlink job for pageview window counts
    flink_stats_consumer.py  # Copies Flink stats from Kafka to Redis
    flink_smoke_test.py  # End-to-end Flink/Kafka/Redis/API smoke test
    rabbitmq_worker.py   # Processes RabbitMQ jobs
    api.py               # FastAPI endpoints for querying pipeline data

tests/
    test_config.py           # Env var overrides and defaults
    test_producer.py         # Event creation and validation
    test_kafka_consumer.py   # Redis/Cassandra updates and message fan-out
    test_flink_stats_consumer.py  # Flink stats parsing and Redis writes
    test_flink_smoke_test.py      # Smoke-test helper behavior
    test_rabbitmq_worker.py  # Job processing and acknowledgement
    test_api.py              # API endpoint responses
```
