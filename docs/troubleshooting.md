# Troubleshooting and Inspection

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

# Get optional ZooKeeper coordination status
curl http://localhost:8000/zookeeper/status
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

// GET /zookeeper/status
{
  "connected": true,
  "leader": {"coordinator_id": "coordinator-host-1234-abcd"},
  "coordinators": ["coordinator-host-1234-abcd"],
  "workers": ["worker-host-1234-abcd"],
  "consumers": ["consumer-host-1234-abcd"],
  "flink": {"active_job": {"job_name": "pageview-stats"}},
  "control": {"paused": false}
}
```

FastAPI also generates interactive API docs at http://localhost:8000/docs.

## Flink Output Through FastAPI

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

## Inspecting Kafka

To list topics:

```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

To see topic details including partition count:

```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic pageviews
```

To read messages from all partitions from the beginning:

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic pageviews --from-beginning
```

To read messages from a specific partition, such as partition 0 for usernames
`a-g`:

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic pageviews --partition 0 --from-beginning
```

To see consumer group offsets per partition, which consumer owns which
partition, and how far behind each is:

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

```text
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

- RabbitMQ Management: http://localhost:15672 with `guest` / `guest`
