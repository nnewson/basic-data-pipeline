# Flink

The project includes an optional Apache Flink sidecar job. It reads pageview
events from Kafka, computes page counts over 10-second tumbling windows, and
writes JSON records to the `pageview_stats` Kafka topic.

The local Flink cluster is configured with default parallelism `4` and one
TaskManager with `4` task slots. That matches the `pageviews` topic's 4 Kafka
partitions, so Flink can consume the topic with 4-way parallelism without
requiring more Flink containers. For a more distributed local setup, you can
also run multiple TaskManagers as long as the total task slots stay at least 4.

The job uses event-time windows. It sets a source idle timeout so an idle Kafka
partition does not hold back the global event-time watermark and prevent small
demo windows from closing.

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

## Local Flink Cluster

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

## Job Submission

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

## Output Topic

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

## Deterministic Manual Events

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
