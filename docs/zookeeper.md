# ZooKeeper Coordination

ZooKeeper is used as an optional coordination demo, not as application storage
and not as a requirement for event processing.

The coordinator process registers itself under `/pipeline/coordinators` and
uses ZooKeeper leader election under `/pipeline/election`. The active leader
writes metadata to `/pipeline/leader`. Consumers and workers register
ephemeral znodes under `/pipeline/consumers` and `/pipeline/workers`; those
nodes disappear automatically when the process exits or loses its ZooKeeper
session. The Flink job submitter records the active job under
`/pipeline/flink/active-job` when ZooKeeper is available.

## Run Coordinators

Run one coordinator:

```bash
uv run coordinator
```

Run a second coordinator in another terminal to see leader election in action.
Only one process writes `/pipeline/leader`.

## Status

Inspect status through FastAPI:

```bash
curl http://localhost:8000/zookeeper/status
```

Or print the same status in a compact terminal format:

```bash
uv run zookeeper-status
```

Use `--json` when you want the raw API-style payload:

```bash
uv run zookeeper-status --json
```

## Safe Failover Demo

For a failover demo, run `uv run api` separately from the coordinator
processes. If the coordinator is supervised by `honcho`, terminating it can stop
the rest of the Procfile, including the API.

```bash
# terminal 1
uv run api

# terminal 2
COORDINATOR_ID=leader-demo-1 uv run coordinator

# terminal 3
COORDINATOR_ID=leader-demo-2 uv run coordinator

# terminal 4
uv run zookeeper-status
kill -TERM <leader_pid_from_status>
uv run zookeeper-status
```

The second `zookeeper-status` run should show the standby coordinator as the
new leader.

## Direct Znode Inspection

Inspect znodes directly:

```bash
docker compose exec zookeeper zookeeper-shell localhost:2181 ls /pipeline
docker compose exec zookeeper zookeeper-shell localhost:2181 get /pipeline/leader
```

Useful znodes:

```text
/pipeline/election
/pipeline/leader
/pipeline/coordinators
/pipeline/consumers
/pipeline/workers
/pipeline/flink/active-job
/pipeline/control/pause
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `ZOOKEEPER_HOSTS` | `localhost:2181` | ZooKeeper connection string |
| `ZOOKEEPER_ROOT` | `/pipeline` | Root znode for the demo coordination tree |
| `ZOOKEEPER_TIMEOUT_SECONDS` | `3` | ZooKeeper client connection timeout |
