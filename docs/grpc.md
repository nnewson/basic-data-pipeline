# gRPC Event Insights Demo

The project includes a small gRPC example alongside the HTTP and WebSocket API.
It reads real pipeline data from Cassandra instead of maintaining its own toy
store.

The service contract lives in `src/pipeline/protos/event_insights.proto`:

```proto
service EventInsights {
  rpc ListUsers(ListUsersRequest) returns (ListUsersResponse);
  rpc GetUserStats(UserStatsRequest) returns (UserStatsResponse);
}
```

Generated Python protobuf stubs are checked in under `src/pipeline/protos/` so
local development does not need a separate protobuf generation step.

## Run the Server

Start the infrastructure and Cassandra schema first:

```bash
docker compose up -d
docker compose cp cassandra_schema.cql cassandra:/tmp/schema.cql
docker compose exec cassandra cqlsh -f /tmp/schema.cql
```

Start the gRPC endpoint:

```bash
uv run grpc-events-server
```

By default it listens on `localhost:50051` for clients and binds the server to
`[::]:50051`.

The default `honcho start` process also includes this server.

## Query Users

Once the producer and consumers have written events to Cassandra, list users
seen in recent pageview rows:

```bash
uv run grpc-events-client list-users --limit 5
```

Example output:

```json
{"user_ids": ["john_doe", "jane_smith"]}
```

Fetch details for one user:

```bash
uv run grpc-events-client user-stats john_doe --limit 20
```

Example output:

```json
{
  "event_count": 3,
  "last_page": "/docs",
  "page_counts": [
    {"count": 2, "page": "/pricing"},
    {"count": 1, "page": "/docs"}
  ],
  "recent_events": [
    {
      "event_id": "a1b2c3d4-...",
      "event_time": "2026-07-10T12:00:00",
      "page": "/docs"
    }
  ],
  "user_id": "john_doe"
}
```

Or list users and fetch stats for the first returned user:

```bash
uv run grpc-events-client inspect-first-user --limit 5
```

`event_count` and `page_counts` are calculated from the recent events returned
by the request. Increase `--limit` when you want a wider sample. The server
caps the limit at 100 to keep the local demo responsive.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_HOST` | `[::]` | Server bind host |
| `GRPC_PORT` | `50051` | Server bind port |
| `GRPC_TARGET` | `localhost:50051` | Client target |

Use a custom target when calling a non-default server:

```bash
uv run grpc-events-client --target localhost:50052 list-users --limit 5
```

## Code Map

| File | Purpose |
|------|---------|
| `src/pipeline/protos/event_insights.proto` | Protobuf service contract |
| `src/pipeline/protos/event_insights_pb2.py` | Generated message classes |
| `src/pipeline/protos/event_insights_pb2_grpc.py` | Generated gRPC client/server bindings |
| `src/pipeline/grpc_event_insights_server.py` | Cassandra-backed service implementation and server entry point |
| `src/pipeline/grpc_event_insights_client.py` | Simple Python command-line client |
| `tests/test_grpc_event_insights.py` | Direct service tests plus an in-process gRPC roundtrip |
