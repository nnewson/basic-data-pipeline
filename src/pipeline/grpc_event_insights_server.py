from collections import Counter
from concurrent import futures
from datetime import datetime
import logging
from typing import Protocol

import grpc
from cassandra.cluster import Cluster, Session

from pipeline.config import CASSANDRA_HOST, GRPC_HOST, GRPC_PORT, KEYSPACE
from pipeline.protos import event_insights_pb2, event_insights_pb2_grpc

logger = logging.getLogger("grpc-event-insights-server")
DEFAULT_LIMIT = 10
MAX_LIMIT = 100


class EventRepository(Protocol):
    def list_users(self, limit: int) -> list[str]: ...

    def user_events(self, user_id: str, limit: int) -> list[object]: ...


def clamp_limit(limit: int, default: int = DEFAULT_LIMIT) -> int:
    if limit <= 0:
        return default
    return min(limit, MAX_LIMIT)


def row_value(row: object, name: str):
    if hasattr(row, name):
        return getattr(row, name)
    if hasattr(row, "_asdict"):
        return row._asdict()[name]
    return row[name]


def format_event_time(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


class CassandraEventRepository:
    def __init__(self, session: Session):
        self._session = session

    def list_users(self, limit: int) -> list[str]:
        scan_limit = min(limit * 20, 500)
        rows = self._session.execute(
            f"SELECT user_id FROM pageviews LIMIT {scan_limit}"
        )

        users = []
        seen = set()
        for row in rows:
            user_id = row_value(row, "user_id")
            if user_id in seen:
                continue
            seen.add(user_id)
            users.append(user_id)
            if len(users) >= limit:
                break
        return users

    def user_events(self, user_id: str, limit: int) -> list[object]:
        return list(
            self._session.execute(
                (
                    "SELECT event_id, event_time, page FROM pageviews "
                    f"WHERE user_id=%s LIMIT {limit}"
                ),
                (user_id,),
            )
        )


class EventInsightsService(event_insights_pb2_grpc.EventInsightsServicer):
    def __init__(self, repository: EventRepository):
        self._repository = repository

    def ListUsers(self, request, context):
        limit = clamp_limit(request.limit)
        return event_insights_pb2.ListUsersResponse(
            user_ids=self._repository.list_users(limit)
        )

    def GetUserStats(self, request, context):
        if not request.user_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "user_id is required")

        limit = clamp_limit(request.limit)
        rows = self._repository.user_events(request.user_id, limit)
        page_counter = Counter(row_value(row, "page") for row in rows)
        recent_events = [
            event_insights_pb2.PageView(
                event_id=str(row_value(row, "event_id")),
                event_time=format_event_time(row_value(row, "event_time")),
                page=row_value(row, "page"),
            )
            for row in rows
        ]
        page_counts = [
            event_insights_pb2.PageCount(page=page, count=count)
            for page, count in sorted(
                page_counter.items(), key=lambda item: (-item[1], item[0])
            )
        ]

        return event_insights_pb2.UserStatsResponse(
            user_id=request.user_id,
            event_count=len(rows),
            page_counts=page_counts,
            recent_events=recent_events,
            last_page=recent_events[0].page if recent_events else "",
        )


def create_server(
    bind_address: str | None = None,
    repository: EventRepository | None = None,
) -> tuple[grpc.Server, int]:
    if repository is None:
        cluster = Cluster([CASSANDRA_HOST])
        repository = CassandraEventRepository(cluster.connect(KEYSPACE))

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    event_insights_pb2_grpc.add_EventInsightsServicer_to_server(
        EventInsightsService(repository),
        server,
    )
    port = server.add_insecure_port(bind_address or f"{GRPC_HOST}:{GRPC_PORT}")
    if port == 0:
        raise RuntimeError("failed to bind gRPC event insights server")
    return server, port


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    bind_address = f"{GRPC_HOST}:{GRPC_PORT}"
    cluster = Cluster([CASSANDRA_HOST])
    repository = CassandraEventRepository(cluster.connect(KEYSPACE))
    server, port = create_server(bind_address, repository)
    server.start()
    logger.info(
        "gRPC event insights server listening on %s (port %s)", bind_address, port
    )

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("stopping gRPC event insights server")
        server.stop(grace=1).wait()
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
