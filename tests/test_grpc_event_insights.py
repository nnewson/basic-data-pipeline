from collections import namedtuple
from datetime import datetime

import grpc

from pipeline.grpc_event_insights_client import get_user_stats, list_users
from pipeline.grpc_event_insights_server import EventInsightsService, create_server
from pipeline.protos import event_insights_pb2, event_insights_pb2_grpc

EventRow = namedtuple("EventRow", ["user_id", "event_id", "event_time", "page"])


class FakeContext:
    def abort(self, code, details):
        raise grpc.RpcError(f"{code}: {details}")


class FakeRepository:
    def __init__(self):
        self.events = {
            "alice": [
                EventRow("alice", "event-1", datetime(2026, 7, 10, 12, 0, 2), "/docs"),
                EventRow(
                    "alice", "event-2", datetime(2026, 7, 10, 12, 0, 1), "/pricing"
                ),
                EventRow(
                    "alice", "event-3", datetime(2026, 7, 10, 12, 0, 0), "/pricing"
                ),
            ],
            "bob": [
                EventRow("bob", "event-4", datetime(2026, 7, 10, 12, 0, 3), "/"),
            ],
        }

    def list_users(self, limit: int) -> list[str]:
        return list(self.events)[:limit]

    def user_events(self, user_id: str, limit: int) -> list[EventRow]:
        return self.events.get(user_id, [])[:limit]


def test_event_insights_service_lists_users():
    service = EventInsightsService(FakeRepository())

    response = service.ListUsers(event_insights_pb2.ListUsersRequest(limit=1), None)

    assert list(response.user_ids) == ["alice"]


def test_event_insights_service_returns_user_page_counts():
    service = EventInsightsService(FakeRepository())

    response = service.GetUserStats(
        event_insights_pb2.UserStatsRequest(user_id="alice", limit=10),
        FakeContext(),
    )

    assert response.user_id == "alice"
    assert response.event_count == 3
    assert response.last_page == "/docs"
    assert [(count.page, count.count) for count in response.page_counts] == [
        ("/pricing", 2),
        ("/docs", 1),
    ]
    assert [event.event_id for event in response.recent_events] == [
        "event-1",
        "event-2",
        "event-3",
    ]


def test_event_insights_grpc_roundtrip():
    server, port = create_server("localhost:0", FakeRepository())
    server.start()
    try:
        with grpc.insecure_channel(f"localhost:{port}") as channel:
            grpc.channel_ready_future(channel).result(timeout=5)
            stub = event_insights_pb2_grpc.EventInsightsStub(channel)

            users_response = list_users(stub, limit=10)
            stats_response = get_user_stats(stub, "alice", limit=10)
    finally:
        server.stop(grace=0).wait()

    assert list(users_response.user_ids) == ["alice", "bob"]
    assert stats_response.user_id == "alice"
    assert stats_response.event_count == 3
    assert stats_response.page_counts[0].page == "/pricing"
    assert stats_response.page_counts[0].count == 2
