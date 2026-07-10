import argparse
import json

import grpc

from pipeline.config import GRPC_TARGET
from pipeline.protos import event_insights_pb2, event_insights_pb2_grpc


def list_users(
    stub: event_insights_pb2_grpc.EventInsightsStub,
    limit: int,
) -> event_insights_pb2.ListUsersResponse:
    return stub.ListUsers(event_insights_pb2.ListUsersRequest(limit=limit))


def get_user_stats(
    stub: event_insights_pb2_grpc.EventInsightsStub,
    user_id: str,
    limit: int,
) -> event_insights_pb2.UserStatsResponse:
    return stub.GetUserStats(
        event_insights_pb2.UserStatsRequest(user_id=user_id, limit=limit)
    )


def users_to_dict(response: event_insights_pb2.ListUsersResponse) -> dict:
    return {"user_ids": list(response.user_ids)}


def stats_to_dict(response: event_insights_pb2.UserStatsResponse) -> dict:
    return {
        "user_id": response.user_id,
        "event_count": response.event_count,
        "last_page": response.last_page or None,
        "page_counts": [
            {"page": page_count.page, "count": page_count.count}
            for page_count in response.page_counts
        ],
        "recent_events": [
            {
                "event_id": event.event_id,
                "event_time": event.event_time,
                "page": event.page,
            }
            for event in response.recent_events
        ],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Call the demo gRPC event insights service"
    )
    parser.add_argument(
        "--target",
        default=GRPC_TARGET,
        help=f"gRPC target address, default: {GRPC_TARGET}",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list-users", help="List users in events")
    list_parser.add_argument("--limit", type=int, default=10)

    stats_parser = subparsers.add_parser(
        "user-stats", help="Get page counts for a user"
    )
    stats_parser.add_argument("user_id")
    stats_parser.add_argument("--limit", type=int, default=10)

    inspect_parser = subparsers.add_parser(
        "inspect-first-user",
        help="List users, then fetch stats for the first returned user",
    )
    inspect_parser.add_argument("--limit", type=int, default=10)

    return parser


def run(args: argparse.Namespace) -> list[dict]:
    with grpc.insecure_channel(args.target) as channel:
        stub = event_insights_pb2_grpc.EventInsightsStub(channel)

        if args.command == "list-users":
            return [users_to_dict(list_users(stub, args.limit))]
        if args.command == "user-stats":
            return [stats_to_dict(get_user_stats(stub, args.user_id, args.limit))]
        if args.command == "inspect-first-user":
            users = list_users(stub, args.limit)
            if not users.user_ids:
                return [users_to_dict(users)]
            stats = get_user_stats(stub, users.user_ids[0], args.limit)
            return [users_to_dict(users), stats_to_dict(stats)]

    raise ValueError(f"unknown command: {args.command}")


def main() -> None:
    args = build_parser().parse_args()
    for response in run(args):
        print(json.dumps(response, sort_keys=True))


if __name__ == "__main__":
    main()
