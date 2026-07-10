import argparse
import json

from pipeline.zookeeper import read_status


def format_status(status: dict) -> str:
    summary = status["summary"]
    leader = status.get("leader") or {}
    active_job = (status.get("flink") or {}).get("active_job") or {}

    lines = [
        f"connected: {status['connected']}",
        f"zookeeper_root: {status['zookeeper_root']}",
        f"has_leader: {summary['has_leader']}",
        f"leader_id: {leader.get('coordinator_id') or '-'}",
        f"leader_pid: {leader.get('pid') or '-'}",
        f"coordinators: {summary['coordinator_count']}",
        f"workers: {summary['worker_count']}",
        f"consumers: {summary['consumer_count']}",
        f"active_flink_job_id: {summary['active_flink_job_id'] or '-'}",
        f"active_flink_job_name: {active_job.get('job_name') or '-'}",
    ]
    if not status["connected"]:
        lines.append(f"error: {status.get('error') or '-'}")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print ZooKeeper demo status.")
    parser.add_argument("--json", action="store_true", help="Print raw JSON status.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    status = read_status()
    if args.json:
        print(json.dumps(status, indent=2, sort_keys=True))
    else:
        print(format_status(status))


if __name__ == "__main__":
    main()
