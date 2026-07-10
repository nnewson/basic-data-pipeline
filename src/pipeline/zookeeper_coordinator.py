import logging
import os
import signal
import socket
import threading

from kazoo.exceptions import KazooException

from pipeline.zookeeper import (
    ZooKeeperPaths,
    create_ephemeral_json,
    delete_if_owned,
    ensure_paths,
    make_client,
    process_identity,
    set_json,
    utc_now,
)

logger = logging.getLogger("zookeeper-coordinator")


def coordinator_metadata(coordinator_id: str) -> dict:
    return {
        "coordinator_id": coordinator_id,
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "started_at": utc_now(),
    }


def register_coordinator(client, paths: ZooKeeperPaths, metadata: dict) -> str:
    path = f"{paths.coordinators}/{metadata['coordinator_id']}"
    create_ephemeral_json(client, path, metadata)
    return path


def run_as_leader(client, paths: ZooKeeperPaths, metadata: dict) -> None:
    logger.info("Coordinator elected leader: %s", metadata["coordinator_id"])
    set_json(client, paths.leader, metadata | {"elected_at": utc_now()})
    stop_event = threading.Event()

    def stop(signum, frame) -> None:
        stop_event.set()

    previous_sigterm = signal.getsignal(signal.SIGTERM)
    previous_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        stop_event.wait()
    finally:
        delete_if_owned(client, paths.leader)
        signal.signal(signal.SIGTERM, previous_sigterm)
        signal.signal(signal.SIGINT, previous_sigint)


def run_coordinator(coordinator_id: str | None = None) -> None:
    paths = ZooKeeperPaths()
    selected_id = coordinator_id or process_identity("coordinator")
    metadata = coordinator_metadata(selected_id)
    client = make_client()
    try:
        client.start()
        ensure_paths(client, paths)
        register_coordinator(client, paths, metadata)
        logger.info("Coordinator registered: %s", selected_id)
        election = client.Election(paths.election, identifier=selected_id)
        election.run(lambda: run_as_leader(client, paths, metadata))
    finally:
        client.stop()
        client.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    coordinator_id = os.environ.get("COORDINATOR_ID")
    try:
        run_coordinator(coordinator_id)
    except KeyboardInterrupt:
        logger.info("Shutting down coordinator")
    except KazooException as exc:
        logger.error("ZooKeeper coordinator failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
