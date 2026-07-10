import json
import logging
import os
import socket
import uuid
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from kazoo.client import KazooClient
from kazoo.exceptions import KazooException, NoNodeError, NodeExistsError

from pipeline.config import (
    ZOOKEEPER_HOSTS,
    ZOOKEEPER_ROOT,
    ZOOKEEPER_TIMEOUT_SECONDS,
)

logger = logging.getLogger("zookeeper")


@dataclass(frozen=True)
class ZooKeeperPaths:
    root: str = ZOOKEEPER_ROOT

    def __post_init__(self) -> None:
        if not self.root.startswith("/"):
            raise ValueError("ZooKeeper root must start with /")

    @property
    def election(self) -> str:
        return f"{self.root}/election"

    @property
    def leader(self) -> str:
        return f"{self.root}/leader"

    @property
    def coordinators(self) -> str:
        return f"{self.root}/coordinators"

    @property
    def consumers(self) -> str:
        return f"{self.root}/consumers"

    @property
    def workers(self) -> str:
        return f"{self.root}/workers"

    @property
    def flink(self) -> str:
        return f"{self.root}/flink"

    @property
    def active_flink_job(self) -> str:
        return f"{self.flink}/active-job"

    @property
    def control(self) -> str:
        return f"{self.root}/control"

    @property
    def pause(self) -> str:
        return f"{self.control}/pause"

    @property
    def base_paths(self) -> list[str]:
        return [
            self.root,
            self.election,
            self.coordinators,
            self.consumers,
            self.workers,
            self.flink,
            self.control,
        ]


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def process_identity(prefix: str) -> str:
    return f"{prefix}-{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"


def json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True).encode("utf-8")


def json_from_bytes(data: bytes | None) -> dict[str, Any] | None:
    if not data:
        return None
    return json.loads(data.decode("utf-8"))


def make_client(
    hosts: str = ZOOKEEPER_HOSTS,
    timeout_seconds: float = ZOOKEEPER_TIMEOUT_SECONDS,
) -> KazooClient:
    return KazooClient(hosts=hosts, timeout=timeout_seconds)


def ensure_paths(client: KazooClient, paths: ZooKeeperPaths) -> None:
    for path in paths.base_paths:
        client.ensure_path(path)


def get_json(client: KazooClient, path: str) -> dict[str, Any] | None:
    try:
        data, _ = client.get(path)
    except NoNodeError:
        return None
    return json_from_bytes(data)


def get_children(client: KazooClient, path: str) -> list[str]:
    try:
        return sorted(client.get_children(path))
    except NoNodeError:
        return []


def set_json(client: KazooClient, path: str, payload: dict[str, Any]) -> None:
    value = json_bytes(payload)
    try:
        client.set(path, value)
    except NoNodeError:
        client.create(path, value, makepath=True)


def create_ephemeral_json(
    client: KazooClient, path: str, payload: dict[str, Any]
) -> None:
    value = json_bytes(payload)
    try:
        client.create(path, value, ephemeral=True, makepath=True)
    except NodeExistsError:
        client.set(path, value)


def delete_if_owned(client: KazooClient, path: str) -> None:
    with suppress(KazooException):
        client.delete(path)


def status_snapshot(client: KazooClient, paths: ZooKeeperPaths) -> dict[str, Any]:
    leader = get_json(client, paths.leader)
    coordinators = get_children(client, paths.coordinators)
    workers = get_children(client, paths.workers)
    consumers = get_children(client, paths.consumers)
    active_job = get_json(client, paths.active_flink_job)
    paused = get_json(client, paths.pause)
    return {
        "connected": True,
        "zookeeper_root": paths.root,
        "leader": leader,
        "coordinators": coordinators,
        "workers": workers,
        "consumers": consumers,
        "flink": {
            "active_job": active_job,
        },
        "control": {
            "paused": bool(paused and paused.get("paused")),
        },
        "summary": {
            "has_leader": leader is not None,
            "coordinator_count": len(coordinators),
            "worker_count": len(workers),
            "consumer_count": len(consumers),
            "active_flink_job_id": active_job.get("job_id") if active_job else None,
        },
    }


def unavailable_status(error: str) -> dict[str, Any]:
    return {
        "connected": False,
        "error": error,
        "zookeeper_root": ZOOKEEPER_ROOT,
        "leader": None,
        "coordinators": [],
        "workers": [],
        "consumers": [],
        "flink": {"active_job": None},
        "control": {"paused": False},
        "summary": {
            "has_leader": False,
            "coordinator_count": 0,
            "worker_count": 0,
            "consumer_count": 0,
            "active_flink_job_id": None,
        },
    }


def read_status(
    client_factory: Callable[[], KazooClient] = make_client,
    paths: ZooKeeperPaths | None = None,
) -> dict[str, Any]:
    client = client_factory()
    selected_paths = paths or ZooKeeperPaths()
    try:
        client.start()
        ensure_paths(client, selected_paths)
        return status_snapshot(client, selected_paths)
    except KazooException as exc:
        logger.warning("ZooKeeper status unavailable: %s", exc)
        return unavailable_status(str(exc))
    finally:
        with suppress(Exception):
            client.stop()
        with suppress(Exception):
            client.close()


class OptionalRegistration:
    def __init__(
        self,
        client: KazooClient | None,
        path: str | None,
    ) -> None:
        self.client = client
        self.path = path

    def close(self) -> None:
        if not self.client:
            return
        if self.path:
            delete_if_owned(self.client, self.path)
        with suppress(Exception):
            self.client.stop()
        with suppress(Exception):
            self.client.close()


def register_ephemeral(
    group: str,
    identity: str,
    metadata: dict[str, Any],
    client_factory: Callable[[], KazooClient] = make_client,
    paths: ZooKeeperPaths | None = None,
) -> OptionalRegistration:
    selected_paths = paths or ZooKeeperPaths()
    client = client_factory()
    path = f"{selected_paths.root}/{group}/{identity}"
    try:
        client.start()
        ensure_paths(client, selected_paths)
        create_ephemeral_json(client, path, metadata)
        logger.info("Registered ZooKeeper %s: %s", group, identity)
        return OptionalRegistration(client, path)
    except KazooException as exc:
        logger.warning("ZooKeeper registration skipped for %s: %s", identity, exc)
        with suppress(Exception):
            client.stop()
        with suppress(Exception):
            client.close()
        return OptionalRegistration(None, None)


def runtime_metadata(
    kind: str, identity: str, extra: dict[str, Any] | None = None
) -> dict:
    payload = {
        "kind": kind,
        "id": identity,
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "started_at": utc_now(),
    }
    if extra:
        payload.update(extra)
    return payload


def write_active_flink_job(
    payload: dict[str, Any],
    client_factory: Callable[[], KazooClient] = make_client,
    paths: ZooKeeperPaths | None = None,
) -> bool:
    selected_paths = paths or ZooKeeperPaths()
    client = client_factory()
    try:
        client.start()
        ensure_paths(client, selected_paths)
        set_json(client, selected_paths.active_flink_job, payload)
        return True
    except KazooException as exc:
        logger.warning("ZooKeeper Flink job tracking skipped: %s", exc)
        return False
    finally:
        with suppress(Exception):
            client.stop()
        with suppress(Exception):
            client.close()
