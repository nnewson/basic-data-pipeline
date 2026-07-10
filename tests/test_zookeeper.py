import json

from kazoo.exceptions import KazooException

from pipeline.zookeeper import (
    ZooKeeperPaths,
    json_bytes,
    json_from_bytes,
    read_status,
    register_ephemeral,
    runtime_metadata,
    status_snapshot,
    unavailable_status,
)
from pipeline.zookeeper_coordinator import coordinator_metadata
from pipeline.zookeeper_status import format_status


class FakeClient:
    def __init__(self):
        self.started = False
        self.stopped = False
        self.closed = False
        self.paths = []
        self.values = {}
        self.children = {}
        self.created = []
        self.deleted = []

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True

    def close(self):
        self.closed = True

    def ensure_path(self, path):
        self.paths.append(path)

    def get(self, path):
        return self.values[path], None

    def get_children(self, path):
        return self.children.get(path, [])

    def create(self, path, value, ephemeral=False, makepath=False):
        self.created.append(
            (path, json.loads(value.decode("utf-8")), ephemeral, makepath)
        )
        self.values[path] = value

    def set(self, path, value):
        self.values[path] = value

    def delete(self, path):
        self.deleted.append(path)


class FailingClient(FakeClient):
    def start(self):
        raise KazooException("unavailable")


def test_zookeeper_paths_use_root():
    paths = ZooKeeperPaths("/demo")

    assert paths.leader == "/demo/leader"
    assert paths.election == "/demo/election"
    assert paths.active_flink_job == "/demo/flink/active-job"
    assert "/demo/workers" in paths.base_paths


def test_json_helpers_round_trip():
    payload = {"coordinator_id": "c1"}

    assert json_from_bytes(json_bytes(payload)) == payload


def test_status_snapshot_shapes_children_and_json_values():
    client = FakeClient()
    paths = ZooKeeperPaths("/pipeline")
    client.values[paths.leader] = json_bytes({"coordinator_id": "c1"})
    client.values[paths.active_flink_job] = json_bytes({"job_id": "abc"})
    client.values[paths.pause] = json_bytes({"paused": True})
    client.children[paths.coordinators] = ["c1"]
    client.children[paths.workers] = ["w1"]
    client.children[paths.consumers] = ["consumer1"]

    assert status_snapshot(client, paths) == {
        "connected": True,
        "zookeeper_root": "/pipeline",
        "leader": {"coordinator_id": "c1"},
        "coordinators": ["c1"],
        "workers": ["w1"],
        "consumers": ["consumer1"],
        "flink": {"active_job": {"job_id": "abc"}},
        "control": {"paused": True},
        "summary": {
            "has_leader": True,
            "coordinator_count": 1,
            "worker_count": 1,
            "consumer_count": 1,
            "active_flink_job_id": "abc",
        },
    }


def test_read_status_returns_unavailable_shape():
    status = read_status(lambda: FailingClient(), ZooKeeperPaths("/pipeline"))

    assert status == unavailable_status("unavailable")


def test_register_ephemeral_creates_presence_node():
    client = FakeClient()

    registration = register_ephemeral(
        "workers",
        "worker-1",
        {"kind": "worker"},
        client_factory=lambda: client,
        paths=ZooKeeperPaths("/pipeline"),
    )

    assert client.started is True
    assert client.created == [
        ("/pipeline/workers/worker-1", {"kind": "worker"}, True, True)
    ]

    registration.close()

    assert client.stopped is True
    assert client.closed is True


def test_runtime_metadata_includes_extra_fields():
    metadata = runtime_metadata("worker", "worker-1", {"queue": "analytics_jobs_0"})

    assert metadata["kind"] == "worker"
    assert metadata["id"] == "worker-1"
    assert metadata["queue"] == "analytics_jobs_0"
    assert "started_at" in metadata


def test_coordinator_metadata_shape():
    metadata = coordinator_metadata("coordinator-1")

    assert metadata["coordinator_id"] == "coordinator-1"
    assert "hostname" in metadata
    assert "pid" in metadata
    assert "started_at" in metadata


def test_format_status_prints_leader_and_counts():
    output = format_status(
        {
            "connected": True,
            "zookeeper_root": "/pipeline",
            "leader": {"coordinator_id": "c1", "pid": 123},
            "flink": {"active_job": {"job_id": "job-1", "job_name": "pageview-stats"}},
            "summary": {
                "has_leader": True,
                "coordinator_count": 2,
                "worker_count": 4,
                "consumer_count": 4,
                "active_flink_job_id": "job-1",
            },
        }
    )

    assert "leader_id: c1" in output
    assert "leader_pid: 123" in output
    assert "coordinators: 2" in output
    assert "active_flink_job_name: pageview-stats" in output
