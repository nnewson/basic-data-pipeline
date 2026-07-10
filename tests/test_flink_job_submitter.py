import subprocess

import pytest

from pipeline import flink_job_submitter


def completed(stdout: str = ""):
    return subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")


def test_flink_command_targets_jobmanager_service():
    assert flink_job_submitter.flink_command("list", "-r") == [
        "docker",
        "compose",
        "exec",
        "-T",
        "flink-jobmanager",
        "flink",
        "list",
        "-r",
    ]


@pytest.mark.parametrize(
    "output, expected",
    [
        ("01.01.2026 : abc : pageview-stats (RUNNING)", True),
        ("01.01.2026 : abc : another-job (RUNNING)", False),
        ("", False),
    ],
)
def test_is_job_running(output, expected):
    assert flink_job_submitter.is_job_running(output, "pageview-stats") is expected


def test_ensure_pageview_stats_job_skips_submit_when_running(monkeypatch):
    calls = []
    tracked = []

    def runner(command, **kwargs):
        calls.append(command)
        return completed(
            "01.01.2026 : "
            "0123456789abcdef0123456789abcdef : pageview-stats (RUNNING)"
        )

    monkeypatch.setattr(flink_job_submitter, "write_active_flink_job", tracked.append)
    submitted = flink_job_submitter.ensure_pageview_stats_job(runner)

    assert submitted is False
    assert calls == [flink_job_submitter.flink_command("list", "-r")]
    assert tracked[0]["job_id"] == "0123456789abcdef0123456789abcdef"
    assert tracked[0]["status"] == "running"
    assert tracked[0]["job_name"] == "pageview-stats"
    assert "updated_at" in tracked[0]


def test_ensure_pageview_stats_job_tracks_running_job_without_parseable_id(monkeypatch):
    tracked = []

    def runner(command, **kwargs):
        return completed("01.01.2026 : abc : pageview-stats (RUNNING)")

    monkeypatch.setattr(flink_job_submitter, "write_active_flink_job", tracked.append)
    submitted = flink_job_submitter.ensure_pageview_stats_job(runner)

    assert submitted is False
    assert tracked[0]["job_id"] is None
    assert tracked[0]["job_name"] == "pageview-stats"
    assert tracked[0]["status"] == "running"
    assert "updated_at" in tracked[0]


def test_ensure_pageview_stats_job_submits_when_missing(monkeypatch):
    calls = []
    tracked = []

    def runner(command, **kwargs):
        calls.append(command)
        if command[-2:] == ["list", "-r"]:
            return completed("")
        return completed(
            "Job has been submitted with JobID "
            "fedcba9876543210fedcba9876543210"
        )

    monkeypatch.setattr(flink_job_submitter, "write_active_flink_job", tracked.append)
    submitted = flink_job_submitter.ensure_pageview_stats_job(runner)

    assert submitted is True
    assert calls == [
        flink_job_submitter.flink_command("list", "-r"),
        flink_job_submitter.flink_command(
            "run",
            "-d",
            "-py",
            "/opt/pipeline/src/pipeline/flink_pageview_stats.py",
        ),
    ]
    assert tracked[0]["job_id"] == "fedcba9876543210fedcba9876543210"
    assert tracked[0]["status"] == "submitted"
    assert tracked[0]["job_name"] == "pageview-stats"
    assert "updated_at" in tracked[0]


def test_ensure_pageview_stats_job_tracks_submitted_job_without_parseable_id(
    monkeypatch,
):
    tracked = []

    def runner(command, **kwargs):
        if command[-2:] == ["list", "-r"]:
            return completed("")
        return completed("Job has been submitted.")

    monkeypatch.setattr(flink_job_submitter, "write_active_flink_job", tracked.append)
    submitted = flink_job_submitter.ensure_pageview_stats_job(runner)

    assert submitted is True
    assert tracked[0]["job_id"] is None
    assert tracked[0]["job_name"] == "pageview-stats"
    assert tracked[0]["status"] == "submitted"
    assert "updated_at" in tracked[0]


def test_job_id_from_text_matches_job_name():
    output = (
        "01.01.2026 : 11111111111111111111111111111111 : other (RUNNING)\n"
        "01.01.2026 : 22222222222222222222222222222222 : pageview-stats (RUNNING)"
    )

    assert (
        flink_job_submitter.job_id_from_text(output, "pageview-stats")
        == "22222222222222222222222222222222"
    )
