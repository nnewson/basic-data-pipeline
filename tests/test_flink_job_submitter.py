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


def test_ensure_pageview_stats_job_skips_submit_when_running():
    calls = []

    def runner(command, **kwargs):
        calls.append(command)
        return completed("01.01.2026 : abc : pageview-stats (RUNNING)")

    submitted = flink_job_submitter.ensure_pageview_stats_job(runner)

    assert submitted is False
    assert calls == [flink_job_submitter.flink_command("list", "-r")]


def test_ensure_pageview_stats_job_submits_when_missing():
    calls = []

    def runner(command, **kwargs):
        calls.append(command)
        if command[-2:] == ["list", "-r"]:
            return completed("")
        return completed("Job has been submitted with JobID abc")

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
