import logging
import subprocess
import time
from collections.abc import Callable, Sequence

from pipeline.config import (
    FLINK_JOB_SUBMIT_INTERVAL_SECONDS,
    FLINK_JOBMANAGER_SERVICE,
    FLINK_PAGEVIEW_STATS_JOB_NAME,
    FLINK_PAGEVIEW_STATS_JOB_PATH,
)

logger = logging.getLogger("flink-job-submitter")

CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def flink_command(*args: str) -> list[str]:
    return [
        "docker",
        "compose",
        "exec",
        "-T",
        FLINK_JOBMANAGER_SERVICE,
        "flink",
        *args,
    ]


def run_command(
    command: Sequence[str], runner: CommandRunner = subprocess.run
) -> subprocess.CompletedProcess[str]:
    return runner(
        command,
        capture_output=True,
        check=True,
        text=True,
        timeout=120,
    )


def is_job_running(list_output: str, job_name: str) -> bool:
    return any(job_name in line for line in list_output.splitlines())


def list_running_jobs(runner: CommandRunner = subprocess.run) -> str:
    result = run_command(flink_command("list", "-r"), runner)
    return result.stdout


def submit_pageview_stats_job(runner: CommandRunner = subprocess.run) -> str:
    result = run_command(
        flink_command("run", "-d", "-py", FLINK_PAGEVIEW_STATS_JOB_PATH), runner
    )
    return result.stdout


def ensure_pageview_stats_job(
    runner: CommandRunner = subprocess.run,
    job_name: str = FLINK_PAGEVIEW_STATS_JOB_NAME,
) -> bool:
    running_jobs = list_running_jobs(runner)
    if is_job_running(running_jobs, job_name):
        logger.info("Flink job %s is already running", job_name)
        return False

    logger.info("Flink job %s is not running; submitting it", job_name)
    output = submit_pageview_stats_job(runner)
    logger.info("Flink submit output: %s", output.strip())
    return True


def run_forever(
    interval_seconds: int = FLINK_JOB_SUBMIT_INTERVAL_SECONDS,
    runner: CommandRunner = subprocess.run,
) -> None:
    while True:
        try:
            ensure_pageview_stats_job(runner)
        except subprocess.CalledProcessError as exc:
            logger.warning(
                "Flink job check failed with exit code %s: %s%s",
                exc.returncode,
                exc.stderr,
                exc.stdout,
            )
        except subprocess.TimeoutExpired:
            logger.warning("Flink job check timed out")

        time.sleep(interval_seconds)


def main() -> None:
    try:
        run_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down Flink job submitter")


if __name__ == "__main__":
    main()
