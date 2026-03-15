import logging
import time
from typing import Callable, TypeVar

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("pipeline")

T = TypeVar("T")


def wait_for_connection(
    name: str, connect: Callable[[], T], retries: int = 10, delay: int = 5
) -> T:
    """Retry a connection function until it succeeds or retries are exhausted."""
    for attempt in range(1, retries + 1):
        try:
            return connect()
        except Exception as e:
            logger.warning(f"{name}: connection attempt {attempt}/{retries} failed: {e}")
            if attempt == retries:
                raise
            time.sleep(delay)
    raise RuntimeError(f"{name}: failed to connect")  # unreachable, keeps pyright happy
