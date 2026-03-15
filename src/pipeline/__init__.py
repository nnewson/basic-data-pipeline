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


def get_partition(username: str, num_partitions: int) -> int:
    """Route a username to a partition based on its first letter.

    Splits the alphabet evenly across partitions:
      4 partitions: a-g → 0, h-m → 1, n-t → 2, u-z → 3
    """
    first_char = username[0].lower()
    if not first_char.isalpha():
        return 0
    index = ord(first_char) - ord("a")  # 0-25
    bucket_size = 26 / num_partitions
    return int(index // bucket_size)
