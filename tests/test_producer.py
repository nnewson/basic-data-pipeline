from uuid import UUID

import pytest
from faker import Faker

from pipeline.producer import create_event, get_partition


@pytest.fixture
def fake():
    return Faker()


REQUIRED_KEYS = {"event_id", "user_id", "page", "timestamp"}


def test_create_event_has_required_keys(fake):
    event = create_event(fake, ["/"])

    assert set(event.keys()) == REQUIRED_KEYS


@pytest.mark.parametrize(
    "pages",
    [
        ["/"],
        ["/pricing", "/docs"],
        ["/", "/pricing", "/docs", "/checkout"],
    ],
)
def test_create_event_page_is_from_list(fake, pages):
    event = create_event(fake, pages)

    assert event["page"] in pages


def test_create_event_id_is_valid_uuid(fake):
    event = create_event(fake, ["/"])

    UUID(event["event_id"])  # raises ValueError if invalid


def test_create_event_user_id_is_string(fake):
    event = create_event(fake, ["/"])

    assert isinstance(event["user_id"], str)
    assert len(event["user_id"]) > 0


def test_create_event_timestamp_is_float(fake):
    event = create_event(fake, ["/"])

    assert isinstance(event["timestamp"], float)


def test_create_event_generates_unique_ids(fake):
    events = [create_event(fake, ["/"]) for _ in range(10)]
    ids = [e["event_id"] for e in events]

    assert len(set(ids)) == 10


@pytest.mark.parametrize(
    "pages",
    [
        ["/"],
        ["/a", "/b"],
        ["/x", "/y", "/z"],
    ],
)
def test_create_event_single_page_selected(fake, pages):
    """Event should contain exactly one page, not a list."""
    event = create_event(fake, pages)

    assert isinstance(event["page"], str)


# --- get_partition ---


@pytest.mark.parametrize(
    "username, num_partitions, expected",
    [
        # 4 partitions: a-g → 0, h-m → 1, n-t → 2, u-z → 3
        ("alice", 4, 0),
        ("george", 4, 0),
        ("harry", 4, 1),
        ("mike", 4, 1),
        ("nancy", 4, 2),
        ("tara", 4, 2),
        ("uma", 4, 3),
        ("zara", 4, 3),
        # Case insensitive
        ("Alice", 4, 0),
        ("ZARA", 4, 3),
        # 2 partitions: a-m → 0, n-z → 1
        ("alice", 2, 0),
        ("mike", 2, 0),
        ("nancy", 2, 1),
        ("zara", 2, 1),
        # 1 partition: everything → 0
        ("alice", 1, 0),
        ("zara", 1, 0),
    ],
)
def test_get_partition_routes_by_first_letter(username, num_partitions, expected):
    assert get_partition(username, num_partitions) == expected


def test_get_partition_non_alpha_defaults_to_zero():
    assert get_partition("123user", 4) == 0
    assert get_partition("_underscore", 4) == 0


def test_get_partition_result_always_in_range():
    """Every letter should map to a valid partition index."""
    for letter in "abcdefghijklmnopqrstuvwxyz":
        result = get_partition(letter, 4)
        assert 0 <= result < 4


def test_get_partition_even_distribution():
    """With 4 partitions and 26 letters, each partition should get 6-7 letters."""
    counts = [0, 0, 0, 0]
    for letter in "abcdefghijklmnopqrstuvwxyz":
        counts[get_partition(letter, 4)] += 1
    for count in counts:
        assert count in (6, 7)
