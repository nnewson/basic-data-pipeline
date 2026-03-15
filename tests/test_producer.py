from uuid import UUID

import pytest
from faker import Faker

from pipeline.producer import create_event


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
