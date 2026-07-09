from pipeline.flink_smoke_test import result_from_message


def test_result_from_message_returns_matching_page():
    result = result_from_message(
        {
            "page": "/pricing-smoke",
            "count": 40,
            "window_start": "2026-07-09T12:00:00.000Z",
            "window_end": "2026-07-09T12:00:10.000Z",
        },
        "/pricing-smoke",
    )

    assert result is not None
    assert result.page == "/pricing-smoke"
    assert result.count == 40
    assert result.window_start == "2026-07-09T12:00:00.000Z"
    assert result.window_end == "2026-07-09T12:00:10.000Z"


def test_result_from_message_ignores_other_pages():
    result = result_from_message(
        {
            "page": "/docs",
            "count": 40,
            "window_start": "2026-07-09T12:00:00.000Z",
            "window_end": "2026-07-09T12:00:10.000Z",
        },
        "/pricing-smoke",
    )

    assert result is None
