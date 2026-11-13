"""Unit tests for timeframe domain models and duration parsing."""

from datetime import datetime, timedelta, timezone

import pytest

from open_quant_intelligence.domain.timeframe import (
    InvalidTimeframeError,
    SupportedTimeframe,
    compute_close_time,
    parse_timeframe,
    timeframe_to_timedelta,
)


def test_supported_timeframes_parse_correctly() -> None:
    expected_mapping = {
        "1m": SupportedTimeframe.M1,
        "5m": SupportedTimeframe.M5,
        "15m": SupportedTimeframe.M15,
        "30m": SupportedTimeframe.M30,
        "1h": SupportedTimeframe.H1,
        "4h": SupportedTimeframe.H4,
        "1d": SupportedTimeframe.D1,
    }
    for tf_str, expected_enum in expected_mapping.items():
        assert parse_timeframe(tf_str) == expected_enum
        assert parse_timeframe(expected_enum) == expected_enum

    # Hours and days are case-insensitive
    assert parse_timeframe("1H") == SupportedTimeframe.H1
    assert parse_timeframe("4H") == SupportedTimeframe.H4
    assert parse_timeframe("1D") == SupportedTimeframe.D1


def test_supported_timeframe_durations() -> None:
    assert timeframe_to_timedelta("1m") == timedelta(minutes=1)
    assert timeframe_to_timedelta("5m") == timedelta(minutes=5)
    assert timeframe_to_timedelta("15m") == timedelta(minutes=15)
    assert timeframe_to_timedelta("30m") == timedelta(minutes=30)
    assert timeframe_to_timedelta("1h") == timedelta(hours=1)
    assert timeframe_to_timedelta("4h") == timedelta(hours=4)
    assert timeframe_to_timedelta("1d") == timedelta(days=1)


@pytest.mark.parametrize(
    "invalid_input",
    [
        "2m",
        "10m",
        "2h",
        "6h",
        "12h",
        "1w",
        "1M",
        "1y",
        "foo",
        "",
        "   ",
        None,
        123,
    ],
)
def test_unsupported_timeframes_rejected(invalid_input: object) -> None:
    with pytest.raises(InvalidTimeframeError):
        parse_timeframe(invalid_input)  # type: ignore[arg-type]


def test_compute_close_time() -> None:
    open_t = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    expected_close = datetime(2026, 1, 1, 13, 0, tzinfo=timezone.utc)
    assert compute_close_time(open_t, "1h") == expected_close

    expected_4h_close = datetime(2026, 1, 1, 16, 0, tzinfo=timezone.utc)
    assert compute_close_time(open_t, "4h") == expected_4h_close
