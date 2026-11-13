"""Timeframe definitions, parsing, and duration calculations."""

from datetime import datetime, timedelta
from enum import Enum


class InvalidTimeframeError(ValueError):
    """Raised when an unsupported or malformed timeframe string is provided."""


class SupportedTimeframe(str, Enum):
    """Explicitly supported candle timeframe intervals."""

    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H4 = "4h"
    D1 = "1d"


_TIMEFRAME_TO_TIMEDELTA: dict[SupportedTimeframe, timedelta] = {
    SupportedTimeframe.M1: timedelta(minutes=1),
    SupportedTimeframe.M5: timedelta(minutes=5),
    SupportedTimeframe.M15: timedelta(minutes=15),
    SupportedTimeframe.M30: timedelta(minutes=30),
    SupportedTimeframe.H1: timedelta(hours=1),
    SupportedTimeframe.H4: timedelta(hours=4),
    SupportedTimeframe.D1: timedelta(days=1),
}


def parse_timeframe(value: str | SupportedTimeframe) -> SupportedTimeframe:
    """Parse and validate a timeframe string into a SupportedTimeframe.

    Args:
        value: Timeframe string or SupportedTimeframe enum member.

    Returns:
        SupportedTimeframe instance.

    Raises:
        InvalidTimeframeError: If value is not an explicitly supported timeframe.
    """
    if isinstance(value, SupportedTimeframe):
        return value

    if not isinstance(value, str):
        raise InvalidTimeframeError(
            f"Timeframe must be a string or SupportedTimeframe, got {type(value).__name__}: {value!r}"
        )

    clean = value.strip()
    supported_list = ", ".join(t.value for t in SupportedTimeframe)

    # In exchange conventions, uppercase 'M' designates month, which is unsupported
    if "M" in clean:
        raise InvalidTimeframeError(
            f"Unsupported timeframe: {value!r}. Note that 'M' denotes months (unsupported). "
            f"Supported intervals are: {supported_list}"
        )

    normalized = clean.lower()
    for tf in SupportedTimeframe:
        if tf.value == normalized:
            return tf

    raise InvalidTimeframeError(f"Unsupported timeframe: {value!r}. Supported intervals are: {supported_list}")


def timeframe_to_timedelta(value: str | SupportedTimeframe) -> timedelta:
    """Convert a supported timeframe to its corresponding timedelta.

    Args:
        value: Timeframe string or SupportedTimeframe.

    Returns:
        timedelta representing the duration of one candle interval.

    Raises:
        InvalidTimeframeError: If value is not a supported timeframe.
    """
    tf = parse_timeframe(value)
    return _TIMEFRAME_TO_TIMEDELTA[tf]


def compute_close_time(
    open_time: datetime,
    timeframe: str | SupportedTimeframe,
) -> datetime:
    """Compute the deterministic exclusive upper boundary close_time for a candle.

    Convention:
        The candle interval follows [open_time, close_time).
        `close_time` represents the exclusive boundary (open_time + duration).
        For example, a 1-hour candle starting at 10:00:00Z has a close boundary
        at 11:00:00Z. A trade occurring at 11:00:00Z belongs to the next candle.

    Args:
        open_time: Timezone-aware candle open timestamp (inclusive start).
        timeframe: Supported timeframe string or enum.

    Returns:
        Timezone-aware candle close timestamp (exclusive upper boundary).
    """
    duration = timeframe_to_timedelta(timeframe)
    return open_time + duration
