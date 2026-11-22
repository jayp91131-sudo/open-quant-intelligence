"""Data quality, gap analysis, and coverage reporting for Open Quant Intelligence."""

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Sequence

from open_quant_intelligence.domain.candle import Candle
from open_quant_intelligence.domain.timeframe import SupportedTimeframe
from open_quant_intelligence.ingestion.time_utils import timeframe_to_duration_ms

EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)


class QualityStatus(str, Enum):
    """Quality classification of the dataset."""

    EMPTY = "EMPTY"
    GAP_FREE = "GAP_FREE"
    GAP_CONTAINING = "GAP_CONTAINING"


class GapType(str, Enum):
    """Categorization of detected gap events across the requested window."""

    LEADING = "LEADING"
    INTERNAL = "INTERNAL"
    TRAILING = "TRAILING"
    FULL_WINDOW = "FULL_WINDOW"


class InconsistentGapStatisticsError(ValueError):
    """Raised when record_count + missing_intervals != expected_records."""


class UnalignedCandleError(ValueError):
    """Raised when an admitted candle open timestamp does not align to the timeframe grid."""


@dataclass(frozen=True)
class QualityGapEvent:
    """Detailed record of a missing interval gap event."""

    gap_type: GapType
    expected_open_utc: datetime
    subsequent_open_utc: datetime | None
    missing_count: int
    expected_open_ms: int = 0
    subsequent_open_ms: int | None = None

    def __init__(
        self,
        gap_type: GapType,
        expected_open_utc: datetime | None = None,
        subsequent_open_utc: datetime | None = None,
        missing_count: int = 1,
        expected_open_ms: int | None = None,
        subsequent_open_ms: int | None = None,
        **kwargs: Any,
    ) -> None:
        object.__setattr__(self, "gap_type", gap_type)
        if expected_open_utc is None and expected_open_ms is not None:
            expected_open_utc = datetime.fromtimestamp(expected_open_ms / 1000, tz=timezone.utc)
        if expected_open_ms is None and expected_open_utc is not None:
            expected_open_ms = int(expected_open_utc.timestamp() * 1000)
        if subsequent_open_utc is None and subsequent_open_ms is not None:
            subsequent_open_utc = datetime.fromtimestamp(subsequent_open_ms / 1000, tz=timezone.utc)
        if subsequent_open_ms is None and subsequent_open_utc is not None:
            subsequent_open_ms = int(subsequent_open_utc.timestamp() * 1000)

        object.__setattr__(self, "expected_open_utc", expected_open_utc)
        object.__setattr__(self, "subsequent_open_utc", subsequent_open_utc)
        object.__setattr__(self, "missing_count", missing_count)
        object.__setattr__(self, "expected_open_ms", expected_open_ms or 0)
        object.__setattr__(self, "subsequent_open_ms", subsequent_open_ms)


@dataclass(frozen=True)
class DataQualityReport:
    """Comprehensive data quality and coverage analysis report for a dataset."""

    requested_since_ms: int
    requested_until_ms: int
    timeframe: SupportedTimeframe
    expected_records: int
    record_count: int
    missing_intervals: int
    gap_count: int
    gaps: tuple[QualityGapEvent, ...]
    quality_status: str  # 'EMPTY', 'GAP_FREE', 'GAP_CONTAINING'

    @property
    def status(self) -> QualityStatus:
        """Enum representation of quality_status."""
        return QualityStatus(self.quality_status)

    @property
    def is_gap_free(self) -> bool:
        """True if all expected intervals were observed without gaps."""
        return self.missing_intervals == 0 and self.record_count == self.expected_records


def analyze_dataset_quality(
    candles: Sequence[Candle],
    arg2: Any,
    arg3: Any,
    arg4: Any | None = None,
) -> DataQualityReport:
    """Analyze full requested-window coverage, detecting leading, internal, and trailing gaps.

    Supports both (candles, since_ms, until_ms, timeframe) and (candles, timeframe, since_ms, until_ms).

    Args:
        candles: Sequence of validated immutable Candle domain instances sorted chronologically.
        since_ms: Exact inclusive start boundary in integer epoch milliseconds.
        until_ms: Exact exclusive upper boundary in integer epoch milliseconds.
        timeframe: Supported candle timeframe enum member.

    Returns:
        DataQualityReport with comprehensive gap distribution and coverage stats.

    Raises:
        ValueError: If until_ms <= since_ms or boundaries are not grid-aligned.
        UnalignedCandleError: If any candle open timestamp does not align to the timeframe grid.
        InconsistentGapStatisticsError: If record_count + missing_intervals != expected_records.
    """
    if isinstance(arg2, SupportedTimeframe):
        timeframe = arg2
        since_ms = int(arg3)
        until_ms = int(arg4)
    else:
        since_ms = int(arg2)
        until_ms = int(arg3)
        timeframe = arg4
    if until_ms <= since_ms:
        raise ValueError(f"until_ms ({until_ms}) must be strictly greater than since_ms ({since_ms})")

    step_ms = timeframe_to_duration_ms(timeframe)
    if since_ms % step_ms != 0:
        raise ValueError(f"since_ms ({since_ms}) is not grid-aligned to {timeframe.value} ({step_ms}ms)")
    if until_ms % step_ms != 0:
        raise ValueError(f"until_ms ({until_ms}) is not grid-aligned to {timeframe.value} ({step_ms}ms)")

    expected_records = (until_ms - since_ms) // step_ms
    record_count = len(candles)

    # 1. Zero-candle branch (Legitimate EMPTY)
    if record_count == 0:
        full_gap = QualityGapEvent(
            gap_type=GapType.FULL_WINDOW,
            expected_open_ms=since_ms,
            expected_open_utc=EPOCH_UTC.fromtimestamp(since_ms / 1000, tz=timezone.utc),
            subsequent_open_ms=None,
            subsequent_open_utc=None,
            missing_count=expected_records,
        )
        return DataQualityReport(
            requested_since_ms=since_ms,
            requested_until_ms=until_ms,
            timeframe=timeframe,
            expected_records=expected_records,
            record_count=0,
            missing_intervals=expected_records,
            gap_count=1,
            gaps=(full_gap,),
            quality_status="EMPTY",
        )

    # 2. Non-empty branch: verify grid alignment and order
    gaps: list[QualityGapEvent] = []
    candle_open_times_ms: list[int] = []

    for idx, c in enumerate(candles):
        c_open_ms = int(c.open_time.timestamp() * 1000)
        if (c_open_ms - since_ms) % step_ms != 0:
            raise UnalignedCandleError(
                f"Candle at row {idx} open_time {c.open_time.isoformat()} ({c_open_ms}ms) "
                f"is not aligned to {timeframe.value} grid relative to since_ms {since_ms}ms."
            )
        if c_open_ms < since_ms or c_open_ms >= until_ms:
            raise ValueError(
                f"Candle at row {idx} open_time {c.open_time.isoformat()} ({c_open_ms}ms) "
                f"falls outside requested window [{since_ms}, {until_ms})."
            )
        candle_open_times_ms.append(c_open_ms)

    # Check leading gap
    first_open_ms = candle_open_times_ms[0]
    if first_open_ms > since_ms:
        missing_leading = (first_open_ms - since_ms) // step_ms
        gaps.append(
            QualityGapEvent(
                gap_type=GapType.LEADING,
                expected_open_ms=since_ms,
                expected_open_utc=datetime.fromtimestamp(since_ms / 1000, tz=timezone.utc),
                subsequent_open_ms=first_open_ms,
                subsequent_open_utc=datetime.fromtimestamp(first_open_ms / 1000, tz=timezone.utc),
                missing_count=missing_leading,
            )
        )

    # Check internal gaps
    for i in range(len(candle_open_times_ms) - 1):
        curr_open = candle_open_times_ms[i]
        next_open = candle_open_times_ms[i + 1]
        expected_next = curr_open + step_ms
        if next_open > expected_next:
            missing_internal = (next_open - expected_next) // step_ms
            gaps.append(
                QualityGapEvent(
                    gap_type=GapType.INTERNAL,
                    expected_open_ms=expected_next,
                    expected_open_utc=datetime.fromtimestamp(expected_next / 1000, tz=timezone.utc),
                    subsequent_open_ms=next_open,
                    subsequent_open_utc=datetime.fromtimestamp(next_open / 1000, tz=timezone.utc),
                    missing_count=missing_internal,
                )
            )

    # Check trailing gap
    last_open_ms = candle_open_times_ms[-1]
    last_close_ms = last_open_ms + step_ms
    if last_close_ms < until_ms:
        missing_trailing = (until_ms - last_close_ms) // step_ms
        gaps.append(
            QualityGapEvent(
                gap_type=GapType.TRAILING,
                expected_open_ms=last_close_ms,
                expected_open_utc=datetime.fromtimestamp(last_close_ms / 1000, tz=timezone.utc),
                subsequent_open_ms=None,
                subsequent_open_utc=None,
                missing_count=missing_trailing,
            )
        )

    total_missing = sum(g.missing_count for g in gaps)
    if record_count + total_missing != expected_records:
        raise InconsistentGapStatisticsError(
            f"Coverage mismatch: record_count ({record_count}) + missing_intervals ({total_missing}) "
            f"!= expected_records ({expected_records})."
        )

    quality_status = "GAP_FREE" if total_missing == 0 else "GAP_CONTAINING"

    return DataQualityReport(
        requested_since_ms=since_ms,
        requested_until_ms=until_ms,
        timeframe=timeframe,
        expected_records=expected_records,
        record_count=record_count,
        missing_intervals=total_missing,
        gap_count=len(gaps),
        gaps=tuple(gaps),
        quality_status=quality_status,
    )
