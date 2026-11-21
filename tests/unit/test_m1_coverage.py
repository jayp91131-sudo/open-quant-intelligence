"""Unit tests for window coverage analysis and gap invariant assertions."""

from datetime import datetime, timezone

import pytest
from open_quant_intelligence.data.candle import Candle
from open_quant_intelligence.data.market import MarketIdentity
from open_quant_intelligence.data.timeframe import SupportedTimeframe

from open_quant_intelligence.data.quality import (
    GapType,
    QualityStatus,
    UnalignedCandleError,
    analyze_dataset_quality,
)


def _make_candle(open_ms: int, tf: SupportedTimeframe = SupportedTimeframe.H1) -> Candle:
    dt = datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc)
    return Candle(
        market=MarketIdentity(exchange_id="binance", market_type="spot", symbol="BTC/USDT"),
        timeframe=tf,
        open_time=dt,
        open=100.0,
        high=105.0,
        low=95.0,
        close=102.0,
        base_volume=10.0,
    )


def test_coverage_invariant_holds() -> None:
    """Requirement 10: Asserts record_count + missing_intervals == expected_records across all permutations."""
    # 5 intervals total: [0, 18_000_000) (0h, 1h, 2h, 3h, 4h)
    # Permutation 1: Full gap-free
    candles_full = [_make_candle(i * 3_600_000) for i in range(5)]
    rep_full = analyze_dataset_quality(candles_full, SupportedTimeframe.H1, 0, 18_000_000)
    assert rep_full.status == QualityStatus.GAP_FREE
    assert rep_full.record_count == 5
    assert rep_full.missing_intervals == 0
    assert rep_full.record_count + rep_full.missing_intervals == rep_full.expected_records

    # Permutation 2: Leading gap (missing 0h, 1h; present 2h, 3h, 4h)
    candles_leading = [_make_candle(i * 3_600_000) for i in range(2, 5)]
    rep_leading = analyze_dataset_quality(candles_leading, SupportedTimeframe.H1, 0, 18_000_000)
    assert rep_leading.status == QualityStatus.GAP_CONTAINING
    assert rep_leading.record_count == 3
    assert rep_leading.missing_intervals == 2
    assert rep_leading.record_count + rep_leading.missing_intervals == rep_leading.expected_records

    # Permutation 3: Internal gap (present 0h, 4h; missing 1h, 2h, 3h)
    candles_internal = [_make_candle(0), _make_candle(4 * 3_600_000)]
    rep_internal = analyze_dataset_quality(candles_internal, SupportedTimeframe.H1, 0, 18_000_000)
    assert rep_internal.status == QualityStatus.GAP_CONTAINING
    assert rep_internal.record_count == 2
    assert rep_internal.missing_intervals == 3
    assert rep_internal.record_count + rep_internal.missing_intervals == rep_internal.expected_records

    # Permutation 4: Empty (0 records, 5 missing)
    rep_empty = analyze_dataset_quality([], SupportedTimeframe.H1, 0, 18_000_000)
    assert rep_empty.status == QualityStatus.EMPTY
    assert rep_empty.record_count == 0
    assert rep_empty.missing_intervals == 5
    assert rep_empty.record_count + rep_empty.missing_intervals == rep_empty.expected_records


def test_full_window_coverage_leading_gap() -> None:
    """Requirement 6: Asserts missing first interval is classified as LEADING gap and increments missing_intervals."""
    # Window [0, 10_800_000) (3 hours: 0h, 1h, 2h)
    # Candles start at 1h (missing 0h)
    candles = [_make_candle(3_600_000), _make_candle(7_200_000)]
    rep = analyze_dataset_quality(candles, SupportedTimeframe.H1, 0, 10_800_000)

    assert rep.status == QualityStatus.GAP_CONTAINING
    assert rep.record_count == 2
    assert rep.missing_intervals == 1
    assert rep.gap_count == 1
    gap = rep.gaps[0]
    assert gap.gap_type == GapType.LEADING
    assert gap.missing_count == 1
    assert gap.subsequent_open_utc is not None


def test_full_window_coverage_trailing_gap() -> None:
    """Requirement 7: Asserts missing last interval is classified as TRAILING gap with subsequent_open = None."""
    # Window [0, 10_800_000) (3 hours: 0h, 1h, 2h)
    # Candles end at 1h (missing 2h)
    candles = [_make_candle(0), _make_candle(3_600_000)]
    rep = analyze_dataset_quality(candles, SupportedTimeframe.H1, 0, 10_800_000)

    assert rep.status == QualityStatus.GAP_CONTAINING
    assert rep.record_count == 2
    assert rep.missing_intervals == 1
    assert rep.gap_count == 1
    gap = rep.gaps[0]
    assert gap.gap_type == GapType.TRAILING
    assert gap.missing_count == 1
    assert gap.subsequent_open_utc is None


def test_full_window_coverage_internal_gap() -> None:
    """Requirement 8: Asserts gap between consecutive candles is recorded as INTERNAL."""
    # Window [0, 10_800_000) (3 hours: 0h, 1h, 2h)
    # Candles at 0h and 2h (missing 1h)
    candles = [_make_candle(0), _make_candle(7_200_000)]
    rep = analyze_dataset_quality(candles, SupportedTimeframe.H1, 0, 10_800_000)

    assert rep.status == QualityStatus.GAP_CONTAINING
    assert rep.record_count == 2
    assert rep.missing_intervals == 1
    assert rep.gap_count == 1
    gap = rep.gaps[0]
    assert gap.gap_type == GapType.INTERNAL
    assert gap.missing_count == 1
    assert gap.subsequent_open_utc is not None


def test_unaligned_candle_timestamp_aborts() -> None:
    """Requirement 9: Asserts candle with timestamp not on timeframe grid raises UnalignedCandleError."""
    # 1h candle at 1_000 ms (unaligned)
    candles = [_make_candle(1_000)]
    with pytest.raises(UnalignedCandleError, match="not aligned"):
        analyze_dataset_quality(candles, SupportedTimeframe.H1, 0, 3_600_000)
