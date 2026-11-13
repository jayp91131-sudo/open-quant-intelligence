"""Unit tests for candle CSV and in-memory series validation."""

from datetime import datetime, timezone
from io import StringIO

import pytest

from open_quant_intelligence.data.validation import (
    validate_candle_csv,
    validate_candles,
)
from tests.fixtures.synthetic_data import (
    candles_to_csv_string,
    create_synthetic_candle,
    generate_synthetic_series,
)


def test_valid_hourly_candle_series_passes() -> None:
    candles = generate_synthetic_series(count=24, timeframe="1h")
    csv_str = candles_to_csv_string(candles)

    result = validate_candle_csv(StringIO(csv_str))

    assert result.is_valid is True
    assert result.total_rows == 24
    assert len(result.valid_candles) == 24
    assert len(result.errors) == 0
    assert len(result.gaps) == 0
    assert result.symbol == "BTC/USDT"
    assert result.timeframe == "1h"
    assert result.market_type == "spot"


def test_strictly_increasing_timestamps_pass() -> None:
    candles = generate_synthetic_series(count=10, timeframe="15m")
    result = validate_candles(candles)

    assert result.is_valid is True
    assert len(result.valid_candles) == 10
    assert len(result.errors) == 0


def test_naive_timestamp_rejected() -> None:
    csv_data = (
        "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
        "binance,spot,BTC/USDT,1h,2026-01-01T00:00:00,50000.0,50500.0,49500.0,50200.0,10.0\n"
    )
    result = validate_candle_csv(StringIO(csv_data))

    assert result.is_valid is False
    assert any(err.issue_type == "TIMEZONE_NAIVE" for err in result.errors)


def test_invalid_timezone_timestamp_string_rejected() -> None:
    csv_data = (
        "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
        "binance,spot,BTC/USDT,1h,not-a-valid-timestamp,50000.0,50500.0,49500.0,50200.0,10.0\n"
    )
    result = validate_candle_csv(StringIO(csv_data))

    assert result.is_valid is False
    assert any(err.issue_type == "INVALID_TIMESTAMP_FORMAT" for err in result.errors)


def test_duplicate_open_time_rejected() -> None:
    t1 = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    c1 = create_synthetic_candle(open_time=t1)
    c2 = create_synthetic_candle(open_time=t1)  # Exact duplicate timestamp

    csv_data = candles_to_csv_string([c1, c2])
    result = validate_candle_csv(StringIO(csv_data))

    assert result.is_valid is False
    assert any(err.issue_type == "DUPLICATE_TIMESTAMP" for err in result.errors)


def test_out_of_order_candles_rejected_not_auto_sorted() -> None:
    t1 = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc)
    t3 = datetime(2026, 1, 1, 2, 0, tzinfo=timezone.utc)

    c1 = create_synthetic_candle(open_time=t1)
    c2 = create_synthetic_candle(open_time=t2)
    c3 = create_synthetic_candle(open_time=t3)

    # Inverted order: c1, c3, c2
    csv_data = candles_to_csv_string([c1, c3, c2])
    result = validate_candle_csv(StringIO(csv_data))

    assert result.is_valid is False
    assert any(err.issue_type == "OUT_OF_ORDER_TIMESTAMP" for err in result.errors)


def test_missing_interval_reported_and_no_row_inserted() -> None:
    t1 = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc)
    # Skipping 2 hours: next is 04:00:00 (gap of 2 candles: 02:00 and 03:00)
    t3 = datetime(2026, 1, 1, 4, 0, tzinfo=timezone.utc)

    c1 = create_synthetic_candle(open_time=t1)
    c2 = create_synthetic_candle(open_time=t2)
    c3 = create_synthetic_candle(open_time=t3)

    csv_data = candles_to_csv_string([c1, c2, c3])
    result = validate_candle_csv(StringIO(csv_data))

    # Standard validation passes valid rows and records the gap
    assert result.is_valid is True
    assert result.total_rows == 3
    # Exactly 3 candles ingested; NO synthetic rows inserted!
    assert len(result.valid_candles) == 3
    assert len(result.gaps) == 1
    assert result.gaps[0].missing_count == 2
    assert result.gaps[0].expected_open == datetime(2026, 1, 1, 2, 0, tzinfo=timezone.utc)
    assert result.gaps[0].actual_open == t3

    # With fail_on_gaps=True, result is marked invalid
    strict_result = validate_candle_csv(StringIO(csv_data), fail_on_gaps=True)
    assert strict_result.is_valid is False


@pytest.mark.parametrize("zero_or_neg_price", ["0.0", "-15.0"])
def test_zero_or_negative_price_rejected_in_csv(zero_or_neg_price: str) -> None:
    csv_data = (
        "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
        f"binance,spot,BTC/USDT,1h,2026-01-01T00:00:00Z,{zero_or_neg_price},50500.0,49500.0,50200.0,10.0\n"
    )
    result = validate_candle_csv(StringIO(csv_data))
    assert result.is_valid is False
    assert any(err.issue_type == "CANDLE_CONTRACT_VIOLATION" for err in result.errors)


@pytest.mark.parametrize("non_finite_price", ["nan", "inf", "-inf"])
def test_non_finite_price_rejected_in_csv(non_finite_price: str) -> None:
    csv_data = (
        "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
        f"binance,spot,BTC/USDT,1h,2026-01-01T00:00:00Z,50000.0,{non_finite_price},49500.0,50200.0,10.0\n"
    )
    result = validate_candle_csv(StringIO(csv_data))
    assert result.is_valid is False
    assert any(err.issue_type == "CANDLE_CONTRACT_VIOLATION" for err in result.errors)


@pytest.mark.parametrize("bad_volume", ["-1.0", "nan", "inf"])
def test_negative_or_non_finite_volume_rejected_in_csv(bad_volume: str) -> None:
    csv_data = (
        "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
        f"binance,spot,BTC/USDT,1h,2026-01-01T00:00:00Z,50000.0,50500.0,49500.0,50200.0,{bad_volume}\n"
    )
    result = validate_candle_csv(StringIO(csv_data))
    assert result.is_valid is False
    assert any(err.issue_type == "CANDLE_CONTRACT_VIOLATION" for err in result.errors)


def test_high_below_open_close_low_rejected_in_csv() -> None:
    csv_data = (
        "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
        "binance,spot,BTC/USDT,1h,2026-01-01T00:00:00Z,50000.0,49000.0,48000.0,49500.0,10.0\n"
    )
    result = validate_candle_csv(StringIO(csv_data))
    assert result.is_valid is False
    assert any(err.issue_type == "CANDLE_CONTRACT_VIOLATION" for err in result.errors)


def test_low_above_open_close_high_rejected_in_csv() -> None:
    csv_data = (
        "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
        "binance,spot,BTC/USDT,1h,2026-01-01T00:00:00Z,50000.0,51000.0,50500.0,50200.0,10.0\n"
    )
    result = validate_candle_csv(StringIO(csv_data))
    assert result.is_valid is False
    assert any(err.issue_type == "CANDLE_CONTRACT_VIOLATION" for err in result.errors)


def test_mixed_symbols_rejected() -> None:
    csv_data = (
        "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
        "binance,spot,BTC/USDT,1h,2026-01-01T00:00:00Z,50000.0,50500.0,49500.0,50200.0,10.0\n"
        "binance,spot,ETH/USDT,1h,2026-01-01T01:00:00Z,3000.0,3050.0,2950.0,3020.0,10.0\n"
    )
    result = validate_candle_csv(StringIO(csv_data))
    assert result.is_valid is False
    assert any(err.issue_type == "MIXED_SYMBOLS" for err in result.errors)


def test_mixed_timeframes_rejected() -> None:
    csv_data = (
        "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
        "binance,spot,BTC/USDT,1h,2026-01-01T00:00:00Z,50000.0,50500.0,49500.0,50200.0,10.0\n"
        "binance,spot,BTC/USDT,15m,2026-01-01T01:00:00Z,50200.0,50600.0,50100.0,50400.0,10.0\n"
    )
    result = validate_candle_csv(StringIO(csv_data))
    assert result.is_valid is False
    assert any(err.issue_type == "MIXED_TIMEFRAMES" for err in result.errors)


def test_unsupported_timeframe_in_csv_rejected() -> None:
    csv_data = (
        "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
        "binance,spot,BTC/USDT,2h,2026-01-01T00:00:00Z,50000.0,50500.0,49500.0,50200.0,10.0\n"
    )
    result = validate_candle_csv(StringIO(csv_data))
    assert result.is_valid is False
    assert any(err.issue_type == "UNSUPPORTED_TIMEFRAME" for err in result.errors)


def test_missing_csv_columns_rejected() -> None:
    csv_data = "exchange_id,market_type,symbol,timeframe\nbinance,spot,BTC/USDT,1h\n"
    result = validate_candle_csv(StringIO(csv_data))
    assert result.is_valid is False
    assert any(err.issue_type == "MISSING_COLUMNS" for err in result.errors)


def test_empty_csv_file_rejected() -> None:
    csv_data = ""
    result = validate_candle_csv(StringIO(csv_data))
    assert result.is_valid is False
    assert any(err.issue_type == "EMPTY_FILE" for err in result.errors)
