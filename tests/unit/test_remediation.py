"""Regression tests for independent M0 review defect findings and contract hardening.

Covers:
- M0-01: Explicit candle close_time contradicting timeframe duration.
- M0-02: validate_candles crashing with AttributeError when first item is not a Candle.
- M0-03: validate_candle_stream crashing on short/malformed CSV rows missing identity fields.
- M0-04: validate_candle_stream silently dropping unheaded extra row values.
- M0-05: Non-interval-aligned candle timestamps accepted and misleading gap reporting.
- Contract hardening:
  - is_closed must be strictly a boolean.
  - trade_count must be non-negative integer or None.
  - bool values rejected for prices and volumes.
  - MarketIdentity symbol validation, currency agreement, malformed symbol rejection.
  - Empty series policy (in-memory sequence and header-only CSV marked invalid).
"""

import csv
import io
from datetime import datetime, timedelta, timezone

import pytest

from open_quant_intelligence.data.validation import (
    validate_candle_csv,
    validate_candle_stream,
    validate_candles,
)
from open_quant_intelligence.domain.candle import Candle, InvalidCandleError
from open_quant_intelligence.domain.market import InvalidMarketError, MarketIdentity

# ==============================================================================
# M0-01: Explicit Candle close_time validation
# ==============================================================================


def test_m0_01_reproduce_contradictory_close_time_rejected() -> None:
    """Reproduction and fix for M0-01: Contradictory explicit close_time must be rejected."""
    t0 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    # Contradictory: 1h candle but close_time set to 10:30 UTC
    contradictory_t = datetime(2026, 1, 1, 10, 30, 0, tzinfo=timezone.utc)

    with pytest.raises(InvalidCandleError, match="Contradictory close_time"):
        Candle(
            exchange_id="binance",
            market_type="spot",
            symbol="BTC/USDT",
            timeframe="1h",
            open_time=t0,
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
            base_volume=10.0,
            close_time=contradictory_t,
        )


def test_m0_01_exact_matching_close_time_accepted() -> None:
    """Verifies that an explicitly provided close_time matching open_time + duration is accepted."""
    t0 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    exact_close = datetime(2026, 1, 1, 11, 0, 0, tzinfo=timezone.utc)

    candle = Candle(
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        open_time=t0,
        open=50000.0,
        high=51000.0,
        low=49000.0,
        close=50500.0,
        base_volume=10.0,
        close_time=exact_close,
    )
    assert candle.close_time == exact_close


# ==============================================================================
# M0-02: In-memory validator crash when first item is not a Candle
# ==============================================================================


def test_m0_02_reproduce_first_element_not_a_candle() -> None:
    """Reproduction and fix for M0-02: validate_candles([object()]) must not crash with AttributeError."""
    res = validate_candles([object()])  # type: ignore[list-item]
    assert not res.is_valid
    assert len(res.errors) == 1
    assert res.errors[0].issue_type == "INVALID_OBJECT_TYPE"
    assert "Expected Candle instance, got object" in res.errors[0].message


def test_m0_02_mixed_invalid_elements_sequence() -> None:
    """Verifies that invalid objects at start, middle, and end are caught without uncaught exception."""
    t0 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    valid_c = Candle(
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        open_time=t0,
        open=50000.0,
        high=51000.0,
        low=49000.0,
        close=50500.0,
        base_volume=10.0,
    )
    res = validate_candles(["not_a_candle", valid_c, 12345])  # type: ignore[list-item]
    assert not res.is_valid
    assert len(res.errors) == 2
    assert res.errors[0].issue_type == "INVALID_OBJECT_TYPE"
    assert res.errors[1].issue_type == "INVALID_OBJECT_TYPE"


# ==============================================================================
# M0-03: Short / malformed CSV rows
# ==============================================================================


def test_m0_03_reproduce_short_csv_row_no_attribute_error() -> None:
    """Reproduction and fix for M0-03: A row containing only 'binance' must not raise AttributeError."""
    csv_text = (
        "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
        "binance\n"
    )
    reader = csv.DictReader(io.StringIO(csv_text))
    res = validate_candle_stream(reader)
    assert not res.is_valid
    assert any(err.issue_type == "MALFORMED_ROW" for err in res.errors)


def test_m0_03_missing_identity_fields_controlled_error() -> None:
    """CSV row with empty or missing identity fields returns structured errors."""
    csv_text = (
        "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
        ",spot,BTC/USDT,1h,2026-01-01T00:00:00Z,50000,51000,49000,50500,10\n"
        "binance,,BTC/USDT,1h,2026-01-01T01:00:00Z,50500,51500,50000,51000,10\n"
        "binance,spot,,1h,2026-01-01T02:00:00Z,51000,52000,50500,51500,10\n"
        "binance,spot,BTC/USDT,,2026-01-01T03:00:00Z,51500,52500,51000,52000,10\n"
    )
    reader = csv.DictReader(io.StringIO(csv_text))
    res = validate_candle_stream(reader)
    assert not res.is_valid
    assert len(res.errors) >= 4
    for err in res.errors:
        assert err.issue_type == "MISSING_IDENTITY_FIELD"


# ==============================================================================
# M0-04: Extra CSV unheaded row values
# ==============================================================================


def test_m0_04_reproduce_extra_unheaded_csv_columns() -> None:
    """Reproduction and fix for M0-04: Extra 11th column value must be detected as EXTRA_FIELDS."""
    csv_text = (
        "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
        "binance,spot,BTC/USDT,1h,2026-01-01T00:00:00Z,50000,51000,49000,50500,10,unexpected_value\n"
    )
    reader = csv.DictReader(io.StringIO(csv_text))
    res = validate_candle_stream(reader)
    assert not res.is_valid
    assert any(err.issue_type == "EXTRA_FIELDS" for err in res.errors)
    extra_err = next(err for err in res.errors if err.issue_type == "EXTRA_FIELDS")
    assert "unexpected_value" in extra_err.message


# ==============================================================================
# M0-05: Non-interval-aligned timestamps and gap count accuracy
# ==============================================================================


def test_m0_05_reproduce_misaligned_gap_calculation() -> None:
    """Reproduction and fix for M0-05: 00:00 UTC to 03:10 UTC (3h10m) must be rejected as MISALIGNED_INTERVAL."""
    csv_text = (
        "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
        "binance,spot,BTC/USDT,1h,2026-01-01T00:00:00Z,50000,51000,49000,50500,10\n"
        "binance,spot,BTC/USDT,1h,2026-01-01T03:10:00Z,51000,52000,50000,51500,10\n"
    )
    reader = csv.DictReader(io.StringIO(csv_text))
    res = validate_candle_stream(reader)
    assert not res.is_valid
    assert any(err.issue_type == "MISALIGNED_INTERVAL" for err in res.errors)
    assert len(res.gaps) == 0  # No gap computed for off-grid delta


def test_m0_05_in_memory_misaligned_interval() -> None:
    """Verifies that validate_candles catches misaligned intervals for in-memory candles."""
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 1, 1, 3, 10, 0, tzinfo=timezone.utc)
    c1 = Candle(
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        open_time=t0,
        open=50000.0,
        high=51000.0,
        low=49000.0,
        close=50500.0,
        base_volume=10.0,
    )
    c2 = Candle(
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        open_time=t1,
        open=50000.0,
        high=51000.0,
        low=49000.0,
        close=50500.0,
        base_volume=10.0,
    )
    res = validate_candles([c1, c2])
    assert not res.is_valid
    assert any(err.issue_type == "MISALIGNED_INTERVAL" for err in res.errors)
    assert len(res.gaps) == 0


def test_m0_05_exact_grid_gap_count() -> None:
    """Verifies that an exact multiple of timeframe (e.g. 00:00 to 03:00 for 1h) reports exactly 2 missing candles."""
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 1, 1, 3, 0, 0, tzinfo=timezone.utc)
    c1 = Candle(
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        open_time=t0,
        open=50000.0,
        high=51000.0,
        low=49000.0,
        close=50500.0,
        base_volume=10.0,
    )
    c2 = Candle(
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        open_time=t1,
        open=50000.0,
        high=51000.0,
        low=49000.0,
        close=50500.0,
        base_volume=10.0,
    )
    res = validate_candles([c1, c2])
    assert res.is_valid  # in audit mode, gaps are recorded but valid unless fail_on_gaps=True
    assert len(res.gaps) == 1
    assert res.gaps[0].missing_count == 2
    assert res.gaps[0].expected_open == t0 + timedelta(hours=1)
    assert res.gaps[0].actual_open == t1


# ==============================================================================
# Additional Contract Hardening Tests
# ==============================================================================


def test_hardening_is_closed_must_be_strictly_bool() -> None:
    """Reject string 'false' or numeric 0/1 for is_closed."""
    t0 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(InvalidCandleError, match="is_closed must be a boolean"):
        Candle(
            exchange_id="binance",
            market_type="spot",
            symbol="BTC/USDT",
            timeframe="1h",
            open_time=t0,
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
            base_volume=10.0,
            is_closed="false",  # type: ignore[arg-type]
        )


def test_hardening_trade_count_validation() -> None:
    """Reject negative values, floats, or bools for trade_count."""
    t0 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    base_kwargs = {
        "exchange_id": "binance",
        "market_type": "spot",
        "symbol": "BTC/USDT",
        "timeframe": "1h",
        "open_time": t0,
        "open": 50000.0,
        "high": 51000.0,
        "low": 49000.0,
        "close": 50500.0,
        "base_volume": 10.0,
    }

    with pytest.raises(InvalidCandleError, match="trade_count must be a non-negative integer"):
        Candle(**base_kwargs, trade_count=-1)

    with pytest.raises(InvalidCandleError, match="trade_count must be a non-negative integer"):
        Candle(**base_kwargs, trade_count=True)  # type: ignore[arg-type]

    candle = Candle(**base_kwargs, trade_count=42)
    assert candle.trade_count == 42


def test_hardening_bool_rejected_as_price_or_volume() -> None:
    """Reject boolean True/False masquerading as numeric prices or volume."""
    t0 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(InvalidCandleError, match="open price must be a numeric value"):
        Candle(
            exchange_id="binance",
            market_type="spot",
            symbol="BTC/USDT",
            timeframe="1h",
            open_time=t0,
            open=True,  # type: ignore[arg-type]
            high=51000.0,
            low=49000.0,
            close=50500.0,
            base_volume=10.0,
        )

    with pytest.raises(InvalidCandleError, match="base_volume must be numeric"):
        Candle(
            exchange_id="binance",
            market_type="spot",
            symbol="BTC/USDT",
            timeframe="1h",
            open_time=t0,
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
            base_volume=False,  # type: ignore[arg-type]
        )


def test_hardening_market_identity_malformed_symbol_rejected() -> None:
    """Reject symbols without separators or missing base/quote parts."""
    with pytest.raises(InvalidMarketError, match="Symbol must contain a valid pair separator"):
        MarketIdentity(exchange_id="binance", market_type="spot", symbol="BTCUSDT")

    with pytest.raises(InvalidMarketError, match="Base and quote components must be non-empty"):
        MarketIdentity(exchange_id="binance", market_type="spot", symbol="BTC/")

    with pytest.raises(InvalidMarketError, match="Base and quote components must be non-empty"):
        MarketIdentity(exchange_id="binance", market_type="spot", symbol="/USDT")


def test_hardening_market_identity_currency_contradiction_rejected() -> None:
    """Reject contradictory explicit base/quote currencies."""
    with pytest.raises(InvalidMarketError, match="base_currency 'ETH' does not agree with symbol 'BTC/USDT'"):
        MarketIdentity(
            exchange_id="binance",
            market_type="spot",
            symbol="BTC/USDT",
            base_currency="ETH",
            quote_currency="USDT",
        )

    with pytest.raises(InvalidMarketError, match="quote_currency 'EUR' does not agree with symbol 'BTC/USDT'"):
        MarketIdentity(
            exchange_id="binance",
            market_type="spot",
            symbol="BTC/USDT",
            base_currency="BTC",
            quote_currency="EUR",
        )


def test_hardening_empty_dataset_policy() -> None:
    """Empty in-memory sequences and header-only CSVs are explicitly marked invalid with EMPTY_DATASET issue."""
    # In-memory empty sequence
    res_mem = validate_candles([])
    assert not res_mem.is_valid
    assert res_mem.total_rows == 0
    assert any(err.issue_type == "EMPTY_DATASET" for err in res_mem.errors)

    # Header-only CSV
    header_only = "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
    res_csv = validate_candle_csv(io.StringIO(header_only))
    assert not res_csv.is_valid
    assert res_csv.total_rows == 0
    assert any(err.issue_type == "EMPTY_DATASET" for err in res_csv.errors)
