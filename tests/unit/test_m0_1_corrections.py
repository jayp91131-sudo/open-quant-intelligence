"""Regression tests for M0.1 contract validation fixes.

Covers:
1. Timeframe case handling in CSV and stream validation:
   - '1m' must mean one minute and pass.
   - '1M' denotes monthly and must be rejected as unsupported.
   - '1H' normalizes to '1h' and passes.
   - parse_timeframe() remains strict and unweakened.
2. Market identity symbol contract enforcement:
   - Malformed symbols like 'BTCUSDT' (no separator), 'BTC/' (missing quote), '/USDT' (missing base)
     must be rejected in CSV and in-memory validation.
   - Valid symbols like 'BTC/USDT' and 'ETH-USDT' must pass.
   - Controlled validation errors (INVALID_MARKET_IDENTITY) returned instead of uncaught exceptions.
"""

import csv
import io
from datetime import datetime, timezone

import pytest

from open_quant_intelligence.data.validation import (
    validate_candle_csv,
    validate_candle_stream,
    validate_candles,
)
from open_quant_intelligence.domain.candle import Candle, InvalidCandleError
from open_quant_intelligence.domain.timeframe import (
    InvalidTimeframeError,
    SupportedTimeframe,
    parse_timeframe,
)

# ==============================================================================
# 1. Timeframe Case Handling Tests
# ==============================================================================


def test_timeframe_1m_accepted_1M_rejected_in_csv() -> None:
    """Ensure CSV validation accepts '1m' but strictly rejects '1M' (monthly)."""
    # 1m valid CSV
    csv_1m = (
        "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
        "binance,spot,BTC/USDT,1m,2026-01-01T00:00:00Z,50000,50100,49900,50050,1.5\n"
    )
    res_1m = validate_candle_stream(csv.DictReader(io.StringIO(csv_1m)))
    assert res_1m.is_valid
    assert res_1m.timeframe == "1m"
    assert len(res_1m.valid_candles) == 1
    assert res_1m.valid_candles[0].timeframe == "1m"

    # 1M invalid CSV (must not be lowercased to 1m)
    csv_1M = (
        "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
        "binance,spot,BTC/USDT,1M,2026-01-01T00:00:00Z,50000,50100,49900,50050,1.5\n"
    )
    res_1M = validate_candle_stream(csv.DictReader(io.StringIO(csv_1M)))
    assert not res_1M.is_valid
    assert any(err.issue_type == "UNSUPPORTED_TIMEFRAME" for err in res_1M.errors)
    err = next(err for err in res_1M.errors if err.issue_type == "UNSUPPORTED_TIMEFRAME")
    assert "months" in err.message or "Unsupported timeframe" in err.message


def test_timeframe_1H_normalizes_to_1h_in_csv() -> None:
    """Ensure CSV validation allows uppercase '1H' and normalizes to canonical '1h'."""
    csv_1H = (
        "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
        "binance,spot,BTC/USDT,1H,2026-01-01T00:00:00Z,50000,51000,49000,50500,10.0\n"
    )
    res = validate_candle_csv(io.StringIO(csv_1H))
    assert res.is_valid
    assert res.timeframe == "1h"
    assert len(res.valid_candles) == 1
    assert res.valid_candles[0].timeframe == SupportedTimeframe.H1.value


def test_parse_timeframe_direct_preservation() -> None:
    """Verify parse_timeframe directly rejects '1M' and accepts '1m' and '1H'."""
    assert parse_timeframe("1m") == SupportedTimeframe.M1
    assert parse_timeframe("1H") == SupportedTimeframe.H1
    with pytest.raises(InvalidTimeframeError, match="denotes months"):
        parse_timeframe("1M")


# ==============================================================================
# 2. Market Symbol Validation Tests
# ==============================================================================


def test_csv_validation_rejects_malformed_symbol_btcusdt() -> None:
    """CSV validation must reject malformed symbol 'BTCUSDT' (missing separator)."""
    csv_text = (
        "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
        "binance,spot,BTCUSDT,1h,2026-01-01T00:00:00Z,50000,51000,49000,50500,10.0\n"
    )
    res = validate_candle_stream(csv.DictReader(io.StringIO(csv_text)))
    assert not res.is_valid
    assert any(err.issue_type == "INVALID_MARKET_IDENTITY" for err in res.errors)
    err = next(err for err in res.errors if err.issue_type == "INVALID_MARKET_IDENTITY")
    assert "pair separator" in err.message


def test_csv_validation_rejects_malformed_symbol_missing_parts() -> None:
    """CSV validation must reject 'BTC/' and '/USDT' with structured errors."""
    for bad_sym in ("BTC/", "/USDT", "-USDT", "BTC-"):
        csv_text = (
            f"exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
            f"binance,spot,{bad_sym},1h,2026-01-01T00:00:00Z,50000,51000,49000,50500,10.0\n"
        )
        res = validate_candle_stream(csv.DictReader(io.StringIO(csv_text)))
        assert not res.is_valid
        assert any(err.issue_type == "INVALID_MARKET_IDENTITY" for err in res.errors)


def test_csv_validation_accepts_valid_symbols() -> None:
    """CSV validation must accept valid symbols like 'BTC/USDT' and 'ETH-USDT'."""
    for sym in ("BTC/USDT", "ETH-USDT", "SOL_USDC"):
        csv_text = (
            f"exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
            f"binance,spot,{sym},1h,2026-01-01T00:00:00Z,50000,51000,49000,50500,10.0\n"
        )
        res = validate_candle_stream(csv.DictReader(io.StringIO(csv_text)))
        assert res.is_valid
        assert res.symbol == sym


def test_candle_domain_model_enforces_market_identity() -> None:
    """In-memory Candle construction cannot bypass MarketIdentity validation."""
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    base_kwargs = {
        "exchange_id": "binance",
        "market_type": "spot",
        "timeframe": "1h",
        "open_time": t0,
        "open": 50000.0,
        "high": 51000.0,
        "low": 49000.0,
        "close": 50500.0,
        "base_volume": 10.0,
    }

    # Malformed symbol BTCUSDT
    with pytest.raises(InvalidCandleError, match="Invalid market identity.*pair separator"):
        Candle(symbol="BTCUSDT", **base_kwargs)

    # Malformed symbol BTC/
    with pytest.raises(InvalidCandleError, match="Invalid market identity.*Base and quote"):
        Candle(symbol="BTC/", **base_kwargs)

    # Valid symbol passes
    candle = Candle(symbol="BTC/USDT", **base_kwargs)
    assert candle.symbol == "BTC/USDT"


def test_in_memory_validate_candles_with_valid_and_invalid_symbols() -> None:
    """validate_candles handles Candle instances that pass or fail symbol invariants."""
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    valid_c1 = Candle(
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
    valid_c2 = Candle(
        exchange_id="binance",
        market_type="spot",
        symbol="ETH-USDT",
        timeframe="1h",
        open_time=t0,
        open=3000.0,
        high=3100.0,
        low=2900.0,
        close=3050.0,
        base_volume=10.0,
    )

    res_single = validate_candles([valid_c1])
    assert res_single.is_valid
    assert res_single.symbol == "BTC/USDT"

    # Mixed symbols rejected
    res_mixed = validate_candles([valid_c1, valid_c2])
    assert not res_mixed.is_valid
    assert any(err.issue_type == "MIXED_SYMBOLS" for err in res_mixed.errors)
