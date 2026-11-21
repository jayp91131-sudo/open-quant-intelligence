"""Unit tests for preflight validation checks."""

import pytest
from open_quant_intelligence.data.timeframe import SupportedTimeframe

from open_quant_intelligence.ingestion.adapter import SpotSymbolMetadata
from open_quant_intelligence.ingestion.preflight import (
    DelistedSymbolError,
    FutureWindowError,
    UntradableSymbolError,
    WindowAlignmentError,
    validate_request_window_preflight,
    validate_spot_market_preflight,
)
from tests.fakes.fake_binance_adapter import FakeBinanceAdapter


def test_spot_market_preflight_valid() -> None:
    fake = FakeBinanceAdapter()
    meta = validate_spot_market_preflight(fake, "BTC/USDT")
    assert meta.symbol == "BTC/USDT"


def test_spot_market_preflight_untradable_delisted() -> None:
    custom_symbols = {
        "DELISTED/USDT": SpotSymbolMetadata(
            symbol="DELISTED/USDT",
            base="DELISTED",
            quote="USDT",
            active=False,
            spot=True,
            trading_status="BREAK",
        ),
        "FUTURES/USDT": SpotSymbolMetadata(
            symbol="FUTURES/USDT",
            base="FUTURES",
            quote="USDT",
            active=True,
            spot=False,
            trading_status="TRADING",
        ),
    }
    fake = FakeBinanceAdapter(symbols_metadata=custom_symbols)

    with pytest.raises(DelistedSymbolError, match="not active"):
        validate_spot_market_preflight(fake, "DELISTED/USDT")

    with pytest.raises(UntradableSymbolError, match="not eligible for spot"):
        validate_spot_market_preflight(fake, "FUTURES/USDT")


def test_request_window_preflight_valid() -> None:
    # Frame is 1h (3600_000 ms)
    # since=0, until=7200_000 (2 hours), server_time=10_000_000
    validate_request_window_preflight(
        timeframe=SupportedTimeframe.H1,
        since_ms=0,
        until_ms=7_200_000,
        server_time_ms=10_000_000,
    )


def test_request_window_grid_unaligned_raises() -> None:
    # 1h = 3,600,000 ms; since=1000 is not aligned
    with pytest.raises(WindowAlignmentError, match="not aligned"):
        validate_request_window_preflight(
            timeframe=SupportedTimeframe.H1,
            since_ms=1_000,
            until_ms=7_200_000,
            server_time_ms=10_000_000,
        )

    # until=7_201_000 is not aligned
    with pytest.raises(WindowAlignmentError, match="not aligned"):
        validate_request_window_preflight(
            timeframe=SupportedTimeframe.H1,
            since_ms=0,
            until_ms=7_201_000,
            server_time_ms=10_000_000,
        )


def test_request_window_inverted_or_empty_raises() -> None:
    with pytest.raises(WindowAlignmentError, match="strictly before"):
        validate_request_window_preflight(
            timeframe=SupportedTimeframe.H1,
            since_ms=7_200_000,
            until_ms=7_200_000,
            server_time_ms=10_000_000,
        )


def test_future_window_rejected_zero_requests() -> None:
    """Requirement 5: Asserts requesting until_ms > server_time_ms raises
    FutureWindowError and issues zero Kline queries.
    """
    fake = FakeBinanceAdapter(server_time_ms=5_000_000)

    # until_ms = 7_200_000 > server_time_ms = 5_000_000
    with pytest.raises(FutureWindowError, match="extends into future"):
        validate_request_window_preflight(
            timeframe=SupportedTimeframe.H1,
            since_ms=0,
            until_ms=7_200_000,
            server_time_ms=fake.current_server_time_ms,
        )

    # Verify zero kline queries were issued
    assert len(fake.queried_kline_calls) == 0
