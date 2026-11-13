"""Unit tests for immutable Candle domain model and validation invariants."""

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone

import pytest

from open_quant_intelligence.domain.candle import Candle, InvalidCandleError
from open_quant_intelligence.domain.timeframe import InvalidTimeframeError
from tests.fixtures.synthetic_data import create_synthetic_candle


def test_valid_candle_creation() -> None:
    t = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    candle = Candle(
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        open_time=t,
        open=50000.0,
        high=50500.0,
        low=49500.0,
        close=50200.0,
        base_volume=10.5,
    )
    assert candle.exchange_id == "binance"
    assert candle.symbol == "BTC/USDT"
    assert candle.timeframe == "1h"
    assert candle.open_time == t
    assert candle.close_time == datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc)
    assert candle.open == 50000.0
    assert candle.high == 50500.0
    assert candle.low == 49500.0
    assert candle.close == 50200.0
    assert candle.base_volume == 10.5
    assert candle.is_closed is True


def test_timezone_naive_timestamp_rejected() -> None:
    naive_t = datetime(2026, 1, 1, 0, 0)  # No tzinfo
    with pytest.raises(InvalidCandleError, match="Naive timestamp rejected"):
        create_synthetic_candle(open_time=naive_t)


def test_non_utc_timezone_normalized_to_utc() -> None:
    # UTC+2 offset
    tz_plus_2 = timezone(timedelta(hours=2))
    local_t = datetime(2026, 1, 1, 14, 0, tzinfo=tz_plus_2)
    candle = create_synthetic_candle(open_time=local_t)

    assert candle.open_time.tzinfo == timezone.utc
    assert candle.open_time == datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize("bad_price", [0.0, -0.01, -100.0])
def test_zero_or_negative_price_rejected(bad_price: float) -> None:
    with pytest.raises(InvalidCandleError, match="strictly positive"):
        create_synthetic_candle(open_price=bad_price)
    with pytest.raises(InvalidCandleError, match="strictly positive"):
        create_synthetic_candle(high_price=bad_price)
    with pytest.raises(InvalidCandleError, match="strictly positive"):
        create_synthetic_candle(low_price=bad_price)
    with pytest.raises(InvalidCandleError, match="strictly positive"):
        create_synthetic_candle(close_price=bad_price)


@pytest.mark.parametrize("bad_price", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_price_rejected(bad_price: float) -> None:
    with pytest.raises(InvalidCandleError, match="finite"):
        create_synthetic_candle(open_price=bad_price)
    with pytest.raises(InvalidCandleError, match="finite"):
        create_synthetic_candle(close_price=bad_price)


@pytest.mark.parametrize("bad_volume", [-0.0001, -10.0])
def test_negative_volume_rejected(bad_volume: float) -> None:
    with pytest.raises(InvalidCandleError, match="non-negative"):
        create_synthetic_candle(base_volume=bad_volume)


@pytest.mark.parametrize("bad_volume", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_volume_rejected(bad_volume: float) -> None:
    with pytest.raises(InvalidCandleError, match="finite"):
        create_synthetic_candle(base_volume=bad_volume)


def test_high_below_open_close_low_rejected() -> None:
    # High lower than open
    with pytest.raises(InvalidCandleError, match="Inconsistent high price"):
        create_synthetic_candle(open_price=50000.0, high_price=49900.0, low_price=49000.0, close_price=49500.0)

    # High lower than close
    with pytest.raises(InvalidCandleError, match="Inconsistent high price"):
        create_synthetic_candle(open_price=50000.0, high_price=50200.0, low_price=49000.0, close_price=50300.0)

    # High lower than low
    with pytest.raises(InvalidCandleError, match="Inconsistent high price"):
        create_synthetic_candle(open_price=50000.0, high_price=48000.0, low_price=49000.0, close_price=48500.0)


def test_low_above_open_close_high_rejected() -> None:
    # Low higher than open
    with pytest.raises(InvalidCandleError, match="Inconsistent low price"):
        create_synthetic_candle(open_price=50000.0, high_price=51000.0, low_price=50100.0, close_price=50500.0)

    # Low higher than close
    with pytest.raises(InvalidCandleError, match="Inconsistent low price"):
        create_synthetic_candle(open_price=50000.0, high_price=51000.0, low_price=49800.0, close_price=49700.0)


def test_non_spot_market_type_rejected() -> None:
    with pytest.raises(InvalidCandleError, match="market_type"):
        create_synthetic_candle(market_type="futures")


def test_unsupported_timeframe_rejected() -> None:
    with pytest.raises(InvalidTimeframeError):
        create_synthetic_candle(timeframe="2h")


def test_candle_is_immutable() -> None:
    c = create_synthetic_candle()
    with pytest.raises(FrozenInstanceError):
        c.close = 60000.0  # type: ignore[misc]


def test_candle_to_dict() -> None:
    c = create_synthetic_candle()
    d = c.to_dict()
    assert d["symbol"] == "BTC/USDT"
    assert d["exchange_id"] == "binance"
    assert d["market_type"] == "spot"
    assert d["open"] == 50000.0
