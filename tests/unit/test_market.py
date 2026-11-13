"""Unit tests for MarketIdentity domain model."""

from dataclasses import FrozenInstanceError

import pytest

from open_quant_intelligence.domain.market import InvalidMarketError, MarketIdentity


def test_valid_spot_market_identity() -> None:
    market = MarketIdentity(
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
    )
    assert market.exchange_id == "binance"
    assert market.market_type == "spot"
    assert market.symbol == "BTC/USDT"
    assert market.base_currency == "BTC"
    assert market.quote_currency == "USDT"


def test_market_identity_explicit_currencies() -> None:
    market = MarketIdentity(
        exchange_id="binance",
        market_type="spot",
        symbol="ETH/BTC",
        base_currency="ETH",
        quote_currency="BTC",
    )
    assert market.base_currency == "ETH"
    assert market.quote_currency == "BTC"


@pytest.mark.parametrize("invalid_market_type", ["futures", "margin", "perpetual", "swap", "options", "delivery"])
def test_non_spot_market_types_rejected(invalid_market_type: str) -> None:
    with pytest.raises(InvalidMarketError, match="Unsupported market_type"):
        MarketIdentity(
            exchange_id="binance",
            market_type=invalid_market_type,
            symbol="BTC/USDT",
        )


@pytest.mark.parametrize("empty_val", ["", "   ", None])
def test_empty_exchange_or_symbol_rejected(empty_val: str | None) -> None:
    with pytest.raises(InvalidMarketError):
        MarketIdentity(
            exchange_id=empty_val,  # type: ignore[arg-type]
            market_type="spot",
            symbol="BTC/USDT",
        )

    with pytest.raises(InvalidMarketError):
        MarketIdentity(
            exchange_id="binance",
            market_type="spot",
            symbol=empty_val,  # type: ignore[arg-type]
        )


def test_market_identity_is_immutable() -> None:
    market = MarketIdentity(
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
    )
    with pytest.raises(FrozenInstanceError):
        market.symbol = "ETH/USDT"  # type: ignore[misc]
