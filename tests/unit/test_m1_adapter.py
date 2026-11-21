"""Unit tests for BinanceVisionPublicAdapter and PublicExchangeAdapter contract."""

import pytest

from open_quant_intelligence.ingestion.adapter import (
    BinanceVisionPublicAdapter,
    PublicExchangeAdapter,
    SpotSymbolMetadata,
)
from tests.fakes.fake_binance_adapter import FakeBinanceAdapter


def test_public_exchange_adapter_protocol_compliance() -> None:
    fake = FakeBinanceAdapter()
    assert isinstance(fake, PublicExchangeAdapter)


def test_binance_vision_public_adapter_url_sanitization() -> None:
    adapter = BinanceVisionPublicAdapter()
    # Check CCXT client URLs
    raw_urls = adapter.client.urls
    api_urls = raw_urls.get("api", {})

    # Prohibited endpoints must not exist or must be None/empty
    prohibited_keys = ["sapi", "fapi", "dapi", "eapi", "papi", "wapi", "vapi"]
    for key in prohibited_keys:
        assert key not in api_urls or api_urls[key] is None

    # Public spot endpoints must be retained
    assert "public" in api_urls
    public_val = api_urls["public"]
    if isinstance(public_val, str):
        assert "data-api.binance.vision" in public_val or "api.binance.com" in public_val


def test_adapter_sealed_trading_methods() -> None:
    adapter = BinanceVisionPublicAdapter()
    trading_methods = [
        "create_order",
        "cancel_order",
        "cancel_all_orders",
        "edit_order",
        "fetch_balance",
        "fetch_my_trades",
        "fetch_orders",
        "fetch_open_orders",
        "fetch_closed_orders",
        "transfer",
        "withdraw",
        "deposit",
    ]
    for method_name in trading_methods:
        func = getattr(adapter.client, method_name, None)
        if func is not None:
            with pytest.raises(RuntimeError, match="Sealed"):
                func()


def test_fake_adapter_symbol_metadata() -> None:
    fake = FakeBinanceAdapter()
    meta = fake.fetch_spot_symbol_metadata("BTC/USDT")
    assert isinstance(meta, SpotSymbolMetadata)
    assert meta.symbol == "BTC/USDT"
    assert meta.base == "BTC"
    assert meta.quote == "USDT"
    assert meta.spot is True
    assert meta.active is True
    assert meta.trading_status == "TRADING"

    with pytest.raises(KeyError):
        fake.fetch_spot_symbol_metadata("INVALID/PAIR")
