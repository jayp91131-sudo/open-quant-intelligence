"""Unit tests for binary content fingerprinting and canonical SHA-256 digests."""

from datetime import datetime, timezone

from open_quant_intelligence.data.candle import Candle
from open_quant_intelligence.data.market import MarketIdentity
from open_quant_intelligence.data.timeframe import SupportedTimeframe

from open_quant_intelligence.storage.fingerprint import compute_content_fingerprint


def _make_candle(price: float, vol: float = 1.0) -> Candle:
    return Candle(
        market=MarketIdentity(exchange_id="binance", market_type="spot", symbol="BTC/USDT"),
        timeframe=SupportedTimeframe.H1,
        open_time=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
        open=price,
        high=price + 5.0,
        low=price - 5.0,
        close=price + 2.0,
        base_volume=vol,
    )


def test_empty_dataset_canonical_digest_stability_and_uniqueness() -> None:
    """Empty datasets must produce stable SHA-256 based on market identity,
    and different identities must produce different hashes.
    """
    market_btc = MarketIdentity(exchange_id="binance", market_type="spot", symbol="BTC/USDT")
    market_eth = MarketIdentity(exchange_id="binance", market_type="spot", symbol="ETH/USDT")
    tf = SupportedTimeframe.H1

    hash_btc_1 = compute_content_fingerprint([], market_btc, tf)
    hash_btc_2 = compute_content_fingerprint([], market_btc, tf)
    hash_eth = compute_content_fingerprint([], market_eth, tf)

    assert hash_btc_1.startswith("sha256:")
    assert hash_btc_1 == hash_btc_2
    assert hash_btc_1 != hash_eth


def test_fingerprint_negative_zero_normalization() -> None:
    """Float -0.0 and +0.0 must yield identical binary digests."""
    market = MarketIdentity(exchange_id="binance", market_type="spot", symbol="BTC/USDT")
    tf = SupportedTimeframe.H1

    # Candle with 0.0 volume vs -0.0 volume
    c_pos = _make_candle(100.0, vol=0.0)
    c_neg = _make_candle(100.0, vol=-0.0)

    hash_pos = compute_content_fingerprint([c_pos], market, tf)
    hash_neg = compute_content_fingerprint([c_neg], market, tf)

    assert hash_pos == hash_neg


def test_fingerprint_deterministic_and_sensitive_to_values() -> None:
    market = MarketIdentity(exchange_id="binance", market_type="spot", symbol="BTC/USDT")
    tf = SupportedTimeframe.H1

    c1 = _make_candle(100.0)
    c2 = _make_candle(100.01)

    h1 = compute_content_fingerprint([c1], market, tf)
    h2 = compute_content_fingerprint([c2], market, tf)

    assert h1 != h2
