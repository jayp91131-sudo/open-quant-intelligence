"""Tests for StrategyEvaluator Stage A: Authoritative M1 snapshot verification."""

from datetime import datetime, timezone

import pytest

from open_quant_intelligence.data.quality import analyze_dataset_quality
from open_quant_intelligence.domain.candle import Candle
from open_quant_intelligence.domain.market import MarketIdentity
from open_quant_intelligence.domain.timeframe import SupportedTimeframe
from open_quant_intelligence.storage.fingerprint import compute_content_fingerprint
from open_quant_intelligence.strategy.contracts import (
    EmptyDatasetError,
    EvaluationContext,
    HeterogeneousSeriesError,
    SnapshotConsistencyError,
    StrategyEvaluationError,
)
from open_quant_intelligence.strategy.evaluator import StrategyEvaluator


def _make_utc_dt(hour: int = 10, minute: int = 0) -> datetime:
    return datetime(2025, 1, 1, hour, minute, 0, tzinfo=timezone.utc)


def _make_series(count: int = 3) -> list[Candle]:
    candles = []
    for i in range(count):
        op = _make_utc_dt(10 + i, 0)
        cl = _make_utc_dt(11 + i, 0)
        candles.append(
            Candle(
                exchange_id="binance",
                market_type="spot",
                symbol="BTC/USDT",
                timeframe="1h",
                open_time=op,
                close_time=cl,
                open=50000.0 + i * 100,
                high=50500.0 + i * 100,
                low=49500.0 + i * 100,
                close=50200.0 + i * 100,
                base_volume=10.0 + i,
                is_closed=True,
                source="binance_vision",
                fetched_at=_make_utc_dt(11 + i, 5),
            )
        )
    return candles


def _make_valid_context(candles: list[Candle]) -> EvaluationContext:
    market = MarketIdentity(exchange_id="binance", market_type="spot", symbol="BTC/USDT")
    since_ms = int(candles[0].open_time.timestamp() * 1000)
    until_ms = int(candles[-1].close_time.timestamp() * 1000)
    report = analyze_dataset_quality(candles, since_ms=since_ms, until_ms=until_ms, timeframe=SupportedTimeframe.H1)
    fp = compute_content_fingerprint(
        candles=candles,
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe=SupportedTimeframe.H1,
        requested_since_ms=since_ms,
        requested_until_ms=until_ms,
    )
    return EvaluationContext(
        market=market,
        timeframe=SupportedTimeframe.H1,
        requested_since_ms=since_ms,
        requested_until_ms=until_ms,
        content_fingerprint=fp,
        quality_report=report,
    )


def test_stage_a_empty_input_rejection() -> None:
    candles = _make_series(2)
    ctx = _make_valid_context(candles)

    with pytest.raises(EmptyDatasetError, match="Supplied candle sequence is empty"):
        StrategyEvaluator.verify_snapshot([], ctx)


def test_stage_a_fingerprint_mismatch_rejection() -> None:
    candles = _make_series(3)
    ctx = _make_valid_context(candles)

    # Subtly alter price in candles without changing timestamps or record count
    tampered_candles = list(candles)
    tampered_candles[1] = Candle(
        exchange_id=candles[1].exchange_id,
        market_type=candles[1].market_type,
        symbol=candles[1].symbol,
        timeframe=candles[1].timeframe,
        open_time=candles[1].open_time,
        close_time=candles[1].close_time,
        open=candles[1].open + 1.0,  # Price altered
        high=candles[1].high + 1.0,
        low=candles[1].low,
        close=candles[1].close,
        base_volume=candles[1].base_volume,
        is_closed=True,
    )

    with pytest.raises(SnapshotConsistencyError, match="Recomputed content fingerprint.*does not match"):
        StrategyEvaluator.verify_snapshot(tampered_candles, ctx)


def test_stage_a_m0_validation_failure_rejection() -> None:
    candles = _make_series(2)
    ctx = _make_valid_context(candles)

    # Supply duplicate candles to trigger M0 DUPLICATE_TIMESTAMP failure
    duplicate_series = [candles[0], candles[0]]

    with pytest.raises(StrategyEvaluationError, match="M0 candle validation failed"):
        StrategyEvaluator.verify_snapshot(duplicate_series, ctx)


def test_stage_a_record_count_mismatch_rejection() -> None:
    candles = _make_series(3)
    ctx = _make_valid_context(candles)

    # Supply only 2 candles instead of 3
    with pytest.raises(SnapshotConsistencyError, match="Supplied candle count .* does not match"):
        StrategyEvaluator.verify_snapshot(candles[:2], ctx)


def test_stage_a_window_bounds_mismatch_rejection() -> None:
    candles = _make_series(3)
    ctx = _make_valid_context(candles)

    # First candle starts earlier than requested_since
    early_candle = Candle(
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        open_time=_make_utc_dt(9, 0),  # 1 hour earlier
        close_time=_make_utc_dt(10, 0),
        open=50000.0,
        high=50500.0,
        low=49500.0,
        close=50200.0,
        base_volume=10.0,
        is_closed=True,
    )
    with pytest.raises(SnapshotConsistencyError, match="First candle open_time .* is earlier"):
        StrategyEvaluator.verify_snapshot([early_candle, candles[1], candles[2]], ctx)


def test_stage_a_identity_mismatch_rejection() -> None:
    candles = _make_series(2)
    ctx = _make_valid_context(candles)

    # Exchange mismatch
    c_mismatched = Candle(
        exchange_id="kraken",  # mismatch
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        open_time=candles[1].open_time,
        close_time=candles[1].close_time,
        open=candles[1].open,
        high=candles[1].high,
        low=candles[1].low,
        close=candles[1].close,
        base_volume=candles[1].base_volume,
        is_closed=True,
    )
    with pytest.raises(HeterogeneousSeriesError, match="Mixed exchange_id rejected|exchange_id.*does not match"):
        StrategyEvaluator.verify_snapshot([candles[0], c_mismatched], ctx)


def test_stage_a_timeframe_mismatch_rejection() -> None:
    candles = _make_series(2)
    ctx = _make_valid_context(candles)

    c_mismatched_tf = Candle(
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="4h",  # mismatch
        open_time=candles[1].open_time,
        close_time=_make_utc_dt(15, 0),  # 11:00 + 4h = 15:00
        open=candles[1].open,
        high=candles[1].high,
        low=candles[1].low,
        close=candles[1].close,
        base_volume=candles[1].base_volume,
        is_closed=True,
    )
    with pytest.raises(HeterogeneousSeriesError, match="Mixed timeframes rejected|timeframe.*does not match"):
        StrategyEvaluator.verify_snapshot([candles[0], c_mismatched_tf], ctx)


def test_stage_a_successful_verification() -> None:
    candles = _make_series(3)
    ctx = _make_valid_context(candles)

    # Does not raise
    StrategyEvaluator.verify_snapshot(candles, ctx)
