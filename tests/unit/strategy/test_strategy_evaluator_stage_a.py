"""Tests for StrategyEvaluator Stage A: Authoritative M1 snapshot verification."""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import pytest

from open_quant_intelligence.domain.candle import Candle
from open_quant_intelligence.strategy.contracts import (
    EmptyDatasetError,
    GapPolicy,
    HeterogeneousSeriesError,
    SnapshotConsistencyError,
    StrategyCandle,
    StrategyConfig,
    StrategyContext,
    StrategyEvaluationError,
    VerifiedSnapshot,
)
from open_quant_intelligence.strategy.evaluator import StrategyEvaluator
from open_quant_intelligence.strategy.signals import ReasonCode, ResearchSignal, StrategyOutput
from tests.unit.strategy._catalog_helpers import commit_and_build_context


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


def test_stage_a_empty_input_rejection(tmp_path: Path) -> None:
    candles = _make_series(2)
    _, _, ctx = commit_and_build_context(tmp_path, candles)

    with pytest.raises(EmptyDatasetError, match="Supplied candle sequence is empty"):
        StrategyEvaluator.verify_snapshot([], ctx)


def test_stage_a_fingerprint_mismatch_rejection(tmp_path: Path) -> None:
    candles = _make_series(3)
    _, _, ctx = commit_and_build_context(tmp_path, candles)

    # Subtly alter price in candles without changing timestamps or record count
    tampered_candles = list(candles)
    tampered_candles[1] = Candle(
        exchange_id=candles[1].exchange_id,
        market_type=candles[1].market_type,
        symbol=candles[1].symbol,
        timeframe=candles[1].timeframe,
        open_time=candles[1].open_time,
        close_time=candles[1].close_time,
        open=candles[1].open + 1.0,
        high=candles[1].high + 1.0,
        low=candles[1].low,
        close=candles[1].close,
        base_volume=candles[1].base_volume,
        is_closed=True,
    )

    with pytest.raises(SnapshotConsistencyError, match="Recomputed content fingerprint.*does not match"):
        StrategyEvaluator.verify_snapshot(tampered_candles, ctx)


def test_stage_a_m0_validation_failure_rejection(tmp_path: Path) -> None:
    candles = _make_series(2)
    _, _, ctx = commit_and_build_context(tmp_path, candles)

    duplicate_series = [candles[0], candles[0]]

    with pytest.raises(StrategyEvaluationError, match="M0 candle validation failed"):
        StrategyEvaluator.verify_snapshot(duplicate_series, ctx)


def test_stage_a_record_count_mismatch_rejection(tmp_path: Path) -> None:
    candles = _make_series(3)
    _, _, ctx = commit_and_build_context(tmp_path, candles)

    with pytest.raises(SnapshotConsistencyError, match="Supplied candle count .* does not match"):
        StrategyEvaluator.verify_snapshot(candles[:2], ctx)


def test_stage_a_window_bounds_mismatch_rejection(tmp_path: Path) -> None:
    candles = _make_series(3)
    _, _, ctx = commit_and_build_context(tmp_path, candles)

    early_candle = Candle(
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        open_time=_make_utc_dt(9, 0),
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


def test_stage_a_identity_mismatch_rejection(tmp_path: Path) -> None:
    candles = _make_series(2)
    _, _, ctx = commit_and_build_context(tmp_path, candles)

    c_mismatched = Candle(
        exchange_id="kraken",
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


def test_stage_a_timeframe_mismatch_rejection(tmp_path: Path) -> None:
    candles = _make_series(2)
    _, _, ctx = commit_and_build_context(tmp_path, candles)

    c_mismatched_tf = Candle(
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="4h",
        open_time=candles[1].open_time,
        close_time=_make_utc_dt(15, 0),
        open=candles[1].open,
        high=candles[1].high,
        low=candles[1].low,
        close=candles[1].close,
        base_volume=candles[1].base_volume,
        is_closed=True,
    )
    with pytest.raises(HeterogeneousSeriesError, match="Mixed timeframes rejected|timeframe.*does not match"):
        StrategyEvaluator.verify_snapshot([candles[0], c_mismatched_tf], ctx)


def test_stage_a_successful_verification(tmp_path: Path) -> None:
    candles = _make_series(3)
    _, _, ctx = commit_and_build_context(tmp_path, candles)

    verified = StrategyEvaluator.verify_snapshot(candles, ctx)
    assert isinstance(verified, VerifiedSnapshot)
    assert verified.candle_count == 3
    assert verified.context == ctx


def test_stage_b_cannot_bypass_stage_a_regression(tmp_path: Path) -> None:
    """Regression test: evaluate_at() MUST run Stage A verification and reject tampered candles."""
    candles = _make_series(3)
    _, _, ctx = commit_and_build_context(tmp_path, candles)

    @dataclass(frozen=True)
    class _Cfg(StrategyConfig):
        pass

    class _SpyStrategy:
        def __init__(self) -> None:
            self.called = False
            self._cfg = _Cfg()

        @property
        def strategy_id(self) -> str:
            return "spy"

        @property
        def version(self) -> str:
            return "1.0.0"

        @property
        def config(self) -> StrategyConfig:
            return self._cfg

        @property
        def minimum_history_candles(self) -> int:
            return 2

        @property
        def gap_policy(self) -> GapPolicy:
            return GapPolicy.STRICT_GAP_FREE

        def evaluate(self, history: Sequence[StrategyCandle], context: StrategyContext) -> StrategyOutput:
            self.called = True
            target = history[-1]
            return StrategyOutput(
                strategy_id=self.strategy_id,
                strategy_version=self.version,
                config_fingerprint=self.config.compute_fingerprint(),
                symbol=context.symbol,
                timeframe=context.timeframe,
                source_candle_open_time=target.open_time,
                source_candle_close_time=target.close_time,
                decision_time=target.close_time,
                signal=ResearchSignal.HOLD,
                reason_code=ReasonCode.NO_CONDITION_MET,
                metadata={},
            )

    spy = _SpyStrategy()
    tampered = list(candles)
    tampered[0] = Candle(
        exchange_id=candles[0].exchange_id,
        market_type=candles[0].market_type,
        symbol=candles[0].symbol,
        timeframe=candles[0].timeframe,
        open_time=candles[0].open_time,
        close_time=candles[0].close_time,
        open=candles[0].open + 5.0,
        high=candles[0].high + 5.0,
        low=candles[0].low,
        close=candles[0].close,
        base_volume=candles[0].base_volume,
        is_closed=True,
    )

    with pytest.raises(SnapshotConsistencyError, match="Recomputed content fingerprint"):
        StrategyEvaluator.evaluate_at(tampered, target_index=2, context=ctx, strategy=spy)

    # Strategy.evaluate() must never be invoked when Stage A fails
    assert spy.called is False

    # Direct _evaluate_at_verified with unverified object also fails
    with pytest.raises(SnapshotConsistencyError, match="requires a VerifiedSnapshot"):
        StrategyEvaluator._evaluate_at_verified(target_index=2, verified_snapshot=ctx, strategy=spy)  # type: ignore


def test_verified_snapshot_bound_to_exact_dataset_a_not_b(tmp_path: Path) -> None:
    """Regression test: VerifiedSnapshot for dataset A cannot evaluate different same-length dataset B."""
    dataset_a = _make_series(3)
    _, record_a, ctx_a = commit_and_build_context(tmp_path / "a", dataset_a)

    # Dataset B: identical count, market identity, timeframe, and timestamps, but different OHLCV values
    dataset_b = [
        Candle(
            exchange_id=c.exchange_id,
            market_type=c.market_type,
            symbol=c.symbol,
            timeframe=c.timeframe,
            open_time=c.open_time,
            close_time=c.close_time,
            open=c.open + 1000.0,
            high=c.high + 1000.0,
            low=c.low + 1000.0,
            close=c.close + 1000.0,
            base_volume=c.base_volume + 5.0,
            is_closed=True,
        )
        for c in dataset_a
    ]

    @dataclass(frozen=True)
    class _Cfg(StrategyConfig):
        pass

    class _RecordingStrategy:
        def __init__(self) -> None:
            self._cfg = _Cfg()
            self.seen_closes: list[float] = []

        @property
        def strategy_id(self) -> str:
            return "recorder"

        @property
        def version(self) -> str:
            return "1.0.0"

        @property
        def config(self) -> StrategyConfig:
            return self._cfg

        @property
        def minimum_history_candles(self) -> int:
            return 2

        @property
        def gap_policy(self) -> GapPolicy:
            return GapPolicy.STRICT_GAP_FREE

        def evaluate(self, history: Sequence[StrategyCandle], context: StrategyContext) -> StrategyOutput:
            self.seen_closes = [sc.close for sc in history]
            target = history[-1]
            return StrategyOutput(
                strategy_id=self.strategy_id,
                strategy_version=self.version,
                config_fingerprint=self.config.compute_fingerprint(),
                symbol=context.symbol,
                timeframe=context.timeframe,
                source_candle_open_time=target.open_time,
                source_candle_close_time=target.close_time,
                decision_time=target.close_time,
                signal=ResearchSignal.LONG,
                reason_code=ReasonCode.LONG_SIGNAL_GENERATED,
                metadata={"close": target.close},
            )

    strat = _RecordingStrategy()

    # 1. Verify dataset A -> VerifiedSnapshot owns dataset A's exact candles
    verified_a = StrategyEvaluator.verify_snapshot(dataset_a, ctx_a)
    assert verified_a.candles == tuple(dataset_a)

    # 2. Supplying dataset B against ctx_a fails Stage A (cannot evaluate B with A's provenance)
    with pytest.raises(SnapshotConsistencyError, match="Recomputed content fingerprint"):
        StrategyEvaluator.evaluate_at(dataset_b, target_index=2, context=ctx_a, strategy=strat)

    # 3. _evaluate_at_verified consumes only verified_a.candles (dataset A), never dataset B
    decision = StrategyEvaluator._evaluate_at_verified(target_index=2, verified_snapshot=verified_a, strategy=strat)
    assert strat.seen_closes == [c.close for c in dataset_a]
    assert decision.dataset_content_fingerprint == record_a.content_fingerprint_sha256
    assert decision.snapshot_id == record_a.snapshot_id

    # 4. Normal evaluate_at and evaluate_series still work on dataset A
    single_dec = StrategyEvaluator.evaluate_at(dataset_a, target_index=2, context=ctx_a, strategy=strat)
    series_decs = StrategyEvaluator.evaluate_series(dataset_a, context=ctx_a, strategy=strat)
    assert single_dec == decision
    assert len(series_decs) == 2
