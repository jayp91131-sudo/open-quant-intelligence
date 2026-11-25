"""Tests for StrategyEvaluator Stage B: Point-in-time causal prefix evaluation."""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import pytest

from open_quant_intelligence.domain.candle import Candle
from open_quant_intelligence.domain.timeframe import SupportedTimeframe
from open_quant_intelligence.strategy.contracts import (
    GapPolicy,
    IncompleteCandleError,
    InsufficientHistoryError,
    StrategyCandle,
    StrategyConfig,
    StrategyContext,
    StrategyOutputMismatchError,
)
from open_quant_intelligence.strategy.evaluator import StrategyEvaluator
from open_quant_intelligence.strategy.signals import (
    ReasonCode,
    ResearchSignal,
    StrategyOutput,
)
from tests.unit.strategy._catalog_helpers import commit_and_build_context


def _make_utc_dt(hour: int = 10, minute: int = 0) -> datetime:
    return datetime(2025, 1, 1, hour, minute, 0, tzinfo=timezone.utc)


@dataclass(frozen=True)
class DummyConfig(StrategyConfig):
    threshold: float = 1.0


class DummyStrategy:
    def __init__(
        self,
        strategy_id: str = "dummy_strat",
        version: str = "1.0.0",
        minimum_history_candles: int = 2,
        gap_policy: GapPolicy = GapPolicy.STRICT_GAP_FREE,
    ) -> None:
        self._strategy_id = strategy_id
        self._version = version
        self._minimum_history_candles = minimum_history_candles
        self._gap_policy = gap_policy
        self._config = DummyConfig()
        self.last_received_candles: Sequence[StrategyCandle] | None = None
        self.last_received_context: StrategyContext | None = None

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    @property
    def version(self) -> str:
        return self._version

    @property
    def config(self) -> StrategyConfig:
        return self._config

    @property
    def minimum_history_candles(self) -> int:
        return self._minimum_history_candles

    @property
    def gap_policy(self) -> GapPolicy:
        return self._gap_policy

    def evaluate(self, history: Sequence[StrategyCandle], context: StrategyContext) -> StrategyOutput:
        self.last_received_candles = history
        self.last_received_context = context
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
            signal=ResearchSignal.LONG if target.close > target.open else ResearchSignal.FLAT,
            reason_code=ReasonCode.LONG_SIGNAL_GENERATED
            if target.close > target.open
            else ReasonCode.FLAT_SIGNAL_GENERATED,
            metadata={"close": target.close},
        )


def _make_series(count: int = 4) -> list[Candle]:
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


def test_stage_b_lookback_requirement_rejection(tmp_path: Path) -> None:
    candles = _make_series(4)
    _, _, ctx = commit_and_build_context(tmp_path, candles)
    strat = DummyStrategy(minimum_history_candles=3)

    # Evaluating at target_index = 1 means len(prefix) == 2, which is < minimum_history_candles (3)
    with pytest.raises(InsufficientHistoryError, match="strictly less than strategy minimum"):
        StrategyEvaluator.evaluate_at(candles, target_index=1, context=ctx, strategy=strat)

    # Evaluating at target_index = 2 means len(prefix) == 3, which satisfies minimum
    decision = StrategyEvaluator.evaluate_at(candles, target_index=2, context=ctx, strategy=strat)
    assert decision.signal == ResearchSignal.LONG


def test_stage_b_unclosed_prefix_candle_rejection(tmp_path: Path) -> None:
    candles = _make_series(4)
    strat = DummyStrategy(minimum_history_candles=2)

    # Make candle at index 1 unclosed
    bad_candle = Candle(
        exchange_id=candles[1].exchange_id,
        market_type=candles[1].market_type,
        symbol=candles[1].symbol,
        timeframe=candles[1].timeframe,
        open_time=candles[1].open_time,
        close_time=candles[1].close_time,
        open=candles[1].open,
        high=candles[1].high,
        low=candles[1].low,
        close=candles[1].close,
        base_volume=candles[1].base_volume,
        is_closed=False,  # Unclosed
    )
    series_with_unclosed = [candles[0], bad_candle, candles[2], candles[3]]
    _, _, ctx = commit_and_build_context(tmp_path, series_with_unclosed)

    # Evaluating at index 1 or 2 contains the unclosed candle
    with pytest.raises(IncompleteCandleError, match="is incomplete \\(is_closed=False\\)"):
        StrategyEvaluator.evaluate_at(series_with_unclosed, target_index=2, context=ctx, strategy=strat)


def test_stage_b_unclosed_future_candle_isolation(tmp_path: Path) -> None:
    candles = _make_series(4)
    strat = DummyStrategy(minimum_history_candles=2)

    # Make candle at index 3 (future candle relative to T=1) unclosed
    unclosed_future = Candle(
        exchange_id=candles[3].exchange_id,
        market_type=candles[3].market_type,
        symbol=candles[3].symbol,
        timeframe=candles[3].timeframe,
        open_time=candles[3].open_time,
        close_time=candles[3].close_time,
        open=candles[3].open,
        high=candles[3].high,
        low=candles[3].low,
        close=candles[3].close,
        base_volume=candles[3].base_volume,
        is_closed=False,
    )
    series = [candles[0], candles[1], candles[2], unclosed_future]
    _, _, ctx = commit_and_build_context(tmp_path, series)

    # Evaluating at T=1 must succeed because candle 3 is in the future
    decision = StrategyEvaluator.evaluate_at(series, target_index=1, context=ctx, strategy=strat)
    assert decision.source_candle_close_time == candles[1].close_time


def test_stage_b_strategy_output_mismatch_rejections(tmp_path: Path) -> None:
    candles = _make_series(3)
    _, _, ctx = commit_and_build_context(tmp_path, candles)

    # Strategy that returns mismatched strategy_id
    class BadIdStrategy(DummyStrategy):
        def evaluate(self, history: Sequence[StrategyCandle], context: StrategyContext) -> StrategyOutput:
            out = super().evaluate(history, context)
            object.__setattr__(out, "strategy_id", "wrong_id")
            return out

    with pytest.raises(StrategyOutputMismatchError, match="strategy_id 'wrong_id' does not match"):
        StrategyEvaluator.evaluate_at(candles, target_index=2, context=ctx, strategy=BadIdStrategy())

    # Strategy that returns mismatched symbol
    class BadSymbolStrategy(DummyStrategy):
        def evaluate(self, history: Sequence[StrategyCandle], context: StrategyContext) -> StrategyOutput:
            out = super().evaluate(history, context)
            object.__setattr__(out, "symbol", "ETH/USDT")
            return out

    with pytest.raises(StrategyOutputMismatchError, match="symbol 'ETH/USDT' does not match"):
        StrategyEvaluator.evaluate_at(candles, target_index=2, context=ctx, strategy=BadSymbolStrategy())

    # Strategy that returns mismatched timeframe
    class BadTfStrategy(DummyStrategy):
        def evaluate(self, history: Sequence[StrategyCandle], context: StrategyContext) -> StrategyOutput:
            out = super().evaluate(history, context)
            object.__setattr__(out, "timeframe", SupportedTimeframe.H4)
            return out

    with pytest.raises(StrategyOutputMismatchError, match="timeframe '4h' does not match"):
        StrategyEvaluator.evaluate_at(candles, target_index=2, context=ctx, strategy=BadTfStrategy())

    # Strategy that returns mismatched strategy_version
    class BadVersionStrategy(DummyStrategy):
        def evaluate(self, history: Sequence[StrategyCandle], context: StrategyContext) -> StrategyOutput:
            out = super().evaluate(history, context)
            object.__setattr__(out, "strategy_version", "9.9.9")
            return out

    with pytest.raises(StrategyOutputMismatchError, match="strategy_version '9.9.9' does not match"):
        StrategyEvaluator.evaluate_at(candles, target_index=2, context=ctx, strategy=BadVersionStrategy())

    # Strategy that returns mismatched config_fingerprint
    class BadConfigFpStrategy(DummyStrategy):
        def evaluate(self, history: Sequence[StrategyCandle], context: StrategyContext) -> StrategyOutput:
            out = super().evaluate(history, context)
            object.__setattr__(out, "config_fingerprint", "sha256:" + "0" * 64)
            return out

    with pytest.raises(StrategyOutputMismatchError, match="config_fingerprint .* does not match"):
        StrategyEvaluator.evaluate_at(candles, target_index=2, context=ctx, strategy=BadConfigFpStrategy())

    # Strategy that returns mismatched source_candle_open_time / close_time / decision_time
    class BadOpenTimeStrategy(DummyStrategy):
        def evaluate(self, history: Sequence[StrategyCandle], context: StrategyContext) -> StrategyOutput:
            out = super().evaluate(history, context)
            object.__setattr__(out, "source_candle_open_time", history[0].open_time)
            return out

    with pytest.raises(StrategyOutputMismatchError, match="source_candle_open_time .* does not match"):
        StrategyEvaluator.evaluate_at(candles, target_index=2, context=ctx, strategy=BadOpenTimeStrategy())

    class BadCloseTimeStrategy(DummyStrategy):
        def evaluate(self, history: Sequence[StrategyCandle], context: StrategyContext) -> StrategyOutput:
            out = super().evaluate(history, context)
            object.__setattr__(out, "source_candle_close_time", history[0].close_time)
            return out

    with pytest.raises(StrategyOutputMismatchError, match="source_candle_close_time .* does not match"):
        StrategyEvaluator.evaluate_at(candles, target_index=2, context=ctx, strategy=BadCloseTimeStrategy())

    class BadDecisionTimeStrategy(DummyStrategy):
        def evaluate(self, history: Sequence[StrategyCandle], context: StrategyContext) -> StrategyOutput:
            out = super().evaluate(history, context)
            object.__setattr__(out, "decision_time", history[0].close_time)
            return out

    with pytest.raises(StrategyOutputMismatchError, match="decision_time .* does not match"):
        StrategyEvaluator.evaluate_at(candles, target_index=2, context=ctx, strategy=BadDecisionTimeStrategy())


def test_stage_b_ingestion_metadata_isolation(tmp_path: Path) -> None:
    candles = _make_series(3)
    _, _, ctx = commit_and_build_context(tmp_path, candles)
    strat = DummyStrategy(minimum_history_candles=2)

    StrategyEvaluator.evaluate_at(candles, target_index=2, context=ctx, strategy=strat)
    assert strat.last_received_candles is not None
    for sc in strat.last_received_candles:
        assert isinstance(sc, StrategyCandle)
        assert not hasattr(sc, "source")
        assert not hasattr(sc, "fetched_at")
        assert not hasattr(sc, "exchange_id")
        assert not hasattr(sc, "market_type")


def test_stage_b_canonical_market_data_equivalence(tmp_path: Path) -> None:
    candles1 = _make_series(3)
    # Build identical market data candles but with different source and fetched_at
    candles2 = [
        Candle(
            exchange_id="binance",
            market_type="spot",
            symbol="BTC/USDT",
            timeframe="1h",
            open_time=c.open_time,
            close_time=c.close_time,
            open=c.open,
            high=c.high,
            low=c.low,
            close=c.close,
            base_volume=c.base_volume,
            is_closed=True,
            source="ccxt_live",  # Different source
            fetched_at=_make_utc_dt(20, 0),  # Different fetched_at
        )
        for c in candles1
    ]

    _, _, ctx1 = commit_and_build_context(tmp_path / "c1", candles1)
    _, _, ctx2 = commit_and_build_context(tmp_path / "c2", candles2)

    strat1 = DummyStrategy(minimum_history_candles=2)
    strat2 = DummyStrategy(minimum_history_candles=2)

    dec1 = StrategyEvaluator.evaluate_at(candles1, target_index=2, context=ctx1, strategy=strat1)
    dec2 = StrategyEvaluator.evaluate_at(candles2, target_index=2, context=ctx2, strategy=strat2)

    # Identical decisions across all fields
    assert dec1.signal == dec2.signal
    assert dec1.reason_code == dec2.reason_code
    assert dec1.decision_time == dec2.decision_time
    assert dec1.metadata == dec2.metadata


def test_stage_b_evaluate_series_execution(tmp_path: Path) -> None:
    candles = _make_series(4)
    _, _, ctx = commit_and_build_context(tmp_path, candles)
    strat = DummyStrategy(minimum_history_candles=2)

    decisions = StrategyEvaluator.evaluate_series(candles, context=ctx, strategy=strat)
    # Eligible target indices: 1, 2, 3 (total 3 decisions)
    assert len(decisions) == 3
    assert decisions[0].source_candle_close_time == candles[1].close_time
    assert decisions[1].source_candle_close_time == candles[2].close_time
    assert decisions[2].source_candle_close_time == candles[3].close_time
