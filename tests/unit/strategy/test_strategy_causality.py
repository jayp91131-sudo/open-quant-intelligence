"""Tests for Point-in-Time Causality, Prefix-Invariance, and Determinism."""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from open_quant_intelligence.domain.candle import Candle
from open_quant_intelligence.strategy.contracts import (
    GapPolicy,
    StrategyCandle,
    StrategyConfig,
    StrategyContext,
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
class MAConfig(StrategyConfig):
    threshold: float = 0.5


class MovingAverageMockStrategy:
    """Mock strategy that computes moving averages across historical candles."""

    def __init__(self) -> None:
        self._config = MAConfig()

    @property
    def strategy_id(self) -> str:
        return "ma_strategy"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def config(self) -> StrategyConfig:
        return self._config

    @property
    def minimum_history_candles(self) -> int:
        return 2

    @property
    def gap_policy(self) -> GapPolicy:
        return GapPolicy.STRICT_GAP_FREE

    def evaluate(self, history: Sequence[StrategyCandle], context: StrategyContext) -> StrategyOutput:
        target = history[-1]
        closes = [c.close for c in history]
        avg_close = sum(closes) / len(closes)

        is_long = target.close > avg_close
        return StrategyOutput(
            strategy_id=self.strategy_id,
            strategy_version=self.version,
            config_fingerprint=self.config.compute_fingerprint(),
            symbol=context.symbol,
            timeframe=context.timeframe,
            source_candle_open_time=target.open_time,
            source_candle_close_time=target.close_time,
            decision_time=target.close_time,
            signal=ResearchSignal.LONG if is_long else ResearchSignal.FLAT,
            reason_code=ReasonCode.LONG_SIGNAL_GENERATED if is_long else ReasonCode.FLAT_SIGNAL_GENERATED,
            metadata={"avg_close": avg_close, "latest_close": target.close},
        )


def _make_candle(hour: int, open_p: float, close_p: float) -> Candle:
    op = _make_utc_dt(hour, 0)
    cl = _make_utc_dt(hour + 1, 0)
    return Candle(
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        open_time=op,
        close_time=cl,
        open=open_p,
        high=max(open_p, close_p) + 100.0,
        low=min(open_p, close_p) - 100.0,
        close=close_p,
        base_volume=10.0,
        is_closed=True,
    )


def test_causality_future_candle_modification_invariance(tmp_path: Path) -> None:
    """Assert prefix-invariance: modifying future candles strictly after T does not alter decision at T."""
    c0 = _make_candle(10, 50000.0, 50200.0)
    c1 = _make_candle(11, 50200.0, 50600.0)  # Target candle T (index 1)

    # Dataset 1: Future candle c2 has normal price
    c2_v1 = _make_candle(12, 50600.0, 50800.0)
    dataset1 = [c0, c1, c2_v1]
    _, _, ctx1 = commit_and_build_context(tmp_path / "d1", dataset1)

    # Dataset 2: Future candle c2 has completely wild/modified price and volume
    c2_v2 = _make_candle(12, 10000.0, 95000.0)
    dataset2 = [c0, c1, c2_v2]
    _, _, ctx2 = commit_and_build_context(tmp_path / "d2", dataset2)

    strategy = MovingAverageMockStrategy()

    # Evaluate both at target_index = 1
    dec1 = StrategyEvaluator.evaluate_at(dataset1, target_index=1, context=ctx1, strategy=strategy)
    dec2 = StrategyEvaluator.evaluate_at(dataset2, target_index=1, context=ctx2, strategy=strategy)

    # Exact parity on all discrete decision fields
    assert dec1.signal == dec2.signal
    assert dec1.reason_code == dec2.reason_code
    assert dec1.strategy_id == dec2.strategy_id
    assert dec1.strategy_version == dec2.strategy_version
    assert dec1.config_fingerprint == dec2.config_fingerprint
    assert dec1.symbol == dec2.symbol
    assert dec1.timeframe == dec2.timeframe
    assert dec1.source_candle_open_time == dec2.source_candle_open_time
    assert dec1.source_candle_close_time == dec2.source_candle_close_time
    assert dec1.decision_time == dec2.decision_time
    assert dec1.metadata == dec2.metadata


def test_deterministic_repeat_evaluations(tmp_path: Path) -> None:
    """Assert evaluation is completely deterministic across repeated executions."""
    c0 = _make_candle(10, 50000.0, 50200.0)
    c1 = _make_candle(11, 50200.0, 50600.0)
    c2 = _make_candle(12, 50600.0, 50800.0)
    candles = [c0, c1, c2]
    _, _, ctx = commit_and_build_context(tmp_path, candles)
    strategy = MovingAverageMockStrategy()

    runs = [StrategyEvaluator.evaluate_series(candles, context=ctx, strategy=strategy) for _ in range(3)]

    for run_idx in range(1, len(runs)):
        assert len(runs[run_idx]) == len(runs[0])
        for dec_a, dec_b in zip(runs[0], runs[run_idx], strict=True):
            assert dec_a.signal == dec_b.signal
            assert dec_a.reason_code == dec_b.reason_code
            assert dec_a.decision_time == dec_b.decision_time
            assert dec_a.metadata == dec_b.metadata
