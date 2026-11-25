"""Tests for Strategy Gap Policy enforcement and causal prefix scoping."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Sequence

import pytest

from open_quant_intelligence.data.quality import analyze_dataset_quality
from open_quant_intelligence.domain.candle import Candle
from open_quant_intelligence.domain.market import MarketIdentity
from open_quant_intelligence.domain.timeframe import SupportedTimeframe
from open_quant_intelligence.storage.fingerprint import compute_content_fingerprint
from open_quant_intelligence.strategy.contracts import (
    EvaluationContext,
    GapPolicy,
    StrategyCandle,
    StrategyConfig,
    StrategyContext,
    StrategyDataGapError,
)
from open_quant_intelligence.strategy.evaluator import StrategyEvaluator
from open_quant_intelligence.strategy.signals import (
    ReasonCode,
    ResearchSignal,
    StrategyOutput,
)


def _make_utc_dt(hour: int = 10, minute: int = 0) -> datetime:
    return datetime(2025, 1, 1, hour, minute, 0, tzinfo=timezone.utc)


@dataclass(frozen=True)
class DummyConfig(StrategyConfig):
    pass


class GapTestStrategy:
    def __init__(self, gap_policy: GapPolicy) -> None:
        self._gap_policy = gap_policy
        self._config = DummyConfig()

    @property
    def strategy_id(self) -> str:
        return "gap_strat"

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
        return self._gap_policy

    def evaluate(self, history: Sequence[StrategyCandle], context: StrategyContext) -> StrategyOutput:
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
            signal=ResearchSignal.FLAT,
            reason_code=ReasonCode.NO_CONDITION_MET,
            metadata={},
        )


def _make_candle(open_hour: int) -> Candle:
    op = _make_utc_dt(open_hour, 0)
    cl = _make_utc_dt(open_hour + 1, 0)
    return Candle(
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        open_time=op,
        close_time=cl,
        open=50000.0,
        high=50500.0,
        low=49500.0,
        close=50200.0,
        base_volume=10.0,
        is_closed=True,
    )


def test_strict_gap_free_policy_rejects_leading_and_internal_gaps() -> None:
    # Candles at hours: 11, 12 (requested_since is hour 10 -> Leading gap of 1h)
    c1 = _make_candle(11)
    c2 = _make_candle(12)
    candles = [c1, c2]

    since_ms = int(_make_utc_dt(10, 0).timestamp() * 1000)
    until_ms = int(_make_utc_dt(13, 0).timestamp() * 1000)
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
    ctx = EvaluationContext(
        market=MarketIdentity("binance", "spot", "BTC/USDT"),
        timeframe=SupportedTimeframe.H1,
        requested_since_ms=since_ms,
        requested_until_ms=until_ms,
        content_fingerprint=fp,
        quality_report=report,
    )

    strat = GapTestStrategy(gap_policy=GapPolicy.STRICT_GAP_FREE)

    # Evaluating at c2 (index 1) contains a leading gap (hour 10 missing)
    with pytest.raises(StrategyDataGapError, match="policy STRICT_GAP_FREE violated"):
        StrategyEvaluator.evaluate_at(candles, target_index=1, context=ctx, strategy=strat)


def test_reject_internal_gaps_permits_leading_gap_but_rejects_internal_gap() -> None:
    # Case A: Leading gap (hour 10 missing, candles at hours 11, 12)
    c1 = _make_candle(11)
    c2 = _make_candle(12)
    candles = [c1, c2]

    since_ms = int(_make_utc_dt(10, 0).timestamp() * 1000)
    until_ms = int(_make_utc_dt(13, 0).timestamp() * 1000)
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
    ctx = EvaluationContext(
        market=MarketIdentity("binance", "spot", "BTC/USDT"),
        timeframe=SupportedTimeframe.H1,
        requested_since_ms=since_ms,
        requested_until_ms=until_ms,
        content_fingerprint=fp,
        quality_report=report,
    )

    strat = GapTestStrategy(gap_policy=GapPolicy.REJECT_INTERNAL_GAPS)
    # Permitted because there are NO internal gaps between c1 and c2
    dec = StrategyEvaluator.evaluate_at(candles, target_index=1, context=ctx, strategy=strat)
    assert dec.signal == ResearchSignal.FLAT

    # Case B: Internal gap (candles at hours 10 and 12, missing hour 11)
    c_gap1 = _make_candle(10)
    c_gap2 = _make_candle(12)  # Missing hour 11
    gap_candles = [c_gap1, c_gap2]

    gap_since_ms = int(_make_utc_dt(10, 0).timestamp() * 1000)
    gap_until_ms = int(_make_utc_dt(13, 0).timestamp() * 1000)
    gap_report = analyze_dataset_quality(
        gap_candles, since_ms=gap_since_ms, until_ms=gap_until_ms, timeframe=SupportedTimeframe.H1
    )
    gap_fp = compute_content_fingerprint(
        candles=gap_candles,
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe=SupportedTimeframe.H1,
        requested_since_ms=gap_since_ms,
        requested_until_ms=gap_until_ms,
    )
    gap_ctx = EvaluationContext(
        market=MarketIdentity("binance", "spot", "BTC/USDT"),
        timeframe=SupportedTimeframe.H1,
        requested_since_ms=gap_since_ms,
        requested_until_ms=gap_until_ms,
        content_fingerprint=gap_fp,
        quality_report=gap_report,
    )

    with pytest.raises(StrategyDataGapError, match="policy REJECT_INTERNAL_GAPS violated"):
        StrategyEvaluator.evaluate_at(gap_candles, target_index=1, context=gap_ctx, strategy=strat)


def test_future_gap_does_not_affect_causal_evaluation() -> None:
    # Candles at hours 10, 11 (contiguous prefix), and hour 14 (missing hours 12, 13 in future)
    c1 = _make_candle(10)
    c2 = _make_candle(11)
    c3 = _make_candle(14)
    candles = [c1, c2, c3]

    since_ms = int(_make_utc_dt(10, 0).timestamp() * 1000)
    until_ms = int(_make_utc_dt(15, 0).timestamp() * 1000)
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
    ctx = EvaluationContext(
        market=MarketIdentity("binance", "spot", "BTC/USDT"),
        timeframe=SupportedTimeframe.H1,
        requested_since_ms=since_ms,
        requested_until_ms=until_ms,
        content_fingerprint=fp,
        quality_report=report,
    )

    strat = GapTestStrategy(gap_policy=GapPolicy.STRICT_GAP_FREE)

    # Evaluating at c2 (index 1) has ZERO gaps in its prefix [hour 10, hour 12).
    # The gap between hour 12 and hour 14 is in the future relative to c2.
    decision = StrategyEvaluator.evaluate_at(candles, target_index=1, context=ctx, strategy=strat)
    assert decision.source_candle_close_time == c2.close_time
    assert decision.signal == ResearchSignal.FLAT
