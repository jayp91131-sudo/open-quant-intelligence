"""Tests for ResearchSignal, ReasonCode, StrategyOutput, and StrategyDecision."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from open_quant_intelligence.domain.candle import Candle
from open_quant_intelligence.domain.timeframe import SupportedTimeframe
from open_quant_intelligence.strategy.contracts import SnapshotConsistencyError
from open_quant_intelligence.strategy.evaluator import StrategyEvaluator
from open_quant_intelligence.strategy.signals import (
    ReasonCode,
    ResearchSignal,
    StrategyDecision,
    StrategyOutput,
)
from tests.unit.strategy._catalog_helpers import commit_and_build_context


def _make_utc_dt(hour: int = 10, minute: int = 0) -> datetime:
    return datetime(2025, 1, 1, hour, minute, 0, tzinfo=timezone.utc)


def _make_valid_output(
    decision_time: datetime | None = None,
    source_candle_close_time: datetime | None = None,
    metadata: dict | None = None,
) -> StrategyOutput:
    t_open = _make_utc_dt(10, 0)
    t_close = source_candle_close_time or _make_utc_dt(11, 0)
    t_decision = decision_time or t_close

    return StrategyOutput(
        strategy_id="trend_follow",
        strategy_version="1.0.0",
        config_fingerprint="sha256:" + "a" * 64,
        symbol="BTC/USDT",
        timeframe=SupportedTimeframe.H1,
        source_candle_open_time=t_open,
        source_candle_close_time=t_close,
        decision_time=t_decision,
        signal=ResearchSignal.LONG,
        reason_code=ReasonCode.LONG_SIGNAL_GENERATED,
        metadata={"fast_ma": 50100.0, "slow_ma": 49800.0} if metadata is None else metadata,
    )


def test_research_signal_values() -> None:
    assert ResearchSignal.LONG.value == "LONG"
    assert ResearchSignal.FLAT.value == "FLAT"
    assert ResearchSignal.HOLD.value == "HOLD"


def test_reason_code_constants() -> None:
    assert ReasonCode.WARMUP_INSUFFICIENT == "WARMUP_INSUFFICIENT"
    assert ReasonCode.LONG_SIGNAL_GENERATED == "LONG_SIGNAL_GENERATED"
    assert ReasonCode.FLAT_SIGNAL_GENERATED == "FLAT_SIGNAL_GENERATED"
    assert ReasonCode.NO_CONDITION_MET == "NO_CONDITION_MET"


def test_strategy_output_valid_construction() -> None:
    out = _make_valid_output()
    assert out.strategy_id == "trend_follow"
    assert out.signal == ResearchSignal.LONG
    assert out.metadata["fast_ma"] == 50100.0
    assert out.decision_time == out.source_candle_close_time


def test_strategy_output_causality_violation_rejection() -> None:
    t_close = _make_utc_dt(11, 0)
    t_future = _make_utc_dt(11, 1)

    with pytest.raises(ValueError, match="Causality violation"):
        _make_valid_output(decision_time=t_future, source_candle_close_time=t_close)


def test_strategy_output_timestamp_rejections() -> None:
    t_open = _make_utc_dt(11, 0)
    t_close = _make_utc_dt(10, 0)

    # close <= open
    with pytest.raises(ValueError, match="source_candle_close_time must be strictly after"):
        StrategyOutput(
            strategy_id="strat",
            strategy_version="1.0.0",
            config_fingerprint="sha256:" + "a" * 64,
            symbol="BTC/USDT",
            timeframe=SupportedTimeframe.H1,
            source_candle_open_time=t_open,
            source_candle_close_time=t_close,
            decision_time=t_close,
            signal=ResearchSignal.FLAT,
            reason_code=ReasonCode.FLAT_SIGNAL_GENERATED,
            metadata={},
        )


def test_strategy_output_metadata_validation() -> None:
    # Allowed scalar types: int, float, str, bool
    out = _make_valid_output(metadata={"int_val": 1, "float_val": 2.5, "str_val": "test", "bool_val": True})
    assert out.metadata["int_val"] == 1
    assert out.metadata["float_val"] == 2.5
    assert out.metadata["str_val"] == "test"
    assert out.metadata["bool_val"] is True

    # Reject non-scalar values (e.g., list, dict)
    with pytest.raises(TypeError, match="must be primitive scalar"):
        _make_valid_output(metadata={"nested": [1, 2]})

    with pytest.raises(TypeError, match="must be primitive scalar"):
        _make_valid_output(metadata={"nested": {"a": 1}})

    # Reject non-finite floats
    with pytest.raises(ValueError, match="must be finite"):
        _make_valid_output(metadata={"nan_val": float("nan")})

    with pytest.raises(ValueError, match="must be finite"):
        _make_valid_output(metadata={"inf_val": float("inf")})


def test_strategy_output_immutability() -> None:
    out = _make_valid_output()
    # Cannot mutate attributes
    with pytest.raises(AttributeError):
        out.strategy_id = "new_id"  # type: ignore

    # Cannot mutate metadata mapping proxy
    with pytest.raises(TypeError):
        out.metadata["fast_ma"] = 99999.0  # type: ignore


def test_strategy_decision_assemble_and_delegation(tmp_path: Path) -> None:
    out = _make_valid_output()
    c = Candle(
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        open_time=_make_utc_dt(10, 0),
        close_time=_make_utc_dt(11, 0),
        open=50000.0,
        high=50500.0,
        low=49500.0,
        close=50200.0,
        base_volume=10.0,
        is_closed=True,
    )
    _, record, ctx = commit_and_build_context(tmp_path, [c])
    verified_snapshot = StrategyEvaluator.verify_snapshot([c], ctx)

    decision = StrategyDecision.assemble(
        output=out,
        verified_snapshot=verified_snapshot,
    )

    assert decision.dataset_identity == record.dataset_identity
    assert decision.dataset_content_fingerprint == record.content_fingerprint_sha256
    assert decision.snapshot_id == record.snapshot_id
    assert decision.output == out

    # Delegating properties
    assert decision.strategy_id == out.strategy_id
    assert decision.strategy_version == out.strategy_version
    assert decision.config_fingerprint == out.config_fingerprint
    assert decision.symbol == out.symbol
    assert decision.timeframe == out.timeframe
    assert decision.source_candle_open_time == out.source_candle_open_time
    assert decision.source_candle_close_time == out.source_candle_close_time
    assert decision.decision_time == out.decision_time
    assert decision.signal == out.signal
    assert decision.reason_code == out.reason_code
    assert decision.metadata == out.metadata


def test_strategy_decision_rejections() -> None:
    out = _make_valid_output()
    fp = "sha256:" + "b" * 64

    # Direct instantiation without VerifiedSnapshot is rejected
    with pytest.raises(SnapshotConsistencyError, match="cannot be constructed directly"):
        StrategyDecision(
            dataset_identity="binance:spot:BTC/USDT:1h:0:3600000",
            dataset_content_fingerprint=fp,
            snapshot_id="snap_fake",
            output=out,
        )

    # StrategyDecision.assemble() rejects unverified / non-VerifiedSnapshot objects
    with pytest.raises(SnapshotConsistencyError, match="requires a VerifiedSnapshot"):
        StrategyDecision.assemble(output=out, verified_snapshot="not-verified")  # type: ignore
