"""Tests for strategy contracts, StrategyCandle, contexts, and configurations."""

from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from open_quant_intelligence.data.quality import (
    DataQualityReport,
)
from open_quant_intelligence.domain.candle import Candle
from open_quant_intelligence.domain.market import MarketIdentity
from open_quant_intelligence.domain.timeframe import SupportedTimeframe
from open_quant_intelligence.strategy.contracts import (
    EmptyDatasetError,
    EvaluationContext,
    GapPolicy,
    HeterogeneousSeriesError,
    IncompleteCandleError,
    InsufficientHistoryError,
    SnapshotConsistencyError,
    Strategy,
    StrategyCandle,
    StrategyConfig,
    StrategyContext,
    StrategyDataGapError,
    StrategyEvaluationError,
    StrategyOutputMismatchError,
    _validate_utc_datetime,
)
from open_quant_intelligence.strategy.signals import StrategyOutput

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _make_utc_dt(hour: int = 10, minute: int = 0) -> datetime:
    return datetime(2025, 1, 1, hour, minute, 0, tzinfo=timezone.utc)


def _make_valid_m0_candle(
    open_time: datetime | None = None,
    close_time: datetime | None = None,
    is_closed: bool = True,
    source: str = "binance_vision",
    fetched_at: datetime | None = None,
) -> Candle:
    op = open_time or _make_utc_dt(10, 0)
    cl = close_time or _make_utc_dt(11, 0)
    return Candle(
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        open_time=op,
        close_time=cl,
        open=50000.0,
        high=51000.0,
        low=49500.0,
        close=50500.0,
        base_volume=10.5,
        quote_volume=525000.0,
        trade_count=100,
        is_closed=is_closed,
        source=source,
        fetched_at=fetched_at or _make_utc_dt(11, 5),
    )


# -----------------------------------------------------------------------------
# Tests: Exception Hierarchy
# -----------------------------------------------------------------------------


def test_exception_hierarchy() -> None:
    """Verify all custom exceptions inherit from StrategyEvaluationError."""
    assert issubclass(EmptyDatasetError, StrategyEvaluationError)
    assert issubclass(InsufficientHistoryError, StrategyEvaluationError)
    assert issubclass(IncompleteCandleError, StrategyEvaluationError)
    assert issubclass(HeterogeneousSeriesError, StrategyEvaluationError)
    assert issubclass(StrategyDataGapError, StrategyEvaluationError)
    assert issubclass(SnapshotConsistencyError, StrategyEvaluationError)
    assert issubclass(StrategyOutputMismatchError, StrategyEvaluationError)


# -----------------------------------------------------------------------------
# Tests: _validate_utc_datetime
# -----------------------------------------------------------------------------


def test_validate_utc_datetime_success() -> None:
    dt = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    _validate_utc_datetime(dt, "test_field")


def test_validate_utc_datetime_rejections() -> None:
    # Non-datetime
    with pytest.raises(TypeError, match="must be a datetime instance"):
        _validate_utc_datetime("2025-01-01T00:00:00Z", "test_field")  # type: ignore

    # Naive datetime
    naive_dt = datetime(2025, 1, 1, 0, 0, 0)
    with pytest.raises(ValueError, match="must be a timezone-aware UTC datetime"):
        _validate_utc_datetime(naive_dt, "test_field")

    # Non-zero offset
    from datetime import timedelta
    from datetime import timezone as tz

    offset_dt = datetime(2025, 1, 1, 0, 0, 0, tzinfo=tz(timedelta(hours=5)))
    with pytest.raises(ValueError, match="must be a timezone-aware UTC datetime"):
        _validate_utc_datetime(offset_dt, "test_field")


# -----------------------------------------------------------------------------
# Tests: StrategyCandle
# -----------------------------------------------------------------------------


def test_strategy_candle_valid_construction() -> None:
    sc = StrategyCandle(
        open_time=_make_utc_dt(10, 0),
        close_time=_make_utc_dt(11, 0),
        open=100.0,
        high=110.0,
        low=90.0,
        close=105.0,
        base_volume=50.0,
        quote_volume=5000.0,
        trade_count=20,
    )
    assert sc.open == 100.0
    assert sc.high == 110.0
    assert sc.low == 90.0
    assert sc.close == 105.0
    assert sc.base_volume == 50.0
    assert sc.quote_volume == 5000.0
    assert sc.trade_count == 20

    # Ingestion metadata must not exist
    assert not hasattr(sc, "source")
    assert not hasattr(sc, "fetched_at")
    assert not hasattr(sc, "is_closed")
    assert not hasattr(sc, "exchange_id")
    assert not hasattr(sc, "market_type")


def test_strategy_candle_rejections() -> None:
    t_open = _make_utc_dt(10, 0)
    t_close = _make_utc_dt(11, 0)

    # close_time <= open_time
    with pytest.raises(ValueError, match="must be strictly after open_time"):
        StrategyCandle(
            open_time=t_close,
            close_time=t_open,
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            base_volume=50.0,
        )

    # Non-positive or non-finite prices
    for bad_price in [0.0, -10.0, float("nan"), float("inf"), True]:
        with pytest.raises(ValueError, match="must be finite, strictly positive"):
            StrategyCandle(
                open_time=t_open,
                close_time=t_close,
                open=bad_price,
                high=110.0,
                low=90.0,
                close=105.0,
                base_volume=50.0,
            )

    # High / Low bounding violation
    with pytest.raises(ValueError, match="High and low prices must bound"):
        StrategyCandle(
            open_time=t_open,
            close_time=t_close,
            open=100.0,
            high=95.0,  # high < open
            low=90.0,
            close=92.0,
            base_volume=50.0,
        )

    # Negative base volume
    with pytest.raises(ValueError, match="base_volume must be finite, non-negative"):
        StrategyCandle(
            open_time=t_open,
            close_time=t_close,
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            base_volume=-1.0,
        )

    # Negative quote volume
    with pytest.raises(ValueError, match="quote_volume must be finite, non-negative"):
        StrategyCandle(
            open_time=t_open,
            close_time=t_close,
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            base_volume=1.0,
            quote_volume=-100.0,
        )

    # Negative trade count
    with pytest.raises(ValueError, match="trade_count must be non-negative integer"):
        StrategyCandle(
            open_time=t_open,
            close_time=t_close,
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            base_volume=1.0,
            trade_count=-5,
        )


def test_strategy_candle_from_candle_projection() -> None:
    c = _make_valid_m0_candle()
    sc = StrategyCandle.from_candle(c)

    assert sc.open_time == c.open_time
    assert sc.close_time == c.close_time
    assert sc.open == c.open
    assert sc.high == c.high
    assert sc.low == c.low
    assert sc.close == c.close
    assert sc.base_volume == c.base_volume
    assert sc.quote_volume == c.quote_volume
    assert sc.trade_count == c.trade_count


def test_strategy_candle_from_unclosed_candle_rejection() -> None:
    c = _make_valid_m0_candle(is_closed=False)
    with pytest.raises(IncompleteCandleError, match="Cannot project unclosed candle"):
        StrategyCandle.from_candle(c)


# -----------------------------------------------------------------------------
# Tests: StrategyContext
# -----------------------------------------------------------------------------


def test_strategy_context_validation() -> None:
    report = DataQualityReport(
        requested_since_ms=0,
        requested_until_ms=3600000,
        timeframe=SupportedTimeframe.H1,
        expected_records=1,
        record_count=1,
        missing_intervals=0,
        gap_count=0,
        gaps=(),
        quality_status="GAP_FREE",
    )

    ctx = StrategyContext(
        symbol="BTC/USDT",
        timeframe=SupportedTimeframe.H1,
        prefix_quality_report=report,
    )
    assert ctx.symbol == "BTC/USDT"
    assert ctx.timeframe == SupportedTimeframe.H1
    assert ctx.prefix_quality_report == report

    # Rejection of empty symbol
    with pytest.raises(ValueError, match="symbol must be a non-empty string"):
        StrategyContext(
            symbol="  ",
            timeframe=SupportedTimeframe.H1,
            prefix_quality_report=report,
        )

    # Rejection of non-SupportedTimeframe
    with pytest.raises(TypeError, match="timeframe must be an instance of SupportedTimeframe"):
        StrategyContext(
            symbol="BTC/USDT",
            timeframe="1h",  # type: ignore
            prefix_quality_report=report,
        )

    # Rejection of non-DataQualityReport
    with pytest.raises(TypeError, match="prefix_quality_report must be an instance of DataQualityReport"):
        StrategyContext(
            symbol="BTC/USDT",
            timeframe=SupportedTimeframe.H1,
            prefix_quality_report={},  # type: ignore
        )


# -----------------------------------------------------------------------------
# Tests: EvaluationContext
# -----------------------------------------------------------------------------


def test_evaluation_context_validation() -> None:
    market = MarketIdentity(exchange_id="binance", market_type="spot", symbol="BTC/USDT")
    report = DataQualityReport(
        requested_since_ms=0,
        requested_until_ms=7200000,
        timeframe=SupportedTimeframe.H1,
        expected_records=2,
        record_count=2,
        missing_intervals=0,
        gap_count=0,
        gaps=(),
        quality_status="GAP_FREE",
    )
    fp = "sha256:" + "a" * 64

    ctx = EvaluationContext(
        market=market,
        timeframe=SupportedTimeframe.H1,
        requested_since_ms=0,
        requested_until_ms=7200000,
        content_fingerprint=fp,
        quality_report=report,
    )
    assert ctx.dataset_identity == "binance:spot:BTC/USDT:1h:0:7200000"

    # Reject non-spot market
    with pytest.raises(ValueError, match="Only spot markets are supported"):
        m_non_spot = MarketIdentity(exchange_id="binance", market_type="spot", symbol="BTC/USDT")
        object.__setattr__(m_non_spot, "market_type", "swap")
        EvaluationContext(
            market=m_non_spot,
            timeframe=SupportedTimeframe.H1,
            requested_since_ms=0,
            requested_until_ms=7200000,
            content_fingerprint=fp,
            quality_report=report,
        )

    # Reject boolean / float timestamp bounds
    with pytest.raises(TypeError, match="requested_since_ms must be exact int"):
        EvaluationContext(
            market=market,
            timeframe=SupportedTimeframe.H1,
            requested_since_ms=True,  # type: ignore
            requested_until_ms=7200000,
            content_fingerprint=fp,
            quality_report=report,
        )
    with pytest.raises(TypeError, match="requested_until_ms must be exact int"):
        EvaluationContext(
            market=market,
            timeframe=SupportedTimeframe.H1,
            requested_since_ms=0,
            requested_until_ms=7200000.0,  # type: ignore
            content_fingerprint=fp,
            quality_report=report,
        )

    # Reject off-grid timestamp bounds
    with pytest.raises(ValueError, match="not grid-aligned"):
        EvaluationContext(
            market=market,
            timeframe=SupportedTimeframe.H1,
            requested_since_ms=1000,  # not multiple of 3600000
            requested_until_ms=7200000,
            content_fingerprint=fp,
            quality_report=report,
        )

    # Reject invalid content_fingerprint format
    with pytest.raises(ValueError, match="Invalid content_fingerprint format"):
        EvaluationContext(
            market=market,
            timeframe=SupportedTimeframe.H1,
            requested_since_ms=0,
            requested_until_ms=7200000,
            content_fingerprint="not-a-valid-sha256",
            quality_report=report,
        )

    # Reject inconsistent quality report
    bad_report = DataQualityReport(
        requested_since_ms=0,
        requested_until_ms=7200000,
        timeframe=SupportedTimeframe.H1,
        expected_records=2,
        record_count=1,
        missing_intervals=0,  # expected != record_count + missing_intervals (2 != 1 + 0)
        gap_count=0,
        gaps=(),
        quality_status="GAP_CONTAINING",
    )
    with pytest.raises(ValueError, match="expected_records != record_count \\+ missing_intervals"):
        EvaluationContext(
            market=market,
            timeframe=SupportedTimeframe.H1,
            requested_since_ms=0,
            requested_until_ms=7200000,
            content_fingerprint=fp,
            quality_report=bad_report,
        )


# -----------------------------------------------------------------------------
# Tests: StrategyConfig
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class MockConfig(StrategyConfig):
    param_int: int = 10
    param_float: float = 1.5
    param_str: str = "fast"
    param_bool: bool = True
    param_tuple: tuple[int, ...] = (1, 2, 3)


def test_strategy_config_fingerprint_deterministic() -> None:
    cfg1 = MockConfig()
    cfg2 = MockConfig()
    fp1 = cfg1.compute_fingerprint()
    fp2 = cfg2.compute_fingerprint()

    assert fp1.startswith("sha256:")
    assert len(fp1) == 71  # "sha256:" + 64 hex characters
    assert fp1 == fp2

    cfg3 = MockConfig(param_int=20)
    assert cfg3.compute_fingerprint() != fp1


def test_strategy_config_rejection_of_mutable_and_nonfinite() -> None:
    @dataclass(frozen=True)
    class BadListConfig(StrategyConfig):
        param_list: list = None  # type: ignore

    with pytest.raises(TypeError, match="Unsupported configuration value type: list"):
        BadListConfig(param_list=[1, 2])

    @dataclass(frozen=True)
    class BadDictConfig(StrategyConfig):
        param_dict: dict = None  # type: ignore

    with pytest.raises(TypeError, match="Unsupported configuration value type: dict"):
        BadDictConfig(param_dict={"k": "v"})

    @dataclass(frozen=True)
    class BadFloatConfig(StrategyConfig):
        param_float: float = float("nan")

    with pytest.raises(ValueError, match="Non-finite float value not permitted"):
        BadFloatConfig(param_float=float("nan"))

    with pytest.raises(ValueError, match="Non-finite float value not permitted"):
        BadFloatConfig(param_float=float("inf"))


# -----------------------------------------------------------------------------
# Tests: Strategy Protocol & GapPolicy
# -----------------------------------------------------------------------------


def test_gap_policy_values() -> None:
    assert GapPolicy.STRICT_GAP_FREE.value == "STRICT_GAP_FREE"
    assert GapPolicy.REJECT_INTERNAL_GAPS.value == "REJECT_INTERNAL_GAPS"


def test_strategy_protocol_runtime_checkable() -> None:
    class ValidStrategy:
        @property
        def strategy_id(self) -> str:
            return "test_strat"

        @property
        def version(self) -> str:
            return "1.0.0"

        @property
        def config(self) -> StrategyConfig:
            return MockConfig()

        @property
        def minimum_history_candles(self) -> int:
            return 5

        @property
        def gap_policy(self) -> GapPolicy:
            return GapPolicy.STRICT_GAP_FREE

        def evaluate(self, history: list[StrategyCandle], context: StrategyContext) -> StrategyOutput:
            raise NotImplementedError

    strat = ValidStrategy()
    assert isinstance(strat, Strategy)

    class InvalidStrategy:
        pass

    assert not isinstance(InvalidStrategy(), Strategy)
