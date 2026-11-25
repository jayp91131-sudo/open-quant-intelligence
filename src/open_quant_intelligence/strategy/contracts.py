"""Core strategy interfaces, immutable contracts, and structured exception hierarchy.

This module defines the foundational contracts for research strategy evaluation in
Open Quant Intelligence V2, maintaining strict isolation between research evaluation
and ingestion/storage infrastructure.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol, Sequence, runtime_checkable

from open_quant_intelligence.data.quality import DataQualityReport
from open_quant_intelligence.domain.candle import Candle
from open_quant_intelligence.domain.market import MarketIdentity
from open_quant_intelligence.domain.timeframe import (
    SupportedTimeframe,
    parse_timeframe,
    timeframe_to_timedelta,
)

if TYPE_CHECKING:
    from open_quant_intelligence.storage.catalog import CatalogDatasetRecord
    from open_quant_intelligence.strategy.signals import StrategyOutput

_CATALOG_PROVENANCE_TOKEN = object()
_VERIFIED_SNAPSHOT_TOKEN = object()


# -----------------------------------------------------------------------------
# Structured Exception Hierarchy
# -----------------------------------------------------------------------------


class StrategyEvaluationError(Exception):
    """Base exception for all strategy evaluation failures."""


class EmptyDatasetError(StrategyEvaluationError):
    """Raised when an empty candle sequence is supplied for evaluation."""


class InsufficientHistoryError(StrategyEvaluationError):
    """Raised when available candle history is strictly less than minimum_history_candles."""


class IncompleteCandleError(StrategyEvaluationError):
    """Raised when an unclosed candle (is_closed=False) is encountered in the evaluated prefix."""


class HeterogeneousSeriesError(StrategyEvaluationError):
    """Raised when candles contain mixed symbols, timeframes, or non-spot markets."""


class ChronologicalOrderError(StrategyEvaluationError):
    """Raised when candles are out of order or contain duplicate timestamps."""


class StrategyDataGapError(StrategyEvaluationError):
    """Raised when input candles violate the strategy's declared GapPolicy."""


class SnapshotConsistencyError(StrategyEvaluationError):
    """Raised when supplied candle series fails verification against M1 snapshot metadata or fingerprint."""


class StrategyOutputMismatchError(StrategyEvaluationError):
    """Raised when an emitted StrategyOutput contradicts the strategy identity, timeframe, or target candle."""


# -----------------------------------------------------------------------------
# Shared UTC Validation Helper
# -----------------------------------------------------------------------------


def _validate_utc_datetime(dt: datetime, field_name: str) -> None:
    """Validate that a datetime is a timezone-aware UTC datetime with zero offset."""
    if not isinstance(dt, datetime):
        raise TypeError(f"{field_name} must be a datetime instance, got {type(dt).__name__}")
    if dt.tzinfo is None or dt.utcoffset() != timezone.utc.utcoffset(dt):
        raise ValueError(f"{field_name} must be a timezone-aware UTC datetime with zero offset, got: {dt!r}")


# -----------------------------------------------------------------------------
# Strategy Candle Contract
# -----------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StrategyCandle:
    """Immutable, canonical market data view of a finalized candle for strategy evaluation.

    Contains strictly the canonical fingerprinted market data fields:
    - open_time: UTC-aware inclusive start boundary.
    - close_time: UTC-aware exclusive close boundary.
    - open: Finite, strictly positive open price.
    - high: Finite, strictly positive high price.
    - low: Finite, strictly positive low price.
    - close: Finite, strictly positive close price.
    - base_volume: Finite, non-negative base asset volume.
    - quote_volume: Finite, non-negative quote asset volume (or None).
    - trade_count: Non-negative integer trade count (or None).

    Ingestion-only metadata (source, fetched_at) and dataset coordinates (exchange_id,
    market_type) are strictly excluded from strategy candle inputs.
    Dataset coordinates remain evaluator-owned in EvaluationContext; only the market
    fields declared in StrategyContext (symbol, timeframe) are strategy-visible.
    """

    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    base_volume: float
    quote_volume: float | None = None
    trade_count: int | None = None

    def __post_init__(self) -> None:
        _validate_utc_datetime(self.open_time, "open_time")
        _validate_utc_datetime(self.close_time, "close_time")
        if self.close_time <= self.open_time:
            raise ValueError(
                f"close_time ({self.close_time.isoformat()}) must be strictly after "
                f"open_time ({self.open_time.isoformat()})."
            )

        for name, val in (
            ("open", self.open),
            ("high", self.high),
            ("low", self.low),
            ("close", self.close),
        ):
            if (
                not isinstance(val, (int, float))
                or isinstance(val, bool)
                or math.isnan(val)
                or math.isinf(val)
                or val <= 0
            ):
                raise ValueError(f"Price field '{name}' must be finite, strictly positive number, got: {val}")

        # Domain bounding invariants
        if (
            self.high < self.low
            or self.high < self.open
            or self.high < self.close
            or self.low > self.open
            or self.low > self.close
        ):
            raise ValueError(
                f"High and low prices must bound open, close, and each other. "
                f"Got open={self.open}, high={self.high}, low={self.low}, close={self.close}"
            )

        if (
            not isinstance(self.base_volume, (int, float))
            or isinstance(self.base_volume, bool)
            or math.isnan(self.base_volume)
            or math.isinf(self.base_volume)
            or self.base_volume < 0
        ):
            raise ValueError(f"base_volume must be finite, non-negative number, got: {self.base_volume}")

        if self.quote_volume is not None:
            if (
                not isinstance(self.quote_volume, (int, float))
                or isinstance(self.quote_volume, bool)
                or math.isnan(self.quote_volume)
                or math.isinf(self.quote_volume)
                or self.quote_volume < 0
            ):
                raise ValueError(f"quote_volume must be finite, non-negative number, got: {self.quote_volume}")

        if self.trade_count is not None:
            if type(self.trade_count) is not int or self.trade_count < 0:
                raise ValueError(f"trade_count must be non-negative integer, got: {self.trade_count}")

    @classmethod
    def from_candle(cls, candle: Candle) -> StrategyCandle:
        """Project an M0 Candle into an immutable StrategyCandle view.

        The candle MUST have is_closed == True prior to projection.
        """
        if not candle.is_closed:
            raise IncompleteCandleError(
                f"Cannot project unclosed candle (open_time={candle.open_time.isoformat()}) into StrategyCandle."
            )
        if candle.close_time is None:
            raise ValueError(f"Cannot project candle without close_time (open_time={candle.open_time.isoformat()}).")
        return cls(
            open_time=candle.open_time,
            close_time=candle.close_time,
            open=candle.open,
            high=candle.high,
            low=candle.low,
            close=candle.close,
            base_volume=candle.base_volume,
            quote_volume=candle.quote_volume,
            trade_count=candle.trade_count,
        )


# -----------------------------------------------------------------------------
# Causal Strategy Context & Evaluator Provenance Context
# -----------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StrategyContext:
    """Causal, point-in-time context supplied to strategy evaluation for a specific candle.

    Contains strictly causal information available up to the close of the evaluated candle:
    - symbol: Canonical symbol string derived from verified market identity.
    - timeframe: SupportedTimeframe of the series.
    - prefix_quality_report: Authoritative DataQualityReport computed strictly over [since_ms, candle_T.close_time).

    Future data, full dataset bounds, future gap events, and full-snapshot fingerprints
    are strictly excluded to guarantee zero future-data leakage.
    """

    symbol: str
    timeframe: SupportedTimeframe
    prefix_quality_report: DataQualityReport

    def __post_init__(self) -> None:
        if not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValueError("symbol must be a non-empty string.")
        if not isinstance(self.timeframe, SupportedTimeframe):
            raise TypeError(f"timeframe must be an instance of SupportedTimeframe, got {type(self.timeframe).__name__}")
        if not isinstance(self.prefix_quality_report, DataQualityReport):
            raise TypeError(
                "prefix_quality_report must be an instance of DataQualityReport, "
                f"got {type(self.prefix_quality_report).__name__}"
            )


@dataclass(frozen=True, slots=True)
class EvaluationContext:
    """Authoritative full-dataset provenance and verification metadata owned by StrategyEvaluator.

    Provenance Source:
    `content_fingerprint` MUST originate from the selected committed M1 snapshot record
    (`CatalogDatasetRecord.content_fingerprint_sha256`) in the DuckDB snapshot catalog.
    The caller selects the authoritative committed snapshot and uses its recorded `dataset_identity`,
    `content_fingerprint_sha256`, market coordinates, timeframe, requested bounds, and record metadata
    to construct this context.

    Independent Verification Guarantee:
    Stage A independently recomputes the canonical content fingerprint from the supplied candles
    and compares it against this catalog-sourced `content_fingerprint`. The reference fingerprint
    MUST NOT be calculated from the same supplied candle sequence and treated as independent provenance.

    Hash Distinction:
    The canonical content fingerprint (`OQICCF` stream digest) is strictly distinguished from the
    physical Parquet `file_sha256`.

    Database access remains strictly external to strategies. This context is strictly held by
    StrategyEvaluator and NEVER passed to strategy instances.
    """

    market: MarketIdentity
    timeframe: SupportedTimeframe
    requested_since_ms: int
    requested_until_ms: int
    content_fingerprint: str
    quality_report: DataQualityReport
    snapshot_id: str = ""
    file_sha256: str | None = None
    dataset_identity: str = ""
    _provenance_token: object = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        # 0. Catalog provenance enforcement
        if self._provenance_token is not _CATALOG_PROVENANCE_TOKEN:
            raise SnapshotConsistencyError(
                "EvaluationContext must be constructed from an authoritative committed "
                "CatalogDatasetRecord via EvaluationContext.from_catalog_record()."
            )
        if not isinstance(self.snapshot_id, str) or not self.snapshot_id.strip():
            raise SnapshotConsistencyError("snapshot_id cannot be empty.")

        # 1. MarketIdentity validation
        if not isinstance(self.market, MarketIdentity):
            raise TypeError(f"market must be an instance of MarketIdentity, got {type(self.market).__name__}")
        if self.market.market_type != "spot":
            raise ValueError(f"Only spot markets are supported in OQI V2, got: '{self.market.market_type}'")
        if not self.market.exchange_id.strip():
            raise ValueError("market.exchange_id cannot be empty.")
        if not self.market.symbol.strip():
            raise ValueError("market.symbol cannot be empty.")

        # 2. Exact integer timestamp type validation (reject bool and float)
        if type(self.requested_since_ms) is not int or type(self.requested_since_ms) is bool:
            raise TypeError(f"requested_since_ms must be exact int, got {type(self.requested_since_ms).__name__}")
        if type(self.requested_until_ms) is not int or type(self.requested_until_ms) is bool:
            raise TypeError(f"requested_until_ms must be exact int, got {type(self.requested_until_ms).__name__}")

        # 3. Runtime type validation for timeframe
        if not isinstance(self.timeframe, SupportedTimeframe):
            raise TypeError(f"timeframe must be an instance of SupportedTimeframe, got {type(self.timeframe).__name__}")

        # 4. Timeframe-grid-aligned boundaries using timeframe_to_timedelta()
        step_ms = int(timeframe_to_timedelta(self.timeframe).total_seconds() * 1000)
        if self.requested_since_ms % step_ms != 0:
            raise ValueError(
                f"requested_since_ms ({self.requested_since_ms}) is not grid-aligned to "
                f"{self.timeframe.value} ({step_ms}ms)."
            )
        if self.requested_until_ms % step_ms != 0:
            raise ValueError(
                f"requested_until_ms ({self.requested_until_ms}) is not grid-aligned to "
                f"{self.timeframe.value} ({step_ms}ms)."
            )
        if self.requested_until_ms <= self.requested_since_ms:
            raise ValueError(
                f"requested_until_ms ({self.requested_until_ms}) must be strictly greater "
                f"than requested_since_ms ({self.requested_since_ms})."
            )

        # 5. Dataset identity validation & canonical derivation
        expected_dataset_identity = (
            f"{self.market.exchange_id}:{self.market.market_type}:{self.market.symbol}:"
            f"{self.timeframe.value}:{self.requested_since_ms}:{self.requested_until_ms}"
        )
        if not self.dataset_identity.strip():
            object.__setattr__(self, "dataset_identity", expected_dataset_identity)
        elif self.dataset_identity != expected_dataset_identity:
            raise ValueError(
                f"Contradictory dataset_identity: provided '{self.dataset_identity}', "
                f"expected '{expected_dataset_identity}' based on market, timeframe, and bounds."
            )

        # 6. Full-string regex validation for content fingerprint
        if not re.fullmatch(r"^sha256:[0-9a-f]{64}$", self.content_fingerprint):
            raise ValueError(
                f"Invalid content_fingerprint format: '{self.content_fingerprint}'. "
                "Must match ^sha256:[0-9a-f]{64}$ exactly."
            )

        # 7. Quality report consistency
        if not isinstance(self.quality_report, DataQualityReport):
            raise TypeError(f"quality_report must be DataQualityReport, got {type(self.quality_report).__name__}")
        if self.quality_report.requested_since_ms != self.requested_since_ms:
            raise ValueError(
                f"Quality report requested_since_ms ({self.quality_report.requested_since_ms}) "
                f"does not match context requested_since_ms ({self.requested_since_ms})."
            )
        if self.quality_report.requested_until_ms != self.requested_until_ms:
            raise ValueError(
                f"Quality report requested_until_ms ({self.quality_report.requested_until_ms}) "
                f"does not match context requested_until_ms ({self.requested_until_ms})."
            )
        if self.quality_report.timeframe != self.timeframe:
            raise ValueError(
                f"Quality report timeframe ({self.quality_report.timeframe}) does not match "
                f"context timeframe ({self.timeframe})."
            )

        # 8. Quality report internal mathematical invariants
        if self.quality_report.gap_count != len(self.quality_report.gaps):
            raise ValueError(
                f"Quality report gap_count ({self.quality_report.gap_count}) does not match "
                f"gaps length ({len(self.quality_report.gaps)})."
            )
        if self.quality_report.missing_intervals < 0:
            raise ValueError(
                f"Quality report missing_intervals cannot be negative: {self.quality_report.missing_intervals}"
            )
        if self.quality_report.expected_records != (
            self.quality_report.record_count + self.quality_report.missing_intervals
        ):
            raise ValueError("Quality report expected_records != record_count + missing_intervals.")

        # 9. Missing count sum invariant
        total_gap_missing = sum(gap.missing_count for gap in self.quality_report.gaps)
        if self.quality_report.missing_intervals != total_gap_missing:
            raise ValueError(
                f"Quality report missing_intervals ({self.quality_report.missing_intervals}) does not "
                f"match sum of gap missing_counts ({total_gap_missing})."
            )

    @classmethod
    def from_catalog_record(
        cls,
        record: CatalogDatasetRecord,
        quality_report: DataQualityReport,
    ) -> EvaluationContext:
        """Construct an authoritative EvaluationContext from a committed M1 CatalogDatasetRecord."""
        from open_quant_intelligence.storage.catalog import CatalogDatasetRecord as _CatalogRecord

        if not isinstance(record, _CatalogRecord):
            raise TypeError(f"record must be an instance of CatalogDatasetRecord, got {type(record).__name__}")
        if record.ingestion_status != "COMMITTED":
            raise SnapshotConsistencyError(
                f"Catalog snapshot '{record.snapshot_id}' is not committed "
                f"(ingestion_status='{record.ingestion_status}')."
            )
        if not isinstance(record.snapshot_id, str) or not record.snapshot_id.strip():
            raise SnapshotConsistencyError("CatalogDatasetRecord.snapshot_id cannot be empty.")

        _validate_utc_datetime(
            req_since := (
                record.requested_since.astimezone(timezone.utc)
                if isinstance(record.requested_since, datetime) and record.requested_since.tzinfo is None
                else record.requested_since
            ),
            "record.requested_since",
        )
        _validate_utc_datetime(
            req_until := (
                record.requested_until.astimezone(timezone.utc)
                if isinstance(record.requested_until, datetime) and record.requested_until.tzinfo is None
                else record.requested_until
            ),
            "record.requested_until",
        )

        market = MarketIdentity(
            exchange_id=record.exchange_id,
            market_type=record.market_type,
            symbol=record.symbol,
        )
        timeframe = parse_timeframe(record.timeframe)
        requested_since_ms = int(req_since.timestamp() * 1000)
        requested_until_ms = int(req_until.timestamp() * 1000)

        if not isinstance(quality_report, DataQualityReport):
            raise TypeError(f"quality_report must be DataQualityReport, got {type(quality_report).__name__}")
        if quality_report.expected_records != record.expected_records:
            raise SnapshotConsistencyError(
                f"Quality report expected_records ({quality_report.expected_records}) does not match "
                f"catalog record ({record.expected_records})."
            )
        if quality_report.record_count != record.record_count:
            raise SnapshotConsistencyError(
                f"Quality report record_count ({quality_report.record_count}) does not match "
                f"catalog record ({record.record_count})."
            )
        if quality_report.gap_count != record.gap_count:
            raise SnapshotConsistencyError(
                f"Quality report gap_count ({quality_report.gap_count}) does not match "
                f"catalog record ({record.gap_count})."
            )
        if quality_report.missing_intervals != record.missing_intervals:
            raise SnapshotConsistencyError(
                f"Quality report missing_intervals ({quality_report.missing_intervals}) does not match "
                f"catalog record ({record.missing_intervals})."
            )
        if quality_report.quality_status != record.quality_status:
            raise SnapshotConsistencyError(
                f"Quality report quality_status ('{quality_report.quality_status}') does not match "
                f"catalog record ('{record.quality_status}')."
            )

        return cls(
            market=market,
            timeframe=timeframe,
            requested_since_ms=requested_since_ms,
            requested_until_ms=requested_until_ms,
            content_fingerprint=record.content_fingerprint_sha256,
            quality_report=quality_report,
            snapshot_id=record.snapshot_id,
            file_sha256=record.file_sha256,
            dataset_identity=record.dataset_identity,
            _provenance_token=_CATALOG_PROVENANCE_TOKEN,
        )


@dataclass(frozen=True, slots=True)
class VerifiedSnapshot:
    """Immutable verification proof issued exclusively by StrategyEvaluator.verify_snapshot()."""

    context: EvaluationContext
    candle_count: int
    _token: object = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._token is not _VERIFIED_SNAPSHOT_TOKEN:
            raise SnapshotConsistencyError(
                "VerifiedSnapshot cannot be constructed directly; "
                "it must be obtained from StrategyEvaluator.verify_snapshot()."
            )
        if not isinstance(self.context, EvaluationContext):
            raise TypeError(f"context must be EvaluationContext, got {type(self.context).__name__}")
        if type(self.candle_count) is not int or self.candle_count <= 0:
            raise ValueError(f"candle_count must be positive int, got {self.candle_count}")


def _issue_verified_snapshot(context: EvaluationContext, candle_count: int) -> VerifiedSnapshot:
    """Internal factory for StrategyEvaluator to issue a VerifiedSnapshot."""
    return VerifiedSnapshot(
        context=context,
        candle_count=candle_count,
        _token=_VERIFIED_SNAPSHOT_TOKEN,
    )


# -----------------------------------------------------------------------------
# Strategy Configuration & Fingerprinting
# -----------------------------------------------------------------------------


def _validate_config_value(val: Any) -> None:
    """Validate that configuration values are restricted to immutable primitive scalars or tuples."""
    if type(val) is bool:
        return
    if type(val) is int:
        return
    if type(val) is str:
        return
    if type(val) is float:
        if math.isnan(val) or math.isinf(val):
            raise ValueError(f"Non-finite float value not permitted in configuration: {val}")
        return
    if type(val) is tuple:
        for item in val:
            _validate_config_value(item)
        return
    raise TypeError(
        f"Unsupported configuration value type: {type(val).__name__}. "
        "Only primitive scalars (int, float, str, bool) and immutable tuples thereof are permitted. "
        "Lists, dictionaries, sets, and mutable objects are strictly prohibited."
    )


@dataclass(frozen=True)
class StrategyConfig:
    """Base class for deeply immutable strategy configuration parameters."""

    def __post_init__(self) -> None:
        for _, v in asdict(self).items():
            _validate_config_value(v)

    def compute_fingerprint(self) -> str:
        """Compute canonical SHA-256 fingerprint of configuration attributes."""
        raw = asdict(self)
        payload = json.dumps(
            raw,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        return f"sha256:{hashlib.sha256(payload).hexdigest()}"


# -----------------------------------------------------------------------------
# Gap Policy Specification
# -----------------------------------------------------------------------------


class GapPolicy(str, Enum):
    """Explicit policy defining how a strategy handles historical missing intervals.

    Unaligned candle timestamps are ALWAYS invalid and rejected prior to policy evaluation.
    Missing intervals are NEVER silently patched, forward-filled, or interpolated.
    """

    STRICT_GAP_FREE = "STRICT_GAP_FREE"
    """Strict initial policy: Reject evaluation if ANY gap event exists in the evaluated prefix."""

    REJECT_INTERNAL_GAPS = "REJECT_INTERNAL_GAPS"
    """Reject evaluation if any INTERNAL gap exists between candles in the evaluation prefix.
    Leading gaps outside the evaluated prefix boundaries are permitted."""


# -----------------------------------------------------------------------------
# The Strategy Protocol
# -----------------------------------------------------------------------------


@runtime_checkable
class Strategy(Protocol):
    """Strict, allowlisted strategy protocol for deterministic research evaluation."""

    @property
    def strategy_id(self) -> str:
        """Stable, unique machine-readable identifier for the strategy."""
        ...

    @property
    def version(self) -> str:
        """Semantic version string of the strategy implementation (e.g. '1.0.0')."""
        ...

    @property
    def config(self) -> StrategyConfig:
        """Immutable configuration parameters."""
        ...

    @property
    def minimum_history_candles(self) -> int:
        """Strictly positive integer defining the minimum completed candles required to evaluate."""
        ...

    @property
    def gap_policy(self) -> GapPolicy:
        """Declared gap tolerance policy."""
        ...

    def evaluate(
        self,
        history: Sequence[StrategyCandle],
        context: StrategyContext,
    ) -> StrategyOutput:
        """Evaluate historical prefix ending at history[-1] and return a deterministic StrategyOutput."""
        ...
