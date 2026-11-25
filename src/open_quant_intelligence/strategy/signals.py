"""Signal semantics, strategy outputs, and auditable decision records.

This module defines directional research signals, pure strategy outputs, and
evaluator-assembled auditable decision records that bind causal evaluation results
to immutable snapshot provenance.
"""

from __future__ import annotations

import math
import re
import types
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Mapping

from open_quant_intelligence.domain.timeframe import SupportedTimeframe
from open_quant_intelligence.strategy.contracts import (
    _VERIFIED_SNAPSHOT_TOKEN,
    SnapshotConsistencyError,
    StrategyOutputMismatchError,
    VerifiedSnapshot,
    _validate_utc_datetime,
)

_VALIDATED_OUTPUT_TOKEN = object()
_VERIFIED_DECISION_TOKEN = object()

# -----------------------------------------------------------------------------
# Research Signal & Reason Codes
# -----------------------------------------------------------------------------


class ResearchSignal(str, Enum):
    """Directional research signal emitted by a strategy for the latest completed candle.

    These express quantitative intent, NOT exchange orders:
    - LONG: Target state is exposure to the underlying base asset.
    - FLAT: Target state is zero exposure (100% quote currency / cash).
    - HOLD: Maintain existing stance without initiating a state transition.
    """

    LONG = "LONG"
    FLAT = "FLAT"
    HOLD = "HOLD"


class ReasonCode:
    """Standard reason codes emitted by strategy evaluations."""

    WARMUP_INSUFFICIENT = "WARMUP_INSUFFICIENT"
    LONG_SIGNAL_GENERATED = "LONG_SIGNAL_GENERATED"
    FLAT_SIGNAL_GENERATED = "FLAT_SIGNAL_GENERATED"
    NO_CONDITION_MET = "NO_CONDITION_MET"


# Permitted primitive scalar types for decision metadata
MetadataScalar = int | float | str | bool


def _validate_metadata(metadata: Mapping[str, Any]) -> types.MappingProxyType:
    """Validate metadata mapping keys and values, returning an immutable MappingProxyType."""
    if not isinstance(metadata, Mapping):
        raise TypeError(f"metadata must be a mapping, got {type(metadata).__name__}")
    for k, v in metadata.items():
        if not isinstance(k, str):
            raise TypeError(f"Metadata key must be str, got {type(k).__name__}: {k!r}")
        # Exact allowed scalar types: int, float, str, bool
        if type(v) not in (int, float, str, bool):
            raise TypeError(
                f"Metadata value for key '{k}' must be primitive scalar (int, float, str, bool), "
                f"got {type(v).__name__}: {v!r}"
            )
        # Finite float check
        if type(v) is float and (math.isnan(v) or math.isinf(v)):
            raise ValueError(f"Metadata float value for key '{k}' must be finite, got: {v}")
    return types.MappingProxyType(dict(metadata))


# -----------------------------------------------------------------------------
# Strategy Output
# -----------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StrategyOutput:
    """Pure, causal research output emitted by Strategy.evaluate().

    Contains strictly the strategy's calculated signal, reason code, and indicator metadata
    for the latest completed candle.
    """

    strategy_id: str
    strategy_version: str
    config_fingerprint: str
    symbol: str
    timeframe: SupportedTimeframe
    source_candle_open_time: datetime
    source_candle_close_time: datetime
    decision_time: datetime
    signal: ResearchSignal
    reason_code: str
    metadata: Mapping[str, MetadataScalar]

    def __post_init__(self) -> None:
        # Timezone-aware UTC timestamp validation
        _validate_utc_datetime(self.source_candle_open_time, "source_candle_open_time")
        _validate_utc_datetime(self.source_candle_close_time, "source_candle_close_time")
        _validate_utc_datetime(self.decision_time, "decision_time")

        # Strict Point-in-Time Causality Check
        if self.decision_time != self.source_candle_close_time:
            raise ValueError(
                f"Causality violation: decision_time ({self.decision_time.isoformat()}) "
                f"must strictly equal source_candle_close_time ({self.source_candle_close_time.isoformat()})."
            )
        if self.source_candle_close_time <= self.source_candle_open_time:
            raise ValueError("source_candle_close_time must be strictly after source_candle_open_time.")
        if not self.strategy_id.strip():
            raise ValueError("strategy_id cannot be empty.")
        if not self.reason_code.strip():
            raise ValueError("reason_code cannot be empty.")
        if not re.fullmatch(r"^sha256:[0-9a-f]{64}$", self.config_fingerprint):
            raise ValueError(f"Invalid config_fingerprint format: '{self.config_fingerprint}'.")
        if not isinstance(self.timeframe, SupportedTimeframe):
            raise TypeError(f"timeframe must be an instance of SupportedTimeframe, got {type(self.timeframe).__name__}")
        if not isinstance(self.signal, ResearchSignal):
            raise TypeError(f"signal must be an instance of ResearchSignal, got {type(self.signal).__name__}")

        # Enforce Deep Immutability and wrap in MappingProxyType
        object.__setattr__(self, "metadata", _validate_metadata(self.metadata))


# -----------------------------------------------------------------------------
# Auditable Strategy Decision Record
# -----------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ValidatedStrategyOutput:
    """Evaluator-validated StrategyOutput bound to a specific VerifiedSnapshot and target candle.

    Uses a module-internal construction guard so it can only be issued after
    Stage A snapshot verification and Stage B output validation.
    """

    verified_snapshot: VerifiedSnapshot
    target_index: int
    output: StrategyOutput
    _token: object = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._token is not _VALIDATED_OUTPUT_TOKEN:
            raise SnapshotConsistencyError(
                "ValidatedStrategyOutput cannot be constructed directly; "
                "it must be produced by StrategyEvaluator after output validation."
            )
        if not isinstance(self.verified_snapshot, VerifiedSnapshot) or (
            self.verified_snapshot._token is not _VERIFIED_SNAPSHOT_TOKEN
        ):
            raise SnapshotConsistencyError("ValidatedStrategyOutput requires a VerifiedSnapshot.")
        if not isinstance(self.output, StrategyOutput):
            raise TypeError(f"output must be StrategyOutput, got {type(self.output).__name__}")
        if type(self.target_index) is not int or not (0 <= self.target_index < len(self.verified_snapshot.candles)):
            raise IndexError(
                f"target_index ({self.target_index}) out of range [0, {len(self.verified_snapshot.candles)})."
            )

        target_candle = self.verified_snapshot.candles[self.target_index]
        ctx = self.verified_snapshot.context
        if self.output.symbol != ctx.market.symbol:
            raise StrategyOutputMismatchError(
                f"StrategyOutput symbol '{self.output.symbol}' does not match '{ctx.market.symbol}'."
            )
        if self.output.timeframe != ctx.timeframe:
            raise StrategyOutputMismatchError(
                f"StrategyOutput timeframe '{self.output.timeframe.value}' does not match '{ctx.timeframe.value}'."
            )
        if self.output.source_candle_open_time != target_candle.open_time:
            raise StrategyOutputMismatchError(
                f"StrategyOutput source_candle_open_time ({self.output.source_candle_open_time.isoformat()}) "
                f"does not match target candle open_time ({target_candle.open_time.isoformat()})."
            )
        if self.output.source_candle_close_time != target_candle.close_time:
            raise StrategyOutputMismatchError(
                f"StrategyOutput source_candle_close_time ({self.output.source_candle_close_time.isoformat()}) "
                f"does not match target candle close_time ({target_candle.close_time.isoformat()})."
            )
        if self.output.decision_time != target_candle.close_time:
            raise StrategyOutputMismatchError(
                f"StrategyOutput decision_time ({self.output.decision_time.isoformat()}) "
                f"does not match target candle close_time ({target_candle.close_time.isoformat()})."
            )


def _issue_validated_output(
    verified_snapshot: VerifiedSnapshot,
    target_index: int,
    output: StrategyOutput,
) -> ValidatedStrategyOutput:
    """Internal factory for StrategyEvaluator to bind validated output to a VerifiedSnapshot."""
    return ValidatedStrategyOutput(
        verified_snapshot=verified_snapshot,
        target_index=target_index,
        output=output,
        _token=_VALIDATED_OUTPUT_TOKEN,
    )


@dataclass(frozen=True, slots=True)
class StrategyDecision:
    """Immutable, fully auditable record of a strategy evaluation assembled by StrategyEvaluator.

    Combines the strategy's causal output with the verified M1 dataset snapshot provenance.
    Embeds StrategyOutput directly to prevent field divergence or redundant duplication.
    """

    # Evaluator-Attached Snapshot Provenance
    dataset_identity: str
    dataset_content_fingerprint: str
    snapshot_id: str

    # Embedded Verified Strategy Output
    output: StrategyOutput
    _decision_token: object = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._decision_token is not _VERIFIED_DECISION_TOKEN:
            raise SnapshotConsistencyError(
                "StrategyDecision cannot be constructed directly; "
                "it must be assembled by StrategyEvaluator from a ValidatedStrategyOutput."
            )
        if not isinstance(self.output, StrategyOutput):
            raise TypeError(f"output must be an instance of StrategyOutput, got {type(self.output).__name__}")
        if not self.dataset_identity.strip():
            raise ValueError("dataset_identity cannot be empty.")
        if not self.snapshot_id.strip():
            raise ValueError("snapshot_id cannot be empty.")
        if not re.fullmatch(r"^sha256:[0-9a-f]{64}$", self.dataset_content_fingerprint):
            raise ValueError(f"Invalid dataset_content_fingerprint format: '{self.dataset_content_fingerprint}'.")

    @classmethod
    def _assemble_verified(cls, validated_output: ValidatedStrategyOutput) -> StrategyDecision:
        """Internal assembly path requiring an evaluator-validated ValidatedStrategyOutput."""
        if not isinstance(validated_output, ValidatedStrategyOutput) or (
            validated_output._token is not _VALIDATED_OUTPUT_TOKEN
        ):
            raise SnapshotConsistencyError("StrategyDecision._assemble_verified() requires a ValidatedStrategyOutput.")
        ctx = validated_output.verified_snapshot.context
        return cls(
            dataset_identity=ctx.dataset_identity,
            dataset_content_fingerprint=ctx.content_fingerprint,
            snapshot_id=ctx.snapshot_id,
            output=validated_output.output,
            _decision_token=_VERIFIED_DECISION_TOKEN,
        )

    # Delegating properties for clean consumer ergonomics
    @property
    def strategy_id(self) -> str:
        return self.output.strategy_id

    @property
    def strategy_version(self) -> str:
        return self.output.strategy_version

    @property
    def config_fingerprint(self) -> str:
        return self.output.config_fingerprint

    @property
    def symbol(self) -> str:
        return self.output.symbol

    @property
    def timeframe(self) -> SupportedTimeframe:
        return self.output.timeframe

    @property
    def source_candle_open_time(self) -> datetime:
        return self.output.source_candle_open_time

    @property
    def source_candle_close_time(self) -> datetime:
        return self.output.source_candle_close_time

    @property
    def decision_time(self) -> datetime:
        return self.output.decision_time

    @property
    def signal(self) -> ResearchSignal:
        return self.output.signal

    @property
    def reason_code(self) -> str:
        return self.output.reason_code

    @property
    def metadata(self) -> Mapping[str, MetadataScalar]:
        return self.output.metadata
