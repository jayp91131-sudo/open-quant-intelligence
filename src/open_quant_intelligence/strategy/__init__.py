"""Strategy module for Open Quant Intelligence V2.

Provides the foundational research strategy interfaces, immutable domain contracts,
point-in-time signal semantics, and auditable two-stage evaluator.
"""

from open_quant_intelligence.strategy.contracts import (
    ChronologicalOrderError,
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
)
from open_quant_intelligence.strategy.evaluator import StrategyEvaluator
from open_quant_intelligence.strategy.signals import (
    ReasonCode,
    ResearchSignal,
    StrategyDecision,
    StrategyOutput,
)

__all__ = [
    # Protocols and Configuration
    "Strategy",
    "StrategyConfig",
    "StrategyCandle",
    "StrategyContext",
    "EvaluationContext",
    "GapPolicy",
    # Signals and Outputs
    "ResearchSignal",
    "ReasonCode",
    "StrategyOutput",
    "StrategyDecision",
    # Evaluator
    "StrategyEvaluator",
    # Structured Exceptions
    "StrategyEvaluationError",
    "EmptyDatasetError",
    "InsufficientHistoryError",
    "IncompleteCandleError",
    "HeterogeneousSeriesError",
    "ChronologicalOrderError",
    "StrategyDataGapError",
    "SnapshotConsistencyError",
    "StrategyOutputMismatchError",
]
