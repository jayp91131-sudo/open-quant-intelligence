"""StrategyEvaluator implementation enforcing two-stage verification and execution.

Stage A: Authoritative M1 snapshot verification against DuckDB catalog provenance
and canonical content fingerprint recomputation.
Stage B: Point-in-time causal prefix evaluation with isolated StrategyCandle views,
prefix-only data quality gap policy enforcement, and auditable decision assembly.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Sequence

from open_quant_intelligence.data.quality import GapType, analyze_dataset_quality
from open_quant_intelligence.data.validation import validate_candles
from open_quant_intelligence.domain.candle import Candle
from open_quant_intelligence.storage.fingerprint import compute_content_fingerprint
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
    StrategyContext,
    StrategyDataGapError,
    StrategyEvaluationError,
    StrategyOutputMismatchError,
    VerifiedSnapshot,
    _issue_verified_snapshot,
)
from open_quant_intelligence.strategy.signals import StrategyDecision, StrategyOutput


class StrategyEvaluator:
    """Evaluates quantitative strategies across verified historical snapshots."""

    @classmethod
    def verify_snapshot(
        cls,
        supplied_candles: Sequence[Candle],
        context: EvaluationContext,
    ) -> VerifiedSnapshot:
        """Stage A: Verify supplied candles against authoritative catalog provenance.

        Invariants:
        1. Reject empty candle sequences immediately with EmptyDatasetError without indexing.
        2. Context validation is enforced via EvaluationContext invariants.
        3. Validate candles using M0 validate_candles() public API.
        4. Tie candles to M1 metadata (count, bounds, market identity, quality report).
        5. Independently recompute canonical content fingerprint and assert equality
           with context.content_fingerprint (originating from committed M1 catalog record).
        """
        # Step A1: Empty Sequence Check
        if not supplied_candles:
            raise EmptyDatasetError("Supplied candle sequence is empty.")

        # Step A2: Comprehensive EvaluationContext Consistency Check
        if not isinstance(context, EvaluationContext):
            raise TypeError(f"context must be an instance of EvaluationContext, got {type(context).__name__}")

        # Step A3: M0 Validation API Contract Check
        result = validate_candles(
            candles=supplied_candles,
            fail_on_gaps=False,
            expected_timeframe=context.timeframe,
        )
        if not result.is_valid:
            error_details = "; ".join(f"[{err.issue_type}] {err.message}" for err in result.errors)
            issue_types = {err.issue_type for err in result.errors}
            if issue_types & {
                "MIXED_EXCHANGES",
                "MIXED_SYMBOLS",
                "MIXED_TIMEFRAMES",
                "TIMEFRAME_MISMATCH",
            }:
                raise HeterogeneousSeriesError(
                    f"M0 candle validation failed with heterogeneous series error: {error_details}"
                )
            raise StrategyEvaluationError(f"M0 candle validation failed on supplied dataset: {error_details}")

        # Step A4: Tie Supplied Candles to M1 Snapshot Metadata
        if len(supplied_candles) != context.quality_report.record_count:
            raise SnapshotConsistencyError(
                f"Supplied candle count ({len(supplied_candles)}) does not match "
                f"quality report record_count ({context.quality_report.record_count})."
            )

        expected_since_dt = datetime.fromtimestamp(context.requested_since_ms / 1000, tz=timezone.utc)
        expected_until_dt = datetime.fromtimestamp(context.requested_until_ms / 1000, tz=timezone.utc)

        if supplied_candles[0].open_time < expected_since_dt:
            raise SnapshotConsistencyError(
                f"First candle open_time ({supplied_candles[0].open_time.isoformat()}) "
                f"is earlier than requested_since ({expected_since_dt.isoformat()})."
            )
        if supplied_candles[-1].close_time is None or supplied_candles[-1].close_time > expected_until_dt:
            actual_close_str = (
                supplied_candles[-1].close_time.isoformat() if supplied_candles[-1].close_time else "None"
            )
            raise SnapshotConsistencyError(
                f"Last candle close_time ({actual_close_str}) "
                f"is later than requested_until ({expected_until_dt.isoformat()})."
            )

        # Homogeneity checks
        for idx, c in enumerate(supplied_candles):
            if c.exchange_id != context.market.exchange_id:
                raise HeterogeneousSeriesError(
                    f"Candle at row {idx} exchange_id '{c.exchange_id}' does not match "
                    f"context market exchange_id '{context.market.exchange_id}'."
                )
            if c.market_type != "spot" or c.market_type != context.market.market_type:
                raise HeterogeneousSeriesError(
                    f"Candle at row {idx} market_type '{c.market_type}' does not match "
                    f"context market_type '{context.market.market_type}'."
                )
            if c.symbol != context.market.symbol:
                raise HeterogeneousSeriesError(
                    f"Candle at row {idx} symbol '{c.symbol}' does not match "
                    f"context market symbol '{context.market.symbol}'."
                )
            if c.timeframe != context.timeframe.value:
                raise HeterogeneousSeriesError(
                    f"Candle at row {idx} timeframe '{c.timeframe}' does not match "
                    f"context timeframe '{context.timeframe.value}'."
                )

        # Full Quality Re-check
        actual_quality = analyze_dataset_quality(
            candles=supplied_candles,
            since_ms=context.requested_since_ms,
            until_ms=context.requested_until_ms,
            timeframe=context.timeframe,
        )
        if (
            actual_quality.expected_records != context.quality_report.expected_records
            or actual_quality.record_count != context.quality_report.record_count
            or actual_quality.missing_intervals != context.quality_report.missing_intervals
            or actual_quality.gap_count != context.quality_report.gap_count
            or actual_quality.quality_status != context.quality_report.quality_status
            or actual_quality.gaps != context.quality_report.gaps
        ):
            raise SnapshotConsistencyError(
                "Supplied candle quality analysis differs from authoritative context quality_report."
            )

        # Step A5: Independent Recomputation of Canonical Content Fingerprint
        recomputed_fp = compute_content_fingerprint(
            candles=supplied_candles,
            exchange_id=context.market.exchange_id,
            market_type=context.market.market_type,
            symbol=context.market.symbol,
            timeframe=context.timeframe,
            requested_since_ms=context.requested_since_ms,
            requested_until_ms=context.requested_until_ms,
        )
        if recomputed_fp != context.content_fingerprint:
            raise SnapshotConsistencyError(
                f"Recomputed content fingerprint '{recomputed_fp}' does not match "
                f"authoritative catalog snapshot fingerprint '{context.content_fingerprint}'."
            )

        return _issue_verified_snapshot(
            context=context,
            candle_count=len(supplied_candles),
        )

    @classmethod
    def _evaluate_at_verified(
        cls,
        supplied_candles: Sequence[Candle],
        target_index: int,
        verified_snapshot: VerifiedSnapshot,
        strategy: Strategy,
    ) -> StrategyDecision:
        """Internal Stage B evaluator requiring a VerifiedSnapshot from Stage A."""
        if not isinstance(verified_snapshot, VerifiedSnapshot):
            raise SnapshotConsistencyError(
                "Stage B evaluation requires a VerifiedSnapshot from StrategyEvaluator.verify_snapshot()."
            )
        if len(supplied_candles) != verified_snapshot.candle_count:
            raise SnapshotConsistencyError(
                f"Supplied candle count ({len(supplied_candles)}) does not match "
                f"verified snapshot count ({verified_snapshot.candle_count})."
            )
        context = verified_snapshot.context
        if not (0 <= target_index < len(supplied_candles)):
            raise IndexError(f"target_index ({target_index}) out of range [0, {len(supplied_candles)}).")

        # Step B1: Construct Historical Prefix & Lookback Check
        prefix_history = supplied_candles[: target_index + 1]
        if len(prefix_history) < strategy.minimum_history_candles:
            raise InsufficientHistoryError(
                f"Available history count ({len(prefix_history)}) is strictly less than "
                f"strategy minimum ({strategy.minimum_history_candles})."
            )

        # Step B2: Verify Prefix Completeness (is_closed)
        for idx, c in enumerate(prefix_history):
            if not c.is_closed:
                raise IncompleteCandleError(
                    f"Prefix candle at row {idx} (open_time={c.open_time.isoformat()}) is incomplete (is_closed=False)."
                )
            if c.close_time is None:
                raise IncompleteCandleError(
                    f"Prefix candle at row {idx} (open_time={c.open_time.isoformat()}) missing close_time."
                )

        # Step B3: Derive Causal Prefix Quality Report
        target_candle = prefix_history[-1]
        prefix_until_ms = int(target_candle.close_time.timestamp() * 1000)
        prefix_quality = analyze_dataset_quality(
            candles=prefix_history,
            since_ms=context.requested_since_ms,
            until_ms=prefix_until_ms,
            timeframe=context.timeframe,
        )

        # Step B4: Enforce Strategy Gap Policy on Prefix Only
        if strategy.gap_policy == GapPolicy.STRICT_GAP_FREE:
            if prefix_quality.gap_count > 0:
                raise StrategyDataGapError(
                    f"Strategy gap policy STRICT_GAP_FREE violated: prefix contains "
                    f"{prefix_quality.gap_count} gap event(s) with "
                    f"{prefix_quality.missing_intervals} missing interval(s)."
                )
        elif strategy.gap_policy == GapPolicy.REJECT_INTERNAL_GAPS:
            internal_gaps = [g for g in prefix_quality.gaps if g.gap_type == GapType.INTERNAL]
            if internal_gaps:
                total_missing = sum(g.missing_count for g in internal_gaps)
                raise StrategyDataGapError(
                    f"Strategy gap policy REJECT_INTERNAL_GAPS violated: prefix contains "
                    f"{len(internal_gaps)} internal gap event(s) with {total_missing} missing interval(s)."
                )

        # Step B5: Project M0 Candle Prefix into Strategy-Facing Views
        strategy_candles = tuple(StrategyCandle.from_candle(c) for c in prefix_history)

        # Step B6: Pure Strategy Invocation
        strategy_context = StrategyContext(
            symbol=context.market.symbol,
            timeframe=context.timeframe,
            prefix_quality_report=prefix_quality,
        )
        output = strategy.evaluate(strategy_candles, strategy_context)

        # Step B7: Validate Emitted StrategyOutput against Strategy & Target Candle
        if not isinstance(output, StrategyOutput):
            raise StrategyOutputMismatchError(
                f"Strategy evaluate() must return StrategyOutput, got {type(output).__name__}"
            )
        if output.strategy_id != strategy.strategy_id:
            raise StrategyOutputMismatchError(
                f"StrategyOutput strategy_id '{output.strategy_id}' does not match "
                f"evaluator strategy '{strategy.strategy_id}'."
            )
        if output.strategy_version != strategy.version:
            raise StrategyOutputMismatchError(
                f"StrategyOutput strategy_version '{output.strategy_version}' does not match "
                f"evaluator strategy '{strategy.version}'."
            )
        expected_config_fp = strategy.config.compute_fingerprint()
        if output.config_fingerprint != expected_config_fp:
            raise StrategyOutputMismatchError(
                f"StrategyOutput config_fingerprint '{output.config_fingerprint}' does not match "
                f"evaluator strategy config fingerprint '{expected_config_fp}'."
            )
        if output.symbol != context.market.symbol:
            raise StrategyOutputMismatchError(
                f"StrategyOutput symbol '{output.symbol}' does not match "
                f"dataset context symbol '{context.market.symbol}'."
            )
        if output.timeframe != context.timeframe:
            raise StrategyOutputMismatchError(
                f"StrategyOutput timeframe '{output.timeframe.value}' does not match "
                f"dataset context timeframe '{context.timeframe.value}'."
            )
        if output.source_candle_open_time != target_candle.open_time:
            raise StrategyOutputMismatchError(
                f"StrategyOutput source_candle_open_time ({output.source_candle_open_time.isoformat()}) "
                f"does not match target candle open_time ({target_candle.open_time.isoformat()})."
            )
        if output.source_candle_close_time != target_candle.close_time:
            raise StrategyOutputMismatchError(
                f"StrategyOutput source_candle_close_time ({output.source_candle_close_time.isoformat()}) "
                f"does not match target candle close_time ({target_candle.close_time.isoformat()})."
            )
        if output.decision_time != target_candle.close_time:
            raise StrategyOutputMismatchError(
                f"StrategyOutput decision_time ({output.decision_time.isoformat()}) "
                f"does not match target candle close_time ({target_candle.close_time.isoformat()})."
            )

        # Step B8: Assemble Final Auditable Decision
        return StrategyDecision.assemble(
            output=output,
            verified_snapshot=verified_snapshot,
        )

    @classmethod
    def evaluate_at(
        cls,
        supplied_candles: Sequence[Candle],
        target_index: int,
        context: EvaluationContext,
        strategy: Strategy,
    ) -> StrategyDecision:
        """Verify full snapshot via Stage A, then evaluate causal prefix at target_index."""
        verified_snapshot = cls.verify_snapshot(supplied_candles, context)
        return cls._evaluate_at_verified(
            supplied_candles=supplied_candles,
            target_index=target_index,
            verified_snapshot=verified_snapshot,
            strategy=strategy,
        )

    @classmethod
    def evaluate_series(
        cls,
        supplied_candles: Sequence[Candle],
        context: EvaluationContext,
        strategy: Strategy,
        start_index: int | None = None,
    ) -> list[StrategyDecision]:
        """Verify snapshot once via Stage A, then evaluate strategy across all eligible candles.

        Args:
            supplied_candles: Sequence of original M0 Candle instances.
            context: Authoritative catalog-backed EvaluationContext provenance.
            strategy: Strategy instance adhering to Strategy protocol.
            start_index: Optional starting index. If None, defaults to
                         strategy.minimum_history_candles - 1 (evaluating all eligible candles).
                         If explicitly provided, begins at start_index (raising InsufficientHistoryError
                         if start_index + 1 < minimum_history_candles).

        Returns:
            List of auditable StrategyDecision records.
        """
        verified_snapshot = cls.verify_snapshot(supplied_candles, context)

        if start_index is None:
            first_eligible_index = strategy.minimum_history_candles - 1
            if first_eligible_index >= len(supplied_candles):
                return []
            start_idx = first_eligible_index
        else:
            start_idx = start_index

        decisions: list[StrategyDecision] = []
        for i in range(start_idx, len(supplied_candles)):
            decision = cls._evaluate_at_verified(
                supplied_candles=supplied_candles,
                target_index=i,
                verified_snapshot=verified_snapshot,
                strategy=strategy,
            )
            decisions.append(decision)
        return decisions

    # Instance method aliases for flexibility
    def verify(self, supplied_candles: Sequence[Candle], context: EvaluationContext) -> VerifiedSnapshot:
        """Instance alias for verify_snapshot."""
        return self.verify_snapshot(supplied_candles, context)

    def evaluate(
        self,
        supplied_candles: Sequence[Candle],
        context: EvaluationContext,
        strategy: Strategy,
        start_index: int | None = None,
    ) -> list[StrategyDecision]:
        """Instance alias for evaluate_series."""
        return self.evaluate_series(
            supplied_candles=supplied_candles,
            context=context,
            strategy=strategy,
            start_index=start_index,
        )
