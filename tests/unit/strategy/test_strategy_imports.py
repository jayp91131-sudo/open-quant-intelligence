"""Tests for import safety, circular dependency prevention, and public exports."""

import subprocess
import sys


def test_public_exports() -> None:
    import open_quant_intelligence.strategy as strategy_module

    expected_exports = [
        "Strategy",
        "StrategyConfig",
        "StrategyCandle",
        "StrategyContext",
        "EvaluationContext",
        "VerifiedSnapshot",
        "GapPolicy",
        "ResearchSignal",
        "ReasonCode",
        "StrategyOutput",
        "StrategyDecision",
        "StrategyEvaluator",
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

    for export_name in expected_exports:
        assert hasattr(strategy_module, export_name), f"Missing public export: {export_name}"
        assert export_name in strategy_module.__all__


def test_circular_import_safety_isolated_processes() -> None:
    """Verify that importing signals or contracts in isolation never raises circular ImportError."""
    scripts = [
        "import open_quant_intelligence.strategy.contracts",
        "import open_quant_intelligence.strategy.signals",
        "import open_quant_intelligence.strategy.signals; import open_quant_intelligence.strategy.contracts",
        "import open_quant_intelligence.strategy.contracts; import open_quant_intelligence.strategy.signals",
        "from open_quant_intelligence.strategy import StrategyEvaluator, StrategyDecision",
    ]

    for script in scripts:
        res = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
        )
        assert res.returncode == 0, f"Failed executing '{script}':\nSTDERR:\n{res.stderr}"
