"""Unit tests for M1 CLI commands (ingest, datasets list, inspect, verify)."""

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from open_quant_intelligence.cli import main
from tests.fakes.fake_binance_adapter import FakeBinanceAdapter


def test_cli_ingest_and_datasets_lifecycle(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    klines: list[list[Any]] = [
        [0, 100.0, 105.0, 95.0, 102.0, 10.0],
        [3_600_000, 102.0, 106.0, 101.0, 105.0, 15.0],
    ]
    fake = FakeBinanceAdapter(server_time_ms=10_000_000, default_klines=klines)

    # Ingest 2 hours of data
    with patch(
        "open_quant_intelligence.cli.IngestionPipeline",
        side_effect=lambda output_dir: __import__(
            "open_quant_intelligence.ingestion.pipeline", fromlist=["IngestionPipeline"]
        ).IngestionPipeline(output_dir=output_dir, adapter=fake),
    ):
        exit_code = main(
            [
                "ingest",
                "--symbol",
                "BTC/USDT",
                "--timeframe",
                "1h",
                "--since",
                "1970-01-01T00:00:00Z",
                "--until",
                "1970-01-01T02:00:00Z",
                "--output-dir",
                str(tmp_path),
            ]
        )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Open Quant Intelligence — Historical Ingestion Report" in captured.out
    assert "Quality Status:        GAP_FREE" in captured.out
    assert "Recorded Intervals:    2 (0 missing)" in captured.out

    db_path = str(tmp_path / "catalog.duckdb")

    # Datasets list
    list_code = main(["datasets", "list", "--db-path", db_path])
    list_out = capsys.readouterr().out
    assert list_code == 0
    assert "BTC/USDT" in list_out
    assert "GAP_FREE" in list_out

    # Datasets inspect
    inspect_code = main(["datasets", "inspect", "BTC/USDT", "--db-path", db_path])
    inspect_out = capsys.readouterr().out
    assert inspect_code == 0
    assert "Dataset Snapshot:" in inspect_out
    assert "Quality Status:      GAP_FREE" in inspect_out

    # Datasets verify
    verify_code = main(["datasets", "verify", "BTC/USDT", "--db-path", db_path])
    verify_out = capsys.readouterr().out
    assert verify_code == 0
    assert "VERIFIED: Parquet artifact checksum matches catalog" in verify_out
