"""Unit tests for Open Quant Intelligence CLI commands."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from open_quant_intelligence.cli import main
from tests.fixtures.synthetic_data import (
    candles_to_csv_string,
    create_synthetic_candle,
    generate_synthetic_series,
)


def test_cli_returns_success_for_valid_file(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    valid_csv = tmp_path / "valid_candles.csv"
    candles = generate_synthetic_series(count=12, timeframe="1h")
    valid_csv.write_text(candles_to_csv_string(candles), encoding="utf-8")

    exit_code = main(["validate", str(valid_csv)])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Validation Status: PASSED" in captured.out
    assert "Errors Found: 0" in captured.out


def test_cli_returns_failure_for_invalid_file(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    invalid_csv = tmp_path / "corrupt_candles.csv"
    invalid_csv.write_text(
        "exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n"
        "binance,spot,BTC/USDT,1h,2026-01-01T00:00:00Z,-500.0,500.0,400.0,450.0,10.0\n",
        encoding="utf-8",
    )

    exit_code = main(["validate", str(invalid_csv)])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Validation Status: FAILED" in captured.out
    assert "Errors Found: 1" in captured.out


def test_cli_returns_failure_for_non_existent_file(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    non_existent = tmp_path / "does_not_exist.csv"

    exit_code = main(["validate", str(non_existent)])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "FILE_NOT_FOUND" in captured.out


def test_cli_fail_on_gaps_flag(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    gap_csv = tmp_path / "gap_candles.csv"
    # Row 1 at 00:00, Row 2 at 03:00 (2 missing hours)
    c1 = create_synthetic_candle(open_time=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc))
    c2 = create_synthetic_candle(open_time=datetime(2026, 1, 1, 3, 0, tzinfo=timezone.utc))
    gap_csv.write_text(candles_to_csv_string([c1, c2]), encoding="utf-8")

    # Without --fail-on-gaps, valid rows pass, gap is reported, exit code is 0
    exit_code_normal = main(["validate", str(gap_csv)])
    assert exit_code_normal == 0

    # With --fail-on-gaps, exit code is 1
    exit_code_strict = main(["validate", str(gap_csv), "--fail-on-gaps"])
    assert exit_code_strict == 1


def test_cli_quiet_flag(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    valid_csv = tmp_path / "valid.csv"
    candles = generate_synthetic_series(count=5, timeframe="1h")
    valid_csv.write_text(candles_to_csv_string(candles), encoding="utf-8")

    exit_code = main(["validate", str(valid_csv), "--quiet"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.out == ""
