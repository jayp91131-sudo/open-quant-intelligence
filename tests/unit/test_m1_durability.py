"""Targeted tests for directory synchronization, filesystem durability, and atomic publication."""

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from open_quant_intelligence.cli import main
from open_quant_intelligence.domain.timeframe import SupportedTimeframe
from open_quant_intelligence.ingestion.pipeline import IngestionPipeline, IngestionRequest
from open_quant_intelligence.storage.fingerprint import compute_file_sha256
from open_quant_intelligence.storage.manifest import (
    DurabilityError,
    DurabilityMode,
    DurabilityStatus,
    ManifestManager,
    sync_directory,
)
from open_quant_intelligence.storage.parquet_store import (
    DatasetPathCollisionError,
    publish_staging_file_no_clobber,
)
from tests.fakes.fake_binance_adapter import FakeBinanceAdapter


def test_sync_directory_posix_success(tmp_path: Path) -> None:
    """Verifies sync_directory completes successfully on valid POSIX directory."""
    test_dir = tmp_path / "durable_dir"
    test_dir.mkdir()
    with patch("sys.platform", "linux"):
        with patch("os.open", return_value=99):
            with patch("os.fsync") as mock_fsync:
                with patch("os.close"):
                    res = sync_directory(test_dir, durability_mode=DurabilityMode.STRICT)
                    assert res is True
                    mock_fsync.assert_called_once_with(99)


def test_sync_directory_windows_semantics(tmp_path: Path) -> None:
    """On Windows, directory-level flush is unsupported: STRICT raises, DEGRADED_ALLOWED returns False."""
    test_dir = tmp_path / "win_dir"
    test_dir.mkdir()
    with patch("sys.platform", "win32"):
        # STRICT mode raises DurabilityError
        with pytest.raises(DurabilityError, match="unsupported on Windows"):
            sync_directory(test_dir, durability_mode=DurabilityMode.STRICT)

        # DEGRADED_ALLOWED returns False
        res = sync_directory(test_dir, durability_mode=DurabilityMode.DEGRADED_ALLOWED)
        assert res is False


def test_sync_directory_non_directory_noop(tmp_path: Path) -> None:
    """Non-directory path safely returns True on POSIX."""
    non_dir = tmp_path / "not_a_dir.txt"
    with patch("sys.platform", "linux"):
        res = sync_directory(non_dir, durability_mode=DurabilityMode.STRICT)
        assert res is True


def test_sync_directory_posix_simulated_failure_degraded(tmp_path: Path) -> None:
    """Simulated POSIX fsync failure with DEGRADED_ALLOWED returns False."""
    target_dir = tmp_path / "sync_fail_dir"
    target_dir.mkdir()
    with patch("sys.platform", "linux"):
        with patch("os.open", return_value=99):
            with patch("os.fsync", side_effect=OSError("NFS / Docker fsync unsupported")):
                with patch("os.close"):
                    res = sync_directory(target_dir, durability_mode=DurabilityMode.DEGRADED_ALLOWED)
                    assert res is False


def test_sync_directory_posix_simulated_failure_raises(tmp_path: Path) -> None:
    """Simulated POSIX fsync failure with STRICT raises DurabilityError."""
    target_dir = tmp_path / "sync_fail_dir_raises"
    target_dir.mkdir()
    with patch("sys.platform", "linux"):
        with patch("os.open", return_value=99):
            with patch("os.fsync", side_effect=OSError("Disk failure / I/O error")):
                with patch("os.close"):
                    with pytest.raises(DurabilityError, match="Directory fsync failed"):
                        sync_directory(target_dir, durability_mode=DurabilityMode.STRICT)


def test_publication_collision_preserves_target_byte_for_byte(tmp_path: Path) -> None:
    """No-clobber publication ensures existing target file is never overwritten or mutated."""
    staging = tmp_path / "stage.parquet"
    staging.write_bytes(b"STAGING DATA CONTENT A")

    target = tmp_path / "published.parquet"
    target.write_bytes(b"PREEXISTING TARGET CONTENT B")
    target_bytes_orig = target.read_bytes()
    target_sha_orig = compute_file_sha256(target)

    with pytest.raises(DatasetPathCollisionError, match="differs from staging artifact"):
        publish_staging_file_no_clobber(staging, target)

    # Verify target was untouched
    assert target.read_bytes() == target_bytes_orig
    assert compute_file_sha256(target) == target_sha_orig
    # Staging file also remains intact for recovery investigation
    assert staging.exists()


def test_publication_idempotent_when_hashes_match(tmp_path: Path) -> None:
    """If staging and target have identical content, publication succeeds idempotently."""
    staging = tmp_path / "stage_idem.parquet"
    staging.write_bytes(b"EXACT MATCH CONTENT 12345")

    target = tmp_path / "published_idem.parquet"
    target.write_bytes(b"EXACT MATCH CONTENT 12345")
    expected_sha = compute_file_sha256(target)

    returned_sha = publish_staging_file_no_clobber(staging, target)
    assert returned_sha == expected_sha
    assert target.exists()
    assert compute_file_sha256(target) == expected_sha
    # Staging cleaned up
    assert not staging.exists()


def test_pipeline_strict_durability_mode_fails_safely_on_sync_failure(tmp_path: Path) -> None:
    """In STRICT durability mode, sync failure raises DurabilityError and aborts safely."""
    klines: list[list[Any]] = [
        [0, 100.0, 105.0, 95.0, 102.0, 10.0],
    ]
    fake = FakeBinanceAdapter(server_time_ms=10_000_000, default_klines=klines)
    pipeline = IngestionPipeline(
        output_dir=tmp_path,
        adapter=fake,
        durability_mode=DurabilityMode.STRICT,
    )

    request = IngestionRequest(
        symbol="BTC/USDT",
        timeframe=SupportedTimeframe.H1,
        since_ms=0,
        until_ms=3_600_000,
        durability_mode=DurabilityMode.STRICT,
    )

    with patch(
        "open_quant_intelligence.storage.manifest.sync_directory",
        side_effect=DurabilityError("Simulated directory fsync hardware error"),
    ):
        with pytest.raises(DurabilityError, match="Simulated directory fsync hardware error"):
            pipeline.ingest(request)


def test_strict_pipeline_inherits_durability_mode_when_unspecified(tmp_path: Path) -> None:
    """A STRICT pipeline receiving an IngestionRequest with no explicit mode remains STRICT."""
    klines: list[list[Any]] = [
        [0, 100.0, 105.0, 95.0, 102.0, 10.0],
    ]
    fake = FakeBinanceAdapter(server_time_ms=10_000_000, default_klines=klines)
    pipeline = IngestionPipeline(
        output_dir=tmp_path,
        adapter=fake,
        durability_mode=DurabilityMode.STRICT,
    )

    # Note: durability_mode is omitted (None) on the request
    request = IngestionRequest(
        symbol="BTC/USDT",
        timeframe=SupportedTimeframe.H1,
        since_ms=0,
        until_ms=3_600_000,
        durability_mode=None,
    )

    with patch(
        "open_quant_intelligence.storage.manifest.sync_directory",
        side_effect=DurabilityError("Strict pipeline directory sync aborted"),
    ):
        with pytest.raises(DurabilityError, match="Strict pipeline directory sync aborted"):
            pipeline.ingest(request)


def test_pipeline_degraded_durability_mode_exposes_status(tmp_path: Path) -> None:
    """In DEGRADED_ALLOWED mode, directory sync failure exposes DEGRADED durability status."""
    klines: list[list[Any]] = [
        [0, 100.0, 105.0, 95.0, 102.0, 10.0],
    ]
    fake = FakeBinanceAdapter(server_time_ms=10_000_000, default_klines=klines)
    pipeline = IngestionPipeline(
        output_dir=tmp_path,
        adapter=fake,
        durability_mode=DurabilityMode.DEGRADED_ALLOWED,
    )

    request = IngestionRequest(
        symbol="BTC/USDT",
        timeframe=SupportedTimeframe.H1,
        since_ms=0,
        until_ms=3_600_000,
        durability_mode=DurabilityMode.DEGRADED_ALLOWED,
    )

    # Simulate sync_directory returning False (failure on non-strict/degraded mode)
    with patch(
        "open_quant_intelligence.storage.manifest.sync_directory",
        return_value=False,
    ):
        res = pipeline.ingest(request)

    assert res.durability_status == DurabilityStatus.DEGRADED.value
    assert res.durability_warning is not None
    assert "directory" in res.durability_warning.lower()
    assert res.manifest.durability_status == DurabilityStatus.DEGRADED.value


def test_unlink_manifest_returns_durability_sync_status(tmp_path: Path) -> None:
    """unlink_manifest returns bool indicating whether directory sync succeeded."""
    mgr = ManifestManager(tmp_path / "manifests")
    test_id = "test_unlink_id"
    # Create fake manifest file
    p = mgr.manifest_path_for(test_id)
    p.write_text("{}", encoding="utf-8")

    with patch(
        "open_quant_intelligence.storage.manifest.sync_directory",
        return_value=False,
    ):
        res = mgr.unlink_manifest(test_id)
        assert res is False
        assert not p.exists()

    p2 = mgr.manifest_path_for("test_id_2")
    p2.write_text("{}", encoding="utf-8")
    with patch(
        "open_quant_intelligence.storage.manifest.sync_directory",
        return_value=True,
    ):
        res2 = mgr.unlink_manifest("test_id_2")
        assert res2 is True
        assert not p2.exists()


def test_cli_ingest_exposes_durability_status_and_accepts_mode(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI ingest outputs durability status and accepts --durability-mode flag."""
    klines: list[list[Any]] = [
        [0, 100.0, 105.0, 95.0, 102.0, 10.0],
    ]
    fake = FakeBinanceAdapter(server_time_ms=10_000_000, default_klines=klines)

    with patch(
        "open_quant_intelligence.cli.IngestionPipeline",
        side_effect=lambda output_dir, durability_mode=DurabilityMode.DEGRADED_ALLOWED: IngestionPipeline(
            output_dir=output_dir,
            adapter=fake,
            durability_mode=durability_mode,
        ),
    ):
        code = main(
            [
                "ingest",
                "--symbol",
                "BTC/USDT",
                "--timeframe",
                "1h",
                "--since",
                "1970-01-01T00:00:00Z",
                "--until",
                "1970-01-01T01:00:00Z",
                "--output-dir",
                str(tmp_path),
                "--durability-mode",
                "degraded_allowed",
            ]
        )
        assert code == 0
        captured = capsys.readouterr()
        assert "Durability Status:" in captured.out
