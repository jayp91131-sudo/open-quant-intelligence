"""Targeted tests for directory synchronization, filesystem durability, and atomic publication."""

from pathlib import Path
from unittest.mock import patch

import pytest

from open_quant_intelligence.storage.fingerprint import compute_file_sha256
from open_quant_intelligence.storage.manifest import DurabilityError, sync_directory
from open_quant_intelligence.storage.parquet_store import (
    DatasetPathCollisionError,
    publish_staging_file_no_clobber,
)


def test_sync_directory_success_or_noop(tmp_path: Path) -> None:
    """Verifies sync_directory completes successfully on valid directory."""
    test_dir = tmp_path / "durable_dir"
    test_dir.mkdir()
    res = sync_directory(test_dir, raise_on_failure=True)
    assert res is True


def test_sync_directory_non_directory_noop(tmp_path: Path) -> None:
    """Non-directory path safely returns True without error."""
    non_dir = tmp_path / "not_a_dir.txt"
    res = sync_directory(non_dir, raise_on_failure=True)
    assert res is True


def test_sync_directory_posix_simulated_failure_degraded(tmp_path: Path) -> None:
    """Simulated POSIX fsync failure with raise_on_failure=False returns False (degraded durability)."""
    target_dir = tmp_path / "sync_fail_dir"
    target_dir.mkdir()
    with patch("sys.platform", "linux"):
        with patch("os.open", return_value=99):
            with patch("os.fsync", side_effect=OSError("NFS / Docker fsync unsupported")):
                with patch("os.close"):
                    res = sync_directory(target_dir, raise_on_failure=False)
                    assert res is False


def test_sync_directory_posix_simulated_failure_raises(tmp_path: Path) -> None:
    """Simulated POSIX fsync failure with raise_on_failure=True raises DurabilityError."""
    target_dir = tmp_path / "sync_fail_dir_raises"
    target_dir.mkdir()
    with patch("sys.platform", "linux"):
        with patch("os.open", return_value=99):
            with patch("os.fsync", side_effect=OSError("Disk failure / I/O error")):
                with patch("os.close"):
                    with pytest.raises(DurabilityError, match="Directory fsync failed"):
                        sync_directory(target_dir, raise_on_failure=True)


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
