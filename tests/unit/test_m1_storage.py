"""Unit tests for Parquet staging, row count verification, and no-clobber publication."""

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from open_quant_intelligence.domain.candle import Candle
from open_quant_intelligence.storage.fingerprint import compute_file_sha256
from open_quant_intelligence.storage.parquet_store import (
    RecoveryConflictError,
    publish_staging_file_no_clobber,
    write_staging_parquet,
)


def _make_candles(count: int) -> list[Candle]:
    res: list[Candle] = []
    for i in range(count):
        open_time = datetime.fromtimestamp(i * 3600, tz=timezone.utc)
        res.append(
            Candle(
                exchange_id="binance",
                market_type="spot",
                symbol="BTC/USDT",
                timeframe="1h",
                open_time=open_time,
                open=100.0 + i,
                high=105.0 + i,
                low=95.0 + i,
                close=102.0 + i,
                base_volume=10.0,
            )
        )
    return res


def test_parquet_staging_row_count_verification(tmp_path: Path) -> None:
    """Staging parquet writer verifies physical row count == record_count."""
    staging_file = tmp_path / "staging.parquet"
    candles = _make_candles(10)

    # Correct record count passes
    sha = write_staging_parquet(staging_file, candles, record_count=10)
    assert sha.startswith("sha256:")
    assert staging_file.exists()

    # Verify physical row count via DuckDB
    with duckdb.connect() as con:
        count = con.execute(f"SELECT count(*) FROM read_parquet('{staging_file.as_posix()}')").fetchone()[0]
        assert count == 10

    # Mismatched record count raises AssertionError
    staging_file_bad = tmp_path / "bad.parquet"
    with pytest.raises(AssertionError, match="Physical Parquet row count"):
        write_staging_parquet(staging_file_bad, candles, record_count=12)


def test_preexisting_target_collision_preserved_byte_for_byte(tmp_path: Path) -> None:
    """Requirement 11: Pre-existing target file with mismatched hash
    causes RecoveryConflictError and remains unmodified.
    """
    staging_file = tmp_path / "staging.parquet"
    candles = _make_candles(5)
    write_staging_parquet(staging_file, candles, record_count=5)

    target_file = tmp_path / "target.parquet"
    target_original_content = b"ORIGINAL_CORRUPT_OR_DIFFERENT_DATA_1234567890"
    target_file.write_bytes(target_original_content)
    target_sha_before = compute_file_sha256(target_file)

    # Attempting to publish must fail with RecoveryConflictError
    with pytest.raises(RecoveryConflictError, match="differs from staging"):
        publish_staging_file_no_clobber(staging_file, target_file)

    # Target file must remain byte-for-byte unmodified!
    assert target_file.read_bytes() == target_original_content
    assert compute_file_sha256(target_file) == target_sha_before


def test_no_clobber_publication_idempotent_when_hash_matches(tmp_path: Path) -> None:
    """If target already exists and has identical hash, publication succeeds idempotently."""
    staging_file = tmp_path / "staging.parquet"
    candles = _make_candles(5)
    write_staging_parquet(staging_file, candles, record_count=5)

    target_file = tmp_path / "target.parquet"
    target_file.parent.mkdir(parents=True, exist_ok=True)
    # Copy exact same content to target
    target_file.write_bytes(staging_file.read_bytes())

    # Publication succeeds without error
    publish_staging_file_no_clobber(staging_file, target_file)
    assert target_file.exists()
