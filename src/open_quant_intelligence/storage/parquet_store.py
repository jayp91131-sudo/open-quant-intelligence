"""Atomic Parquet storage writer and cross-platform no-clobber publisher."""

import ctypes
import os
import sys
from pathlib import Path
from typing import Any

import duckdb

from open_quant_intelligence.storage.fingerprint import compute_file_sha256


class StorageError(Exception):
    """Base exception for storage errors."""


class DatasetPathCollisionError(StorageError, FileExistsError):
    """Raised when the target publication file already exists (no-clobber invariant)."""


RecoveryConflictError = DatasetPathCollisionError


class StagingRowMismatchError(StorageError):
    """Raised when staging file row count does not match admitted record count."""


def write_staging_parquet(
    arg1: Any,
    arg2: Any,
    record_count: int | None = None,
) -> str | int:
    """Write validated candles to a temporary Parquet staging file via DuckDB.

    Accepts both (candles, staging_path) and (staging_path, candles).
    Flushes file to disk with fsync / _commit and verifies row count.
    """
    if isinstance(arg1, (str, Path)):
        path = Path(arg1)
        candles = arg2
    else:
        candles = arg1
        path = Path(arg2)

    path.parent.mkdir(parents=True, exist_ok=True)

    if not candles:
        return 0

    con = duckdb.connect(":memory:")
    try:
        # Register in-memory data
        con.execute(
            """
            CREATE TABLE stage_candles (
                exchange_id VARCHAR,
                market_type VARCHAR,
                symbol VARCHAR,
                timeframe VARCHAR,
                open_time TIMESTAMP,
                close_time TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                base_volume DOUBLE,
                quote_volume DOUBLE,
                trade_count BIGINT
            )
            """
        )
        row_tuples = [
            (
                c.exchange_id,
                c.market_type,
                c.symbol,
                c.timeframe,
                c.open_time,
                c.close_time,
                c.open,
                c.high,
                c.low,
                c.close,
                c.base_volume,
                c.quote_volume,
                c.trade_count,
            )
            for c in candles
        ]
        con.executemany(
            "INSERT INTO stage_candles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            row_tuples,
        )
        # Export to Parquet with snappy compression
        escaped_path = str(path).replace("'", "''")
        con.execute(f"COPY stage_candles TO '{escaped_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    finally:
        con.close()

    # Flush file buffers to physical disk
    with open(path, "a+b") as f:
        f.flush()
        os.fsync(f.fileno())

    # Verify staging row count
    verify_con = duckdb.connect(":memory:")
    try:
        escaped_path = str(path).replace("'", "''")
        res = verify_con.execute(f"SELECT count(*) FROM read_parquet('{escaped_path}')").fetchone()
        row_count = res[0] if res else 0
    finally:
        verify_con.close()

    if row_count != len(candles):
        raise StagingRowMismatchError(
            f"Staging Parquet row count ({row_count}) does not match admitted candles count ({len(candles)})."
        )

    if record_count is not None and row_count != record_count:
        raise AssertionError(
            f"Physical Parquet row count ({row_count}) does not match expected record_count ({record_count})."
        )

    return compute_file_sha256(path)


def publish_staging_file_no_clobber(
    staging_path: Path | str,
    target_path: Path | str,
) -> str:
    """Atomically publish staging Parquet file to target path without clobbering.

    Windows: Uses MoveFileExW with MOVEFILE_WRITE_THROUGH and omits MOVEFILE_REPLACE_EXISTING.
    POSIX: Uses os.link() followed by os.unlink(), with directory fsync.

    Raises:
        DatasetPathCollisionError: If target_path already exists.
    """
    s_path = Path(staging_path)
    t_path = Path(target_path)
    t_path.parent.mkdir(parents=True, exist_ok=True)

    if t_path.exists():
        staging_sha = compute_file_sha256(s_path)
        target_sha = compute_file_sha256(t_path)
        if staging_sha == target_sha:
            try:
                s_path.unlink(missing_ok=True)
            except OSError:
                pass
            return target_sha
        else:
            raise DatasetPathCollisionError(
                f"Target file {t_path} already exists with content that differs from staging artifact."
            )

    if sys.platform == "win32":
        MOVEFILE_WRITE_THROUGH = 0x8
        # Call MoveFileExW directly via ctypes to ensure exact Win32 flags
        kernel32 = ctypes.windll.kernel32
        MoveFileExW = kernel32.MoveFileExW
        MoveFileExW.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32]
        MoveFileExW.restype = ctypes.c_bool

        success = MoveFileExW(str(s_path), str(t_path), MOVEFILE_WRITE_THROUGH)
        if not success:
            err = ctypes.GetLastError()
            ERROR_ALREADY_EXISTS = 183
            ERROR_FILE_EXISTS = 80
            if err in (ERROR_ALREADY_EXISTS, ERROR_FILE_EXISTS):
                raise DatasetPathCollisionError(f"Target file {t_path} already exists on filesystem.")
            raise OSError(f"Win32 MoveFileExW failed with error code: {err}")
    else:
        # POSIX hard link no-clobber
        try:
            os.link(str(s_path), str(t_path))
        except FileExistsError as e:
            raise DatasetPathCollisionError(f"Target file {t_path} already exists on filesystem.") from e

        # Unlink staging file
        os.unlink(str(s_path))

        # Parent directory sync
        try:
            dir_fd = os.open(str(t_path.parent), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass

    return compute_file_sha256(t_path)
