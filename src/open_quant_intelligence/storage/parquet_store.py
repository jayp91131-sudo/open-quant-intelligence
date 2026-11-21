"""Atomic Parquet storage writer and cross-platform no-clobber publisher."""

import ctypes
import os
import sys
from pathlib import Path
from typing import Sequence

import duckdb

from open_quant_intelligence.domain.candle import Candle
from open_quant_intelligence.storage.fingerprint import compute_file_sha256


class StorageError(Exception):
    """Base exception for storage errors."""


class DatasetPathCollisionError(StorageError, FileExistsError):
    """Raised when the target publication file already exists (no-clobber invariant)."""


class StagingRowMismatchError(StorageError):
    """Raised when staging file row count does not match admitted record count."""


def write_staging_parquet(
    candles: Sequence[Candle],
    staging_path: Path | str,
) -> int:
    """Write validated candles to a temporary Parquet staging file via DuckDB.

    Flushes file to disk with fsync / _commit and verifies row count.
    """
    path = Path(staging_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if not candles:
        return 0

    # Build relational table structure for DuckDB COPY
    records = []
    for c in candles:
        records.append(
            {
                "exchange_id": c.exchange_id,
                "market_type": c.market_type,
                "symbol": c.symbol,
                "timeframe": c.timeframe,
                "open_time": c.open_time,
                "close_time": c.close_time,
                "open": c.open,
                "high": c.high,
                "low": c.low,
                "close": c.close,
                "base_volume": c.base_volume,
                "quote_volume": c.quote_volume,
                "trade_count": c.trade_count,
            }
        )

    con = duckdb.connect(":memory:")
    try:
        # Register in-memory data
        con.execute(
            """
            CREATE TABLE stage_candles AS
            SELECT
                exchange_id, market_type, symbol, timeframe,
                open_time, close_time,
                open, high, low, close, base_volume,
                quote_volume, trade_count
            FROM (SELECT * FROM records)
            """
        )
        # Export to Parquet with snappy compression
        escaped_path = str(path).replace("'", "''")
        con.execute(f"COPY stage_candles TO '{escaped_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    finally:
        con.close()

    # Flush file buffers to physical disk
    with open(path, "rb") as f:
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

    return row_count


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
        raise DatasetPathCollisionError(
            f"Target file {t_path} already exists. Ingestion never overwrites existing historical datasets."
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
