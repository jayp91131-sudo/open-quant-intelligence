"""Canonical binary content fingerprinting and physical file SHA-256 calculation."""

import hashlib
import math
import struct
from pathlib import Path
from typing import Sequence

from open_quant_intelligence.domain.candle import Candle


class UnsortedStreamError(ValueError):
    """Raised when candles to be fingerprinted are not strictly sorted by open_time."""


def normalize_float(val: float) -> float:
    """Normalize -0.0 to +0.0 in IEEE-754 arithmetic."""
    return 0.0 if val == 0.0 else val


def compute_content_fingerprint(
    candles: Sequence[Candle],
    exchange_id: str,
    market_type: str,
    symbol: str,
    timeframe: str,
    requested_since_ms: int,
    requested_until_ms: int,
) -> str:
    """Compute canonical binary content fingerprint SHA-256 digest.

    Binary Format:
      Header Block:
        - Magic: ASCII 'OQICCF' (6 bytes)
        - Schema Version: UINT16_LE (2 bytes, 1)
        - Exchange ID: UINT16_LE len + UTF-8 bytes
        - Market Type: UINT16_LE len + UTF-8 bytes
        - Symbol: UINT16_LE len + UTF-8 bytes
        - Timeframe: UINT16_LE len + UTF-8 bytes
        - Requested Since ms: INT64_LE (8 bytes)
        - Requested Until ms: INT64_LE (8 bytes)
        - Candle Count: UINT64_LE (8 bytes)
      Candle Records (if count > 0):
        - open_time_ms: INT64_LE
        - close_time_ms: INT64_LE
        - open, high, low, close, volume: IEEE-754 binary64 DOUBLE_LE (normalized)
        - quote_volume present: UINT8 (0 or 1) + [DOUBLE_LE if 1]
        - trade_count present: UINT8 (0 or 1) + [INT64_LE if 1]
    """
    hasher = hashlib.sha256()

    # Magic and schema version
    hasher.update(b"OQICCF")
    hasher.update(struct.pack("<H", 1))

    # String identity fields
    for s_field in [exchange_id, market_type, symbol, timeframe]:
        b = s_field.encode("utf-8")
        hasher.update(struct.pack("<H", len(b)))
        hasher.update(b)

    # Window boundaries and candle count
    candle_count = len(candles)
    hasher.update(struct.pack("<qqQ", requested_since_ms, requested_until_ms, candle_count))

    # If count is 0 (EMPTY dataset), return hash of Header Block
    if candle_count == 0:
        return f"sha256:{hasher.hexdigest().lower()}"

    prev_open_ms: int | None = None
    for idx, c in enumerate(candles):
        open_ms = int(c.open_time.timestamp() * 1000)
        close_ms = int(c.close_time.timestamp() * 1000)

        if prev_open_ms is not None and open_ms <= prev_open_ms:
            raise UnsortedStreamError(
                f"Candle stream is not strictly sorted: row {idx} open_time {open_ms}ms <= {prev_open_ms}ms."
            )
        prev_open_ms = open_ms

        for p_name, p_val in [
            ("open", c.open),
            ("high", c.high),
            ("low", c.low),
            ("close", c.close),
            ("base_volume", c.base_volume),
        ]:
            if not math.isfinite(p_val):
                raise ValueError(f"Candle field {p_name} is not finite: {p_val}")

        hasher.update(
            struct.pack(
                "<qqddddd",
                open_ms,
                close_ms,
                normalize_float(c.open),
                normalize_float(c.high),
                normalize_float(c.low),
                normalize_float(c.close),
                normalize_float(c.base_volume),
            )
        )

        if c.quote_volume is not None:
            hasher.update(struct.pack("<Bd", 1, normalize_float(c.quote_volume)))
        else:
            hasher.update(struct.pack("<B", 0))

        if c.trade_count is not None:
            hasher.update(struct.pack("<Bq", 1, c.trade_count))
        else:
            hasher.update(struct.pack("<B", 0))

    return f"sha256:{hasher.hexdigest().lower()}"


def compute_file_sha256(file_path: Path | str) -> str:
    """Compute physical file SHA-256 digest."""
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {path}")

    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)

    return f"sha256:{hasher.hexdigest().lower()}"
