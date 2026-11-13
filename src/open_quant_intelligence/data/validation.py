"""OHLCV candle data validation and gap detection."""

import csv
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence, TextIO

from open_quant_intelligence.domain.candle import Candle, InvalidCandleError
from open_quant_intelligence.domain.market import InvalidMarketError, MarketIdentity
from open_quant_intelligence.domain.timeframe import (
    InvalidTimeframeError,
    SupportedTimeframe,
    parse_timeframe,
    timeframe_to_timedelta,
)

REQUIRED_COLUMNS: tuple[str, ...] = (
    "exchange_id",
    "market_type",
    "symbol",
    "timeframe",
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "base_volume",
)


@dataclass(frozen=True)
class ValidationIssue:
    """Represents a data validation error or contract violation."""

    row_index: int | None
    issue_type: str
    message: str


@dataclass(frozen=True)
class GapReport:
    """Detailed record of a missing candle interval."""

    expected_open: datetime
    actual_open: datetime
    missing_count: int

    @property
    def start_time(self) -> datetime:
        """First missing candle open time."""
        return self.expected_open

    def __str__(self) -> str:
        return (
            f"Missing {self.missing_count} candle(s) between "
            f"{self.expected_open.isoformat()} and {self.actual_open.isoformat()}"
        )


@dataclass
class ValidationResult:
    """Comprehensive outcome of a candle series validation run."""

    is_valid: bool
    total_rows: int = 0
    valid_candles: list[Candle] = field(default_factory=list)
    errors: list[ValidationIssue] = field(default_factory=list)
    gaps: list[GapReport] = field(default_factory=list)
    exchange_id: str | None = None
    market_type: str | None = None
    symbol: str | None = None
    timeframe: str | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None

    def summary(self) -> str:
        """Return a formatted human-readable summary of the validation report."""
        lines = [
            "============================================================",
            "Open Quant Intelligence — Candle Data Validation Report",
            "============================================================",
            f"Validation Status: {'PASSED' if self.is_valid else 'FAILED'}",
            f"Total Rows Checked: {self.total_rows}",
            f"Valid Candles Ingested: {len(self.valid_candles)}",
        ]

        if self.exchange_id or self.symbol:
            lines.append(
                f"Market: {self.exchange_id or 'N/A'} | {self.market_type or 'N/A'} | "
                f"{self.symbol or 'N/A'} | {self.timeframe or 'N/A'}"
            )

        if self.start_time and self.end_time:
            lines.append(f"Date Range (UTC): {self.start_time.isoformat()} -> {self.end_time.isoformat()}")

        # Gaps reporting
        if self.gaps:
            total_missing = sum(g.missing_count for g in self.gaps)
            lines.append(f"Gaps Detected: {len(self.gaps)} gap event(s), {total_missing} missing interval(s)")
            for i, g in enumerate(self.gaps[:10], start=1):
                lines.append(f"  [{i}] {g}")
            if len(self.gaps) > 10:
                lines.append(f"  ... and {len(self.gaps) - 10} more gap events.")
        else:
            lines.append("Gaps Detected: None (continuous interval sequence)")

        # Errors reporting
        if self.errors:
            lines.append(f"Errors Found: {len(self.errors)}")
            for i, err in enumerate(self.errors[:15], start=1):
                prefix = f"Row {err.row_index}" if err.row_index is not None else "Global"
                lines.append(f"  [{i}] [{prefix}] ({err.issue_type}): {err.message}")
            if len(self.errors) > 15:
                lines.append(f"  ... and {len(self.errors) - 15} more error(s).")
        else:
            lines.append("Errors Found: 0")

        lines.append("============================================================")
        return "\n".join(lines)


def _parse_iso_timestamp(ts_str: str, row_idx: int) -> tuple[datetime | None, ValidationIssue | None]:
    """Parse an ISO timestamp, strictly requiring timezone information."""
    if not ts_str or not isinstance(ts_str, str) or not ts_str.strip():
        return None, ValidationIssue(
            row_index=row_idx,
            issue_type="MISSING_TIMESTAMP",
            message="Timestamp string is empty.",
        )

    clean_str = ts_str.strip()
    try:
        # Python 3.11+ fromisoformat handles standard ISO 8601 (including 'Z')
        dt = datetime.fromisoformat(clean_str)
    except Exception as exc:
        return None, ValidationIssue(
            row_index=row_idx,
            issue_type="INVALID_TIMESTAMP_FORMAT",
            message=f"Malformed ISO-8601 timestamp string {clean_str!r}: {exc}",
        )

    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        return None, ValidationIssue(
            row_index=row_idx,
            issue_type="TIMEZONE_NAIVE",
            message=f"Timezone-naive timestamp rejected: {clean_str!r}. All timestamps must specify UTC timezone.",
        )

    return dt.astimezone(timezone.utc), None


def validate_candles(
    candles: Sequence[Candle],
    fail_on_gaps: bool = False,
    expected_timeframe: str | SupportedTimeframe | None = None,
) -> ValidationResult:
    """Validate an in-memory sequence of Candle domain models.

    Args:
        candles: Sequence of immutable Candle instances.
        fail_on_gaps: If True, detected gaps make the result invalid.
        expected_timeframe: Optional expected timeframe to enforce.

    Returns:
        ValidationResult object.
    """
    errors: list[ValidationIssue] = []
    gaps: list[GapReport] = []
    valid_candles: list[Candle] = []

    if not candles:
        return ValidationResult(
            is_valid=False,
            total_rows=0,
            valid_candles=[],
            errors=[
                ValidationIssue(
                    row_index=None,
                    issue_type="EMPTY_DATASET",
                    message="Dataset is empty. At least one candle is required.",
                )
            ],
            gaps=[],
        )

    # Find the first valid Candle instance to establish expected series identity
    first_valid_candle: Candle | None = None
    for item in candles:
        if isinstance(item, Candle):
            first_valid_candle = item
            break

    if first_valid_candle is None:
        # No elements are valid Candle instances
        for idx, item in enumerate(candles, start=1):
            errors.append(
                ValidationIssue(
                    row_index=idx,
                    issue_type="INVALID_OBJECT_TYPE",
                    message=f"Expected Candle instance, got {type(item).__name__}.",
                )
            )
        return ValidationResult(
            is_valid=False,
            total_rows=len(candles),
            valid_candles=[],
            errors=errors,
            gaps=[],
        )

    expected_exchange = first_valid_candle.exchange_id
    expected_symbol = first_valid_candle.symbol
    tf_str = first_valid_candle.timeframe
    if expected_timeframe:
        parsed_expected = parse_timeframe(expected_timeframe).value
        if tf_str != parsed_expected:
            errors.append(
                ValidationIssue(
                    row_index=1,
                    issue_type="TIMEFRAME_MISMATCH",
                    message=f"Timeframe '{tf_str}' does not match expected '{parsed_expected}'.",
                )
            )

    try:
        interval_delta = timeframe_to_timedelta(tf_str)
    except InvalidTimeframeError as exc:
        errors.append(
            ValidationIssue(
                row_index=1,
                issue_type="UNSUPPORTED_TIMEFRAME",
                message=str(exc),
            )
        )
        interval_delta = None

    prev_time: datetime | None = None

    for idx, c in enumerate(candles, start=1):
        if not isinstance(c, Candle):
            errors.append(
                ValidationIssue(
                    row_index=idx,
                    issue_type="INVALID_OBJECT_TYPE",
                    message=f"Expected Candle instance, got {type(c).__name__}.",
                )
            )
            continue

        # Check homogeneity
        if c.symbol != expected_symbol:
            errors.append(
                ValidationIssue(
                    row_index=idx,
                    issue_type="MIXED_SYMBOLS",
                    message=f"Mixed symbols rejected. Found '{c.symbol}' but expected '{expected_symbol}'.",
                )
            )

        if c.exchange_id != expected_exchange:
            errors.append(
                ValidationIssue(
                    row_index=idx,
                    issue_type="MIXED_EXCHANGES",
                    message=f"Mixed exchange_id rejected. Found '{c.exchange_id}' but expected '{expected_exchange}'.",
                )
            )

        if c.timeframe != tf_str:
            errors.append(
                ValidationIssue(
                    row_index=idx,
                    issue_type="MIXED_TIMEFRAMES",
                    message=f"Mixed timeframes rejected. Found '{c.timeframe}' but expected '{tf_str}'.",
                )
            )

        # Ordering, alignment, and duplicate detection
        if prev_time is not None:
            if c.open_time == prev_time:
                errors.append(
                    ValidationIssue(
                        row_index=idx,
                        issue_type="DUPLICATE_TIMESTAMP",
                        message=f"Duplicate open_time detected at {c.open_time.isoformat()}.",
                    )
                )
            elif c.open_time < prev_time:
                errors.append(
                    ValidationIssue(
                        row_index=idx,
                        issue_type="OUT_OF_ORDER_TIMESTAMP",
                        message=(
                            f"Candles are not in strictly increasing chronological order. "
                            f"Current open_time {c.open_time.isoformat()} is before previous {prev_time.isoformat()}."
                        ),
                    )
                )
            elif interval_delta is not None:
                expected_open = prev_time + interval_delta
                if c.open_time > expected_open:
                    diff_seconds = (c.open_time - prev_time).total_seconds()
                    step_seconds = interval_delta.total_seconds()
                    remainder = diff_seconds % step_seconds
                    # Use a small tolerance for floating point modulo if needed, but seconds are integers here
                    if remainder != 0:
                        errors.append(
                            ValidationIssue(
                                row_index=idx,
                                issue_type="MISALIGNED_INTERVAL",
                                message=(
                                    f"Candle open_time {c.open_time.isoformat()} is not aligned to "
                                    f"timeframe '{tf_str}' grid relative to previous candle {prev_time.isoformat()} "
                                    f"(delta of {diff_seconds}s is not an exact multiple of {step_seconds}s)."
                                ),
                            )
                        )
                    else:
                        missing_count = int(diff_seconds // step_seconds) - 1
                        gaps.append(
                            GapReport(
                                expected_open=expected_open,
                                actual_open=c.open_time,
                                missing_count=missing_count,
                            )
                        )

        prev_time = c.open_time
        valid_candles.append(c)

    has_errors = len(errors) > 0
    is_valid = not has_errors and (not fail_on_gaps or len(gaps) == 0)

    start_t = valid_candles[0].open_time if valid_candles else None
    end_t = valid_candles[-1].open_time if valid_candles else None

    return ValidationResult(
        is_valid=is_valid,
        total_rows=len(candles),
        valid_candles=valid_candles if is_valid else [],
        errors=errors,
        gaps=gaps,
        exchange_id=expected_exchange,
        market_type=first_valid_candle.market_type if first_valid_candle else None,
        symbol=expected_symbol,
        timeframe=tf_str,
        start_time=start_t,
        end_time=end_t,
    )


def validate_candle_stream(
    reader: csv.DictReader,
    fail_on_gaps: bool = False,
) -> ValidationResult:
    """Validate rows from a CSV DictReader according to canonical candle contracts."""
    errors: list[ValidationIssue] = []
    gaps: list[GapReport] = []
    valid_candles: list[Candle] = []

    # Check header
    if not reader.fieldnames:
        errors.append(
            ValidationIssue(
                row_index=0,
                issue_type="EMPTY_FILE",
                message="CSV file contains no headers or data.",
            )
        )
        return ValidationResult(is_valid=False, total_rows=0, errors=errors)

    fieldnames = [f.strip() for f in reader.fieldnames if f]
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in fieldnames]
    if missing_cols:
        errors.append(
            ValidationIssue(
                row_index=0,
                issue_type="MISSING_COLUMNS",
                message=f"Missing required CSV columns: {', '.join(missing_cols)}.",
            )
        )
        return ValidationResult(is_valid=False, total_rows=0, errors=errors)

    first_row = True
    expected_exchange: str | None = None
    expected_market_type: str | None = None
    expected_symbol: str | None = None
    expected_timeframe: str | None = None
    interval_delta = None

    prev_open_time: datetime | None = None
    row_count = 0

    for row_num, raw_row in enumerate(reader, start=1):
        row_count += 1

        # Check for unheaded extra columns (None key in DictReader)
        if None in raw_row and raw_row[None]:
            errors.append(
                ValidationIssue(
                    row_index=row_num,
                    issue_type="EXTRA_FIELDS",
                    message=(
                        f"Row {row_num} contains {len(raw_row[None])} unheaded extra column value(s): "
                        f"{raw_row[None]!r}."
                    ),
                )
            )

        # Check for missing required columns where the row didn't have enough values (value is None)
        missing_row_values = [col for col in REQUIRED_COLUMNS if raw_row.get(col) is None]
        if missing_row_values:
            errors.append(
                ValidationIssue(
                    row_index=row_num,
                    issue_type="MALFORMED_ROW",
                    message=(
                        f"Row {row_num} is malformed or truncated; missing required values for: "
                        f"{', '.join(missing_row_values)}."
                    ),
                )
            )
            continue

        # Strip all keys and string values
        row = {k.strip(): v.strip() if isinstance(v, str) else v for k, v in raw_row.items() if k}

        # Market identity & single-series check
        exch_raw = row.get("exchange_id")
        mtype_raw = row.get("market_type")
        sym_raw = row.get("symbol")
        tf_raw = row.get("timeframe")

        if not exch_raw or not isinstance(exch_raw, str) or not exch_raw.strip():
            errors.append(
                ValidationIssue(
                    row_index=row_num,
                    issue_type="MISSING_IDENTITY_FIELD",
                    message=f"Row {row_num} has empty or missing 'exchange_id'.",
                )
            )
            continue

        if not mtype_raw or not isinstance(mtype_raw, str) or not mtype_raw.strip():
            errors.append(
                ValidationIssue(
                    row_index=row_num,
                    issue_type="MISSING_IDENTITY_FIELD",
                    message=f"Row {row_num} has empty or missing 'market_type'.",
                )
            )
            continue

        if not sym_raw or not isinstance(sym_raw, str) or not sym_raw.strip():
            errors.append(
                ValidationIssue(
                    row_index=row_num,
                    issue_type="MISSING_IDENTITY_FIELD",
                    message=f"Row {row_num} has empty or missing 'symbol'.",
                )
            )
            continue

        if not tf_raw or not isinstance(tf_raw, str) or not tf_raw.strip():
            errors.append(
                ValidationIssue(
                    row_index=row_num,
                    issue_type="MISSING_IDENTITY_FIELD",
                    message=f"Row {row_num} has empty or missing 'timeframe'.",
                )
            )
            continue

        # Validate MarketIdentity contract (exchange_id, market_type, symbol)
        try:
            market_ident = MarketIdentity(
                exchange_id=exch_raw,
                market_type=mtype_raw,
                symbol=sym_raw,
            )
        except InvalidMarketError as exc:
            errors.append(
                ValidationIssue(
                    row_index=row_num,
                    issue_type="INVALID_MARKET_IDENTITY",
                    message=f"Row {row_num} violates market identity contract: {exc}",
                )
            )
            continue

        exch = market_ident.exchange_id
        mtype = market_ident.market_type
        sym = market_ident.symbol

        # Parse timeframe without pre-lowercasing, so '1M' (months) is preserved and rejected
        try:
            parsed_row_tf = parse_timeframe(tf_raw)
            tf = parsed_row_tf.value
        except InvalidTimeframeError as exc:
            errors.append(
                ValidationIssue(
                    row_index=row_num,
                    issue_type="UNSUPPORTED_TIMEFRAME",
                    message=str(exc),
                )
            )
            continue

        if first_row:
            expected_exchange = exch
            expected_market_type = mtype
            expected_symbol = sym
            expected_timeframe = tf
            interval_delta = timeframe_to_timedelta(parsed_row_tf)
            first_row = False

        # Homogeneity enforcement
        if exch != expected_exchange:
            errors.append(
                ValidationIssue(
                    row_index=row_num,
                    issue_type="MIXED_EXCHANGES",
                    message=f"Mixed exchanges rejected. Found '{exch_raw}' but expected '{expected_exchange}'.",
                )
            )

        if sym != expected_symbol:
            errors.append(
                ValidationIssue(
                    row_index=row_num,
                    issue_type="MIXED_SYMBOLS",
                    message=f"Mixed symbols rejected. Found '{sym_raw}' but expected '{expected_symbol}'.",
                )
            )

        if tf != expected_timeframe:
            errors.append(
                ValidationIssue(
                    row_index=row_num,
                    issue_type="MIXED_TIMEFRAMES",
                    message=f"Mixed timeframes rejected. Found '{tf_raw}' but expected '{expected_timeframe}'.",
                )
            )

        if mtype != expected_market_type:
            errors.append(
                ValidationIssue(
                    row_index=row_num,
                    issue_type="MIXED_MARKET_TYPES",
                    message=f"Mixed market_type rejected. Found '{mtype_raw}' but expected '{expected_market_type}'.",
                )
            )

        # Parse timestamp
        open_time_raw = row.get("open_time", "")
        open_time, ts_issue = _parse_iso_timestamp(open_time_raw, row_num)
        if ts_issue:
            errors.append(ts_issue)
            continue

        assert open_time is not None

        # Parse numeric values
        try:
            open_p = float(row.get("open", "nan"))
            high_p = float(row.get("high", "nan"))
            low_p = float(row.get("low", "nan"))
            close_p = float(row.get("close", "nan"))
            base_vol = float(row.get("base_volume", "nan"))
        except (ValueError, TypeError) as exc:
            errors.append(
                ValidationIssue(
                    row_index=row_num,
                    issue_type="INVALID_NUMERIC_FIELD",
                    message=f"Failed to parse numeric OHLCV field at row {row_num}: {exc}",
                )
            )
            continue

        # Optional quote_volume
        quote_vol = None
        if "quote_volume" in row and row["quote_volume"]:
            try:
                quote_vol = float(row["quote_volume"])
            except ValueError:
                errors.append(
                    ValidationIssue(
                        row_index=row_num,
                        issue_type="INVALID_QUOTE_VOLUME",
                        message=f"Invalid quote_volume at row {row_num}: {row['quote_volume']!r}",
                    )
                )

        # Build candle domain object (enforcing domain invariants)
        try:
            candle = Candle(
                exchange_id=exch,
                market_type=mtype,
                symbol=sym,
                timeframe=tf,
                open_time=open_time,
                open=open_p,
                high=high_p,
                low=low_p,
                close=close_p,
                base_volume=base_vol,
                quote_volume=quote_vol,
                source="csv",
            )
        except (InvalidCandleError, InvalidTimeframeError) as exc:
            errors.append(
                ValidationIssue(
                    row_index=row_num,
                    issue_type="CANDLE_CONTRACT_VIOLATION",
                    message=str(exc),
                )
            )
            continue

        # Chronological ordering, alignment, and duplicates
        if prev_open_time is not None:
            if open_time == prev_open_time:
                errors.append(
                    ValidationIssue(
                        row_index=row_num,
                        issue_type="DUPLICATE_TIMESTAMP",
                        message=f"Duplicate open_time timestamp detected at {open_time.isoformat()}.",
                    )
                )
            elif open_time < prev_open_time:
                errors.append(
                    ValidationIssue(
                        row_index=row_num,
                        issue_type="OUT_OF_ORDER_TIMESTAMP",
                        message=(
                            "Candles are not in strictly increasing chronological order. "
                            f"Current open_time {open_time.isoformat()} is before "
                            f"previous {prev_open_time.isoformat()}. Input is not auto-sorted."
                        ),
                    )
                )
            elif interval_delta is not None:
                expected_open = prev_open_time + interval_delta
                if open_time > expected_open:
                    diff_seconds = (open_time - prev_open_time).total_seconds()
                    step_seconds = interval_delta.total_seconds()
                    remainder = diff_seconds % step_seconds
                    if remainder != 0:
                        errors.append(
                            ValidationIssue(
                                row_index=row_num,
                                issue_type="MISALIGNED_INTERVAL",
                                message=(
                                    f"Candle open_time {open_time.isoformat()} is not aligned to "
                                    f"timeframe '{expected_timeframe}' grid relative to previous candle "
                                    f"{prev_open_time.isoformat()} "
                                    f"(delta of {diff_seconds}s is not an exact multiple of {step_seconds}s)."
                                ),
                            )
                        )
                    else:
                        missing_count = int(diff_seconds // step_seconds) - 1
                        gaps.append(
                            GapReport(
                                expected_open=expected_open,
                                actual_open=open_time,
                                missing_count=missing_count,
                            )
                        )

        prev_open_time = open_time
        valid_candles.append(candle)

    # Empty dataset check (header-only CSV)
    if row_count == 0:
        errors.append(
            ValidationIssue(
                row_index=0,
                issue_type="EMPTY_DATASET",
                message="CSV file contains headers but no candle data rows.",
            )
        )

    has_errors = len(errors) > 0
    is_valid = not has_errors and (not fail_on_gaps or len(gaps) == 0)

    start_t = valid_candles[0].open_time if valid_candles else None
    end_t = valid_candles[-1].open_time if valid_candles else None

    return ValidationResult(
        is_valid=is_valid,
        total_rows=row_count,
        valid_candles=valid_candles if not has_errors else [],
        errors=errors,
        gaps=gaps,
        exchange_id=expected_exchange,
        market_type=expected_market_type,
        symbol=expected_symbol,
        timeframe=expected_timeframe,
        start_time=start_t,
        end_time=end_t,
    )


def validate_candle_csv(
    source: str | Path | TextIO,
    fail_on_gaps: bool = False,
) -> ValidationResult:
    """Validate a CSV file or text stream against the canonical OHLCV contract.

    Args:
        source: File path (str or Path), or readable file-like TextIO object.
        fail_on_gaps: If True, detected gaps make the result invalid.

    Returns:
        ValidationResult object.
    """
    if isinstance(source, (str, Path)):
        p = Path(source)
        if not p.exists():
            return ValidationResult(
                is_valid=False,
                total_rows=0,
                errors=[
                    ValidationIssue(
                        row_index=None,
                        issue_type="FILE_NOT_FOUND",
                        message=f"File not found: {p}",
                    )
                ],
            )
        try:
            with open(p, mode="r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                return validate_candle_stream(reader, fail_on_gaps=fail_on_gaps)
        except Exception as exc:
            return ValidationResult(
                is_valid=False,
                total_rows=0,
                errors=[
                    ValidationIssue(
                        row_index=None,
                        issue_type="IO_ERROR",
                        message=f"Error reading file {p}: {exc}",
                    )
                ],
            )

    reader = csv.DictReader(source)
    return validate_candle_stream(reader, fail_on_gaps=fail_on_gaps)
