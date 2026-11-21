"""Deterministic historical OHLCV pagination and incomplete tail candle protection."""

import math
import time

from open_quant_intelligence.domain.timeframe import SupportedTimeframe
from open_quant_intelligence.ingestion.adapter import PublicExchangeAdapter, RawKlineRow
from open_quant_intelligence.ingestion.clock import ServerTimeObserver
from open_quant_intelligence.ingestion.time_utils import timeframe_to_duration_ms


class PaginationError(Exception):
    """Base exception for pagination errors."""


class MalformedPageError(PaginationError):
    """Raised when a page contains unparseable, malformed, or out-of-order rows."""


class MalformedKlineError(PaginationError):
    """Raised when an admitted row has non-numeric, non-finite, or invalid OHLCV fields."""


class PaginationRegressionError(PaginationError):
    """Raised when a page regresses or stalls with no forward cursor progress."""


class PageOverlapError(PaginationError):
    """Raised when an admitted timestamp was already processed in an earlier page."""


class PaginationBudgetExceededError(PaginationError):
    """Raised when request iteration count exceeds the safety ceiling."""


class IncompleteTailCandleError(PaginationError):
    """Raised when an in-range tail candle remains forming after finalization retries."""


class RefetchEmptyError(PaginationError):
    """Raised when re-fetch returns an empty response."""


class RefetchRowCountError(PaginationError):
    """Raised when re-fetch limit=1 returns more or less than 1 row."""


class RefetchTimestampMismatchError(PaginationError):
    """Raised when re-fetched row timestamp does not match requested cursor."""


def validate_raw_kline_fields(row: RawKlineRow) -> None:
    """Validate numeric, price, and volume invariants of an admitted Kline row.

    Raises:
        MalformedKlineError: If fields are non-numeric, non-finite, or violate bounds.
    """
    if not isinstance(row, (list, tuple)) or len(row) != 6:
        raise MalformedKlineError(f"Row is not a 6-element sequence: {row!r}")
    t, o, h, low_price, c, v = row
    if not isinstance(t, int) or isinstance(t, bool) or t < 0:
        raise MalformedKlineError(f"Row timestamp {t!r} is not a valid epoch millisecond integer.")
    for val_name, val in [("open", o), ("high", h), ("low", low_price), ("close", c), ("volume", v)]:
        if not isinstance(val, (int, float)) or isinstance(val, bool):
            raise MalformedKlineError(f"Field {val_name} ({val!r}) must be numeric int or float.")
        if not math.isfinite(val):
            raise MalformedKlineError(f"Field {val_name} ({val!r}) must be finite.")
    if min(o, h, low_price, c) <= 0:
        raise MalformedKlineError(f"Prices must be strictly positive: open={o}, high={h}, low={low_price}, close={c}")
    if v < 0:
        raise MalformedKlineError(f"Volume must be non-negative: volume={v}")


def paginate_ohlcv(
    adapter: PublicExchangeAdapter,
    symbol: str,
    timeframe: SupportedTimeframe,
    since_ms: int,
    until_ms: int,
    clock: ServerTimeObserver,
    max_tail_retries: int = 3,
    retry_delay_seconds: float = 2.0,
) -> list[RawKlineRow]:
    """Deterministically paginate historical OHLCV from Binance vision public endpoints.

    Strict Invariants:
    1. Rows outside [since_ms, until_ms) are discarded before field validation.
    2. Malformed timestamps raise MalformedPageError without silent skipping.
    3. Incomplete tail candles discard initial values, preserve cursor, and re-fetch fresh OHLCV upon finalization.
    4. Re-fetched row must have exactly 1 row, 6 elements, matching timestamp, and valid OHLCV fields.
    5. Cursor advances strictly monotonically; regression or zero progress raises an error.
    """
    step_ms = timeframe_to_duration_ms(timeframe)
    expected_count = (until_ms - since_ms) // step_ms
    max_pages = max(50, ((expected_count + 999) // 1000) * 3)

    current_since_ms = since_ms
    admitted_candles: list[RawKlineRow] = []
    seen_timestamps: set[int] = set()
    page_count = 0

    while current_since_ms < until_ms:
        page_count += 1
        if page_count > max_pages:
            raise PaginationBudgetExceededError(f"Pagination exceeded safety budget of {max_pages} requests.")

        page = adapter.fetch_ohlcv(symbol, timeframe.value, since_ms=current_since_ms, limit=1000)

        # 1. Empty page: exchange has no further data
        if not page:
            break

        # 2. Strict Raw Row Structure & Timestamp Validation
        page_timestamps: list[int] = []
        for i, row in enumerate(page):
            if not isinstance(row, (list, tuple)) or len(row) < 1:
                raise MalformedPageError(f"Row {i} is not a valid sequence: {row!r}")
            t = row[0]
            if not isinstance(t, int) or isinstance(t, bool) or t < 0:
                raise MalformedPageError(f"Row {i} timestamp {t!r} is not a valid integer millisecond timestamp.")
            if page_timestamps and t <= page_timestamps[-1]:
                raise MalformedPageError(f"Page timestamps not strictly increasing: {page_timestamps[-1]} -> {t}")
            page_timestamps.append(t)

        first_raw_t = page_timestamps[0]
        max_raw_t = page_timestamps[-1]

        # 3. Non-Advancing & Regression Checks
        if max_raw_t < current_since_ms:
            raise PaginationRegressionError(
                f"Page regression: max timestamp {max_raw_t}ms is before cursor {current_since_ms}ms"
            )

        # 4. Extract In-Range Rows
        # Out-of-range rows (t < since_ms or t >= until_ms) are discarded without validating OHLCV fields.
        in_range_rows: list[RawKlineRow] = [row for row in page if since_ms <= row[0] < until_ms]

        # Terminal condition: All returned rows are beyond requested range
        if not in_range_rows and first_raw_t >= until_ms:
            break

        # If page had valid timestamps but none in range:
        if not in_range_rows:
            next_cursor = max_raw_t + 1
            if next_cursor <= current_since_ms:
                raise PaginationRegressionError(f"Pagination stalled with no forward progress at {current_since_ms}ms")
            current_since_ms = next_cursor
            continue

        # 5. Process In-Range Rows and Guard Incomplete Tail
        obs_server_time_ms = clock.get_fresh_server_time_ms(force_refresh=False)

        for raw_kline in in_range_rows:
            open_ms = raw_kline[0]
            close_ms = open_ms + step_ms

            if open_ms in seen_timestamps:
                raise PageOverlapError(f"Timestamp {open_ms} was already admitted in a previous page.")

            # Check if candle is incomplete / forming at observation time
            if close_ms > obs_server_time_ms:
                # Incomplete tail candle detected!
                # Discard original incomplete OHLCV values WITHOUT validating them.
                # Preserve cursor: do not advance past it.
                finalized = False
                for _attempt in range(max_tail_retries):
                    time.sleep(retry_delay_seconds)
                    # Genuinely fresh server-time request on every retry
                    fresh_server_time_ms = clock.get_fresh_server_time_ms(force_refresh=True)
                    if close_ms <= fresh_server_time_ms:
                        # Re-fetch fresh OHLCV from the exact cursor to obtain finalized values
                        refetched = adapter.fetch_ohlcv(symbol, timeframe.value, since_ms=open_ms, limit=1)

                        if not refetched:
                            raise RefetchEmptyError(f"Re-fetch at cursor {open_ms}ms returned empty response.")
                        if len(refetched) != 1:
                            raise RefetchRowCountError(
                                f"Re-fetch at cursor {open_ms}ms returned {len(refetched)} rows; expected 1."
                            )
                        refetched_row = refetched[0]
                        if not isinstance(refetched_row, (list, tuple)) or len(refetched_row) != 6:
                            raise MalformedKlineError(f"Re-fetched row is not a 6-element sequence: {refetched_row!r}")
                        if refetched_row[0] != open_ms:
                            raise RefetchTimestampMismatchError(
                                f"Re-fetch at cursor {open_ms}ms returned mismatched timestamp {refetched_row[0]}ms."
                            )

                        validate_raw_kline_fields(refetched_row)
                        admitted_candles.append(refetched_row)
                        seen_timestamps.add(open_ms)
                        current_since_ms = open_ms + 1
                        finalized = True
                        break

                if not finalized:
                    raise IncompleteTailCandleError(
                        f"Candle at open_time {open_ms}ms remained forming after retries. Aborting ingestion."
                    )
            else:
                # Candle is fully closed: validate numeric OHLCV fields and admit
                validate_raw_kline_fields(raw_kline)
                admitted_candles.append(raw_kline)
                seen_timestamps.add(open_ms)
                current_since_ms = open_ms + 1

        # Advance cursor past page maximum if all rows in page were processed
        next_cursor = max_raw_t + 1
        if next_cursor > current_since_ms:
            current_since_ms = next_cursor

    return admitted_candles
