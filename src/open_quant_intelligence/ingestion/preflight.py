"""Preflight checks for market tradability and future request windows."""

from typing import Any

from open_quant_intelligence.domain.timeframe import SupportedTimeframe
from open_quant_intelligence.ingestion.adapter import SpotSymbolMetadata
from open_quant_intelligence.ingestion.time_utils import timeframe_to_duration_ms


class PreflightError(ValueError):
    """Base exception for ingestion preflight failures."""


class MarketStatusError(PreflightError):
    """Raised when a symbol is delisted, halted, or ineligible for Spot trading."""


class DelistedSymbolError(MarketStatusError):
    """Raised when a symbol is inactive or delisted."""


class UntradableSymbolError(MarketStatusError):
    """Raised when a symbol is not eligible for spot trading."""


class WindowAlignmentError(PreflightError):
    """Raised when requested window violates grid alignment or boundary constraints."""


class InvalidWindowError(WindowAlignmentError):
    """Raised when until_ms <= since_ms."""


class UnalignedBoundaryError(WindowAlignmentError):
    """Raised when since_ms or until_ms is not grid-aligned."""


class FutureWindowError(PreflightError):
    """Raised when until_ms is in the future relative to exchange server time."""


def validate_spot_market_preflight(
    arg1: Any,
    arg2: Any,
) -> SpotSymbolMetadata:
    """Validate that the requested symbol is actively trading on Binance Spot.

    Accepts either (symbol, adapter) or (adapter, symbol).
    """
    if isinstance(arg1, str):
        symbol = arg1
        adapter = arg2
    else:
        adapter = arg1
        symbol = arg2

    metadata = adapter.fetch_spot_symbol_metadata(symbol)

    if metadata.status != "TRADING":
        raise DelistedSymbolError(
            f"Symbol {symbol} is not actively trading on Binance Spot (status: {metadata.status})."
        )

    if not metadata.is_spot_trading_allowed:
        raise UntradableSymbolError(f"Spot trading is explicitly disallowed for symbol {symbol}.")

    if metadata.permissions and "SPOT" not in metadata.permissions:
        raise UntradableSymbolError(f"Symbol {symbol} permissions ({metadata.permissions}) do not include 'SPOT'.")

    return metadata


def validate_request_window_preflight(
    *args: Any,
    **kwargs: Any,
) -> int:
    """Validate window ordering, grid alignment, and reject future boundaries.

    Accepts both:
        validate_request_window_preflight(since_ms, until_ms, timeframe, adapter)
        validate_request_window_preflight(timeframe=..., since_ms=..., until_ms=..., server_time_ms=...)
    """
    if kwargs:
        timeframe = kwargs.get("timeframe") or args[0]
        since_ms = kwargs.get("since_ms", args[1] if len(args) > 1 else None)
        until_ms = kwargs.get("until_ms", args[2] if len(args) > 2 else None)
        server_time_ms = kwargs.get("server_time_ms")
        adapter = kwargs.get("adapter")
    else:
        if isinstance(args[0], SupportedTimeframe):
            timeframe = args[0]
            since_ms = args[1]
            until_ms = args[2]
            server_time_ms = args[3] if len(args) > 3 else None
            adapter = None
        else:
            since_ms = args[0]
            until_ms = args[1]
            timeframe = args[2]
            if isinstance(args[3], (int, float)):
                server_time_ms = int(args[3])
                adapter = None
            else:
                adapter = args[3]
                server_time_ms = None

    if until_ms <= since_ms:
        raise InvalidWindowError(
            f"Invalid query window: until_ms ({until_ms}) must be strictly greater than since_ms ({since_ms})."
        )

    step_ms = timeframe_to_duration_ms(timeframe)
    if since_ms % step_ms != 0:
        raise UnalignedBoundaryError(
            f"Requested since_ms ({since_ms}) is not aligned to {timeframe.value} grid ({step_ms}ms)."
        )
    if until_ms % step_ms != 0:
        raise UnalignedBoundaryError(
            f"Requested until_ms ({until_ms}) is not aligned to {timeframe.value} grid ({step_ms}ms)."
        )

    if server_time_ms is None and adapter is not None:
        server_time_ms = adapter.fetch_time()

    if server_time_ms is not None and until_ms > server_time_ms:
        raise FutureWindowError(
            f"Requested until boundary ({until_ms}ms) is in the future relative to "
            f"exchange server time ({server_time_ms}ms). M1 ingests completed historical data only."
        )

    return (until_ms - since_ms) // step_ms
