"""Preflight checks for market tradability and future request windows."""

from open_quant_intelligence.domain.timeframe import SupportedTimeframe
from open_quant_intelligence.ingestion.adapter import PublicExchangeAdapter, SpotSymbolMetadata
from open_quant_intelligence.ingestion.time_utils import timeframe_to_duration_ms


class PreflightError(ValueError):
    """Base exception for ingestion preflight failures."""


class MarketStatusError(PreflightError):
    """Raised when a symbol is delisted, halted, or ineligible for Spot trading."""


class InvalidWindowError(PreflightError):
    """Raised when until_ms <= since_ms."""


class UnalignedBoundaryError(PreflightError):
    """Raised when since_ms or until_ms is not grid-aligned."""


class FutureWindowError(PreflightError):
    """Raised when until_ms is in the future relative to exchange server time."""


def validate_spot_market_preflight(
    symbol: str,
    adapter: PublicExchangeAdapter,
) -> SpotSymbolMetadata:
    """Validate that the requested symbol is actively trading on Binance Spot.

    Args:
        symbol: Market pair identifier (e.g. 'BTC/USDT').
        adapter: Allowlisted public exchange adapter.

    Returns:
        SpotSymbolMetadata if trading is active and authorized.

    Raises:
        MarketStatusError: If market is halted, delisted, or not eligible for spot trading.
    """
    metadata = adapter.fetch_spot_symbol_metadata(symbol)

    if metadata.status != "TRADING":
        raise MarketStatusError(f"Symbol {symbol} is not actively trading on Binance Spot (status: {metadata.status}).")

    if not metadata.is_spot_trading_allowed:
        raise MarketStatusError(f"Spot trading is explicitly disallowed for symbol {symbol}.")

    if metadata.permissions and "SPOT" not in metadata.permissions:
        raise MarketStatusError(f"Symbol {symbol} permissions ({metadata.permissions}) do not include 'SPOT'.")

    return metadata


def validate_request_window_preflight(
    since_ms: int,
    until_ms: int,
    timeframe: SupportedTimeframe,
    adapter: PublicExchangeAdapter,
) -> int:
    """Validate window ordering, grid alignment, and reject future boundaries.

    Args:
        since_ms: Start timestamp in epoch milliseconds.
        until_ms: Upper boundary in epoch milliseconds.
        timeframe: Supported candle timeframe enum member.
        adapter: Allowlisted public exchange adapter.

    Returns:
        Expected number of intervals in the half-open window.

    Raises:
        InvalidWindowError: If until_ms <= since_ms.
        UnalignedBoundaryError: If boundaries are not grid-aligned.
        FutureWindowError: If until_ms > fresh_server_time_ms.
    """
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

    # Fresh server-time preflight check (Zero Kline requests if this fails)
    fresh_server_time_ms = adapter.fetch_time()
    if until_ms > fresh_server_time_ms:
        raise FutureWindowError(
            f"Requested until boundary ({until_ms}ms) is in the future relative to "
            f"exchange server time ({fresh_server_time_ms}ms). M1 ingests completed historical data only."
        )

    return (until_ms - since_ms) // step_ms
