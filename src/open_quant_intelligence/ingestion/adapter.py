"""Public exchange adapter interfaces and Binance Vision client wrapper."""

from dataclasses import dataclass
from typing import Any, Protocol


class PublicAdapterError(Exception):
    """Base exception for public exchange adapter errors."""


class MarketNotFoundError(PublicAdapterError):
    """Raised when the requested symbol is not found on the exchange."""


class PublicAdapterNetworkError(PublicAdapterError):
    """Raised when transport or network communication fails."""


class RateLimitExhaustedError(PublicAdapterError):
    """Raised when HTTP 429 retries are exhausted."""


class IPBannedError(PublicAdapterError):
    """Raised immediately on HTTP 418 DDoS / IP ban."""


@dataclass(frozen=True)
class SpotSymbolMetadata:
    """Standardized metadata for Spot market validation."""

    symbol: str
    status: str
    is_spot_trading_allowed: bool
    permissions: tuple[str, ...]
    base_asset: str
    quote_asset: str


RawKlineRow = list[Any]


class PublicExchangeAdapter(Protocol):
    """Strict allowlisted public market data interface encapsulating CCXT."""

    def fetch_spot_symbol_metadata(self, symbol: str) -> SpotSymbolMetadata:
        """Fetch Spot market metadata from Binance exchangeInfo."""
        ...

    def fetch_ohlcv(self, symbol: str, timeframe: str, since_ms: int, limit: int = 1000) -> list[RawKlineRow]:
        """Fetch raw public OHLCV candles from Binance vision cluster."""
        ...

    def fetch_time(self) -> int:
        """Fetch public Binance server timestamp in milliseconds."""
        ...

    def get_rate_limit_weight_limit(self) -> int:
        """Return maximum IP request weight per minute from exchangeInfo."""
        ...

    def get_last_used_weight_1m(self) -> int:
        """Return the latest recorded X-MBX-USED-WEIGHT-1M header value."""
        ...


class BinanceVisionPublicAdapter:
    """Strict allowlisted public adapter encapsulating sanitized CCXT 4.4.52."""

    def __init__(self, exchange_instance: Any = None) -> None:
        if exchange_instance is None:
            import ccxt

            self._exchange = ccxt.binance(
                {
                    "enableRateLimit": True,
                    "options": {
                        "defaultType": "spot",
                        "adjustForTimeDifference": False,
                    },
                    "urls": {
                        "api": {
                            "public": "https://data-api.binance.vision/api/v3",
                        }
                    },
                }
            )
        else:
            self._exchange = exchange_instance

        # Sanitize URLs explicitly: purge all private, SAPI, futures, options, and WS entries
        self._exchange.urls["api"] = {
            "public": "https://data-api.binance.vision/api/v3",
        }
        if "ws" in self._exchange.urls:
            self._exchange.urls["ws"] = {}

        # Seal unnecessary and private methods
        self._exchange.create_order = None
        self._exchange.cancel_order = None
        self._exchange.fetch_balance = None
        self._exchange.fetch_orders = None

        self._cached_rate_limit: int | None = None
        self._last_used_weight_1m: int = 0

    def _capture_headers(self) -> None:
        headers = getattr(self._exchange, "last_response_headers", {}) or {}
        # Binance sends X-MBX-USED-WEIGHT-1M (case-insensitive in standard mapping)
        for k, v in headers.items():
            if k.lower() == "x-mbx-used-weight-1m":
                try:
                    self._last_used_weight_1m = int(v)
                except (ValueError, TypeError):
                    pass

    def fetch_spot_symbol_metadata(self, symbol: str) -> SpotSymbolMetadata:
        try:
            markets = self._exchange.load_markets()
            self._capture_headers()
        except Exception as e:
            raise PublicAdapterNetworkError(f"Failed to load markets: {e}") from e

        # Normalize symbol mapping (e.g. BTC/USDT)
        if symbol not in markets:
            raise MarketNotFoundError(f"Symbol {symbol} not found on Binance Spot.")

        market_info = markets[symbol]
        info = market_info.get("info", {})

        status = info.get("status", "UNKNOWN")
        is_spot_allowed = bool(info.get("isSpotTradingAllowed", False))
        permissions = tuple(info.get("permissions", []))
        base_asset = market_info.get("base", "")
        quote_asset = market_info.get("quote", "")

        return SpotSymbolMetadata(
            symbol=symbol,
            status=status,
            is_spot_trading_allowed=is_spot_allowed,
            permissions=permissions,
            base_asset=base_asset,
            quote_asset=quote_asset,
        )

    def fetch_ohlcv(self, symbol: str, timeframe: str, since_ms: int, limit: int = 1000) -> list[RawKlineRow]:
        try:
            rows = self._exchange.fetch_ohlcv(symbol, timeframe, since=since_ms, limit=limit)
            self._capture_headers()
            return rows
        except Exception as e:
            err_str = str(e).lower()
            if "418" in err_str:
                raise IPBannedError(f"Received HTTP 418 DDoS / IP Ban from Binance: {e}") from e
            if "429" in err_str:
                raise RateLimitExhaustedError(f"Received HTTP 429 Rate Limit from Binance: {e}") from e
            raise PublicAdapterNetworkError(f"fetch_ohlcv failed: {e}") from e

    def fetch_time(self) -> int:
        try:
            server_time = self._exchange.fetch_time()
            self._capture_headers()
            return int(server_time)
        except Exception as e:
            raise PublicAdapterNetworkError(f"fetch_time failed: {e}") from e

    def get_rate_limit_weight_limit(self) -> int:
        if self._cached_rate_limit is not None:
            return self._cached_rate_limit
        # Default spot request weight limit per minute on Binance
        self._cached_rate_limit = 1200
        return self._cached_rate_limit

    def get_last_used_weight_1m(self) -> int:
        return self._last_used_weight_1m
