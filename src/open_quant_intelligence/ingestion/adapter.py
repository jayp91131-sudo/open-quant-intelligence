"""Public exchange adapter interfaces and Binance Vision client wrapper."""

import time
from dataclasses import dataclass
from typing import Any, Callable, Protocol, Sequence, TypeVar, runtime_checkable

T = TypeVar("T")

DEFAULT_CONFIGURED_WEIGHT_LIMIT_1M: int = 1200


@dataclass
class RateLimitPolicy:
    """Configurable bounded retry policy for public HTTP requests."""

    max_retries: int = 3
    initial_backoff_seconds: float = 1.0
    backoff_factor: float = 2.0
    max_wait_seconds: float = 60.0
    sleep_fn: Callable[[float], None] = time.sleep

    def compute_backoff(self, attempt: int, headers: dict[str, str] | None = None) -> float:
        """Compute backoff wait time in seconds.

        1. If 'Retry-After' header is present and valid positive number, respect it.
        2. Otherwise, apply documented bounded fallback exponential backoff:
           min(max_wait_seconds, initial_backoff_seconds * (backoff_factor ** (attempt - 1)))
        """
        if headers:
            for k, v in headers.items():
                if k.lower() == "retry-after":
                    try:
                        parsed = float(str(v).strip())
                        if parsed >= 0:
                            return parsed
                    except (ValueError, TypeError):
                        break

        # Fallback bounded exponential backoff
        exp_backoff = self.initial_backoff_seconds * (self.backoff_factor ** (attempt - 1))
        return min(self.max_wait_seconds, exp_backoff)


class PublicAdapterError(Exception):
    """Base exception for public exchange adapter errors."""


class MarketNotFoundError(PublicAdapterError, KeyError):
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

    def __init__(
        self,
        symbol: str,
        status: str = "TRADING",
        is_spot_trading_allowed: bool = True,
        permissions: tuple[str, ...] = ("SPOT",),
        base_asset: str = "",
        quote_asset: str = "",
        base: str | None = None,
        quote: str | None = None,
        active: bool | None = None,
        spot: bool | None = None,
        trading_status: str | None = None,
    ) -> None:
        object.__setattr__(self, "symbol", symbol)
        final_status = trading_status if trading_status is not None else status
        if active is False:
            final_status = "BREAK"
        object.__setattr__(self, "status", final_status)
        final_spot = spot if spot is not None else is_spot_trading_allowed
        object.__setattr__(self, "is_spot_trading_allowed", final_spot)
        object.__setattr__(self, "permissions", permissions)
        object.__setattr__(self, "base_asset", base if base is not None else base_asset)
        object.__setattr__(self, "quote_asset", quote if quote is not None else quote_asset)

    @property
    def base(self) -> str:
        return self.base_asset

    @property
    def quote(self) -> str:
        return self.quote_asset

    @property
    def active(self) -> bool:
        return self.status == "TRADING"

    @property
    def spot(self) -> bool:
        return self.is_spot_trading_allowed

    @property
    def trading_status(self) -> str:
        return self.status


RawKlineRow = list[Any]


def _sealed_method(*args: Any, **kwargs: Any) -> None:
    raise RuntimeError("Sealed: private trading and account operations are prohibited in M1.")


@runtime_checkable
class PublicExchangeAdapter(Protocol):
    """Strict allowlisted public market data interface encapsulating CCXT."""

    def fetch_spot_symbol_metadata(self, symbol: str) -> SpotSymbolMetadata:
        """Fetch Spot market metadata from Binance exchangeInfo."""
        ...

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = 1000,
        **kwargs: Any,
    ) -> Sequence[Sequence[Any]]:
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

    def get_last_response_headers(self) -> dict[str, str]:
        """Return raw response headers from the last HTTP request."""
        ...


class BinanceVisionPublicAdapter:
    """Strict allowlisted public adapter encapsulating sanitized CCXT 4.4.52."""

    def __init__(
        self,
        exchange_instance: Any = None,
        rate_limit_policy: RateLimitPolicy | None = None,
    ) -> None:
        self.rate_limit_policy = rate_limit_policy if rate_limit_policy is not None else RateLimitPolicy()
        if exchange_instance is None:
            import ccxt

            self._exchange = ccxt.binance(
                {
                    "enableRateLimit": True,
                    "options": {
                        "defaultType": "spot",
                        "fetchMarkets": ["spot"],
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
        if hasattr(self._exchange, "options") and isinstance(self._exchange.options, dict):
            self._exchange.options["fetchMarkets"] = ["spot"]
        self._exchange.urls["api"] = {
            "public": "https://data-api.binance.vision/api/v3",
        }
        if "ws" in self._exchange.urls:
            self._exchange.urls["ws"] = {}

        # Seal unnecessary and private methods
        trading_methods = [
            "create_order",
            "cancel_order",
            "cancel_all_orders",
            "edit_order",
            "fetch_balance",
            "fetch_my_trades",
            "fetch_orders",
            "fetch_open_orders",
            "fetch_closed_orders",
            "transfer",
            "withdraw",
            "deposit",
        ]
        for m in trading_methods:
            setattr(self._exchange, m, _sealed_method)

        self._cached_rate_limit: int | None = None
        self._last_used_weight_1m: int = 0

    @property
    def client(self) -> Any:
        return self._exchange

    def get_last_response_headers(self) -> dict[str, str]:
        return getattr(self._exchange, "last_response_headers", {}) or {}

    def _capture_headers(self) -> None:
        headers = getattr(self._exchange, "last_response_headers", {}) or {}
        # Binance sends X-MBX-USED-WEIGHT-1M (case-insensitive in standard mapping)
        for k, v in headers.items():
            if k.lower() == "x-mbx-used-weight-1m":
                try:
                    self._last_used_weight_1m = int(v)
                except (ValueError, TypeError):
                    pass

    def _execute_with_rate_limit(self, op_name: str, fn: Callable[[], T]) -> T:
        attempt = 1
        while True:
            try:
                result = fn()
                self._capture_headers()
                return result
            except Exception as e:
                self._capture_headers()
                headers = self.get_last_response_headers()
                err_str = str(e).lower()
                err_type = type(e).__name__.lower()

                # 1. HTTP 418: immediate halt, never retry
                if "418" in err_str or "ddos" in err_type or "banned" in err_str:
                    raise IPBannedError(f"Received HTTP 418 DDoS / IP Ban from Binance during {op_name}: {e}") from e

                # 2. HTTP 429: rate limit handling
                if "429" in err_str or "ratelimit" in err_type:
                    if attempt > self.rate_limit_policy.max_retries:
                        raise RateLimitExhaustedError(
                            f"HTTP 429 rate limit retries exhausted after {self.rate_limit_policy.max_retries} "
                            f"attempts for {op_name}: {e}"
                        ) from e

                    wait_s = self.rate_limit_policy.compute_backoff(attempt, headers)
                    if wait_s > self.rate_limit_policy.max_wait_seconds:
                        raise RateLimitExhaustedError(
                            f"HTTP 429 Retry-After ({wait_s}s) exceeds max allowed wait "
                            f"({self.rate_limit_policy.max_wait_seconds}s) for {op_name}."
                        ) from e

                    self.rate_limit_policy.sleep_fn(wait_s)
                    attempt += 1
                    continue

                # 3. Non-retryable domain / client errors
                if "marketnotfound" in err_type or "symbol not found" in err_str:
                    raise MarketNotFoundError(f"Symbol not found: {e}") from e

                # 4. Other network failures (non-429 / non-418 are not retried)
                raise PublicAdapterNetworkError(f"{op_name} failed: {e}") from e

    def fetch_spot_symbol_metadata(self, symbol: str) -> SpotSymbolMetadata:
        try:
            markets = self._execute_with_rate_limit("load_markets", self._exchange.load_markets)
        except MarketNotFoundError:
            raise
        except Exception as e:
            if isinstance(e, (IPBannedError, RateLimitExhaustedError)):
                raise
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

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = 1000,
        since_ms: int | None = None,
        **kwargs: Any,
    ) -> list[RawKlineRow]:
        effective_since = since if since is not None else since_ms
        return self._execute_with_rate_limit(
            "fetch_ohlcv",
            lambda: self._exchange.fetch_ohlcv(symbol, timeframe, since=effective_since, limit=limit),
        )

    def fetch_time(self) -> int:
        server_time = self._execute_with_rate_limit("fetch_time", self._exchange.fetch_time)
        return int(server_time)

    def get_rate_limit_weight_limit(self) -> int:
        """Return the maximum Spot IP request weight limit per 1-minute window.

        Contract & Derivation:
        1. If exchange metadata specifies a configured weight limit, it is returned.
        2. Otherwise, this returns DEFAULT_CONFIGURED_WEIGHT_LIMIT_1M (1200 weight/min),
           which is the conservative configured default defined in Binance Spot Public API
           documentation. This value is an explicit configured invariant rather than
           synthetic metadata.
        """
        if self._cached_rate_limit is not None:
            return self._cached_rate_limit

        try:
            options = getattr(self._exchange, "options", {}) or {}
            weight_limit = options.get("requestWeightLimit")
            if weight_limit and isinstance(weight_limit, int) and weight_limit > 0:
                self._cached_rate_limit = weight_limit
                return self._cached_rate_limit
        except Exception:
            pass

        self._cached_rate_limit = DEFAULT_CONFIGURED_WEIGHT_LIMIT_1M
        return self._cached_rate_limit

    def get_last_used_weight_1m(self) -> int:
        return self._last_used_weight_1m
