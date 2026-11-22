"""Fake Binance adapter for deterministic offline testing."""

from typing import Any, Callable, Sequence, TypeVar

from open_quant_intelligence.ingestion.adapter import (
    IPBannedError,
    MarketNotFoundError,
    PublicExchangeAdapter,
    RateLimitExhaustedError,
    RateLimitPolicy,
    SpotSymbolMetadata,
)

T = TypeVar("T")


class FakeBinanceAdapter(PublicExchangeAdapter):
    """Deterministic in-memory fake implementing PublicExchangeAdapter."""

    def __init__(
        self,
        server_time_ms: int = 1700000000000,
        symbols_metadata: dict[str, SpotSymbolMetadata] | None = None,
        klines_pages: dict[tuple[str, str, int], list[list[Any]]] | None = None,
        default_klines: list[list[Any]] | None = None,
        rate_limit_policy: RateLimitPolicy | None = None,
        simulate_429_count: int = 0,
        simulate_418: bool = False,
        simulate_error: Exception | None = None,
    ) -> None:
        self.current_server_time_ms = server_time_ms
        self.symbols_metadata = symbols_metadata or {
            "BTC/USDT": SpotSymbolMetadata(
                symbol="BTC/USDT",
                status="TRADING",
                is_spot_trading_allowed=True,
                permissions=("SPOT",),
                base_asset="BTC",
                quote_asset="USDT",
            ),
            "ETH/USDT": SpotSymbolMetadata(
                symbol="ETH/USDT",
                status="TRADING",
                is_spot_trading_allowed=True,
                permissions=("SPOT",),
                base_asset="ETH",
                quote_asset="USDT",
            ),
        }
        self.klines_pages = klines_pages or {}
        self.default_klines = default_klines if default_klines is not None else []
        self.queried_kline_calls: list[dict[str, Any]] = []
        self.queried_time_calls: list[dict[str, Any]] = []
        self.queried_metadata_calls: list[str] = []
        self.custom_headers: dict[str, str] = {
            "x-mbx-used-weight-1m": "10",
        }
        self.server_time_sequence: list[int] = []
        self.rate_limit_policy = rate_limit_policy if rate_limit_policy is not None else RateLimitPolicy()
        self.simulate_429_count = simulate_429_count
        self.simulate_418 = simulate_418
        self.simulate_error = simulate_error
        self.recorded_sleeps: list[float] = []

    def _execute_with_simulated_rate_limit(self, op_name: str, fn: Callable[[], T]) -> T:
        attempt = 1
        while True:
            try:
                if self.simulate_418:
                    raise IPBannedError(f"Received HTTP 418 DDoS / IP Ban from Binance during {op_name}")
                if self.simulate_error:
                    raise self.simulate_error
                return fn()
            except Exception as e:
                err_str = str(e).lower()
                if "418" in err_str:
                    raise
                if "429" in err_str:
                    if attempt > self.rate_limit_policy.max_retries:
                        raise RateLimitExhaustedError(
                            f"HTTP 429 rate limit retries exhausted after {self.rate_limit_policy.max_retries} attempts"
                        ) from e
                    wait_s = self.rate_limit_policy.compute_backoff(attempt, self.custom_headers)
                    if wait_s > self.rate_limit_policy.max_wait_seconds:
                        raise RateLimitExhaustedError(
                            f"HTTP 429 Retry-After ({wait_s}s) exceeds max allowed wait "
                            f"({self.rate_limit_policy.max_wait_seconds}s)"
                        ) from e
                    self.recorded_sleeps.append(wait_s)
                    self.rate_limit_policy.sleep_fn(wait_s)
                    attempt += 1
                    continue
                raise

    def fetch_time(self) -> int:
        def _call() -> int:
            result = self.current_server_time_ms
            if self.server_time_sequence:
                self.current_server_time_ms = self.server_time_sequence.pop(0)
            self.queried_time_calls.append({"returned": result})
            return result

        return self._execute_with_simulated_rate_limit("fetch_time", _call)

    def fetch_spot_symbol_metadata(self, symbol: str) -> SpotSymbolMetadata:
        def _call() -> SpotSymbolMetadata:
            self.queried_metadata_calls.append(symbol)
            if symbol not in self.symbols_metadata:
                raise MarketNotFoundError(f"Symbol {symbol} not found on Binance Spot.")
            return self.symbols_metadata[symbol]

        return self._execute_with_simulated_rate_limit("fetch_spot_symbol_metadata", _call)

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
    ) -> Sequence[Sequence[Any]]:
        def _call() -> Sequence[Sequence[Any]]:
            call_info = {
                "symbol": symbol,
                "timeframe": timeframe,
                "since": since,
                "limit": limit,
            }
            self.queried_kline_calls.append(call_info)
            if self.simulate_429_count > 0:
                self.simulate_429_count -= 1
                raise Exception("429 Too Many Requests")

            key = (symbol, timeframe, since or 0)
            if key in self.klines_pages:
                rows = self.klines_pages[key]
                if limit is not None:
                    return rows[:limit]
                return rows

            if self.default_klines:
                try:
                    first_t = self.default_klines[0][0]
                    if not isinstance(first_t, int) or isinstance(first_t, bool):
                        return self.default_klines[:limit] if limit is not None else self.default_klines
                except Exception:
                    return self.default_klines[:limit] if limit is not None else self.default_klines

                filtered = [r for r in self.default_klines if since is None or r[0] >= since]
                if not filtered and len(self.queried_kline_calls) == 1:
                    return self.default_klines[:limit] if limit is not None else self.default_klines

                if limit is not None:
                    return filtered[:limit]
                return filtered

            return []

        return self._execute_with_simulated_rate_limit("fetch_ohlcv", _call)

    def get_last_response_headers(self) -> dict[str, str]:
        return dict(self.custom_headers)

    def get_rate_limit_weight_limit(self) -> int:
        return 1200

    def get_last_used_weight_1m(self) -> int:
        return 10
