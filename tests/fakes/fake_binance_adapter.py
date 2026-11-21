"""Fake Binance adapter for deterministic offline testing."""

from typing import Any, Sequence

from open_quant_intelligence.ingestion.adapter import PublicExchangeAdapter, SpotSymbolMetadata


class FakeBinanceAdapter(PublicExchangeAdapter):
    """Deterministic in-memory fake implementing PublicExchangeAdapter."""

    def __init__(
        self,
        server_time_ms: int = 1700000000000,
        symbols_metadata: dict[str, SpotSymbolMetadata] | None = None,
        klines_pages: dict[tuple[str, str, int], list[list[Any]]] | None = None,
        default_klines: list[list[Any]] | None = None,
    ) -> None:
        self.current_server_time_ms = server_time_ms
        self.symbols_metadata = symbols_metadata or {
            "BTC/USDT": SpotSymbolMetadata(
                symbol="BTC/USDT",
                base="BTC",
                quote="USDT",
                active=True,
                spot=True,
                trading_status="TRADING",
            ),
            "ETH/USDT": SpotSymbolMetadata(
                symbol="ETH/USDT",
                base="ETH",
                quote="USDT",
                active=True,
                spot=True,
                trading_status="TRADING",
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

    def fetch_time(self) -> int:
        self.queried_time_calls.append({"returned": self.current_server_time_ms})
        if self.server_time_sequence:
            self.current_server_time_ms = self.server_time_sequence.pop(0)
        return self.current_server_time_ms

    def fetch_spot_symbol_metadata(self, symbol: str) -> SpotSymbolMetadata:
        self.queried_metadata_calls.append(symbol)
        if symbol not in self.symbols_metadata:
            raise KeyError(f"Symbol {symbol} not found in fake exchange metadata")
        return self.symbols_metadata[symbol]

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
    ) -> Sequence[Sequence[Any]]:
        call_info = {
            "symbol": symbol,
            "timeframe": timeframe,
            "since": since,
            "limit": limit,
        }
        self.queried_kline_calls.append(call_info)

        key = (symbol, timeframe, since or 0)
        if key in self.klines_pages:
            rows = self.klines_pages[key]
            if limit is not None:
                return rows[:limit]
            return rows

        if self.default_klines:
            # Filter rows with timestamp >= since
            filtered = [r for r in self.default_klines if since is None or r[0] >= since]
            if limit is not None:
                return filtered[:limit]
            return filtered

        return []

    def get_last_response_headers(self) -> dict[str, str]:
        return dict(self.custom_headers)
