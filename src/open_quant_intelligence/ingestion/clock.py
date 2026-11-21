"""Server time observer protocol and cached observation manager."""

import time
from typing import Protocol

from open_quant_intelligence.ingestion.adapter import PublicExchangeAdapter


class ServerTimeObserver(Protocol):
    """Protocol for observing unextrapolated Binance exchange server time."""

    def get_fresh_server_time_ms(self, force_refresh: bool = False) -> int:
        """Return exchange server time in integer milliseconds."""
        ...


class BinanceServerTimeObserver:
    """Manages unextrapolated exchange server time observations with maximum age cache.

    Guarantees:
    - Never adds local monotonic time to extrapolate server time.
    - If cache age exceeds max_age_seconds (default 60s), fetches a fresh sample.
    - If force_refresh=True, unconditionally queries fresh server time.
    """

    def __init__(self, adapter: PublicExchangeAdapter, max_age_seconds: float = 60.0) -> None:
        self._adapter = adapter
        self._max_age_seconds = max_age_seconds
        self._last_server_time_ms: int | None = None
        self._last_observation_mono: float | None = None

    def get_fresh_server_time_ms(self, force_refresh: bool = False) -> int:
        now_mono = time.monotonic()

        if (
            not force_refresh
            and self._last_server_time_ms is not None
            and self._last_observation_mono is not None
            and (now_mono - self._last_observation_mono) <= self._max_age_seconds
        ):
            return self._last_server_time_ms

        # Issue real server time query
        t_server = self._adapter.fetch_time()
        self._last_server_time_ms = t_server
        self._last_observation_mono = time.monotonic()
        return t_server
