"""Unit tests for ServerTimeObserver and time synchronization."""

import time

from open_quant_intelligence.ingestion.clock import BinanceServerTimeObserver
from tests.fakes.fake_binance_adapter import FakeBinanceAdapter


def test_server_time_observer_cache_freshness_and_forced_refresh() -> None:
    """Requirement 3: Asserts samples <= 60s are cached, >60s refresh,
    and force_refresh=True unconditionally queries.
    """
    fake = FakeBinanceAdapter(server_time_ms=1_700_000_000_000)
    observer = BinanceServerTimeObserver(fake)

    # Initial sample
    t1 = observer.get_server_time_ms(force_refresh=False)
    assert t1 == 1_700_000_000_000
    assert len(fake.queried_time_calls) == 1

    # Immediate second call with force_refresh=False must hit cache (no new fetch)
    t2 = observer.get_server_time_ms(force_refresh=False)
    assert t2 >= t1
    assert len(fake.queried_time_calls) == 1

    # Unconditional refresh with force_refresh=True must query exchange
    fake.current_server_time_ms = 1_700_000_010_000
    t3 = observer.get_server_time_ms(force_refresh=True)
    assert t3 == 1_700_000_010_000
    assert len(fake.queried_time_calls) == 2

    # Simulate cache expiration by winding back the observation monotonic timestamp
    observer._last_observation_monotonic = time.monotonic() - 61.0
    fake.current_server_time_ms = 1_700_000_080_000

    t4 = observer.get_server_time_ms(force_refresh=False)
    assert t4 == 1_700_000_080_000
    assert len(fake.queried_time_calls) == 3
