"""Deterministic unit tests for bounded HTTP 429 rate limiting and 418 handling."""

from unittest.mock import MagicMock

import pytest

from open_quant_intelligence.domain.timeframe import SupportedTimeframe
from open_quant_intelligence.ingestion.adapter import (
    DEFAULT_CONFIGURED_WEIGHT_LIMIT_1M,
    BinanceVisionPublicAdapter,
    IPBannedError,
    MarketNotFoundError,
    RateLimitExhaustedError,
    RateLimitPolicy,
)
from open_quant_intelligence.ingestion.clock import BinanceServerTimeObserver
from open_quant_intelligence.ingestion.pagination import paginate_ohlcv
from tests.fakes.fake_binance_adapter import FakeBinanceAdapter


def test_rate_limit_429_with_retry_after() -> None:
    """Verifies that 429 with Retry-After sleeps for the exact duration requested."""
    sleeps: list[float] = []
    policy = RateLimitPolicy(max_retries=3, max_wait_seconds=60.0, sleep_fn=sleeps.append)

    fake = FakeBinanceAdapter(rate_limit_policy=policy, simulate_429_count=1)
    fake.custom_headers = {"Retry-After": "5"}
    fake.default_klines = [[0, 100.0, 105.0, 95.0, 102.0, 10.0]]

    # Should retry once, sleep 5.0 seconds, and succeed on second attempt
    rows = fake.fetch_ohlcv("BTC/USDT", "1h", since=0, limit=1)
    assert len(rows) == 1
    assert sleeps == [5.0]


def test_rate_limit_429_without_retry_after_bounded_exponential_backoff() -> None:
    """Verifies that missing Retry-After header uses documented exponential backoff."""
    sleeps: list[float] = []
    policy = RateLimitPolicy(
        max_retries=3,
        initial_backoff_seconds=1.0,
        backoff_factor=2.0,
        max_wait_seconds=60.0,
        sleep_fn=sleeps.append,
    )

    fake = FakeBinanceAdapter(rate_limit_policy=policy, simulate_429_count=3)
    fake.custom_headers = {}  # No Retry-After
    fake.default_klines = [[0, 100.0, 105.0, 95.0, 102.0, 10.0]]

    # 3 retries: attempt 1 -> 1.0s, attempt 2 -> 2.0s, attempt 3 -> 4.0s
    rows = fake.fetch_ohlcv("BTC/USDT", "1h", since=0, limit=1)
    assert len(rows) == 1
    assert sleeps == [1.0, 2.0, 4.0]


def test_rate_limit_429_invalid_retry_after_falls_back_to_exponential() -> None:
    """Verifies that unparseable or non-numeric Retry-After falls back to exponential backoff."""
    sleeps: list[float] = []
    policy = RateLimitPolicy(
        max_retries=2,
        initial_backoff_seconds=1.5,
        backoff_factor=2.0,
        max_wait_seconds=60.0,
        sleep_fn=sleeps.append,
    )

    fake = FakeBinanceAdapter(rate_limit_policy=policy, simulate_429_count=1)
    fake.custom_headers = {"Retry-After": "not-a-number"}
    fake.default_klines = [[0, 100.0, 105.0, 95.0, 102.0, 10.0]]

    rows = fake.fetch_ohlcv("BTC/USDT", "1h", since=0, limit=1)
    assert len(rows) == 1
    assert sleeps == [1.5]


def test_rate_limit_429_budget_exhaustion_raises() -> None:
    """Verifies that exceeding max_retries raises RateLimitExhaustedError."""
    sleeps: list[float] = []
    policy = RateLimitPolicy(max_retries=2, sleep_fn=sleeps.append)

    # Simulate 3 429s when only 2 are allowed
    fake = FakeBinanceAdapter(rate_limit_policy=policy, simulate_429_count=3)
    fake.custom_headers = {"Retry-After": "1"}

    with pytest.raises(RateLimitExhaustedError, match="retries exhausted after 2 attempts"):
        fake.fetch_ohlcv("BTC/USDT", "1h", since=0, limit=1)

    assert len(sleeps) == 2


def test_rate_limit_429_retry_after_exceeding_max_wait_aborts_immediately() -> None:
    """Verifies that a Retry-After exceeding max_wait_seconds aborts without sleeping."""
    sleeps: list[float] = []
    policy = RateLimitPolicy(max_retries=3, max_wait_seconds=30.0, sleep_fn=sleeps.append)

    fake = FakeBinanceAdapter(rate_limit_policy=policy, simulate_429_count=1)
    fake.custom_headers = {"Retry-After": "120"}  # 120s > 30s max wait

    with pytest.raises(RateLimitExhaustedError, match="exceeds max allowed wait"):
        fake.fetch_ohlcv("BTC/USDT", "1h", since=0, limit=1)

    assert len(sleeps) == 0  # Did not sleep


def test_http_418_ip_ban_stops_immediately_never_retried() -> None:
    """HTTP 418 IP ban raises IPBannedError immediately without any retries."""
    sleeps: list[float] = []
    policy = RateLimitPolicy(max_retries=3, sleep_fn=sleeps.append)

    fake = FakeBinanceAdapter(rate_limit_policy=policy, simulate_418=True)

    with pytest.raises(IPBannedError, match="HTTP 418 DDoS / IP Ban"):
        fake.fetch_ohlcv("BTC/USDT", "1h", since=0, limit=1)

    assert len(sleeps) == 0  # Zero retries


def test_non_retryable_error_not_retried() -> None:
    """Non-retryable errors (e.g. MarketNotFoundError) fail immediately without retries."""
    sleeps: list[float] = []
    policy = RateLimitPolicy(max_retries=3, sleep_fn=sleeps.append)

    fake = FakeBinanceAdapter(
        rate_limit_policy=policy,
        simulate_error=MarketNotFoundError("Symbol UNKNOWN/USDT not found on Binance Spot."),
    )

    with pytest.raises(MarketNotFoundError):
        fake.fetch_ohlcv("UNKNOWN/USDT", "1h", since=0, limit=1)

    assert len(sleeps) == 0


def test_pagination_cursor_preserved_during_retries() -> None:
    """During 429 retries, the exact since cursor requested by paginator is preserved."""
    sleeps: list[float] = []
    policy = RateLimitPolicy(max_retries=3, sleep_fn=sleeps.append)

    fake = FakeBinanceAdapter(
        server_time_ms=10_000_000,
        rate_limit_policy=policy,
        simulate_429_count=2,
    )
    fake.custom_headers = {"Retry-After": "2"}
    fake.default_klines = [
        [3_600_000, 100.0, 105.0, 95.0, 102.0, 10.0],
    ]
    clock = BinanceServerTimeObserver(fake)

    # Request pagination starting at cursor 3_600_000
    rows = paginate_ohlcv(
        adapter=fake,
        clock=clock,
        symbol="BTC/USDT",
        timeframe=SupportedTimeframe.H1,
        since_ms=3_600_000,
        until_ms=7_200_000,
    )

    assert len(rows) == 1
    assert rows[0][0] == 3_600_000
    assert len(sleeps) == 2
    # Check that queried calls during retries all maintained exact cursor since=3_600_000
    assert [c["since"] for c in fake.queried_kline_calls[:3]] == [3_600_000, 3_600_000, 3_600_000]


def test_binance_vision_public_adapter_rate_limit_integration() -> None:
    """Tests BinanceVisionPublicAdapter live retry logic with mock ccxt client."""
    mock_ccxt = MagicMock()
    # First call raises RateLimitExceeded, second call succeeds
    mock_ccxt.last_response_headers = {"retry-after": "3", "x-mbx-used-weight-1m": "25"}
    mock_ccxt.fetch_ohlcv.side_effect = [
        Exception("binance 429 Too Many Requests"),
        [[0, 10.0, 11.0, 9.0, 10.5, 1.0]],
    ]
    mock_ccxt.urls = {"api": {}}

    sleeps: list[float] = []
    policy = RateLimitPolicy(max_retries=2, sleep_fn=sleeps.append)
    adapter = BinanceVisionPublicAdapter(exchange_instance=mock_ccxt, rate_limit_policy=policy)

    rows = adapter.fetch_ohlcv("BTC/USDT", "1h", since=0, limit=1)
    assert len(rows) == 1
    assert sleeps == [3.0]
    assert adapter.get_last_used_weight_1m() == 25


def test_binance_vision_public_adapter_http_418_immediate_halt() -> None:
    """Tests that BinanceVisionPublicAdapter immediately halts on HTTP 418."""
    mock_ccxt = MagicMock()
    mock_ccxt.last_response_headers = {}
    mock_ccxt.fetch_time.side_effect = Exception("binance 418 IP banned until...")
    mock_ccxt.urls = {"api": {}}

    sleeps: list[float] = []
    policy = RateLimitPolicy(max_retries=3, sleep_fn=sleeps.append)
    adapter = BinanceVisionPublicAdapter(exchange_instance=mock_ccxt, rate_limit_policy=policy)

    with pytest.raises(IPBannedError, match="HTTP 418 DDoS / IP Ban"):
        adapter.fetch_time()

    assert len(sleeps) == 0


def test_get_rate_limit_weight_limit_contract() -> None:
    """Verifies get_rate_limit_weight_limit contract returns documented configured default or metadata."""
    mock_ccxt = MagicMock()
    mock_ccxt.urls = {"api": {}}
    mock_ccxt.options = {}

    adapter = BinanceVisionPublicAdapter(exchange_instance=mock_ccxt)
    # Default without metadata
    assert adapter.get_rate_limit_weight_limit() == DEFAULT_CONFIGURED_WEIGHT_LIMIT_1M
    assert adapter.get_rate_limit_weight_limit() == 1200

    # With metadata in options
    mock_ccxt2 = MagicMock()
    mock_ccxt2.urls = {"api": {}}
    mock_ccxt2.options = {"requestWeightLimit": 2400}
    adapter2 = BinanceVisionPublicAdapter(exchange_instance=mock_ccxt2)
    assert adapter2.get_rate_limit_weight_limit() == 2400
