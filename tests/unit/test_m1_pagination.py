"""Unit tests for robust Kline pagination and tail guard handling."""

from typing import Any, Sequence

import pytest
from open_quant_intelligence.data.timeframe import SupportedTimeframe

from open_quant_intelligence.ingestion.clock import BinanceServerTimeObserver
from open_quant_intelligence.ingestion.pagination import (
    IncompleteTailCandleError,
    MalformedKlineError,
    MalformedPageError,
    PaginationRegressionError,
    RefetchEmptyError,
    RefetchRowCountError,
    RefetchTimestampMismatchError,
    paginate_ohlcv,
)
from tests.fakes.fake_binance_adapter import FakeBinanceAdapter


def test_out_of_range_row_corruption_ignored() -> None:
    """Requirement 14: Asserts out-of-range rows are discarded before validation and do not cause failure."""
    # Request window: [0, 7_200_000) (2 hours)
    # Return page:
    # row 0: open_time=0 (in range)
    # row 1: open_time=3_600_000 (in range)
    # row 2: open_time=7_200_000 (out of range, malformed OHLCV: -9999, string close, None volume)
    page: list[list[Any]] = [
        [0, 100.0, 105.0, 95.0, 102.0, 10.0],
        [3_600_000, 102.0, 106.0, 101.0, 105.0, 15.0],
        [7_200_000, "corrupt", -999.0, None, "bad_close", "bad_vol"],
    ]

    fake = FakeBinanceAdapter(
        server_time_ms=10_000_000,
        default_klines=page,
    )
    clock = BinanceServerTimeObserver(fake)

    admitted = paginate_ohlcv(
        adapter=fake,
        clock=clock,
        symbol="BTC/USDT",
        timeframe=SupportedTimeframe.H1,
        since_ms=0,
        until_ms=7_200_000,
    )

    # Must only contain the 2 in-range rows; corrupted out-of-range row must be ignored cleanly
    assert len(admitted) == 2
    assert admitted[0][0] == 0
    assert admitted[1][0] == 3_600_000


def test_malformed_timestamp_raises_structured_error() -> None:
    """Requirement 12: Asserts row with string, missing, or negative timestamp raises MalformedPageError."""
    # Row with string timestamp
    bad_page_str: list[list[Any]] = [["2026-01-01", 100.0, 105.0, 95.0, 102.0, 10.0]]
    fake = FakeBinanceAdapter(server_time_ms=10_000_000, default_klines=bad_page_str)
    clock = BinanceServerTimeObserver(fake)

    with pytest.raises(MalformedPageError, match="timestamp must be an int"):
        paginate_ohlcv(
            adapter=fake,
            clock=clock,
            symbol="BTC/USDT",
            timeframe=SupportedTimeframe.H1,
            since_ms=0,
            until_ms=7_200_000,
        )

    # Row with negative timestamp
    bad_page_neg: list[list[Any]] = [[-500, 100.0, 105.0, 95.0, 102.0, 10.0]]
    fake2 = FakeBinanceAdapter(server_time_ms=10_000_000, default_klines=bad_page_neg)
    clock2 = BinanceServerTimeObserver(fake2)

    with pytest.raises(MalformedPageError, match="negative timestamp"):
        paginate_ohlcv(
            adapter=fake2,
            clock=clock2,
            symbol="BTC/USDT",
            timeframe=SupportedTimeframe.H1,
            since_ms=0,
            until_ms=7_200_000,
        )


def test_pagination_regression_and_stall_rejection() -> None:
    """Requirement 13: Asserts page with timestamps prior to cursor
    or zero progress raises PaginationRegressionError.
    """
    # Cursor starts at 3_600_000, page returns row at 0
    page: list[list[Any]] = [[0, 100.0, 105.0, 95.0, 102.0, 10.0]]
    fake = FakeBinanceAdapter(server_time_ms=10_000_000, default_klines=page)
    clock = BinanceServerTimeObserver(fake)

    with pytest.raises(PaginationRegressionError, match="did not advance cursor"):
        paginate_ohlcv(
            adapter=fake,
            clock=clock,
            symbol="BTC/USDT",
            timeframe=SupportedTimeframe.H1,
            since_ms=3_600_000,
            until_ms=7_200_000,
        )


def test_incomplete_tail_discard_refetch_validate() -> None:
    """Requirement 1: Asserts incomplete candle original values are discarded without price validation,
    cursor is preserved, fresh server time is fetched on retry, and refetched row is fully validated upon finalization.
    """
    # 1h candle at 3_600_000 closes at 7_200_000
    # Initially server_time is 5_000_000 (candle is incomplete)
    # The initial raw row has invalid prices (high < low, negative price) which would fail validation!
    # But because it is forming, original values are discarded without validation.
    initial_forming_row: list[list[Any]] = [
        [3_600_000, -999.0, 50.0, 200.0, -100.0, 10.0]  # Corrupted forming prices
    ]
    # Refetched row upon finalization (server_time >= 7_200_000): valid complete candle
    finalized_row: list[list[Any]] = [[3_600_000, 100.0, 105.0, 95.0, 104.0, 50.0]]

    fake = FakeBinanceAdapter(server_time_ms=5_000_000)
    fake.klines_pages = {
        ("BTC/USDT", "1h", 3_600_000): initial_forming_row,
    }
    # Provide sequence of server times for retries:
    # Retry 1: 5_500_000 (still forming)
    # Retry 2: 7_201_000 (finalized!)
    fake.server_time_sequence = [5_500_000, 7_201_000]

    # When refetched, adapter should return finalized row
    # We dynamically switch the klines page once server time advances
    original_fetch_ohlcv = fake.fetch_ohlcv

    def dynamic_fetch_ohlcv(
        symbol: str, timeframe: str = "1m", since: int | None = None, limit: int | None = None
    ) -> Sequence[Sequence[Any]]:
        if limit == 1 and since == 3_600_000 and fake.current_server_time_ms >= 7_200_000:
            return finalized_row
        return original_fetch_ohlcv(symbol, timeframe, since, limit)

    fake.fetch_ohlcv = dynamic_fetch_ohlcv  # type: ignore

    clock = BinanceServerTimeObserver(fake)

    admitted = paginate_ohlcv(
        adapter=fake,
        clock=clock,
        symbol="BTC/USDT",
        timeframe=SupportedTimeframe.H1,
        since_ms=3_600_000,
        until_ms=7_200_000,
        poll_interval_s=0.01,
    )

    assert len(admitted) == 1
    # Check that the admitted row contains the finalized prices, NOT the corrupted forming prices
    assert admitted[0][0] == 3_600_000
    assert admitted[0][1] == 100.0
    assert admitted[0][4] == 104.0


def test_refetched_response_structural_failure_cases() -> None:
    """Requirement 2: Covers 4 refetch failures: empty, multiple rows, malformed, mismatched timestamp."""
    # 1. Empty response -> RefetchEmptyError
    fake1 = FakeBinanceAdapter(server_time_ms=5_000_000)
    fake1.klines_pages = {("BTC/USDT", "1h", 0): [[0, 10.0, 11.0, 9.0, 10.5, 1.0]]}
    fake1.server_time_sequence = [7_201_000]
    # For limit=1, return empty
    fake1.fetch_ohlcv = lambda s, t="1m", since=None, limit=None: (
        [] if limit == 1 else [[0, 10.0, 11.0, 9.0, 10.5, 1.0]]
    )  # type: ignore
    clock1 = BinanceServerTimeObserver(fake1)

    with pytest.raises(RefetchEmptyError):
        paginate_ohlcv(
            adapter=fake1,
            clock=clock1,
            symbol="BTC/USDT",
            timeframe=SupportedTimeframe.H1,
            since_ms=0,
            until_ms=3_600_000,
            poll_interval_s=0.01,
        )

    # 2. Multiple rows -> RefetchRowCountError
    fake2 = FakeBinanceAdapter(server_time_ms=5_000_000)
    fake2.server_time_sequence = [7_201_000]
    fake2.fetch_ohlcv = lambda s, t="1m", since=None, limit=None: (  # type: ignore
        [[0, 10.0, 11.0, 9.0, 10.5, 1.0], [3_600_000, 10.0, 11.0, 9.0, 10.5, 1.0]]
        if limit == 1
        else [[0, 10.0, 11.0, 9.0, 10.5, 1.0]]
    )
    clock2 = BinanceServerTimeObserver(fake2)

    with pytest.raises(RefetchRowCountError):
        paginate_ohlcv(
            adapter=fake2,
            clock=clock2,
            symbol="BTC/USDT",
            timeframe=SupportedTimeframe.H1,
            since_ms=0,
            until_ms=3_600_000,
            poll_interval_s=0.01,
        )

    # 3. Malformed row (<6 fields) -> MalformedKlineError
    fake3 = FakeBinanceAdapter(server_time_ms=5_000_000)
    fake3.server_time_sequence = [7_201_000]
    fake3.fetch_ohlcv = lambda s, t="1m", since=None, limit=None: (  # type: ignore
        [[0, 10.0, 11.0]] if limit == 1 else [[0, 10.0, 11.0, 9.0, 10.5, 1.0]]
    )
    clock3 = BinanceServerTimeObserver(fake3)

    with pytest.raises(MalformedKlineError):
        paginate_ohlcv(
            adapter=fake3,
            clock=clock3,
            symbol="BTC/USDT",
            timeframe=SupportedTimeframe.H1,
            since_ms=0,
            until_ms=3_600_000,
            poll_interval_s=0.01,
        )

    # 4. Mismatched timestamp -> RefetchTimestampMismatchError
    fake4 = FakeBinanceAdapter(server_time_ms=5_000_000)
    fake4.server_time_sequence = [7_201_000]
    fake4.fetch_ohlcv = lambda s, t="1m", since=None, limit=None: (  # type: ignore
        [[3_600_000, 10.0, 11.0, 9.0, 10.5, 1.0]] if limit == 1 else [[0, 10.0, 11.0, 9.0, 10.5, 1.0]]
    )
    clock4 = BinanceServerTimeObserver(fake4)

    with pytest.raises(RefetchTimestampMismatchError):
        paginate_ohlcv(
            adapter=fake4,
            clock=clock4,
            symbol="BTC/USDT",
            timeframe=SupportedTimeframe.H1,
            since_ms=0,
            until_ms=3_600_000,
            poll_interval_s=0.01,
        )


def test_incomplete_tail_retry_exhaustion_aborts() -> None:
    """Requirement 4: Injects candle remaining incomplete across all 3 retries; asserts IncompleteTailCandleError."""
    fake = FakeBinanceAdapter(server_time_ms=1_000_000)
    # Candle at 0 closes at 3_600_000. All retries return server time < 3_600_000
    fake.server_time_sequence = [1_500_000, 2_000_000, 2_500_000, 3_000_000]
    fake.klines_pages = {("BTC/USDT", "1h", 0): [[0, 10.0, 11.0, 9.0, 10.5, 1.0]]}
    clock = BinanceServerTimeObserver(fake)

    with pytest.raises(IncompleteTailCandleError, match="exhausted all retries"):
        paginate_ohlcv(
            adapter=fake,
            clock=clock,
            symbol="BTC/USDT",
            timeframe=SupportedTimeframe.H1,
            since_ms=0,
            until_ms=3_600_000,
            max_retries=3,
            poll_interval_s=0.01,
        )
