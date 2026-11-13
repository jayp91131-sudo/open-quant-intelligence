"""Synthetic data generators and fixtures for testing Open Quant Intelligence."""

from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import Sequence

from open_quant_intelligence.domain.candle import Candle


def create_synthetic_candle(
    exchange_id: str = "binance",
    market_type: str = "spot",
    symbol: str = "BTC/USDT",
    timeframe: str = "1h",
    open_time: datetime | None = None,
    open_price: float = 50000.0,
    high_price: float = 50500.0,
    low_price: float = 49500.0,
    close_price: float = 50200.0,
    base_volume: float = 12.5,
    quote_volume: float | None = None,
    trade_count: int | None = 150,
) -> Candle:
    """Create a single valid synthetic Candle instance."""
    if open_time is None:
        open_time = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)

    return Candle(
        exchange_id=exchange_id,
        market_type=market_type,
        symbol=symbol,
        timeframe=timeframe,
        open_time=open_time,
        open=open_price,
        high=high_price,
        low=low_price,
        close=close_price,
        base_volume=base_volume,
        quote_volume=quote_volume,
        trade_count=trade_count,
    )


def generate_synthetic_series(
    count: int = 10,
    timeframe: str = "1h",
    start_time: datetime | None = None,
    start_price: float = 50000.0,
    symbol: str = "BTC/USDT",
    exchange_id: str = "binance",
) -> list[Candle]:
    """Generate a sequence of strictly continuous valid candles."""
    if start_time is None:
        start_time = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)

    delta_map = {
        "1m": timedelta(minutes=1),
        "5m": timedelta(minutes=5),
        "15m": timedelta(minutes=15),
        "30m": timedelta(minutes=30),
        "1h": timedelta(hours=1),
        "4h": timedelta(hours=4),
        "1d": timedelta(days=1),
    }
    step = delta_map.get(timeframe, timedelta(hours=1))

    candles: list[Candle] = []
    current_time = start_time
    price = start_price

    for i in range(count):
        # Deterministic price oscillation
        offset = (i % 5) * 10.0
        open_p = price + offset
        high_p = open_p + 150.0
        low_p = open_p - 100.0
        close_p = open_p + 50.0
        vol = 10.0 + (i % 3) * 5.0

        candles.append(
            Candle(
                exchange_id=exchange_id,
                market_type="spot",
                symbol=symbol,
                timeframe=timeframe,
                open_time=current_time,
                open=open_p,
                high=high_p,
                low=low_p,
                close=close_p,
                base_volume=vol,
            )
        )
        current_time += step

    return candles


def candles_to_csv_string(candles: Sequence[Candle], include_headers: bool = True) -> str:
    """Serialize candles into a canonical CSV string."""
    output = StringIO()
    if include_headers:
        output.write("exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume\n")

    for c in candles:
        output.write(
            f"{c.exchange_id},{c.market_type},{c.symbol},{c.timeframe},"
            f"{c.open_time.isoformat()},{c.open},{c.high},{c.low},{c.close},{c.base_volume}\n"
        )

    return output.getvalue()
