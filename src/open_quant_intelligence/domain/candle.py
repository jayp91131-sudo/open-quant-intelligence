"""Immutable Candle domain model and validation rules."""

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from open_quant_intelligence.domain.market import InvalidMarketError, MarketIdentity
from open_quant_intelligence.domain.timeframe import (
    compute_close_time,
    parse_timeframe,
)


class InvalidCandleError(ValueError):
    """Raised when candle fields violate domain consistency rules."""


@dataclass(frozen=True)
class Candle:
    """Canonical immutable representation of an OHLCV market candle.

    Time Interval Semantics:
        - Candle intervals adhere to the half-open interval convention: [open_time, close_time).
        - `open_time` is the inclusive start instant of the interval.
        - `close_time` is the exclusive upper boundary instant (open_time + timeframe duration).
          For example, a 1-hour candle opened at 10:00 UTC has `open_time = 10:00:00Z` and
          `close_time = 11:00:00Z`. The candle covers all trades where 10:00:00Z <= t < 11:00:00Z.
          A trade occurring exactly at 11:00:00Z belongs strictly to the subsequent 11:00 UTC candle.
        - The candle is completed/closed at or after close_time (11:00:00Z).
        - In adjacent series, Candle[i].close_time == Candle[i+1].open_time.

    Invariants:
        - All timestamps are timezone-aware and normalized to UTC.
        - Prices must be finite and strictly positive (> 0).
        - Base volume must be finite and non-negative (>= 0).
        - High and low must bound open, close, and each other.
        - Market type must strictly be 'spot' in M0.
    """

    exchange_id: str
    market_type: str
    symbol: str
    timeframe: str
    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    base_volume: float
    close_time: datetime | None = None
    is_closed: bool = True
    source: str = "csv"
    fetched_at: datetime | None = None
    quote_volume: float | None = None
    trade_count: int | None = None

    def __post_init__(self) -> None:
        # String validations
        if not self.exchange_id or not isinstance(self.exchange_id, str) or not self.exchange_id.strip():
            raise InvalidCandleError("exchange_id must be a non-empty string.")

        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise InvalidCandleError("symbol must be a non-empty string.")

        # Validate market identity contract (market_type, symbol format, base/quote agreement)
        try:
            market_ident = MarketIdentity(
                exchange_id=self.exchange_id,
                market_type=self.market_type,
                symbol=self.symbol,
            )
        except InvalidMarketError as exc:
            raise InvalidCandleError(f"Invalid market identity: {exc}") from exc

        normalized_market_type = market_ident.market_type
        normalized_symbol = market_ident.symbol

        # Validate timeframe
        parsed_tf = parse_timeframe(self.timeframe)
        normalized_timeframe = parsed_tf.value

        # Validate timestamp awareness and normalize to UTC
        if not isinstance(self.open_time, datetime):
            raise InvalidCandleError(f"open_time must be a datetime instance, got {type(self.open_time).__name__}.")

        if self.open_time.tzinfo is None or self.open_time.tzinfo.utcoffset(self.open_time) is None:
            raise InvalidCandleError(
                f"open_time must be timezone-aware (UTC). Naive timestamp rejected: {self.open_time!r}."
            )

        utc_open_time = self.open_time.astimezone(timezone.utc)

        # Validate boolean flags and integer fields (explicit type checking to prevent bool masquerading)
        if not isinstance(self.is_closed, bool):
            raise InvalidCandleError(
                f"is_closed must be a boolean, got {type(self.is_closed).__name__}: {self.is_closed!r}."
            )

        if self.trade_count is not None:
            if isinstance(self.trade_count, bool) or not isinstance(self.trade_count, int) or self.trade_count < 0:
                raise InvalidCandleError(
                    f"trade_count must be a non-negative integer or None, got: {self.trade_count!r}."
                )

        # Handle close_time
        expected_close_time = compute_close_time(utc_open_time, parsed_tf)
        if self.close_time is not None:
            if not isinstance(self.close_time, datetime):
                raise InvalidCandleError(
                    f"close_time must be a datetime instance, got {type(self.close_time).__name__}."
                )
            if self.close_time.tzinfo is None or self.close_time.tzinfo.utcoffset(self.close_time) is None:
                raise InvalidCandleError(
                    f"close_time must be timezone-aware (UTC). Naive timestamp rejected: {self.close_time!r}."
                )
            utc_close_time = self.close_time.astimezone(timezone.utc)
            if utc_close_time != expected_close_time:
                raise InvalidCandleError(
                    f"Contradictory close_time ({utc_close_time.isoformat()}). "
                    f"For timeframe '{normalized_timeframe}' starting at {utc_open_time.isoformat()}, "
                    f"close_time must equal open_time + timeframe duration ({expected_close_time.isoformat()})."
                )
        else:
            utc_close_time = expected_close_time

        # Price and volume validations
        for price_name, price_val in (
            ("open", self.open),
            ("high", self.high),
            ("low", self.low),
            ("close", self.close),
        ):
            if isinstance(price_val, bool) or not isinstance(price_val, (int, float)):
                raise InvalidCandleError(f"{price_name} price must be a numeric value, got {type(price_val).__name__}.")
            f_val = float(price_val)
            if not math.isfinite(f_val):
                raise InvalidCandleError(f"{price_name} price must be finite, got non-finite value: {f_val}.")
            if f_val <= 0.0:
                raise InvalidCandleError(f"{price_name} price must be strictly positive (> 0), got: {f_val}.")

        if isinstance(self.base_volume, bool) or not isinstance(self.base_volume, (int, float)):
            raise InvalidCandleError(f"base_volume must be numeric, got {type(self.base_volume).__name__}.")
        f_volume = float(self.base_volume)
        if not math.isfinite(f_volume):
            raise InvalidCandleError(f"base_volume must be finite, got non-finite value: {f_volume}.")
        if f_volume < 0.0:
            raise InvalidCandleError(f"base_volume must be non-negative (>= 0), got: {f_volume}.")

        # High/low bound checks
        f_open = float(self.open)
        f_high = float(self.high)
        f_low = float(self.low)
        f_close = float(self.close)

        max_oc = max(f_open, f_close)
        min_oc = min(f_open, f_close)

        if f_high < max_oc:
            raise InvalidCandleError(
                f"Inconsistent high price ({f_high}): cannot be lower than max(open, close) ({max_oc})."
            )
        if f_high < f_low:
            raise InvalidCandleError(f"Inconsistent high price ({f_high}): cannot be lower than low price ({f_low}).")
        if f_low > min_oc:
            raise InvalidCandleError(
                f"Inconsistent low price ({f_low}): cannot be higher than min(open, close) ({min_oc})."
            )
        if f_low > f_high:
            raise InvalidCandleError(f"Inconsistent low price ({f_low}): cannot be higher than high price ({f_high}).")

        # Optional quote_volume check
        if self.quote_volume is not None:
            if (
                isinstance(self.quote_volume, bool)
                or not isinstance(self.quote_volume, (int, float))
                or not math.isfinite(float(self.quote_volume))
                or float(self.quote_volume) < 0.0
            ):
                raise InvalidCandleError(f"quote_volume must be finite and non-negative, got: {self.quote_volume}.")

        # Optional fetched_at normalization
        normalized_fetched_at = None
        if self.fetched_at is not None:
            if not isinstance(self.fetched_at, datetime):
                raise InvalidCandleError("fetched_at must be a datetime instance.")
            if self.fetched_at.tzinfo is None:
                raise InvalidCandleError("fetched_at must be timezone-aware.")
            normalized_fetched_at = self.fetched_at.astimezone(timezone.utc)

        # Apply normalized fields
        object.__setattr__(self, "exchange_id", market_ident.exchange_id)
        object.__setattr__(self, "market_type", normalized_market_type)
        object.__setattr__(self, "symbol", normalized_symbol)
        object.__setattr__(self, "timeframe", normalized_timeframe)
        object.__setattr__(self, "open_time", utc_open_time)
        object.__setattr__(self, "close_time", utc_close_time)
        object.__setattr__(self, "open", f_open)
        object.__setattr__(self, "high", f_high)
        object.__setattr__(self, "low", f_low)
        object.__setattr__(self, "close", f_close)
        object.__setattr__(self, "base_volume", f_volume)
        object.__setattr__(self, "source", str(self.source).strip())
        object.__setattr__(self, "fetched_at", normalized_fetched_at)
        if self.quote_volume is not None:
            object.__setattr__(self, "quote_volume", float(self.quote_volume))

    def to_dict(self) -> dict[str, Any]:
        """Serialize candle to dictionary."""
        return {
            "exchange_id": self.exchange_id,
            "market_type": self.market_type,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "open_time": self.open_time.isoformat(),
            "close_time": self.close_time.isoformat() if self.close_time else None,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "base_volume": self.base_volume,
            "is_closed": self.is_closed,
            "source": self.source,
            "fetched_at": self.fetched_at.isoformat() if self.fetched_at else None,
            "quote_volume": self.quote_volume,
            "trade_count": self.trade_count,
        }
