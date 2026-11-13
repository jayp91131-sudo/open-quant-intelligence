"""Market identity and canonical spot contract definitions."""

from dataclasses import dataclass


class InvalidMarketError(ValueError):
    """Raised when market parameters violate canonical market identity contracts."""


@dataclass(frozen=True)
class MarketIdentity:
    """Canonical identity of a traded cryptocurrency market.

    Attributes:
        exchange_id: Unique exchange identifier (e.g. 'binance').
        market_type: Traded market type. In M0, strictly 'spot'.
        symbol: Canonical pair symbol (e.g. 'BTC/USDT').
        base_currency: Base asset (e.g. 'BTC'). Derived if omitted.
        quote_currency: Quote asset (e.g. 'USDT'). Derived if omitted.
    """

    exchange_id: str
    market_type: str
    symbol: str
    base_currency: str = ""
    quote_currency: str = ""

    def __post_init__(self) -> None:
        if not self.exchange_id or not isinstance(self.exchange_id, str) or not self.exchange_id.strip():
            raise InvalidMarketError("exchange_id must be a non-empty string.")

        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise InvalidMarketError("symbol must be a non-empty string.")

        normalized_market_type = str(self.market_type).strip().lower()
        if normalized_market_type != "spot":
            raise InvalidMarketError(
                f"Unsupported market_type: {self.market_type!r}. "
                "Only 'spot' is supported in Milestone M0. Futures and derivatives are out of scope."
            )

        # Derive base and quote currencies if not explicitly populated
        derived_base = self.base_currency.strip().upper() if self.base_currency else ""
        derived_quote = self.quote_currency.strip().upper() if self.quote_currency else ""

        if not derived_base or not derived_quote:
            clean_symbol = self.symbol.strip().upper()
            if "/" in clean_symbol:
                parts = clean_symbol.split("/", 1)
                derived_base = derived_base or parts[0]
                derived_quote = derived_quote or parts[1]
            elif "-" in clean_symbol:
                parts = clean_symbol.split("-", 1)
                derived_base = derived_base or parts[0]
                derived_quote = derived_quote or parts[1]
            elif "_" in clean_symbol:
                parts = clean_symbol.split("_", 1)
                derived_base = derived_base or parts[0]
                derived_quote = derived_quote or parts[1]

        object.__setattr__(self, "exchange_id", self.exchange_id.strip().lower())
        object.__setattr__(self, "market_type", normalized_market_type)
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "base_currency", derived_base)
        object.__setattr__(self, "quote_currency", derived_quote)
