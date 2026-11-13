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

        # Validate symbol format: must contain a separator ('/', '-', or '_')
        # with non-empty base and quote components (e.g. 'BTC/USDT', 'ETH-USD')
        clean_symbol = self.symbol.strip().upper()
        sep = None
        for candidate_sep in ("/", "-", "_"):
            if candidate_sep in clean_symbol:
                sep = candidate_sep
                break

        if not sep:
            raise InvalidMarketError(
                f"Malformed symbol {self.symbol!r}. Symbol must contain a valid pair separator ('/', '-', '_')."
            )

        parts = clean_symbol.split(sep)
        if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
            raise InvalidMarketError(
                f"Malformed symbol {self.symbol!r}. Base and quote components must be non-empty."
            )

        sym_base, sym_quote = parts[0].strip(), parts[1].strip()

        # Derive or validate base and quote currencies
        derived_base = self.base_currency.strip().upper() if self.base_currency else sym_base
        derived_quote = self.quote_currency.strip().upper() if self.quote_currency else sym_quote

        if derived_base != sym_base:
            raise InvalidMarketError(
                f"base_currency {derived_base!r} does not agree with symbol {self.symbol!r} base asset {sym_base!r}."
            )

        if derived_quote != sym_quote:
            raise InvalidMarketError(
                f"quote_currency {derived_quote!r} does not agree with symbol {self.symbol!r} "
                f"quote asset {sym_quote!r}."
            )

        object.__setattr__(self, "exchange_id", self.exchange_id.strip().lower())
        object.__setattr__(self, "market_type", normalized_market_type)
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "base_currency", derived_base)
        object.__setattr__(self, "quote_currency", derived_quote)
