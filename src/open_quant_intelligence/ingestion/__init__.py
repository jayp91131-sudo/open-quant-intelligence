"""Module initialization for ingestion package."""

from open_quant_intelligence.ingestion.adapter import (
    BinanceVisionPublicAdapter,
    PublicExchangeAdapter,
    SpotSymbolMetadata,
)
from open_quant_intelligence.ingestion.clock import BinanceServerTimeObserver, ServerTimeObserver
from open_quant_intelligence.ingestion.pagination import paginate_ohlcv
from open_quant_intelligence.ingestion.pipeline import IngestionPipeline, IngestionResult
from open_quant_intelligence.ingestion.preflight import (
    FutureWindowError,
    InvalidWindowError,
    MarketStatusError,
    UnalignedBoundaryError,
    validate_request_window_preflight,
    validate_spot_market_preflight,
)
from open_quant_intelligence.ingestion.time_utils import (
    datetime_to_epoch_ms,
    epoch_ms_to_datetime,
    timeframe_to_duration_ms,
)

__all__ = [
    "BinanceServerTimeObserver",
    "BinanceVisionPublicAdapter",
    "FutureWindowError",
    "IngestionPipeline",
    "IngestionResult",
    "InvalidWindowError",
    "MarketStatusError",
    "PublicExchangeAdapter",
    "ServerTimeObserver",
    "SpotSymbolMetadata",
    "UnalignedBoundaryError",
    "datetime_to_epoch_ms",
    "epoch_ms_to_datetime",
    "paginate_ohlcv",
    "timeframe_to_duration_ms",
    "validate_request_window_preflight",
    "validate_spot_market_preflight",
]
