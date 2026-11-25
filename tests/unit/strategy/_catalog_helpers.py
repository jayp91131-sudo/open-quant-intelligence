"""Shared temporary DuckDBCatalog provenance helpers for M2 strategy unit tests."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from open_quant_intelligence.data.quality import DataQualityReport, analyze_dataset_quality
from open_quant_intelligence.domain.candle import Candle
from open_quant_intelligence.domain.timeframe import SupportedTimeframe
from open_quant_intelligence.storage.catalog import CatalogDatasetRecord, DuckDBCatalog
from open_quant_intelligence.storage.fingerprint import compute_content_fingerprint
from open_quant_intelligence.strategy.contracts import EvaluationContext


def commit_and_build_context(
    tmp_path: Path,
    candles: Sequence[Candle],
    *,
    exchange_id: str = "binance",
    market_type: str = "spot",
    symbol: str = "BTC/USDT",
    timeframe: SupportedTimeframe = SupportedTimeframe.H1,
    since_dt: datetime | None = None,
    until_dt: datetime | None = None,
    ingestion_status: str = "COMMITTED",
    file_sha256: str = "f" * 64,
    snapshot_id: str | None = None,
) -> tuple[DuckDBCatalog, CatalogDatasetRecord, EvaluationContext]:
    """Commit a dataset snapshot to a temporary DuckDBCatalog and build an EvaluationContext from it."""
    if not candles:
        raise ValueError("candles cannot be empty for catalog fixture setup")

    req_since = since_dt or candles[0].open_time
    req_until = until_dt or candles[-1].close_time
    if req_until is None:
        raise ValueError("until_dt or last candle close_time is required")

    since_ms = int(req_since.timestamp() * 1000)
    until_ms = int(req_until.timestamp() * 1000)

    quality_report: DataQualityReport = analyze_dataset_quality(
        candles,
        since_ms=since_ms,
        until_ms=until_ms,
        timeframe=timeframe,
    )
    content_fp = compute_content_fingerprint(
        candles=candles,
        exchange_id=exchange_id,
        market_type=market_type,
        symbol=symbol,
        timeframe=timeframe,
        requested_since_ms=since_ms,
        requested_until_ms=until_ms,
    )
    dataset_identity = f"{exchange_id}:{market_type}:{symbol}:{timeframe.value}:{since_ms}:{until_ms}"
    snap_id = snapshot_id or f"snap_{uuid.uuid4().hex[:12]}"

    record = CatalogDatasetRecord(
        snapshot_id=snap_id,
        dataset_identity=dataset_identity,
        exchange_id=exchange_id,
        market_type=market_type,
        symbol=symbol,
        timeframe=timeframe.value,
        requested_since=req_since,
        requested_until=req_until,
        actual_first_open=candles[0].open_time,
        actual_last_close=candles[-1].close_time,
        expected_records=quality_report.expected_records,
        record_count=quality_report.record_count,
        gap_count=quality_report.gap_count,
        missing_intervals=quality_report.missing_intervals,
        content_fingerprint_sha256=content_fp,
        file_sha256=file_sha256,
        file_path=str(tmp_path / f"{snap_id}.parquet"),
        file_size_bytes=1024,
        ccxt_version="4.5.93",
        ingestion_uuid=str(uuid.uuid4()),
        ingested_at=datetime.now(timezone.utc),
        ingestion_status=ingestion_status,
        quality_status=quality_report.quality_status,
    )

    catalog = DuckDBCatalog(tmp_path / "catalog.duckdb")
    catalog.commit_dataset_snapshot(record, gaps=quality_report.gaps)

    if ingestion_status == "COMMITTED":
        retrieved = catalog.find_latest_committed(dataset_identity)
        assert retrieved is not None
    else:
        retrieved = catalog.find_by_id_or_fingerprint(snap_id)
        assert retrieved is not None

    ctx = EvaluationContext.from_catalog(catalog, retrieved.snapshot_id, quality_report)
    return catalog, retrieved, ctx
