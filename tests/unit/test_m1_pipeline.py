"""End-to-end integration tests for IngestionPipeline using FakeBinanceAdapter."""

from pathlib import Path
from typing import Any

import pytest
from open_quant_intelligence.data.timeframe import SupportedTimeframe

from open_quant_intelligence.data.quality import QualityStatus
from open_quant_intelligence.ingestion.pipeline import (
    GapFailureError,
    IngestionPipeline,
    IngestionRequest,
)
from tests.fakes.fake_binance_adapter import FakeBinanceAdapter


def test_empty_dataset_path_committed_without_file(tmp_path: Path) -> None:
    """Requirement 15: Zero admitted candles bypass M0 validator, publish no Parquet file,
    compute canonical header-only SHA-256, and register a committed EMPTY snapshot.
    """
    fake = FakeBinanceAdapter(
        server_time_ms=10_000_000,
        default_klines=[],  # Zero candles returned
    )
    pipeline = IngestionPipeline(output_dir=tmp_path, adapter=fake)

    req = IngestionRequest(
        exchange_id="binance",
        symbol="BTC/USDT",
        timeframe=SupportedTimeframe.H1,
        since_ms=0,
        until_ms=7_200_000,
    )

    res = pipeline.ingest(req)

    assert res.quality_report.status == QualityStatus.EMPTY
    assert res.quality_report.record_count == 0
    assert res.quality_report.expected_records == 2
    assert res.content_fingerprint.startswith("sha256:")
    assert res.parquet_artifact_path is None
    assert res.parquet_artifact_sha256 is None
    assert res.ingestion_status == "COMMITTED"

    # Verify committed in catalog with NULL file paths
    record = pipeline.catalog.find_latest_committed(res.manifest.dataset_identity)
    assert record is not None
    assert record.quality_status == "EMPTY"
    assert record.file_path is None
    assert record.file_sha256 is None


def test_pipeline_end_to_end_gap_free(tmp_path: Path) -> None:
    """End-to-end ingestion of a complete gap-free dataset."""
    # 2 hours: [0, 7_200_000)
    klines: list[list[Any]] = [
        [0, 100.0, 105.0, 95.0, 102.0, 10.0],
        [3_600_000, 102.0, 106.0, 101.0, 105.0, 15.0],
    ]
    fake = FakeBinanceAdapter(server_time_ms=10_000_000, default_klines=klines)
    pipeline = IngestionPipeline(output_dir=tmp_path, adapter=fake)

    req = IngestionRequest(
        exchange_id="binance",
        symbol="BTC/USDT",
        timeframe=SupportedTimeframe.H1,
        since_ms=0,
        until_ms=7_200_000,
    )

    res = pipeline.ingest(req)

    assert res.quality_report.status == QualityStatus.GAP_FREE
    assert res.quality_report.record_count == 2
    assert res.parquet_artifact_path is not None
    assert Path(res.parquet_artifact_path).exists()
    assert res.parquet_artifact_sha256 is not None
    assert res.ingestion_status == "COMMITTED"


def test_pipeline_fail_on_gaps(tmp_path: Path) -> None:
    """When fail_on_gaps=True, any missing intervals abort ingestion."""
    # 2 hours window, but only 1 candle returned
    klines: list[list[Any]] = [
        [0, 100.0, 105.0, 95.0, 102.0, 10.0],
    ]
    fake = FakeBinanceAdapter(server_time_ms=10_000_000, default_klines=klines)
    pipeline = IngestionPipeline(output_dir=tmp_path, adapter=fake)

    req = IngestionRequest(
        exchange_id="binance",
        symbol="BTC/USDT",
        timeframe=SupportedTimeframe.H1,
        since_ms=0,
        until_ms=7_200_000,
        fail_on_gaps=True,
    )

    with pytest.raises(GapFailureError, match="contains 1 gap intervals"):
        pipeline.ingest(req)


def test_pipeline_deduplication_and_force_reingestion(tmp_path: Path) -> None:
    """Pipeline re-uses committed snapshot unless force=True."""
    klines: list[list[Any]] = [
        [0, 100.0, 105.0, 95.0, 102.0, 10.0],
    ]
    fake = FakeBinanceAdapter(server_time_ms=10_000_000, default_klines=klines)
    pipeline = IngestionPipeline(output_dir=tmp_path, adapter=fake)

    req = IngestionRequest(
        exchange_id="binance",
        symbol="BTC/USDT",
        timeframe=SupportedTimeframe.H1,
        since_ms=0,
        until_ms=3_600_000,
        force=False,
    )

    # First ingestion
    res1 = pipeline.ingest(req)
    kline_calls_1 = len(fake.queried_kline_calls)

    # Second ingestion without force -> deduplicated, no new kline calls
    res2 = pipeline.ingest(req)
    assert res2.snapshot_id == res1.snapshot_id
    assert len(fake.queried_kline_calls) == kline_calls_1

    # Third ingestion with force=True -> re-queries and creates new snapshot
    req_force = IngestionRequest(
        exchange_id="binance",
        symbol="BTC/USDT",
        timeframe=SupportedTimeframe.H1,
        since_ms=0,
        until_ms=3_600_000,
        force=True,
    )
    res3 = pipeline.ingest(req_force)
    assert res3.snapshot_id != res1.snapshot_id
    assert len(fake.queried_kline_calls) > kline_calls_1
