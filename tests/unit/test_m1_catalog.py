"""Unit tests for DuckDB catalog metadata, gap event logging, and audit verification history."""

from datetime import datetime, timezone
from pathlib import Path

from open_quant_intelligence.data.quality import GapType, QualityGapEvent
from open_quant_intelligence.storage.catalog import (
    CatalogDatasetRecord,
    DuckDBCatalog,
)


def test_duckdb_catalog_lifecycle_and_verification_history(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog.duckdb"
    catalog = DuckDBCatalog(db_path)

    now = datetime.now(timezone.utc)
    record = CatalogDatasetRecord(
        snapshot_id="snap_12345",
        dataset_identity="binance:spot:BTC/USDT:1h:0:3600000",
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since=now,
        requested_until=now,
        actual_first_open=now,
        actual_last_close=now,
        expected_records=1,
        record_count=1,
        gap_count=0,
        missing_intervals=0,
        content_fingerprint_sha256="sha256:fingerprint_abc",
        file_sha256="sha256:file_xyz",
        file_path=str(tmp_path / "artifact.parquet"),
        file_size_bytes=1024,
        ccxt_version="4.4.52",
        ingestion_uuid="uuid_test_999",
        ingested_at=now,
        ingestion_status="COMMITTED",
        quality_status="GAP_FREE",
    )

    catalog.commit_dataset_snapshot(record)

    # Find latest committed by dataset identity
    found = catalog.find_latest_committed("binance:spot:BTC/USDT:1h:0:3600000")
    assert found is not None
    assert found.snapshot_id == "snap_12345"
    assert found.file_sha256 == "sha256:file_xyz"

    # Find by fingerprint or snapshot id
    found_fp = catalog.find_by_id_or_fingerprint("fingerprint_abc")
    assert found_fp is not None
    assert found_fp.snapshot_id == "snap_12345"

    # Listing
    listed = catalog.list_datasets(symbol="BTC/USDT", timeframe="1h")
    assert len(listed) == 1
    assert listed[0].snapshot_id == "snap_12345"

    # Record verification event
    catalog.record_verification(
        verification_id="ver_001",
        snapshot_id="snap_12345",
        result="VERIFIED_MATCH",
        computed_file_sha256="sha256:file_xyz",
    )

    # Verification history must not modify immutable snapshot record
    unchanged = catalog.find_latest_committed("binance:spot:BTC/USDT:1h:0:3600000")
    assert unchanged is not None
    assert unchanged.snapshot_id == "snap_12345"


def test_duckdb_catalog_records_gap_events(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog.duckdb"
    catalog = DuckDBCatalog(db_path)
    now = datetime.now(timezone.utc)

    record = CatalogDatasetRecord(
        snapshot_id="snap_gaps",
        dataset_identity="binance:spot:BTC/USDT:1h:0:7200000",
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since=now,
        requested_until=now,
        actual_first_open=now,
        actual_last_close=now,
        expected_records=2,
        record_count=1,
        gap_count=1,
        missing_intervals=1,
        content_fingerprint_sha256="sha256:fp_gaps",
        file_sha256="sha256:file_gaps",
        file_path=str(tmp_path / "gaps.parquet"),
        file_size_bytes=512,
        ccxt_version="4.4.52",
        ingestion_uuid="uuid_gaps",
        ingested_at=now,
        ingestion_status="COMMITTED",
        quality_status="GAP_CONTAINING",
    )

    gap = QualityGapEvent(
        gap_type=GapType.INTERNAL,
        expected_open_utc=now,
        subsequent_open_utc=now,
        missing_count=1,
    )

    catalog.commit_dataset_snapshot(record, gaps=[gap])

    gaps = catalog.get_gaps("snap_gaps")
    assert len(gaps) == 1
    assert gaps[0]["gap_type"] == "INTERNAL"
    assert gaps[0]["missing_count"] == 1
