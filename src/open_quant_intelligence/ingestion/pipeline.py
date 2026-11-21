"""Complete historical market data ingestion orchestrator."""

import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from open_quant_intelligence.data.quality import (
    DataQualityReport,
    analyze_dataset_quality,
)
from open_quant_intelligence.data.validation import validate_candles
from open_quant_intelligence.domain.candle import Candle
from open_quant_intelligence.domain.timeframe import SupportedTimeframe
from open_quant_intelligence.ingestion.adapter import PublicExchangeAdapter
from open_quant_intelligence.ingestion.clock import BinanceServerTimeObserver
from open_quant_intelligence.ingestion.pagination import paginate_ohlcv
from open_quant_intelligence.ingestion.preflight import (
    validate_request_window_preflight,
    validate_spot_market_preflight,
)
from open_quant_intelligence.storage.catalog import CatalogDatasetRecord, DuckDBCatalog
from open_quant_intelligence.storage.fingerprint import (
    compute_content_fingerprint,
    compute_file_sha256,
)
from open_quant_intelligence.storage.manifest import (
    IngestionManifest,
    ManifestManager,
    ManifestStage,
)
from open_quant_intelligence.storage.parquet_store import (
    publish_staging_file_no_clobber,
    write_staging_parquet,
)


@dataclass(frozen=True)
class IngestionResult:
    """Summary result returned from an ingestion pipeline execution."""

    snapshot_id: str
    ingestion_uuid: str
    dataset_identity: str
    quality_status: str
    record_count: int
    missing_intervals: int
    gap_count: int
    content_fingerprint_sha256: str
    file_path: str | None
    file_sha256: str | None
    skipped: bool = False


class IngestionPipeline:
    """Orchestrates preflight checks, pagination, validation, staging, and catalog registration."""

    def __init__(
        self,
        adapter: PublicExchangeAdapter,
        catalog: DuckDBCatalog,
        manifest_manager: ManifestManager,
        base_data_dir: Path | str = "data",
    ) -> None:
        self.adapter = adapter
        self.catalog = catalog
        self.manifest_manager = manifest_manager
        self.base_data_dir = Path(base_data_dir)
        self.clock = BinanceServerTimeObserver(adapter)

    def run(
        self,
        symbol: str,
        timeframe: SupportedTimeframe,
        since_ms: int,
        until_ms: int,
        force: bool = False,
        fail_on_gaps: bool = False,
    ) -> IngestionResult:
        exchange_id = "binance"
        market_type = "spot"
        dataset_identity = f"{exchange_id}:{market_type}:{symbol}:{timeframe.value}:{since_ms}:{until_ms}"

        # 1. Idempotency check: see if a verified committed snapshot already exists
        if not force:
            existing = self.catalog.find_latest_committed(dataset_identity)
            if existing:
                if existing.quality_status == "EMPTY":
                    return IngestionResult(
                        snapshot_id=existing.snapshot_id,
                        ingestion_uuid=existing.ingestion_uuid,
                        dataset_identity=existing.dataset_identity,
                        quality_status=existing.quality_status,
                        record_count=0,
                        missing_intervals=existing.missing_intervals,
                        gap_count=existing.gap_count,
                        content_fingerprint_sha256=existing.content_fingerprint_sha256,
                        file_path=None,
                        file_sha256=None,
                        skipped=True,
                    )
                if existing.file_path and Path(existing.file_path).is_file():
                    actual_hash = compute_file_sha256(existing.file_path)
                    if actual_hash == existing.file_sha256:
                        return IngestionResult(
                            snapshot_id=existing.snapshot_id,
                            ingestion_uuid=existing.ingestion_uuid,
                            dataset_identity=existing.dataset_identity,
                            quality_status=existing.quality_status,
                            record_count=existing.record_count,
                            missing_intervals=existing.missing_intervals,
                            gap_count=existing.gap_count,
                            content_fingerprint_sha256=existing.content_fingerprint_sha256,
                            file_path=existing.file_path,
                            file_sha256=existing.file_sha256,
                            skipped=True,
                        )

        # 2. Preflight checks
        validate_spot_market_preflight(symbol, self.adapter)
        expected_records = validate_request_window_preflight(since_ms, until_ms, timeframe, self.adapter)

        # 3. Pagination with incomplete tail re-fetch
        raw_rows = paginate_ohlcv(
            adapter=self.adapter,
            symbol=symbol,
            timeframe=timeframe,
            since_ms=since_ms,
            until_ms=until_ms,
            clock=self.clock,
        )

        ingestion_uuid = str(uuid.uuid4())
        snapshot_id = f"snap_{ingestion_uuid[:12]}"
        now_utc_str = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        now_dt = datetime.now(timezone.utc)

        # 4. Pipeline Branching: Zero-Candle Branch vs Non-Zero Branch
        if len(raw_rows) == 0:
            # ZERO-CANDLE BRANCH
            content_fingerprint = compute_content_fingerprint(
                candles=(),
                exchange_id=exchange_id,
                market_type=market_type,
                symbol=symbol,
                timeframe=timeframe.value,
                requested_since_ms=since_ms,
                requested_until_ms=until_ms,
            )

            empty_report = analyze_dataset_quality(
                candles=(),
                since_ms=since_ms,
                until_ms=until_ms,
                timeframe=timeframe,
            )

            catalog_record = CatalogDatasetRecord(
                snapshot_id=snapshot_id,
                dataset_identity=dataset_identity,
                exchange_id=exchange_id,
                market_type=market_type,
                symbol=symbol,
                timeframe=timeframe.value,
                requested_since=datetime.fromtimestamp(since_ms / 1000, tz=timezone.utc),
                requested_until=datetime.fromtimestamp(until_ms / 1000, tz=timezone.utc),
                actual_first_open=None,
                actual_last_close=None,
                expected_records=expected_records,
                record_count=0,
                gap_count=empty_report.gap_count,
                missing_intervals=empty_report.missing_intervals,
                content_fingerprint_sha256=content_fingerprint,
                file_sha256=None,
                file_path=None,
                file_size_bytes=None,
                ccxt_version="4.4.52",
                ingestion_uuid=ingestion_uuid,
                ingested_at=now_dt,
                ingestion_status="COMMITTED",
                quality_status="EMPTY",
            )

            self.catalog.commit_dataset_snapshot(catalog_record, empty_report.gaps)

            return IngestionResult(
                snapshot_id=snapshot_id,
                ingestion_uuid=ingestion_uuid,
                dataset_identity=dataset_identity,
                quality_status="EMPTY",
                record_count=0,
                missing_intervals=expected_records,
                gap_count=1,
                content_fingerprint_sha256=content_fingerprint,
                file_path=None,
                file_sha256=None,
            )

        # NON-ZERO BRANCH: Convert raw rows to M0 Candle instances
        candles: list[Candle] = []
        for row in raw_rows:
            t, o, h, low_val, c, v = row
            c_open_dt = datetime.fromtimestamp(t / 1000, tz=timezone.utc)
            candle = Candle(
                exchange_id=exchange_id,
                market_type=market_type,
                symbol=symbol,
                timeframe=timeframe.value,
                open_time=c_open_dt,
                open=float(o),
                high=float(h),
                low=float(low_val),
                close=float(c),
                base_volume=float(v),
            )
            candles.append(candle)

        # M0 sequence validation
        validation_res = validate_candles(candles, fail_on_gaps=fail_on_gaps)
        if not validation_res.is_valid:
            raise ValueError(f"M0 Candle validation failed: {validation_res.errors}")

        # Quality and edge-gap analysis
        quality_report: DataQualityReport = analyze_dataset_quality(
            candles=candles,
            since_ms=since_ms,
            until_ms=until_ms,
            timeframe=timeframe,
        )

        content_fingerprint = compute_content_fingerprint(
            candles=candles,
            exchange_id=exchange_id,
            market_type=market_type,
            symbol=symbol,
            timeframe=timeframe.value,
            requested_since_ms=since_ms,
            requested_until_ms=until_ms,
        )

        clean_symbol = symbol.replace("/", "_")
        target_dir = (
            self.base_data_dir
            / "datasets"
            / f"exchange={exchange_id}"
            / f"market={market_type}"
            / f"symbol={clean_symbol}"
            / f"timeframe={timeframe.value}"
        )
        target_dir.mkdir(parents=True, exist_ok=True)

        since_str = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(since_ms / 1000))
        until_str = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(until_ms / 1000))
        fp_prefix = content_fingerprint.split(":")[-1][:8]
        uuid_prefix = ingestion_uuid[:8]

        filename = f"dataset_{since_str}_{until_str}_{fp_prefix}_{uuid_prefix}.parquet"
        target_file_path = target_dir / filename
        staging_file_path = target_dir / f"{filename}.tmp_{uuid_prefix}"

        # 5. Manifest Lifecycle & Staging
        manifest = IngestionManifest(
            ingestion_uuid=ingestion_uuid,
            lease_owner_pid=os.getpid(),
            lease_expires_at_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 1800)),
            schema_version=1,
            ccxt_version="4.4.52",
            exchange_id=exchange_id,
            market_type=market_type,
            symbol=symbol,
            timeframe=timeframe.value,
            requested_since_utc=since_str,
            requested_until_utc=until_str,
            requested_since_ms=since_ms,
            requested_until_ms=until_ms,
            staging_file_path=str(staging_file_path),
            target_file_path=str(target_file_path),
            expected_records=expected_records,
            record_count=len(candles),
            missing_intervals=quality_report.missing_intervals,
            content_fingerprint_sha256=content_fingerprint,
            file_sha256="",
            stage=ManifestStage.INITIALIZING.value,
            created_at_utc=now_utc_str,
            updated_at_utc=now_utc_str,
        )
        self.manifest_manager.write_manifest(manifest)

        # Stage writing
        self.manifest_manager.update_stage(manifest, ManifestStage.STAGING)
        write_staging_parquet(candles, staging_file_path)
        manifest.file_sha256 = compute_file_sha256(staging_file_path)
        self.manifest_manager.update_stage(manifest, ManifestStage.STAGED)

        # Publication
        self.manifest_manager.update_stage(manifest, ManifestStage.PUBLISHING)
        file_sha256 = publish_staging_file_no_clobber(staging_file_path, target_file_path)
        self.manifest_manager.update_stage(manifest, ManifestStage.PUBLISHED)

        # Catalog commit
        self.manifest_manager.update_stage(manifest, ManifestStage.COMMITTING_CATALOG)

        catalog_record = CatalogDatasetRecord(
            snapshot_id=snapshot_id,
            dataset_identity=dataset_identity,
            exchange_id=exchange_id,
            market_type=market_type,
            symbol=symbol,
            timeframe=timeframe.value,
            requested_since=datetime.fromtimestamp(since_ms / 1000, tz=timezone.utc),
            requested_until=datetime.fromtimestamp(until_ms / 1000, tz=timezone.utc),
            actual_first_open=candles[0].open_time,
            actual_last_close=candles[-1].close_time,
            expected_records=expected_records,
            record_count=len(candles),
            gap_count=quality_report.gap_count,
            missing_intervals=quality_report.missing_intervals,
            content_fingerprint_sha256=content_fingerprint,
            file_sha256=file_sha256,
            file_path=str(target_file_path),
            file_size_bytes=target_file_path.stat().st_size,
            ccxt_version="4.4.52",
            ingestion_uuid=ingestion_uuid,
            ingested_at=now_dt,
            ingestion_status="COMMITTED",
            quality_status=quality_report.quality_status,
        )

        self.catalog.commit_dataset_snapshot(catalog_record, quality_report.gaps)
        self.manifest_manager.update_stage(manifest, ManifestStage.COMMITTED)

        # Cleanup manifest
        self.manifest_manager.unlink_manifest(ingestion_uuid)

        return IngestionResult(
            snapshot_id=snapshot_id,
            ingestion_uuid=ingestion_uuid,
            dataset_identity=dataset_identity,
            quality_status=quality_report.quality_status,
            record_count=len(candles),
            missing_intervals=quality_report.missing_intervals,
            gap_count=quality_report.gap_count,
            content_fingerprint_sha256=content_fingerprint,
            file_path=str(target_file_path),
            file_sha256=file_sha256,
        )
