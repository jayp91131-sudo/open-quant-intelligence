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
from open_quant_intelligence.ingestion.adapter import BinanceVisionPublicAdapter, PublicExchangeAdapter
from open_quant_intelligence.ingestion.clock import BinanceServerTimeObserver
from open_quant_intelligence.ingestion.pagination import paginate_ohlcv
from open_quant_intelligence.ingestion.preflight import (
    validate_request_window_preflight,
    validate_spot_market_preflight,
)
from open_quant_intelligence.ingestion.recovery import (
    RecoveryAction,
    RecoveryEngine,
    derive_canonical_dataset_paths,
)
from open_quant_intelligence.storage.catalog import CatalogDatasetRecord, DuckDBCatalog
from open_quant_intelligence.storage.fingerprint import (
    compute_content_fingerprint,
    compute_file_sha256,
)
from open_quant_intelligence.storage.manifest import (
    DurabilityMode,
    DurabilityStatus,
    IngestionManifest,
    ManifestManager,
    ManifestStage,
    sync_directory,
)
from open_quant_intelligence.storage.parquet_store import (
    publish_staging_file_no_clobber,
    write_staging_parquet,
)


class GapFailureError(ValueError):
    """Raised when ingestion contains gap intervals and fail_on_gaps is True."""


@dataclass(frozen=True)
class IngestionRequest:
    """Request specification for historical data ingestion."""

    symbol: str
    timeframe: SupportedTimeframe
    since_ms: int
    until_ms: int
    exchange_id: str = "binance"
    fail_on_gaps: bool = False
    force: bool = False
    durability_mode: DurabilityMode | str | None = None


@dataclass
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
    manifest: IngestionManifest | None = None
    quality_report: DataQualityReport | None = None
    actual_first_open: datetime | None = None
    actual_last_close: datetime | None = None
    ingestion_status: str = "COMMITTED"
    durability_status: str = DurabilityStatus.GUARANTEED.value
    durability_warning: str | None = None

    @property
    def content_fingerprint(self) -> str:
        return self.content_fingerprint_sha256

    @property
    def parquet_artifact_path(self) -> str | None:
        return self.file_path

    @property
    def parquet_artifact_sha256(self) -> str | None:
        return self.file_sha256


class IngestionPipeline:
    """Orchestrates preflight checks, pagination, validation, staging, and catalog registration."""

    def __init__(
        self,
        output_dir: Path | str = "data",
        adapter: PublicExchangeAdapter | None = None,
        catalog: DuckDBCatalog | None = None,
        manifest_manager: ManifestManager | None = None,
        base_data_dir: Path | str | None = None,
        durability_mode: DurabilityMode | str = DurabilityMode.DEGRADED_ALLOWED,
    ) -> None:
        target_dir = Path(base_data_dir if base_data_dir is not None else output_dir)
        self.base_data_dir = target_dir
        self.output_dir = target_dir
        self.adapter = adapter if adapter is not None else BinanceVisionPublicAdapter()
        self.catalog = catalog if catalog is not None else DuckDBCatalog(target_dir / "catalog.duckdb")
        self.durability_mode = (
            durability_mode if isinstance(durability_mode, DurabilityMode) else DurabilityMode(str(durability_mode))
        )
        self.manifest_manager = (
            manifest_manager
            if manifest_manager is not None
            else ManifestManager(target_dir / "manifests", durability_mode=self.durability_mode)
        )
        self.clock = BinanceServerTimeObserver(self.adapter)
        self.recovery_engine = RecoveryEngine(self.manifest_manager, self.catalog, base_data_dir=self.base_data_dir)

    def recover_active_manifests(self, allow_active_lease_skip: bool = True) -> list[RecoveryAction]:
        """Scan and recover all in-flight or interrupted manifests."""
        return self.recovery_engine.scan_and_recover(allow_active_lease_skip=allow_active_lease_skip)

    def ingest(self, request: IngestionRequest) -> IngestionResult:
        """Execute ingestion pipeline for a structured IngestionRequest."""
        return self.run(
            symbol=request.symbol,
            timeframe=request.timeframe,
            since_ms=request.since_ms,
            until_ms=request.until_ms,
            force=request.force,
            fail_on_gaps=request.fail_on_gaps,
            durability_mode=request.durability_mode,
        )

    def run(
        self,
        symbol: str,
        timeframe: SupportedTimeframe,
        since_ms: int,
        until_ms: int,
        force: bool = False,
        fail_on_gaps: bool = False,
        durability_mode: DurabilityMode | str | None = None,
    ) -> IngestionResult:
        effective_durability = (
            (durability_mode if isinstance(durability_mode, DurabilityMode) else DurabilityMode(str(durability_mode)))
            if durability_mode is not None
            else self.durability_mode
        )
        # Scan and reconcile any interrupted manifests before starting
        self.recover_active_manifests()

        exchange_id = "binance"
        market_type = "spot"
        dataset_identity = f"{exchange_id}:{market_type}:{symbol}:{timeframe.value}:{since_ms}:{until_ms}"

        # 1. Idempotency check: see if a verified committed snapshot already exists
        if not force:
            existing = self.catalog.find_latest_committed(dataset_identity)
            if existing:
                cached_manifest = IngestionManifest(
                    ingestion_uuid=existing.ingestion_uuid,
                    stage=ManifestStage.COMMITTED.value,
                    exchange_id=existing.exchange_id,
                    market_type=existing.market_type,
                    symbol=existing.symbol,
                    timeframe=existing.timeframe,
                    requested_since_ms=since_ms,
                    requested_until_ms=until_ms,
                    expected_records=existing.expected_records,
                    record_count=existing.record_count,
                    missing_intervals=existing.missing_intervals,
                    content_fingerprint_sha256=existing.content_fingerprint_sha256,
                    file_sha256=existing.file_sha256 or "",
                    target_file_path=existing.file_path or "",
                )
                cached_report = DataQualityReport(
                    requested_since_ms=since_ms,
                    requested_until_ms=until_ms,
                    timeframe=timeframe,
                    expected_records=existing.expected_records,
                    record_count=existing.record_count,
                    missing_intervals=existing.missing_intervals,
                    gap_count=existing.gap_count,
                    gaps=(),
                    quality_status=existing.quality_status,
                )
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
                        manifest=cached_manifest,
                        quality_report=cached_report,
                        actual_first_open=None,
                        actual_last_close=None,
                        ingestion_status="COMMITTED",
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
                            manifest=cached_manifest,
                            quality_report=cached_report,
                            actual_first_open=existing.actual_first_open,
                            actual_last_close=existing.actual_last_close,
                            ingestion_status="COMMITTED",
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

        writer_lock = self.manifest_manager.lock_manifest(ingestion_uuid)
        writer_lock.acquire()
        try:
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

                empty_manifest = IngestionManifest(
                    ingestion_uuid=ingestion_uuid,
                    stage=ManifestStage.COMMITTED.value,
                    exchange_id=exchange_id,
                    market_type=market_type,
                    symbol=symbol,
                    timeframe=timeframe.value,
                    requested_since_ms=since_ms,
                    requested_until_ms=until_ms,
                    expected_records=expected_records,
                    record_count=0,
                    missing_intervals=expected_records,
                    gap_count=empty_report.gap_count,
                    gaps=[
                        {
                            "gap_type": g.gap_type.value,
                            "expected_open_ms": g.expected_open_ms,
                            "subsequent_open_ms": g.subsequent_open_ms,
                            "missing_count": g.missing_count,
                        }
                        for g in empty_report.gaps
                    ],
                    content_fingerprint_sha256=content_fingerprint,
                    durability_status=self.manifest_manager.last_durability_status.value,
                    durability_warning=self.manifest_manager.durability_warning,
                )

                return IngestionResult(
                    snapshot_id=snapshot_id,
                    ingestion_uuid=ingestion_uuid,
                    dataset_identity=dataset_identity,
                    quality_status="EMPTY",
                    record_count=0,
                    missing_intervals=expected_records,
                    gap_count=empty_report.gap_count,
                    content_fingerprint_sha256=content_fingerprint,
                    file_path=None,
                    file_sha256=None,
                    manifest=empty_manifest,
                    quality_report=empty_report,
                    actual_first_open=None,
                    actual_last_close=None,
                    ingestion_status="COMMITTED",
                    durability_status=empty_manifest.durability_status,
                    durability_warning=empty_manifest.durability_warning,
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

            if fail_on_gaps and quality_report.missing_intervals > 0:
                raise GapFailureError(
                    f"Dataset contains {quality_report.missing_intervals} gap intervals "
                    "across requested window and fail_on_gaps is True."
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

            staging_file_path, target_file_path = derive_canonical_dataset_paths(
                base_data_dir=self.base_data_dir,
                exchange_id=exchange_id,
                market_type=market_type,
                symbol=symbol,
                timeframe=timeframe.value,
                since_ms=since_ms,
                until_ms=until_ms,
                content_fingerprint=content_fingerprint,
                ingestion_uuid=ingestion_uuid,
            )
            target_file_path.parent.mkdir(parents=True, exist_ok=True)

            since_str = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(since_ms / 1000))
            until_str = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(until_ms / 1000))

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
                gap_count=quality_report.gap_count,
                gaps=[
                    {
                        "gap_type": g.gap_type.value,
                        "expected_open_ms": g.expected_open_ms,
                        "subsequent_open_ms": g.subsequent_open_ms,
                        "missing_count": g.missing_count,
                    }
                    for g in quality_report.gaps
                ],
                content_fingerprint_sha256=content_fingerprint,
                file_sha256="",
                stage=ManifestStage.INITIALIZING.value,
                created_at_utc=now_utc_str,
                updated_at_utc=now_utc_str,
                durability_status=self.manifest_manager.last_durability_status.value,
                durability_warning=self.manifest_manager.durability_warning,
            )
            self.manifest_manager.write_manifest(manifest)

            # Stage writing
            self.manifest_manager.update_stage(manifest, ManifestStage.STAGING)
            write_staging_parquet(candles, staging_file_path)
            manifest.file_sha256 = compute_file_sha256(staging_file_path)
            self.manifest_manager.update_stage(manifest, ManifestStage.STAGED)

            # Publication
            self.manifest_manager.update_stage(manifest, ManifestStage.PUBLISHING)
            file_sha256 = publish_staging_file_no_clobber(
                staging_file_path,
                target_file_path,
                durability_mode=effective_durability,
            )
            parent_synced = sync_directory(target_file_path.parent, durability_mode=effective_durability)
            if not parent_synced:
                manifest.durability_status = DurabilityStatus.DEGRADED.value
                manifest.durability_warning = f"Directory sync failed for {target_file_path.parent}"
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
            self.manifest_manager.update_stage(manifest, ManifestStage.CLEANUP)

            # Cleanup manifest
            cleanup_synced = self.manifest_manager.unlink_manifest(ingestion_uuid)
            if not cleanup_synced and manifest.durability_status != DurabilityStatus.DEGRADED.value:
                manifest.durability_status = DurabilityStatus.DEGRADED.value
                manifest.durability_warning = (
                    f"Directory sync failed during manifest cleanup for {self.manifest_manager.manifests_dir}"
                )

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
                manifest=manifest,
                quality_report=quality_report,
                actual_first_open=candles[0].open_time,
                actual_last_close=candles[-1].close_time,
                ingestion_status="COMMITTED",
                durability_status=manifest.durability_status,
                durability_warning=manifest.durability_warning,
            )
        finally:
            writer_lock.release()
