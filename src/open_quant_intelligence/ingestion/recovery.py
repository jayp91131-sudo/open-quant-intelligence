"""Crash recovery engine for interrupted ingestion jobs and manifest lifecycle reconciliation."""

import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from open_quant_intelligence.storage.catalog import CatalogDatasetRecord, DuckDBCatalog
from open_quant_intelligence.storage.fingerprint import compute_file_sha256
from open_quant_intelligence.storage.manifest import (
    IngestionManifest,
    ManifestManager,
    ManifestStage,
    is_lease_active,
)
from open_quant_intelligence.storage.parquet_store import (
    DatasetPathCollisionError,
    publish_staging_file_no_clobber,
)


class RecoveryError(Exception):
    """Base exception for crash recovery failures."""


class RecoveryConflictError(RecoveryError, DatasetPathCollisionError):
    """Raised when recovery encounters a pre-existing target file with differing content."""


class RecoveryFailureError(RecoveryError):
    """Raised when an unrecoverable artifact corruption or missing file is encountered."""


class LeaseActiveError(RecoveryError):
    """Raised when an active manifest is locked by another running process."""


@dataclass(frozen=True)
class RecoveryAction:
    """Record of recovery action taken on an interrupted manifest."""

    ingestion_uuid: str
    original_stage: str
    final_stage: str
    action_taken: str
    target_file_path: str | None = None
    target_file_sha256: str | None = None


class RecoveryEngine:
    """Scans and recovers interrupted ingestion manifests deterministically."""

    def __init__(self, manifest_manager: ManifestManager, catalog: DuckDBCatalog) -> None:
        self.manifest_manager = manifest_manager
        self.catalog = catalog

    def scan_and_recover(self, allow_active_lease_skip: bool = True) -> list[RecoveryAction]:
        """Scan active manifests and reconcile each interrupted state.

        Args:
            allow_active_lease_skip: If True, skip manifests actively held by live PIDs
                instead of raising LeaseActiveError.

        Returns:
            List of RecoveryAction records for recovered manifests.

        Raises:
            RecoveryConflictError: If a target file exists with conflicting content.
            RecoveryFailureError: If critical staging artifacts cannot be verified.
            LeaseActiveError: If allow_active_lease_skip=False and a manifest lease is active.
        """
        actions: list[RecoveryAction] = []
        active_manifests = self.manifest_manager.list_active_manifests()

        for manifest in active_manifests:
            action = self.recover_manifest(manifest, allow_active_lease_skip=allow_active_lease_skip)
            if action:
                actions.append(action)

        return actions

    def recover_manifest(
        self,
        manifest: IngestionManifest,
        allow_active_lease_skip: bool = True,
    ) -> RecoveryAction | None:
        """Reconcile a single interrupted manifest according to its stage."""
        # 1. Lease ownership validation
        if is_lease_active(manifest):
            if manifest.lease_owner_pid != os.getpid():
                if allow_active_lease_skip:
                    return None
                raise LeaseActiveError(
                    f"Manifest {manifest.ingestion_uuid} is actively held by PID {manifest.lease_owner_pid}."
                )

        # Claim lease for recovery coordinator
        manifest.lease_owner_pid = os.getpid()
        manifest.lease_expires_at_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 1800))
        self.manifest_manager.write_manifest(manifest)

        orig_stage = manifest.stage
        s_path = Path(manifest.staging_file_path) if manifest.staging_file_path else None
        t_path = Path(manifest.target_file_path) if manifest.target_file_path else None

        # 2. Interrupted State Handling
        if orig_stage in (ManifestStage.INITIALIZING.value, ManifestStage.STAGING.value):
            # Safe cleanup of incomplete staging artifact
            if s_path and s_path.exists():
                s_path.unlink(missing_ok=True)
            self.manifest_manager.transition_stage(manifest, ManifestStage.CLEANUP)
            self.manifest_manager.cleanup_manifest(manifest.ingestion_uuid)
            return RecoveryAction(
                ingestion_uuid=manifest.ingestion_uuid,
                original_stage=orig_stage,
                final_stage="CLEANED",
                action_taken="DISCARDED_INCOMPLETE_STAGING",
            )

        if orig_stage == ManifestStage.STAGED.value:
            # Staging finished, verify staging hash before proceeding
            if not s_path or not s_path.exists():
                self.manifest_manager.transition_stage(manifest, ManifestStage.CLEANUP)
                self.manifest_manager.cleanup_manifest(manifest.ingestion_uuid)
                raise RecoveryFailureError(f"Staging file missing for STAGED manifest {manifest.ingestion_uuid}.")

            current_s_hash = compute_file_sha256(s_path)
            if manifest.file_sha256 and current_s_hash != manifest.file_sha256:
                s_path.unlink(missing_ok=True)
                self.manifest_manager.transition_stage(manifest, ManifestStage.CLEANUP)
                self.manifest_manager.cleanup_manifest(manifest.ingestion_uuid)
                raise RecoveryFailureError(
                    f"Staging file hash {current_s_hash} does not match recorded hash {manifest.file_sha256}."
                )

            manifest.file_sha256 = current_s_hash
            # Proceed to PUBLISHING
            self.manifest_manager.transition_stage(manifest, ManifestStage.PUBLISHING)
            orig_stage = ManifestStage.PUBLISHING.value

        if orig_stage == ManifestStage.PUBLISHING.value:
            # Reconcile publication status
            if not t_path:
                raise RecoveryFailureError(f"Target path not defined in manifest {manifest.ingestion_uuid}.")

            if t_path.exists():
                target_sha = compute_file_sha256(t_path)
                if target_sha == manifest.file_sha256:
                    # Publication already finished before crash
                    if s_path and s_path.exists():
                        s_path.unlink(missing_ok=True)
                    self.manifest_manager.transition_stage(manifest, ManifestStage.PUBLISHED)
                else:
                    # Pre-existing target differs! Must remain untouched byte-for-byte!
                    raise RecoveryConflictError(
                        f"Pre-existing target file {t_path} differs from staging artifact. "
                        f"Expected {manifest.file_sha256}, found {target_sha}."
                    )
            else:
                # Target not yet created; publish from staging
                if not s_path or not s_path.exists():
                    self.manifest_manager.transition_stage(manifest, ManifestStage.CLEANUP)
                    self.manifest_manager.cleanup_manifest(manifest.ingestion_uuid)
                    raise RecoveryFailureError(f"Staging file {s_path} missing during publication recovery.")
                pub_sha = publish_staging_file_no_clobber(s_path, t_path)
                self.manifest_manager.transition_stage(manifest, ManifestStage.PUBLISHED, file_sha256=pub_sha)

            orig_stage = ManifestStage.PUBLISHED.value

        if orig_stage in (ManifestStage.PUBLISHED.value, ManifestStage.COMMITTING_CATALOG.value):
            # Verify published file integrity
            if not t_path or not t_path.exists():
                raise RecoveryFailureError(f"Published file {t_path} missing during catalog commit recovery.")
            target_sha = compute_file_sha256(t_path)
            if target_sha != manifest.file_sha256:
                raise RecoveryFailureError(
                    f"Published file {t_path} hash {target_sha} does not match {manifest.file_sha256}."
                )

            if orig_stage == ManifestStage.PUBLISHED.value:
                self.manifest_manager.transition_stage(manifest, ManifestStage.COMMITTING_CATALOG)

            # Idempotently commit catalog record
            self._reconcile_catalog_record(manifest, t_path, target_sha)
            self.manifest_manager.transition_stage(manifest, ManifestStage.COMMITTED)
            orig_stage = ManifestStage.COMMITTED.value

        if orig_stage == ManifestStage.COMMITTED.value:
            if s_path and s_path.exists():
                s_path.unlink(missing_ok=True)
            self.manifest_manager.transition_stage(manifest, ManifestStage.CLEANUP)
            orig_stage = ManifestStage.CLEANUP.value

        if orig_stage == ManifestStage.CLEANUP.value:
            if s_path and s_path.exists():
                s_path.unlink(missing_ok=True)
            self.manifest_manager.cleanup_manifest(manifest.ingestion_uuid)
            return RecoveryAction(
                ingestion_uuid=manifest.ingestion_uuid,
                original_stage=manifest.stage,
                final_stage="CLEANED",
                action_taken="COMPLETED_INGESTION_AND_CLEANUP",
                target_file_path=str(t_path) if t_path else None,
                target_file_sha256=manifest.file_sha256,
            )

        return None

    def _reconcile_catalog_record(
        self,
        manifest: IngestionManifest,
        target_path: Path,
        file_sha256: str,
    ) -> None:
        """Idempotently insert catalog record if not already present."""
        existing = self.catalog.find_by_id_or_fingerprint(manifest.dataset_identity)
        if existing and existing.file_sha256 == file_sha256:
            return

        # Query actual min/max timestamps directly from published parquet
        actual_first_open = None
        actual_last_close = None
        con = duckdb.connect(":memory:")
        try:
            escaped_path = str(target_path).replace("'", "''")
            res = con.execute(f"SELECT min(open_time), max(close_time) FROM read_parquet('{escaped_path}')").fetchone()
            if res and res[0] is not None and res[1] is not None:
                actual_first_open = res[0]
                actual_last_close = res[1]
        finally:
            con.close()

        quality_status = (
            "EMPTY"
            if manifest.record_count == 0
            else ("GAP_FREE" if manifest.missing_intervals == 0 else "GAP_CONTAINING")
        )
        snapshot_id = f"snap_{manifest.ingestion_uuid[:12]}"
        now_dt = datetime.now(timezone.utc)

        record = CatalogDatasetRecord(
            snapshot_id=snapshot_id,
            dataset_identity=manifest.dataset_identity,
            exchange_id=manifest.exchange_id,
            market_type=manifest.market_type,
            symbol=manifest.symbol,
            timeframe=manifest.timeframe,
            requested_since=datetime.fromtimestamp(manifest.requested_since_ms / 1000, tz=timezone.utc),
            requested_until=datetime.fromtimestamp(manifest.requested_until_ms / 1000, tz=timezone.utc),
            actual_first_open=actual_first_open,
            actual_last_close=actual_last_close,
            expected_records=manifest.expected_records,
            record_count=manifest.record_count,
            gap_count=0 if manifest.missing_intervals == 0 else 1,
            missing_intervals=manifest.missing_intervals,
            content_fingerprint_sha256=manifest.content_fingerprint_sha256,
            file_sha256=file_sha256,
            file_path=str(target_path),
            file_size_bytes=target_path.stat().st_size,
            ccxt_version=manifest.ccxt_version,
            ingestion_uuid=manifest.ingestion_uuid,
            ingested_at=now_dt,
            ingestion_status="COMMITTED",
            quality_status=quality_status,
        )

        self.catalog.commit_dataset_snapshot(record, gaps=())
