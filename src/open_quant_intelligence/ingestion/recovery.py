"""Crash recovery engine for interrupted ingestion jobs and manifest lifecycle reconciliation."""

import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from open_quant_intelligence.data.quality import (
    GapType,
    QualityGapEvent,
    analyze_dataset_quality,
)
from open_quant_intelligence.domain.candle import Candle
from open_quant_intelligence.domain.timeframe import SupportedTimeframe
from open_quant_intelligence.storage.catalog import CatalogDatasetRecord, DuckDBCatalog
from open_quant_intelligence.storage.fingerprint import compute_file_sha256
from open_quant_intelligence.storage.manifest import (
    IngestionManifest,
    ManifestLockError,
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


def validate_dataset_paths(
    staging_path_str: str | None,
    target_path_str: str | None,
    base_data_dir: Path,
) -> tuple[Path | None, Path | None]:
    """Validate that staging and target files strictly obey dataset storage roles.

    Rules:
    - Staging and target files must be inside the authorized dataset storage area (base_data_dir / 'datasets').
    - Staging and target files must be distinct sibling paths in the same directory.
    - Validate expected filename and suffix relationships (target ends with .parquet, staging with .tmp).
    - Reject paths pointing to catalog.duckdb, manifests, configuration files,
      directories, or arbitrary non-dataset files.
    - Reject path traversal ('..') and symlink escapes pointing outside the dataset directory.
    - Returns resolved, validated Path objects.
    - Never deletes or modifies any files on error.
    """
    datasets_root = (base_data_dir / "datasets").resolve()
    manifests_root = (base_data_dir / "manifests").resolve()

    def _validate_single_path(p_str: str | None, is_target: bool) -> Path | None:
        if not p_str:
            return None

        p = Path(p_str)
        if ".." in p.parts:
            raise RecoveryFailureError(f"Path traversal detected in path: {p_str}")

        # Reject directories
        if p.is_dir():
            raise RecoveryFailureError(f"Path points to a directory, not a dataset file: {p_str}")

        # Reject protected files and configuration extensions
        name_lower = p.name.lower()
        if "catalog.duckdb" in name_lower or name_lower.endswith(
            (".duckdb", ".duckdb.wal", ".json", ".yaml", ".yml", ".toml", ".ini", ".conf", ".cfg", ".env")
        ):
            raise RecoveryFailureError(f"Unauthorized path role (protected system or config file): {p_str}")

        resolved = p.resolve()
        if resolved.is_dir():
            raise RecoveryFailureError(f"Resolved path points to a directory, not a dataset file: {p_str}")

        # Must be inside datasets_root
        try:
            if not resolved.is_relative_to(datasets_root):
                raise RecoveryFailureError(
                    f"Path {p_str} resolves outside authorized dataset directory {datasets_root}"
                )
        except AttributeError as e:
            if not str(resolved).startswith(str(datasets_root)):
                raise RecoveryFailureError(
                    f"Path {p_str} resolves outside authorized dataset directory {datasets_root}"
                ) from e

        # Ensure not in manifests or catalog
        try:
            if resolved.is_relative_to(manifests_root):
                raise RecoveryFailureError(f"Path cannot point inside manifests directory: {p_str}")
        except AttributeError:
            pass

        if "catalog.duckdb" in str(resolved).lower():
            raise RecoveryFailureError(f"Path cannot point to catalog database: {p_str}")

        # Symlink escape verification
        curr = p
        while True:
            if curr.is_symlink():
                try:
                    sym_target = curr.resolve()
                    if not sym_target.is_relative_to(datasets_root):
                        raise RecoveryFailureError(
                            f"Symlink escape detected: {curr} points to {sym_target} outside {datasets_root}"
                        )
                except OSError:
                    pass
            if curr.parent == curr:
                break
            curr = curr.parent

        if is_target and not resolved.name.endswith(".parquet"):
            raise RecoveryFailureError(f"Target dataset file must end with .parquet: {resolved.name}")

        return resolved

    resolved_staging = _validate_single_path(staging_path_str, is_target=False)
    resolved_target = _validate_single_path(target_path_str, is_target=True)

    if resolved_staging is not None and resolved_target is not None:
        if resolved_staging == resolved_target:
            raise RecoveryFailureError("Staging path and target path must be distinct paths.")
        if resolved_staging.parent != resolved_target.parent:
            raise RecoveryFailureError("Staging file and target file must be sibling paths in the same directory.")
        # Suffix relationship
        if not (
            resolved_staging.name.startswith(resolved_target.name)
            or resolved_staging.name.startswith(resolved_target.stem)
        ) or not (".tmp" in resolved_staging.name or resolved_staging.name.endswith(".tmp")):
            raise RecoveryFailureError(
                f"Staging filename '{resolved_staging.name}' is not a valid sibling temporary file "
                f"for target '{resolved_target.name}'."
            )

    return resolved_staging, resolved_target


def _verify_parquet_file(file_path: Path, expected_record_count: int) -> None:
    """Verify that a Parquet file exists, can be queried, and matches expected row count."""
    if not file_path.exists() or not file_path.is_file():
        raise RecoveryFailureError(f"Parquet file {file_path} does not exist.")

    con = duckdb.connect(":memory:")
    try:
        escaped = str(file_path).replace("'", "''")
        res = con.execute(f"SELECT count(*) FROM read_parquet('{escaped}')").fetchone()
        actual_count = res[0] if res else 0
    except Exception as e:
        raise RecoveryFailureError(f"Failed to read parquet file {file_path}: {e}") from e
    finally:
        con.close()

    if actual_count != expected_record_count:
        raise RecoveryFailureError(
            f"Physical Parquet row count ({actual_count}) does not match "
            f"manifest.record_count ({expected_record_count})."
        )


def validate_manifest_integrity(
    manifest: IngestionManifest,
    base_dir: Path,
) -> tuple[Path | None, Path | None]:
    """Validate manifest fields, stage, schema_version, UUID, gap metadata, and path roles before action."""
    if manifest.schema_version != 1:
        raise RecoveryFailureError(
            f"Unsupported schema_version {manifest.schema_version} in manifest {manifest.ingestion_uuid}."
        )

    valid_stages = {s.value for s in ManifestStage}
    if manifest.stage not in valid_stages:
        raise RecoveryFailureError(f"Invalid manifest stage: {manifest.stage}")

    try:
        uuid.UUID(manifest.ingestion_uuid)
    except (ValueError, TypeError) as e:
        raise RecoveryFailureError(f"Invalid ingestion_uuid format: {manifest.ingestion_uuid}") from e

    expected_identity = (
        f"{manifest.exchange_id}:{manifest.market_type}:{manifest.symbol}:"
        f"{manifest.timeframe}:{manifest.requested_since_ms}:{manifest.requested_until_ms}"
    )
    if manifest.dataset_identity != expected_identity:
        raise RecoveryFailureError(
            f"Dataset identity mismatch: recorded {manifest.dataset_identity}, expected {expected_identity}"
        )

    if manifest.expected_records <= 0:
        raise RecoveryFailureError(f"expected_records must be positive, found {manifest.expected_records}")

    if manifest.record_count + manifest.missing_intervals != manifest.expected_records:
        raise RecoveryFailureError(
            f"Invariant violation: record_count ({manifest.record_count}) + "
            f"missing_intervals ({manifest.missing_intervals}) != expected_records ({manifest.expected_records})"
        )

    # Gap metadata validation
    if manifest.gaps:
        if len(manifest.gaps) != manifest.gap_count:
            raise RecoveryFailureError(
                f"Inconsistent gap metadata: len(gaps) ({len(manifest.gaps)}) != gap_count ({manifest.gap_count})"
            )
        valid_gap_types = {gt.value for gt in GapType}
        total_missing = 0
        for idx, g in enumerate(manifest.gaps):
            if not isinstance(g, dict):
                raise RecoveryFailureError(f"Gap at index {idx} must be a dictionary.")
            g_type = g.get("gap_type")
            if g_type not in valid_gap_types:
                raise RecoveryFailureError(f"Invalid gap_type at index {idx}: {g_type}")
            missing_count = g.get("missing_count")
            if not isinstance(missing_count, int) or missing_count <= 0:
                raise RecoveryFailureError(f"missing_count at index {idx} must be a positive int: {missing_count}")
            total_missing += missing_count
            exp_open = g.get("expected_open_ms")
            if not isinstance(exp_open, int) or exp_open < 0:
                raise RecoveryFailureError(f"expected_open_ms at index {idx} must be a non-negative int: {exp_open}")
            if g_type in (GapType.LEADING.value, GapType.INTERNAL.value):
                sub_open = g.get("subsequent_open_ms")
                if not isinstance(sub_open, int) or sub_open <= exp_open:
                    raise RecoveryFailureError(
                        f"subsequent_open_ms at index {idx} must be an int strictly greater than expected_open_ms."
                    )

        if total_missing != manifest.missing_intervals:
            raise RecoveryFailureError(
                f"Inconsistent gap metadata: sum(missing_count) ({total_missing}) != "
                f"missing_intervals ({manifest.missing_intervals})"
            )

    resolved_s, resolved_t = validate_dataset_paths(
        manifest.staging_file_path,
        manifest.target_file_path,
        base_dir,
    )

    if manifest.stage in (ManifestStage.STAGED.value, ManifestStage.PUBLISHING.value):
        raw_hash = manifest.file_sha256.removeprefix("sha256:")
        if not raw_hash or len(raw_hash) != 64 or not all(c in "0123456789abcdefABCDEF" for c in raw_hash):
            raise RecoveryFailureError(
                f"STAGED or PUBLISHING manifest {manifest.ingestion_uuid} must contain a valid, "
                f"non-empty SHA-256 digest (found: {manifest.file_sha256!r})."
            )

    return resolved_s, resolved_t


class RecoveryEngine:
    """Scans and recovers interrupted ingestion manifests deterministically."""

    def __init__(
        self,
        manifest_manager: ManifestManager,
        catalog: DuckDBCatalog,
        base_data_dir: Path | str | None = None,
    ) -> None:
        self.manifest_manager = manifest_manager
        self.catalog = catalog
        if base_data_dir is not None:
            self.base_data_dir = Path(base_data_dir)
        else:
            self.base_data_dir = self.manifest_manager.manifests_dir.parent

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
        """Reconcile a single interrupted manifest according to its stage under an atomic lock."""
        try:
            with self.manifest_manager.lock_manifest(manifest.ingestion_uuid):
                fresh = self.manifest_manager.read_manifest(manifest.ingestion_uuid)
                if not fresh:
                    return None
                return self._recover_manifest_locked(
                    fresh,
                    allow_active_lease_skip=allow_active_lease_skip,
                )
        except ManifestLockError as e:
            if allow_active_lease_skip:
                return None
            raise LeaseActiveError(f"Could not acquire atomic lock for manifest {manifest.ingestion_uuid}: {e}") from e

    def _recover_manifest_locked(
        self,
        manifest: IngestionManifest,
        allow_active_lease_skip: bool = True,
    ) -> RecoveryAction | None:
        """Internal recovery routine executing within the held single-writer lock."""
        # 1. Lease ownership validation
        if is_lease_active(manifest):
            if manifest.lease_owner_pid != os.getpid():
                if allow_active_lease_skip:
                    return None
                raise LeaseActiveError(
                    f"Manifest {manifest.ingestion_uuid} is actively held by PID {manifest.lease_owner_pid}."
                )

        # 2. Integrity and safe dataset paths validation (never touches unrelated files on failure)
        s_path, t_path = validate_manifest_integrity(manifest, self.base_data_dir)

        # Claim lease for recovery coordinator
        manifest.lease_owner_pid = os.getpid()
        manifest.lease_expires_at_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 1800))
        self.manifest_manager.write_manifest(manifest)

        orig_stage = manifest.stage

        # 3. Interrupted State Handling
        if orig_stage in (ManifestStage.INITIALIZING.value, ManifestStage.STAGING.value):
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
            if not s_path or not s_path.exists():
                self.manifest_manager.transition_stage(manifest, ManifestStage.CLEANUP)
                self.manifest_manager.cleanup_manifest(manifest.ingestion_uuid)
                raise RecoveryFailureError(f"Staging file missing for STAGED manifest {manifest.ingestion_uuid}.")

            try:
                _verify_parquet_file(s_path, manifest.record_count)
                current_s_hash = compute_file_sha256(s_path)
                if current_s_hash != manifest.file_sha256:
                    raise RecoveryFailureError(
                        f"Staging file hash {current_s_hash} does not match recorded hash {manifest.file_sha256}."
                    )
            except Exception:
                s_path.unlink(missing_ok=True)
                self.manifest_manager.transition_stage(manifest, ManifestStage.CLEANUP)
                self.manifest_manager.cleanup_manifest(manifest.ingestion_uuid)
                raise

            self.manifest_manager.transition_stage(manifest, ManifestStage.PUBLISHING)
            orig_stage = ManifestStage.PUBLISHING.value

        if orig_stage == ManifestStage.PUBLISHING.value:
            if not t_path:
                raise RecoveryFailureError(f"Target path not defined in manifest {manifest.ingestion_uuid}.")

            if t_path.exists():
                target_sha = compute_file_sha256(t_path)
                if target_sha == manifest.file_sha256:
                    _verify_parquet_file(t_path, manifest.record_count)
                    if s_path and s_path.exists():
                        s_path.unlink(missing_ok=True)
                    self.manifest_manager.transition_stage(manifest, ManifestStage.PUBLISHED)
                else:
                    raise RecoveryConflictError(
                        f"Pre-existing target file {t_path} differs from staging artifact. "
                        f"Expected {manifest.file_sha256}, found {target_sha}."
                    )
            else:
                if not s_path or not s_path.exists():
                    self.manifest_manager.transition_stage(manifest, ManifestStage.CLEANUP)
                    self.manifest_manager.cleanup_manifest(manifest.ingestion_uuid)
                    raise RecoveryFailureError(f"Staging file {s_path} missing during publication recovery.")
                _verify_parquet_file(s_path, manifest.record_count)
                pub_sha = publish_staging_file_no_clobber(
                    s_path,
                    t_path,
                    durability_mode=self.manifest_manager.durability_mode,
                )
                self.manifest_manager.transition_stage(manifest, ManifestStage.PUBLISHED, file_sha256=pub_sha)

            orig_stage = ManifestStage.PUBLISHED.value

        if orig_stage in (ManifestStage.PUBLISHED.value, ManifestStage.COMMITTING_CATALOG.value):
            if not t_path or not t_path.exists():
                raise RecoveryFailureError(f"Published file {t_path} missing during catalog commit recovery.")

            _verify_parquet_file(t_path, manifest.record_count)

            target_sha = compute_file_sha256(t_path)
            if target_sha != manifest.file_sha256:
                raise RecoveryFailureError(
                    f"Published file {t_path} hash {target_sha} does not match {manifest.file_sha256}."
                )

            if orig_stage == ManifestStage.PUBLISHED.value:
                self.manifest_manager.transition_stage(manifest, ManifestStage.COMMITTING_CATALOG)

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
        """Idempotently insert catalog record and exact gap events if not already present."""
        existing = self.catalog.find_by_id_or_fingerprint(manifest.dataset_identity)
        if existing and existing.file_sha256 == file_sha256:
            return

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

        # Reconstruct or deserialize complete gap events
        gaps: list[QualityGapEvent] = []
        if manifest.gaps:
            for g in manifest.gaps:
                gaps.append(
                    QualityGapEvent(
                        gap_type=GapType(g["gap_type"]),
                        expected_open_ms=g.get("expected_open_ms"),
                        subsequent_open_ms=g.get("subsequent_open_ms"),
                        missing_count=g["missing_count"],
                    )
                )
        elif manifest.missing_intervals > 0:
            if manifest.record_count == 0:
                report = analyze_dataset_quality(
                    candles=(),
                    since_ms=manifest.requested_since_ms,
                    until_ms=manifest.requested_until_ms,
                    timeframe=SupportedTimeframe(manifest.timeframe),
                )
                gaps = list(report.gaps)
            else:
                con = duckdb.connect(":memory:")
                try:
                    escaped_path = str(target_path).replace("'", "''")
                    rows = con.execute(
                        f"SELECT open_time FROM read_parquet('{escaped_path}') ORDER BY open_time ASC"
                    ).fetchall()
                finally:
                    con.close()

                recon_candles = [
                    Candle(
                        exchange_id=manifest.exchange_id,
                        market_type=manifest.market_type,
                        symbol=manifest.symbol,
                        timeframe=manifest.timeframe,
                        open_time=r[0].replace(tzinfo=timezone.utc) if r[0].tzinfo is None else r[0],
                        open=1.0,
                        high=1.0,
                        low=1.0,
                        close=1.0,
                        base_volume=1.0,
                    )
                    for r in rows
                ]
                report = analyze_dataset_quality(
                    candles=recon_candles,
                    since_ms=manifest.requested_since_ms,
                    until_ms=manifest.requested_until_ms,
                    timeframe=SupportedTimeframe(manifest.timeframe),
                )
                gaps = list(report.gaps)

        gap_count = len(gaps) if gaps else manifest.gap_count
        missing_intervals = sum(g.missing_count for g in gaps) if gaps else manifest.missing_intervals

        quality_status = (
            "EMPTY" if manifest.record_count == 0 else ("GAP_FREE" if missing_intervals == 0 else "GAP_CONTAINING")
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
            gap_count=gap_count,
            missing_intervals=missing_intervals,
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

        self.catalog.commit_dataset_snapshot(record, gaps=gaps)
