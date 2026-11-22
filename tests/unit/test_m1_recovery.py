"""Tests for crash recovery, manifest lifecycle reconciliation, integrity, and gap preservation."""

import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

import duckdb
import pytest

from open_quant_intelligence.data.quality import GapType, analyze_dataset_quality
from open_quant_intelligence.domain.candle import Candle
from open_quant_intelligence.domain.timeframe import SupportedTimeframe
from open_quant_intelligence.ingestion.recovery import (
    LeaseActiveError,
    RecoveryConflictError,
    RecoveryEngine,
    RecoveryFailureError,
    derive_canonical_dataset_paths,
)
from open_quant_intelligence.storage.catalog import DuckDBCatalog
from open_quant_intelligence.storage.fingerprint import (
    compute_content_fingerprint,
    compute_file_sha256,
)
from open_quant_intelligence.storage.manifest import (
    IngestionManifest,
    InvalidStageTransitionError,
    ManifestManager,
    ManifestStage,
    is_lease_active,
)
from open_quant_intelligence.storage.parquet_store import write_staging_parquet


def _make_test_candles(
    count: int = 2,
    symbol: str = "BTC/USDT",
    timeframe: str = "1h",
    start_ms: int = 0,
    step_ms: int = 3_600_000,
) -> list[Candle]:
    """Helper to generate sequential test candles."""
    candles = []
    for i in range(count):
        dt = datetime.fromtimestamp((start_ms + i * step_ms) / 1000, tz=timezone.utc)
        candles.append(
            Candle(
                exchange_id="binance",
                market_type="spot",
                symbol=symbol,
                timeframe=timeframe,
                open_time=dt,
                open=100.0 + i,
                high=105.0 + i,
                low=95.0 + i,
                close=102.0 + i,
                base_volume=10.0 + i,
            )
        )
    return candles


def make_test_manifest_and_paths(
    base_dir: Path,
    test_uuid: str,
    stage: ManifestStage | str,
    symbol: str = "BTC/USDT",
    timeframe: str = "1h",
    since_ms: int = 0,
    until_ms: int = 7_200_000,
    expected_records: int = 2,
    record_count: int = 2,
    missing_intervals: int = 0,
    gap_count: int = 0,
    gaps: list[dict[str, Any]] | None = None,
    content_fingerprint: str = "",
    file_sha256: str = "",
) -> tuple[IngestionManifest, Path, Path]:
    """Generate canonical test manifest and file paths strictly bound to dataset identity."""
    staging_file, target_file = derive_canonical_dataset_paths(
        base_data_dir=base_dir,
        exchange_id="binance",
        market_type="spot",
        symbol=symbol,
        timeframe=timeframe,
        since_ms=since_ms,
        until_ms=until_ms,
        content_fingerprint=content_fingerprint,
        ingestion_uuid=test_uuid,
    )
    staging_file.parent.mkdir(parents=True, exist_ok=True)
    stage_val = stage.value if hasattr(stage, "value") else str(stage)
    manifest = IngestionManifest(
        ingestion_uuid=test_uuid,
        stage=stage_val,
        symbol=symbol,
        timeframe=timeframe,
        requested_since_ms=since_ms,
        requested_until_ms=until_ms,
        expected_records=expected_records,
        record_count=record_count,
        missing_intervals=missing_intervals,
        gap_count=gap_count,
        gaps=gaps or [],
        content_fingerprint_sha256=content_fingerprint,
        file_sha256=file_sha256,
        staging_file_path=str(staging_file),
        target_file_path=str(target_file),
    )
    return manifest, staging_file, target_file


@pytest.fixture
def recovery_env(tmp_path: Path) -> tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine]:
    manifests_dir = tmp_path / "manifests"
    manifest_mgr = ManifestManager(manifests_dir)
    catalog_path = tmp_path / "catalog.duckdb"
    catalog = DuckDBCatalog(catalog_path)
    datasets_dir = tmp_path / "datasets"
    datasets_dir.mkdir(parents=True, exist_ok=True)
    engine = RecoveryEngine(manifest_mgr, catalog, base_data_dir=tmp_path)
    return manifest_mgr, catalog, tmp_path, engine


def test_monotonic_stage_transitions(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Verifies that non-monotonic or illegal transitions raise InvalidStageTransitionError."""
    mgr, _, _, _ = recovery_env
    test_uuid = str(uuid.uuid4())
    manifest = IngestionManifest(ingestion_uuid=test_uuid, stage=ManifestStage.INITIALIZING.value)
    mgr.write_manifest(manifest)

    # Valid: INITIALIZING -> STAGING
    mgr.transition_stage(manifest, ManifestStage.STAGING)
    assert manifest.stage == ManifestStage.STAGING.value

    # Invalid: STAGING -> PUBLISHED (skipping STAGED and PUBLISHING)
    with pytest.raises(InvalidStageTransitionError, match="Illegal stage transition"):
        mgr.transition_stage(manifest, ManifestStage.PUBLISHED)

    # Invalid: STAGING -> INITIALIZING (backwards)
    with pytest.raises(InvalidStageTransitionError, match="Illegal stage transition"):
        mgr.transition_stage(manifest, ManifestStage.INITIALIZING)

    # Valid: STAGING -> CLEANUP (aborting)
    mgr.transition_stage(manifest, ManifestStage.CLEANUP)
    assert manifest.stage == ManifestStage.CLEANUP.value

    # Invalid: CLEANUP -> any other stage
    with pytest.raises(InvalidStageTransitionError, match="Illegal stage transition"):
        mgr.transition_stage(manifest, ManifestStage.COMMITTED)


def test_crash_recovery_initializing_and_staging(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Crash at INITIALIZING/STAGING cleans up partial files and unlinks manifest."""
    mgr, catalog, base_dir, engine = recovery_env
    test_uuid = str(uuid.uuid4())
    manifest, staging_file, target_file = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.STAGING,
        record_count=0,
        missing_intervals=2,
    )
    staging_file.write_text("partial corrupt staging bytes", encoding="utf-8")
    mgr.write_manifest(manifest)

    actions = engine.scan_and_recover()
    assert len(actions) == 1
    assert actions[0].action_taken == "DISCARDED_INCOMPLETE_STAGING"
    assert not staging_file.exists()
    assert mgr.read_manifest(test_uuid) is None


def test_crash_recovery_staged_valid(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Crash at STAGED with valid staging file completes publication and catalog commit."""
    mgr, catalog, base_dir, engine = recovery_env
    test_uuid = str(uuid.uuid4())
    candles = _make_test_candles(2)
    c_fp = compute_content_fingerprint(candles, symbol="BTC/USDT", timeframe="1h")

    manifest, staging_file, target_file = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.STAGED,
        content_fingerprint=c_fp,
    )
    write_staging_parquet(staging_file, candles, record_count=2)
    s_hash = compute_file_sha256(staging_file)
    manifest.file_sha256 = s_hash
    mgr.write_manifest(manifest)

    actions = engine.scan_and_recover()
    assert len(actions) == 1
    assert actions[0].action_taken == "COMPLETED_INGESTION_AND_CLEANUP"
    assert target_file.exists()
    assert compute_file_sha256(target_file) == s_hash
    assert not staging_file.exists()
    assert mgr.read_manifest(test_uuid) is None

    # Verify catalog record created
    cat_rec = catalog.find_by_id_or_fingerprint(manifest.dataset_identity)
    assert cat_rec is not None
    assert cat_rec.file_sha256 == s_hash
    assert cat_rec.record_count == 2


def test_crash_recovery_staged_corrupted_fails_safely(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Crash at STAGED with corrupted staging file raises RecoveryFailureError and cleans up."""
    mgr, _, base_dir, engine = recovery_env
    test_uuid = str(uuid.uuid4())
    dummy_hash = "sha256:" + ("a" * 64)
    manifest, staging_file, target_file = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.STAGED,
        file_sha256=dummy_hash,
    )
    staging_file.write_text("corrupted bytes", encoding="utf-8")
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError):
        engine.scan_and_recover()

    assert not staging_file.exists()
    assert mgr.read_manifest(test_uuid) is None


def test_crash_recovery_publishing_already_published(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Crash at PUBLISHING where publication completed before crash: reconciles published file."""
    mgr, catalog, base_dir, engine = recovery_env
    test_uuid = str(uuid.uuid4())
    candles = _make_test_candles(2)
    c_fp = compute_content_fingerprint(candles, symbol="BTC/USDT", timeframe="1h")

    manifest, staging_file, target_file = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.PUBLISHING,
        content_fingerprint=c_fp,
    )
    write_staging_parquet(target_file, candles, record_count=2)
    t_hash = compute_file_sha256(target_file)
    manifest.file_sha256 = t_hash
    mgr.write_manifest(manifest)

    actions = engine.scan_and_recover()
    assert len(actions) == 1
    assert actions[0].action_taken == "COMPLETED_INGESTION_AND_CLEANUP"
    assert mgr.read_manifest(test_uuid) is None

    cat_rec = catalog.find_by_id_or_fingerprint(manifest.dataset_identity)
    assert cat_rec is not None
    assert cat_rec.file_sha256 == t_hash


def test_crash_recovery_publishing_conflict_preserved_byte_for_byte(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """When target already exists with differing hash, RecoveryConflictError is raised and target is preserved."""
    mgr, catalog, base_dir, engine = recovery_env
    test_uuid = str(uuid.uuid4())
    candles_stg = _make_test_candles(2, start_ms=0)
    candles_tgt = _make_test_candles(2, start_ms=3_600_000)
    c_fp = compute_content_fingerprint(candles_stg, symbol="BTC/USDT", timeframe="1h")

    manifest, staging_file, target_file = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.PUBLISHING,
        content_fingerprint=c_fp,
    )
    write_staging_parquet(staging_file, candles_stg, record_count=2)
    write_staging_parquet(target_file, candles_tgt, record_count=2)

    stg_hash = compute_file_sha256(staging_file)
    tgt_hash = compute_file_sha256(target_file)
    target_bytes_orig = target_file.read_bytes()

    manifest.file_sha256 = stg_hash
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryConflictError, match="Pre-existing target file"):
        engine.scan_and_recover()

    # Crucial invariant: Target file must remain byte-for-byte untouched!
    assert target_file.exists()
    assert target_file.read_bytes() == target_bytes_orig
    assert compute_file_sha256(target_file) == tgt_hash


def test_crash_recovery_published_and_committing_catalog_idempotency(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Crash at PUBLISHED or COMMITTING_CATALOG reconciles catalog idempotently without duplicates."""
    mgr, catalog, base_dir, engine = recovery_env
    test_uuid = str(uuid.uuid4())
    candles = _make_test_candles(2)
    c_fp = compute_content_fingerprint(candles, symbol="BTC/USDT", timeframe="1h")

    manifest, staging_file, target_file = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.PUBLISHED,
        content_fingerprint=c_fp,
    )
    write_staging_parquet(target_file, candles, record_count=2)
    t_hash = compute_file_sha256(target_file)
    manifest.file_sha256 = t_hash
    mgr.write_manifest(manifest)

    # First recovery commit
    actions = engine.scan_and_recover()
    assert len(actions) == 1
    assert actions[0].action_taken == "COMPLETED_INGESTION_AND_CLEANUP"

    # Simulate running recovery again with COMMITTING_CATALOG manifest for same dataset
    manifest.stage = ManifestStage.COMMITTING_CATALOG.value
    mgr.write_manifest(manifest)

    actions2 = engine.scan_and_recover()
    assert len(actions2) == 1
    assert actions2[0].action_taken == "COMPLETED_INGESTION_AND_CLEANUP"

    # Verify catalog contains exactly 1 entry for this dataset identity
    with duckdb.connect(str(catalog.db_path)) as con:
        count = con.execute(
            "SELECT count(*) FROM datasets WHERE dataset_identity = ?",
            [manifest.dataset_identity],
        ).fetchone()[0]
        assert count == 1


def test_crash_recovery_active_lease_protection(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Active lease held by another live process is protected and not hijacked."""
    mgr, _, base_dir, engine = recovery_env
    test_uuid = str(uuid.uuid4())
    manifest, staging_file, target_file = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.STAGING,
        record_count=0,
        missing_intervals=2,
    )
    manifest.lease_owner_pid = 99999
    manifest.lease_expires_at_utc = "2099-01-01T00:00:00Z"
    mgr.write_manifest(manifest)

    with patch("open_quant_intelligence.storage.manifest.is_pid_alive", return_value=True):
        # By default, skip active leases
        actions = engine.scan_and_recover(allow_active_lease_skip=True)
        assert len(actions) == 0
        assert mgr.read_manifest(test_uuid) is not None

        # When allow_active_lease_skip=False, raise LeaseActiveError
        with pytest.raises(LeaseActiveError, match="actively held by PID 99999"):
            engine.scan_and_recover(allow_active_lease_skip=False)


def test_crash_recovery_preserves_multiple_distinct_gaps(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Recovery must preserve LEADING, INTERNAL, and TRAILING gaps with exact metadata and dataset_gaps rows."""
    mgr, catalog, base_dir, engine = recovery_env

    c1 = Candle(
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        open_time=datetime.fromtimestamp(3_600_000 / 1000, tz=timezone.utc),
        open=100.0,
        high=105.0,
        low=95.0,
        close=102.0,
        base_volume=10.0,
    )
    c4 = Candle(
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        open_time=datetime.fromtimestamp(14_400_000 / 1000, tz=timezone.utc),
        open=102.0,
        high=106.0,
        low=98.0,
        close=104.0,
        base_volume=12.0,
    )
    candles = [c1, c4]

    expected_report = analyze_dataset_quality(
        candles=candles,
        since_ms=0,
        until_ms=21_600_000,
        timeframe=SupportedTimeframe.H1,
    )
    assert expected_report.gap_count == 3
    assert expected_report.missing_intervals == 4
    assert [g.gap_type for g in expected_report.gaps] == [
        GapType.LEADING,
        GapType.INTERNAL,
        GapType.TRAILING,
    ]

    test_uuid = str(uuid.uuid4())
    c_fp = compute_content_fingerprint(candles, symbol="BTC/USDT", timeframe="1h")
    gaps_list = [
        {
            "gap_type": g.gap_type.value,
            "expected_open_ms": g.expected_open_ms,
            "subsequent_open_ms": g.subsequent_open_ms,
            "missing_count": g.missing_count,
        }
        for g in expected_report.gaps
    ]
    manifest, staging_file, target_file = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.PUBLISHED,
        since_ms=0,
        until_ms=21_600_000,
        expected_records=6,
        record_count=2,
        missing_intervals=4,
        gap_count=expected_report.gap_count,
        gaps=gaps_list,
        content_fingerprint=c_fp,
    )
    write_staging_parquet(target_file, candles, record_count=2)
    t_hash = compute_file_sha256(target_file)
    manifest.file_sha256 = t_hash
    mgr.write_manifest(manifest)

    # Simulate crash right after publication before first catalog insertion
    engine.scan_and_recover()

    # Reconciled catalog entry check
    cat_rec = catalog.find_by_id_or_fingerprint(manifest.dataset_identity)
    assert cat_rec is not None
    assert cat_rec.gap_count == 3
    assert cat_rec.missing_intervals == 4
    assert cat_rec.record_count == 2
    assert cat_rec.expected_records == 6
    assert cat_rec.quality_status == "GAP_CONTAINING"

    # Verify complete dataset_gaps table entries
    gaps_in_db = catalog.get_gaps(cat_rec.snapshot_id)
    assert len(gaps_in_db) == 3


def test_crash_recovery_missing_hash_fails_safely(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """A STAGED or PUBLISHING manifest missing a valid sha256 must fail rather than adopting a new hash."""
    mgr, _, base_dir, engine = recovery_env
    test_uuid = str(uuid.uuid4())
    manifest, staging_file, target_file = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.STAGED,
        file_sha256="",  # Empty hash!
    )
    staging_file.write_text("some data", encoding="utf-8")
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError, match="must contain a valid"):
        engine.scan_and_recover()


def test_crash_recovery_row_count_mismatch_fails_safely(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """When actual Parquet rows do not equal manifest.record_count, fail safely."""
    mgr, _, base_dir, engine = recovery_env
    test_uuid = str(uuid.uuid4())
    manifest, staging_file, target_file = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.STAGED,
        record_count=10,  # Claims 10 records
    )
    candles = _make_test_candles(2)  # Physical has only 2!
    write_staging_parquet(staging_file, candles, record_count=2)
    s_hash = compute_file_sha256(staging_file)
    manifest.file_sha256 = s_hash
    manifest.expected_records = 10
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError, match="does not match manifest.record_count"):
        engine.scan_and_recover()


def test_crash_recovery_invariant_violation_fails_safely(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Manifest violating record_count + missing_intervals == expected_records must fail validation."""
    mgr, _, base_dir, engine = recovery_env
    test_uuid = str(uuid.uuid4())
    manifest, staging_file, target_file = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.STAGING,
        expected_records=2,
        record_count=1,
        missing_intervals=0,  # 1 + 0 != 2!
    )
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError, match="Invariant violation"):
        engine.scan_and_recover()


def test_crash_recovery_invalid_manifest_fails_safely(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Invalid schema version, invalid UUID, or invalid stage fails safely without touching files."""
    mgr, _, base_dir, engine = recovery_env
    test_uuid = str(uuid.uuid4())
    manifest, staging_file, target_file = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.STAGING,
        record_count=0,
        missing_intervals=2,
    )
    staging_file.write_text("precious data", encoding="utf-8")

    # Invalid UUID
    manifest.ingestion_uuid = "not-a-valid-uuid-format"
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError, match="Invalid ingestion_uuid format"):
        engine.scan_and_recover()
    assert staging_file.exists()
    mgr.cleanup_manifest("not-a-valid-uuid-format")

    # Invalid schema version
    test_uuid2 = str(uuid.uuid4())
    manifest2, staging_file2, target_file2 = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid2,
        stage=ManifestStage.STAGING,
        record_count=0,
        missing_intervals=2,
    )
    manifest2.schema_version = 99
    staging_file2.write_text("precious data 2", encoding="utf-8")
    mgr.write_manifest(manifest2)

    with pytest.raises(RecoveryFailureError, match="Unsupported schema_version"):
        engine.scan_and_recover()
    assert staging_file2.exists()


def test_crash_recovery_path_traversal_and_symlinks_rejected(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Path traversal ('..') and external symlinks are rejected and never deleted or overwritten."""
    mgr, _, base_dir, engine = recovery_env
    datasets_dir = base_dir / "datasets"

    # 1. Path traversal with ..
    traversal_path = str(datasets_dir / "sub" / ".." / ".." / "unrelated.parquet")
    test_uuid = str(uuid.uuid4())
    manifest, _, target_file = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.STAGING,
        record_count=0,
        missing_intervals=2,
    )
    manifest.staging_file_path = traversal_path
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError, match="Path traversal detected"):
        engine.scan_and_recover()

    mgr.cleanup_manifest(test_uuid)

    # 2. External path outside datasets_dir
    external_dir = base_dir.parent / "unrelated_dir"
    external_dir.mkdir(parents=True, exist_ok=True)
    unrelated_file = external_dir / "secret.parquet"
    unrelated_file.write_text("top secret", encoding="utf-8")

    test_uuid2 = str(uuid.uuid4())
    manifest2, _, _ = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid2,
        stage=ManifestStage.STAGING,
        record_count=0,
        missing_intervals=2,
    )
    manifest2.staging_file_path = str(unrelated_file)
    mgr.write_manifest(manifest2)

    with pytest.raises(RecoveryFailureError, match="outside authorized dataset directory|partition mismatch"):
        engine.scan_and_recover()

    # Verify unrelated file was NEVER touched or deleted
    assert unrelated_file.exists()
    assert unrelated_file.read_text(encoding="utf-8") == "top secret"


def test_crash_recovery_rejects_catalog_as_staging(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """A valid manifest attempting to use catalog.duckdb as its staging path must be rejected and catalog untouched."""
    mgr, catalog, base_dir, engine = recovery_env
    catalog_path = catalog.db_path
    catalog_bytes_orig = catalog_path.read_bytes()

    test_uuid = str(uuid.uuid4())
    manifest, _, _ = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.STAGING,
        record_count=0,
        missing_intervals=2,
    )
    manifest.staging_file_path = str(catalog_path)
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError, match="Unauthorized path role|outside authorized dataset"):
        engine.scan_and_recover()

    # Catalog remains byte-for-byte untouched
    assert catalog_path.read_bytes() == catalog_bytes_orig


def test_crash_recovery_rejects_manifest_as_staging(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """A valid manifest attempting to use a manifest JSON file as its staging path must be rejected and untouched."""
    mgr, _, base_dir, engine = recovery_env
    manifest_file = base_dir / "manifests" / "important_manifest.json"
    manifest_file.write_text('{"important": "metadata"}', encoding="utf-8")
    orig_bytes = manifest_file.read_bytes()

    test_uuid = str(uuid.uuid4())
    manifest, _, _ = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.STAGING,
        record_count=0,
        missing_intervals=2,
    )
    manifest.staging_file_path = str(manifest_file)
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError, match="Unauthorized path role|outside authorized dataset"):
        engine.scan_and_recover()

    assert manifest_file.read_bytes() == orig_bytes


def test_crash_recovery_rejects_config_file_as_staging(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """A valid manifest attempting to use a configuration file as staging path is rejected and untouched."""
    mgr, _, base_dir, engine = recovery_env
    config_file = base_dir / "config.yaml"
    config_file.write_text("database:\n  host: localhost\n", encoding="utf-8")
    orig_bytes = config_file.read_bytes()

    test_uuid = str(uuid.uuid4())
    manifest, _, _ = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.STAGING,
        record_count=0,
        missing_intervals=2,
    )
    manifest.staging_file_path = str(config_file)
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError, match="Unauthorized path role|outside authorized dataset"):
        engine.scan_and_recover()

    assert config_file.read_bytes() == orig_bytes


def test_crash_recovery_rejects_same_path_for_staging_and_target(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Staging and target using the exact same path must be rejected."""
    mgr, _, base_dir, engine = recovery_env
    test_uuid = str(uuid.uuid4())
    manifest, _, target_file = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.STAGING,
        record_count=0,
        missing_intervals=2,
    )
    manifest.staging_file_path = str(target_file)
    manifest.target_file_path = str(target_file)
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError, match="distinct paths"):
        engine.scan_and_recover()


def test_crash_recovery_rejects_directory_as_staging(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Staging path pointing to a directory must be rejected."""
    mgr, _, base_dir, engine = recovery_env
    sub_dir = base_dir / "datasets" / "sub_folder"
    sub_dir.mkdir(parents=True, exist_ok=True)

    test_uuid = str(uuid.uuid4())
    manifest, _, target_file = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.STAGING,
        record_count=0,
        missing_intervals=2,
    )
    manifest.staging_file_path = str(sub_dir)
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError, match="points to a directory"):
        engine.scan_and_recover()


def test_crash_recovery_gap_metadata_validation(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Inconsistent gap metadata is rejected prior to catalog insertion."""
    mgr, _, base_dir, engine = recovery_env
    candles = _make_test_candles(2)
    c_fp = compute_content_fingerprint(candles, symbol="BTC/USDT", timeframe="1h")

    test_uuid = str(uuid.uuid4())
    # 1. len(gaps) != gap_count
    manifest_bad_count, staging_file, target_file = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.PUBLISHED,
        since_ms=0,
        until_ms=10_800_000,
        expected_records=3,
        record_count=2,
        missing_intervals=1,
        gap_count=5,  # Claims 5, but gaps list has 1!
        gaps=[
            {
                "gap_type": "LEADING",
                "expected_open_ms": 0,
                "subsequent_open_ms": 3_600_000,
                "missing_count": 1,
            }
        ],
        content_fingerprint=c_fp,
    )
    write_staging_parquet(target_file, candles, record_count=2)
    t_hash = compute_file_sha256(target_file)
    manifest_bad_count.file_sha256 = t_hash
    mgr.write_manifest(manifest_bad_count)

    with pytest.raises(RecoveryFailureError, match="Inconsistent gap metadata: len.*gap_count"):
        engine.scan_and_recover()

    mgr.cleanup_manifest(test_uuid)

    # 2. sum(missing_count) != missing_intervals
    test_uuid2 = str(uuid.uuid4())
    manifest_bad_sum, staging_file2, target_file2 = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid2,
        stage=ManifestStage.PUBLISHED,
        since_ms=0,
        until_ms=10_800_000,
        expected_records=3,
        record_count=2,
        missing_intervals=1,  # Invariant: 2 + 1 == 3
        gap_count=1,
        gaps=[
            {
                "gap_type": "LEADING",
                "expected_open_ms": 0,
                "subsequent_open_ms": 7_200_000,
                "missing_count": 2,  # Sum is 2, but missing_intervals is 1!
            }
        ],
        content_fingerprint=c_fp,
    )
    write_staging_parquet(target_file2, candles, record_count=2)
    t_hash2 = compute_file_sha256(target_file2)
    manifest_bad_sum.file_sha256 = t_hash2
    mgr.write_manifest(manifest_bad_sum)

    with pytest.raises(RecoveryFailureError, match="Inconsistent gap metadata: sum.*missing_intervals"):
        engine.scan_and_recover()


# --- New Requirement Tests (B, C, D) ---


def test_active_writer_beyond_lease_expiry_protected(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Requirement B.8: An active live process is never recovered just because lease timestamp expired."""
    mgr, _, base_dir, engine = recovery_env
    test_uuid = str(uuid.uuid4())
    manifest, staging_file, target_file = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.STAGING,
        record_count=0,
        missing_intervals=2,
    )
    # Expired 2 hours ago! But owner PID is current process (which is alive!)
    manifest.lease_owner_pid = os.getpid()
    manifest.lease_expires_at_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 7200))
    staging_file.write_text("active writer staging data", encoding="utf-8")
    mgr.write_manifest(manifest)

    # is_lease_active must return True because process is alive!
    assert is_lease_active(manifest) is True

    # Under mock where process is alive from perspective of another process
    with patch("os.getpid", return_value=os.getpid() + 1):
        # Recovery scan skips active writer
        actions = engine.scan_and_recover(allow_active_lease_skip=True)
        assert len(actions) == 0

        # Attempting explicit recovery raises LeaseActiveError
        with pytest.raises(LeaseActiveError, match="actively held by PID"):
            engine.recover_manifest(manifest, allow_active_lease_skip=False)

    # Staging data must remain completely untouched
    assert staging_file.exists()
    assert staging_file.read_text(encoding="utf-8") == "active writer staging data"


def test_concurrent_ingestion_and_recovery_safety(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Requirement B.9: Concurrent recovery cannot delete or mutate the active writer's staging file."""
    mgr, _, base_dir, engine = recovery_env
    test_uuid = str(uuid.uuid4())
    manifest, staging_file, target_file = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.STAGING,
        record_count=0,
        missing_intervals=2,
    )
    manifest.lease_owner_pid = os.getpid()
    staging_file.write_text("precious live staging bytes", encoding="utf-8")
    mgr.write_manifest(manifest)

    # Writer actively holds lock
    writer_lock = mgr.lock_manifest(test_uuid)
    writer_lock.acquire()
    try:
        # Concurrent recovery attempt while lock is held
        actions = engine.scan_and_recover(allow_active_lease_skip=True)
        assert len(actions) == 0

        # Attempting recovery with allow_active_lease_skip=False raises LeaseActiveError
        with pytest.raises(LeaseActiveError):
            engine.recover_manifest(manifest, allow_active_lease_skip=False)

        # Staging file was NEVER deleted
        assert staging_file.exists()
        assert staging_file.read_text(encoding="utf-8") == "precious live staging bytes"
    finally:
        writer_lock.release()


def test_cross_ingestion_staging_and_target_rejection(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Requirement C: Reject artifacts belonging to another ingestion; original files unchanged byte-for-byte."""
    mgr, _, base_dir, engine = recovery_env

    # 1. Valid Dataset A
    uuid_a = str(uuid.uuid4())
    candles_a = _make_test_candles(2)
    c_fp_a = compute_content_fingerprint(candles_a, symbol="BTC/USDT", timeframe="1h")
    manifest_a, staging_a, target_a = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=uuid_a,
        stage=ManifestStage.PUBLISHED,
        content_fingerprint=c_fp_a,
    )
    write_staging_parquet(staging_a, candles_a, record_count=2)
    write_staging_parquet(target_a, candles_a, record_count=2)
    sha_stg_a = compute_file_sha256(staging_a)
    sha_tgt_a = compute_file_sha256(target_a)
    bytes_stg_a = staging_a.read_bytes()
    bytes_tgt_a = target_a.read_bytes()

    # 2. Manifest B with different UUID attempting to steal Dataset A's files
    uuid_b = str(uuid.uuid4())
    # Subtest 1: Pointing to Dataset A's staging file
    manifest_b1, staging_b, target_b = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=uuid_b,
        stage=ManifestStage.STAGING,
        record_count=0,
        missing_intervals=2,
    )
    manifest_b1.staging_file_path = str(staging_a)  # Cross-ingestion attack!
    mgr.write_manifest(manifest_b1)

    with pytest.raises(RecoveryFailureError, match="does not match canonical"):
        engine.scan_and_recover()

    # Verify Dataset A's staging file was NEVER deleted or modified
    assert staging_a.exists()
    assert staging_a.read_bytes() == bytes_stg_a
    assert compute_file_sha256(staging_a) == sha_stg_a
    mgr.cleanup_manifest(uuid_b)

    # Subtest 2: Pointing to Dataset A's target file
    uuid_b2 = str(uuid.uuid4())
    manifest_b2, _, _ = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=uuid_b2,
        stage=ManifestStage.PUBLISHED,
        content_fingerprint=c_fp_a,
        file_sha256=sha_tgt_a,
    )
    manifest_b2.target_file_path = str(target_a)  # Cross-ingestion attack!
    mgr.write_manifest(manifest_b2)

    with pytest.raises(RecoveryFailureError, match="does not match canonical"):
        engine.scan_and_recover()

    # Verify Dataset A's target file is byte-for-byte unchanged
    assert target_a.exists()
    assert target_a.read_bytes() == bytes_tgt_a
    assert compute_file_sha256(target_a) == sha_tgt_a


def test_empty_gap_list_with_nonzero_gap_count_rejected(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Requirement D: An empty gap list with non-zero gap_count or missing_intervals is rejected."""
    mgr, _, base_dir, engine = recovery_env
    test_uuid = str(uuid.uuid4())
    manifest, staging_file, target_file = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=test_uuid,
        stage=ManifestStage.PUBLISHED,
        expected_records=5,
        record_count=3,
        missing_intervals=2,
        gap_count=1,
        gaps=[],  # Empty list despite gap_count=1!
        file_sha256="sha256:" + ("b" * 64),
    )
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError, match="Inconsistent gap metadata: len.*gap_count"):
        engine.scan_and_recover()


def test_gap_timestamp_alignment_and_arithmetic_invariants(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Requirement D: Timestamp alignment and missing-count boundary arithmetic invariants."""
    mgr, _, base_dir, engine = recovery_env

    # 1. Unaligned expected_open_ms
    uuid1 = str(uuid.uuid4())
    m1, _, _ = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=uuid1,
        stage=ManifestStage.PUBLISHED,
        expected_records=4,
        record_count=3,
        missing_intervals=1,
        gap_count=1,
        gaps=[
            {
                "gap_type": "INTERNAL",
                "expected_open_ms": 1000,  # Unaligned (not multiple of 3600000)!
                "subsequent_open_ms": 3_600_000,
                "missing_count": 1,
            }
        ],
        file_sha256="sha256:" + ("c" * 64),
    )
    mgr.write_manifest(m1)
    with pytest.raises(RecoveryFailureError, match="not aligned"):
        engine.scan_and_recover()
    mgr.cleanup_manifest(uuid1)

    # 2. Arithmetic mismatch: (subsequent_open_ms - expected_open_ms) != missing_count * step_ms
    uuid2 = str(uuid.uuid4())
    m2, _, _ = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=uuid2,
        stage=ManifestStage.PUBLISHED,
        expected_records=6,
        record_count=3,
        missing_intervals=3,
        gap_count=1,
        gaps=[
            {
                "gap_type": "INTERNAL",
                "expected_open_ms": 3_600_000,
                "subsequent_open_ms": 7_200_000,  # Difference is 3600000 (1 interval)
                "missing_count": 3,  # But claims 3 missing intervals!
            }
        ],
        file_sha256="sha256:" + ("d" * 64),
    )
    mgr.write_manifest(m2)
    with pytest.raises(RecoveryFailureError, match="missing-count arithmetic error"):
        engine.scan_and_recover()
    mgr.cleanup_manifest(uuid2)

    # 3. LEADING gap must start at since_ms
    uuid3 = str(uuid.uuid4())
    m3, _, _ = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=uuid3,
        stage=ManifestStage.PUBLISHED,
        since_ms=0,
        expected_records=4,
        record_count=3,
        missing_intervals=1,
        gap_count=1,
        gaps=[
            {
                "gap_type": "LEADING",
                "expected_open_ms": 3_600_000,  # Does not equal since_ms (0)!
                "subsequent_open_ms": 7_200_000,
                "missing_count": 1,
            }
        ],
        file_sha256="sha256:" + ("e" * 64),
    )
    mgr.write_manifest(m3)
    with pytest.raises(RecoveryFailureError, match="LEADING gap expected_open_ms"):
        engine.scan_and_recover()
    mgr.cleanup_manifest(uuid3)

    # 4. TRAILING gap must have subsequent_open_ms == None
    uuid4 = str(uuid.uuid4())
    m4, _, _ = make_test_manifest_and_paths(
        base_dir=base_dir,
        test_uuid=uuid4,
        stage=ManifestStage.PUBLISHED,
        since_ms=0,
        until_ms=7_200_000,
        expected_records=2,
        record_count=1,
        missing_intervals=1,
        gap_count=1,
        gaps=[
            {
                "gap_type": "TRAILING",
                "expected_open_ms": 3_600_000,
                "subsequent_open_ms": 7_200_000,  # Must be None!
                "missing_count": 1,
            }
        ],
        file_sha256="sha256:" + ("f" * 64),
    )
    mgr.write_manifest(m4)
    with pytest.raises(RecoveryFailureError, match="TRAILING gap must have subsequent_open_ms == None"):
        engine.scan_and_recover()
