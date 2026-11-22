"""Tests for crash recovery, manifest lifecycle reconciliation, integrity, and gap preservation."""

import uuid
from datetime import datetime, timezone
from pathlib import Path
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
)
from open_quant_intelligence.storage.parquet_store import write_staging_parquet


def _make_test_candles(
    count: int = 2,
    start_ms: int = 0,
    step_ms: int = 3_600_000,
) -> list[Candle]:
    candles = []
    for i in range(count):
        t_ms = start_ms + i * step_ms
        open_dt = datetime.fromtimestamp(t_ms / 1000, tz=timezone.utc)
        candles.append(
            Candle(
                exchange_id="binance",
                market_type="spot",
                symbol="BTC/USDT",
                timeframe="1h",
                open_time=open_dt,
                open=100.0 + i,
                high=105.0 + i,
                low=95.0 + i,
                close=102.0 + i,
                base_volume=10.0 + i,
            )
        )
    return candles


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
    datasets_dir = base_dir / "datasets"
    target_file = datasets_dir / "target.parquet"
    staging_file = datasets_dir / "target.parquet.tmp"
    staging_file.write_text("partial corrupt staging bytes", encoding="utf-8")

    test_uuid = str(uuid.uuid4())
    manifest = IngestionManifest(
        ingestion_uuid=test_uuid,
        stage=ManifestStage.STAGING.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=7_200_000,
        expected_records=2,
        record_count=0,
        missing_intervals=2,
        staging_file_path=str(staging_file),
        target_file_path=str(target_file),
    )
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
    datasets_dir = base_dir / "datasets"
    target_file = datasets_dir / "final_target.parquet"
    staging_file = datasets_dir / "final_target.parquet.tmp"

    candles = _make_test_candles(2)
    write_staging_parquet(staging_file, candles, record_count=2)
    s_hash = compute_file_sha256(staging_file)
    c_fp = compute_content_fingerprint(candles, symbol="BTC/USDT", timeframe="1h")

    test_uuid = str(uuid.uuid4())
    manifest = IngestionManifest(
        ingestion_uuid=test_uuid,
        stage=ManifestStage.STAGED.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=7_200_000,
        expected_records=2,
        record_count=2,
        missing_intervals=0,
        content_fingerprint_sha256=c_fp,
        file_sha256=s_hash,
        staging_file_path=str(staging_file),
        target_file_path=str(target_file),
    )
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
    datasets_dir = base_dir / "datasets"
    target_file = datasets_dir / "target.parquet"
    staging_file = datasets_dir / "target.parquet.tmp"
    staging_file.write_text("corrupted bytes", encoding="utf-8")

    test_uuid = str(uuid.uuid4())
    dummy_hash = "sha256:" + ("a" * 64)
    manifest = IngestionManifest(
        ingestion_uuid=test_uuid,
        stage=ManifestStage.STAGED.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=7_200_000,
        expected_records=2,
        record_count=2,
        missing_intervals=0,
        file_sha256=dummy_hash,
        staging_file_path=str(staging_file),
        target_file_path=str(target_file),
    )
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
    datasets_dir = base_dir / "datasets"
    target_file = datasets_dir / "published_target.parquet"
    staging_file = datasets_dir / "published_target.parquet.tmp"
    candles = _make_test_candles(2)
    write_staging_parquet(target_file, candles, record_count=2)
    t_hash = compute_file_sha256(target_file)

    test_uuid = str(uuid.uuid4())
    manifest = IngestionManifest(
        ingestion_uuid=test_uuid,
        stage=ManifestStage.PUBLISHING.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=7_200_000,
        expected_records=2,
        record_count=2,
        missing_intervals=0,
        file_sha256=t_hash,
        staging_file_path=str(staging_file),
        target_file_path=str(target_file),
    )
    mgr.write_manifest(manifest)

    actions = engine.scan_and_recover()
    assert len(actions) == 1
    assert actions[0].action_taken == "COMPLETED_INGESTION_AND_CLEANUP"
    assert mgr.read_manifest(test_uuid) is None

    # Catalog record committed
    cat_rec = catalog.find_by_id_or_fingerprint(manifest.dataset_identity)
    assert cat_rec is not None
    assert cat_rec.file_sha256 == t_hash


def test_crash_recovery_publishing_conflict_preserved_byte_for_byte(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Pre-existing conflicting target is preserved byte-for-byte; raises RecoveryConflictError."""
    mgr, _, base_dir, engine = recovery_env
    datasets_dir = base_dir / "datasets"
    target_file = datasets_dir / "conflict_target.parquet"
    staging_file = datasets_dir / "conflict_target.parquet.tmp"
    target_file.write_text("ORIGINAL CONFLICTING DATA BYTES", encoding="utf-8")
    orig_bytes = target_file.read_bytes()
    orig_sha = compute_file_sha256(target_file)

    test_uuid = str(uuid.uuid4())
    diff_sha = "0" * 64
    manifest = IngestionManifest(
        ingestion_uuid=test_uuid,
        stage=ManifestStage.PUBLISHING.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=7_200_000,
        expected_records=2,
        record_count=2,
        missing_intervals=0,
        file_sha256=diff_sha,
        staging_file_path=str(staging_file),
        target_file_path=str(target_file),
    )
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryConflictError, match="Pre-existing target file .* differs"):
        engine.scan_and_recover()

    # Assert target file was left completely untouched byte-for-byte
    assert target_file.read_bytes() == orig_bytes
    assert compute_file_sha256(target_file) == orig_sha


def test_crash_recovery_published_and_committing_catalog_idempotency(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Crash at PUBLISHED/COMMITTING_CATALOG commits catalog idempotently without duplicate records."""
    mgr, catalog, base_dir, engine = recovery_env
    datasets_dir = base_dir / "datasets"
    target_file = datasets_dir / "idempotent_target.parquet"
    staging_file = datasets_dir / "idempotent_target.parquet.tmp"
    candles = _make_test_candles(2)
    write_staging_parquet(target_file, candles, record_count=2)
    t_hash = compute_file_sha256(target_file)

    test_uuid = str(uuid.uuid4())
    manifest = IngestionManifest(
        ingestion_uuid=test_uuid,
        stage=ManifestStage.COMMITTING_CATALOG.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=7_200_000,
        expected_records=2,
        record_count=2,
        missing_intervals=0,
        file_sha256=t_hash,
        staging_file_path=str(staging_file),
        target_file_path=str(target_file),
    )
    mgr.write_manifest(manifest)

    # First recovery commit
    engine.scan_and_recover()

    # Re-insert identical manifest and run recovery again to prove idempotency
    manifest.stage = ManifestStage.COMMITTING_CATALOG.value
    mgr.write_manifest(manifest)
    engine.scan_and_recover()

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
    datasets_dir = base_dir / "datasets"
    future_time = "2099-01-01T00:00:00Z"
    test_uuid = str(uuid.uuid4())
    manifest = IngestionManifest(
        ingestion_uuid=test_uuid,
        stage=ManifestStage.STAGING.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=7_200_000,
        expected_records=2,
        record_count=0,
        missing_intervals=2,
        lease_owner_pid=99999,
        lease_expires_at_utc=future_time,
        staging_file_path=str(datasets_dir / "target.parquet.tmp"),
        target_file_path=str(datasets_dir / "target.parquet"),
    )
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
    datasets_dir = base_dir / "datasets"

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

    target_file = datasets_dir / "multi_gap_target.parquet"
    staging_file = datasets_dir / "multi_gap_target.parquet.tmp"
    write_staging_parquet(target_file, candles, record_count=2)
    t_hash = compute_file_sha256(target_file)

    test_uuid = str(uuid.uuid4())
    manifest = IngestionManifest(
        ingestion_uuid=test_uuid,
        stage=ManifestStage.PUBLISHED.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=21_600_000,
        expected_records=6,
        record_count=2,
        missing_intervals=4,
        gap_count=expected_report.gap_count,
        gaps=[
            {
                "gap_type": g.gap_type.value,
                "expected_open_ms": g.expected_open_ms,
                "subsequent_open_ms": g.subsequent_open_ms,
                "missing_count": g.missing_count,
            }
            for g in expected_report.gaps
        ],
        file_sha256=t_hash,
        staging_file_path=str(staging_file),
        target_file_path=str(target_file),
    )
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
    assert [g["gap_type"] for g in gaps_in_db] == ["LEADING", "INTERNAL", "TRAILING"]
    assert [g["missing_count"] for g in gaps_in_db] == [1, 2, 1]


def test_crash_recovery_missing_hash_fails_safely(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """A STAGED manifest with missing or invalid hash must fail validation safely."""
    mgr, _, base_dir, engine = recovery_env
    datasets_dir = base_dir / "datasets"
    staging_file = datasets_dir / "stage_missing_hash.parquet.tmp"
    target_file = datasets_dir / "stage_missing_hash.parquet"
    candles = _make_test_candles(2)
    write_staging_parquet(staging_file, candles, record_count=2)

    test_uuid = str(uuid.uuid4())
    manifest = IngestionManifest(
        ingestion_uuid=test_uuid,
        stage=ManifestStage.STAGED.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=7_200_000,
        expected_records=2,
        record_count=2,
        missing_intervals=0,
        file_sha256="",  # Empty hash!
        staging_file_path=str(staging_file),
        target_file_path=str(target_file),
    )
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError, match="must contain a valid, non-empty SHA-256"):
        engine.scan_and_recover()

    # Staging file is preserved because manifest validation failed before file operations
    assert staging_file.exists()


def test_crash_recovery_row_count_mismatch_fails_safely(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Row count mismatch between physical parquet file and manifest must raise RecoveryFailureError."""
    mgr, _, base_dir, engine = recovery_env
    datasets_dir = base_dir / "datasets"
    target_file = datasets_dir / "row_count_mismatch.parquet"
    staging_file = datasets_dir / "row_count_mismatch.parquet.tmp"
    candles = _make_test_candles(2)
    write_staging_parquet(target_file, candles, record_count=2)
    t_hash = compute_file_sha256(target_file)

    test_uuid = str(uuid.uuid4())
    manifest = IngestionManifest(
        ingestion_uuid=test_uuid,
        stage=ManifestStage.PUBLISHED.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=10_800_000,
        expected_records=3,
        record_count=3,  # Claims 3, but physical parquet has 2!
        missing_intervals=0,
        file_sha256=t_hash,
        staging_file_path=str(staging_file),
        target_file_path=str(target_file),
    )
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError, match="Physical Parquet row count .* does not match"):
        engine.scan_and_recover()


def test_crash_recovery_invariant_violation_fails_safely(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Manifest violating record_count + missing_intervals == expected_records fails safely."""
    mgr, _, base_dir, engine = recovery_env
    datasets_dir = base_dir / "datasets"
    test_uuid = str(uuid.uuid4())
    manifest = IngestionManifest(
        ingestion_uuid=test_uuid,
        stage=ManifestStage.STAGING.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=7_200_000,
        expected_records=2,
        record_count=1,
        missing_intervals=0,  # 1 + 0 != 2!
        staging_file_path=str(datasets_dir / "target.parquet.tmp"),
        target_file_path=str(datasets_dir / "target.parquet"),
    )
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError, match="Invariant violation"):
        engine.scan_and_recover()


def test_crash_recovery_invalid_manifest_fails_safely(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Invalid schema version, invalid UUID, or invalid stage fails safely without touching files."""
    mgr, _, base_dir, engine = recovery_env
    datasets_dir = base_dir / "datasets"
    staging_file = datasets_dir / "do_not_delete.parquet.tmp"
    staging_file.write_text("precious data", encoding="utf-8")

    # Invalid UUID
    manifest1 = IngestionManifest(
        ingestion_uuid="not-a-valid-uuid-format",
        stage=ManifestStage.STAGING.value,
        staging_file_path=str(staging_file),
        target_file_path=str(datasets_dir / "do_not_delete.parquet"),
    )
    mgr.write_manifest(manifest1)

    with pytest.raises(RecoveryFailureError, match="Invalid ingestion_uuid format"):
        engine.scan_and_recover()
    assert staging_file.exists()
    mgr.cleanup_manifest("not-a-valid-uuid-format")

    # Invalid schema version
    test_uuid = str(uuid.uuid4())
    manifest2 = IngestionManifest(
        ingestion_uuid=test_uuid,
        schema_version=99,
        stage=ManifestStage.STAGING.value,
        staging_file_path=str(staging_file),
        target_file_path=str(datasets_dir / "do_not_delete.parquet"),
    )
    mgr.write_manifest(manifest2)

    with pytest.raises(RecoveryFailureError, match="Unsupported schema_version"):
        engine.scan_and_recover()
    assert staging_file.exists()


def test_crash_recovery_path_traversal_and_symlinks_rejected(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Path traversal ('..') and external symlinks are rejected and never deleted or overwritten."""
    mgr, _, base_dir, engine = recovery_env
    datasets_dir = base_dir / "datasets"

    # 1. Path traversal with ..
    traversal_path = str(datasets_dir / "sub" / ".." / ".." / "unrelated.parquet")
    test_uuid = str(uuid.uuid4())
    manifest = IngestionManifest(
        ingestion_uuid=test_uuid,
        stage=ManifestStage.STAGING.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=7_200_000,
        expected_records=2,
        record_count=0,
        missing_intervals=2,
        staging_file_path=traversal_path,
        target_file_path=str(datasets_dir / "target.parquet"),
    )
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
    manifest2 = IngestionManifest(
        ingestion_uuid=test_uuid2,
        stage=ManifestStage.STAGING.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=7_200_000,
        expected_records=2,
        record_count=0,
        missing_intervals=2,
        staging_file_path=str(unrelated_file),
        target_file_path=str(datasets_dir / "target.parquet"),
    )
    mgr.write_manifest(manifest2)

    with pytest.raises(RecoveryFailureError, match="outside authorized dataset directory"):
        engine.scan_and_recover()

    # Verify unrelated file was NEVER touched or deleted
    assert unrelated_file.exists()
    assert unrelated_file.read_text(encoding="utf-8") == "top secret"


def test_crash_recovery_rejects_catalog_as_staging(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """A valid manifest attempting to use catalog.duckdb as its staging path must be rejected and catalog untouched."""
    mgr, catalog, base_dir, engine = recovery_env
    datasets_dir = base_dir / "datasets"
    catalog_path = catalog.db_path
    catalog_bytes_orig = catalog_path.read_bytes()

    test_uuid = str(uuid.uuid4())
    manifest = IngestionManifest(
        ingestion_uuid=test_uuid,
        stage=ManifestStage.STAGING.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=7_200_000,
        expected_records=2,
        record_count=0,
        missing_intervals=2,
        staging_file_path=str(catalog_path),
        target_file_path=str(datasets_dir / "target.parquet"),
    )
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
    datasets_dir = base_dir / "datasets"
    manifest_file = base_dir / "manifests" / "important_manifest.json"
    manifest_file.write_text('{"important": "metadata"}', encoding="utf-8")
    orig_bytes = manifest_file.read_bytes()

    test_uuid = str(uuid.uuid4())
    manifest = IngestionManifest(
        ingestion_uuid=test_uuid,
        stage=ManifestStage.STAGING.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=7_200_000,
        expected_records=2,
        record_count=0,
        missing_intervals=2,
        staging_file_path=str(manifest_file),
        target_file_path=str(datasets_dir / "target.parquet"),
    )
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError, match="Unauthorized path role|outside authorized dataset"):
        engine.scan_and_recover()

    assert manifest_file.read_bytes() == orig_bytes


def test_crash_recovery_rejects_config_file_as_staging(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """A valid manifest attempting to use a configuration file as staging path is rejected and untouched."""
    mgr, _, base_dir, engine = recovery_env
    datasets_dir = base_dir / "datasets"
    config_file = base_dir / "config.yaml"
    config_file.write_text("database:\n  host: localhost\n", encoding="utf-8")
    orig_bytes = config_file.read_bytes()

    test_uuid = str(uuid.uuid4())
    manifest = IngestionManifest(
        ingestion_uuid=test_uuid,
        stage=ManifestStage.STAGING.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=7_200_000,
        expected_records=2,
        record_count=0,
        missing_intervals=2,
        staging_file_path=str(config_file),
        target_file_path=str(datasets_dir / "target.parquet"),
    )
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError, match="Unauthorized path role|outside authorized dataset"):
        engine.scan_and_recover()

    assert config_file.read_bytes() == orig_bytes


def test_crash_recovery_rejects_same_path_for_staging_and_target(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Staging and target using the exact same path must be rejected."""
    mgr, _, base_dir, engine = recovery_env
    datasets_dir = base_dir / "datasets"
    same_path = datasets_dir / "same_dataset.parquet"

    test_uuid = str(uuid.uuid4())
    manifest = IngestionManifest(
        ingestion_uuid=test_uuid,
        stage=ManifestStage.STAGING.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=7_200_000,
        expected_records=2,
        record_count=0,
        missing_intervals=2,
        staging_file_path=str(same_path),
        target_file_path=str(same_path),
    )
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError, match="distinct paths"):
        engine.scan_and_recover()


def test_crash_recovery_rejects_directory_as_staging(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Staging path pointing to a directory must be rejected."""
    mgr, _, base_dir, engine = recovery_env
    datasets_dir = base_dir / "datasets"
    sub_dir = datasets_dir / "sub_folder"
    sub_dir.mkdir(parents=True, exist_ok=True)

    test_uuid = str(uuid.uuid4())
    manifest = IngestionManifest(
        ingestion_uuid=test_uuid,
        stage=ManifestStage.STAGING.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=7_200_000,
        expected_records=2,
        record_count=0,
        missing_intervals=2,
        staging_file_path=str(sub_dir),
        target_file_path=str(datasets_dir / "target.parquet"),
    )
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError, match="points to a directory"):
        engine.scan_and_recover()


def test_crash_recovery_gap_metadata_validation(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Inconsistent gap metadata is rejected prior to catalog insertion."""
    mgr, _, base_dir, engine = recovery_env
    datasets_dir = base_dir / "datasets"
    target_file = datasets_dir / "gap_test.parquet"
    staging_file = datasets_dir / "gap_test.parquet.tmp"
    candles = _make_test_candles(2)
    write_staging_parquet(target_file, candles, record_count=2)
    t_hash = compute_file_sha256(target_file)

    test_uuid = str(uuid.uuid4())
    # 1. len(gaps) != gap_count
    manifest_bad_count = IngestionManifest(
        ingestion_uuid=test_uuid,
        stage=ManifestStage.PUBLISHED.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=10_800_000,
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
        file_sha256=t_hash,
        staging_file_path=str(staging_file),
        target_file_path=str(target_file),
    )
    mgr.write_manifest(manifest_bad_count)

    with pytest.raises(RecoveryFailureError, match="Inconsistent gap metadata: len.*gap_count"):
        engine.scan_and_recover()

    mgr.cleanup_manifest(test_uuid)

    # 2. sum(missing_count) != missing_intervals
    test_uuid2 = str(uuid.uuid4())
    manifest_bad_sum = IngestionManifest(
        ingestion_uuid=test_uuid2,
        stage=ManifestStage.PUBLISHED.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=10_800_000,
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
        file_sha256=t_hash,
        staging_file_path=str(staging_file),
        target_file_path=str(target_file),
    )
    mgr.write_manifest(manifest_bad_sum)

    with pytest.raises(RecoveryFailureError, match="Inconsistent gap metadata: sum.*missing_intervals"):
        engine.scan_and_recover()
