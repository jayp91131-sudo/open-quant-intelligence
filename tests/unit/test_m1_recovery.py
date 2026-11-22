"""Comprehensive deterministic unit tests for M1 crash recovery and manifest reconciliation."""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from open_quant_intelligence.domain.candle import Candle
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


def _make_test_candles(count: int = 2) -> list[Candle]:
    candles = []
    for i in range(count):
        t_ms = i * 3_600_000
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
    engine = RecoveryEngine(manifest_mgr, catalog)
    return manifest_mgr, catalog, tmp_path, engine


def test_monotonic_stage_transitions(recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine]) -> None:
    """Verifies that non-monotonic or illegal transitions raise InvalidStageTransitionError."""
    mgr, _, _, _ = recovery_env
    manifest = IngestionManifest(ingestion_uuid="uuid_trans_test", stage=ManifestStage.INITIALIZING.value)
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
    staging_file = base_dir / "staging_partial.parquet.tmp"
    staging_file.write_text("partial corrupt staging bytes", encoding="utf-8")

    manifest = IngestionManifest(
        ingestion_uuid="uuid_crash_staging",
        stage=ManifestStage.STAGING.value,
        staging_file_path=str(staging_file),
        target_file_path=str(base_dir / "target.parquet"),
    )
    mgr.write_manifest(manifest)

    actions = engine.scan_and_recover()
    assert len(actions) == 1
    assert actions[0].action_taken == "DISCARDED_INCOMPLETE_STAGING"
    assert not staging_file.exists()
    assert mgr.read_manifest("uuid_crash_staging") is None


def test_crash_recovery_staged_valid(recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine]) -> None:
    """Crash at STAGED with valid staging file completes publication and catalog commit."""
    mgr, catalog, base_dir, engine = recovery_env
    staging_file = base_dir / "valid_stage.parquet.tmp"
    target_file = base_dir / "final_target.parquet"

    candles = _make_test_candles(2)
    write_staging_parquet(staging_file, candles, record_count=2)
    s_hash = compute_file_sha256(staging_file)
    c_fp = compute_content_fingerprint(candles, symbol="BTC/USDT", timeframe="1h")

    manifest = IngestionManifest(
        ingestion_uuid="uuid_crash_staged",
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
    assert mgr.read_manifest("uuid_crash_staged") is None

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
    staging_file = base_dir / "corrupted.parquet.tmp"
    staging_file.write_text("corrupted bytes", encoding="utf-8")

    manifest = IngestionManifest(
        ingestion_uuid="uuid_corrupt_stage",
        stage=ManifestStage.STAGED.value,
        file_sha256="expected_good_hash_12345",
        staging_file_path=str(staging_file),
        target_file_path=str(base_dir / "target.parquet"),
    )
    mgr.write_manifest(manifest)

    with pytest.raises(RecoveryFailureError, match="does not match recorded hash"):
        engine.scan_and_recover()

    assert not staging_file.exists()
    assert mgr.read_manifest("uuid_corrupt_stage") is None


def test_crash_recovery_publishing_already_published(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Crash at PUBLISHING where publication completed before crash: reconciles published file."""
    mgr, catalog, base_dir, engine = recovery_env
    target_file = base_dir / "published_target.parquet"
    candles = _make_test_candles(2)
    write_staging_parquet(target_file, candles, record_count=2)
    t_hash = compute_file_sha256(target_file)

    manifest = IngestionManifest(
        ingestion_uuid="uuid_publishing_done",
        stage=ManifestStage.PUBLISHING.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=7_200_000,
        expected_records=2,
        record_count=2,
        file_sha256=t_hash,
        target_file_path=str(target_file),
    )
    mgr.write_manifest(manifest)

    actions = engine.scan_and_recover()
    assert len(actions) == 1
    assert actions[0].action_taken == "COMPLETED_INGESTION_AND_CLEANUP"
    assert mgr.read_manifest("uuid_publishing_done") is None

    # Catalog record committed
    cat_rec = catalog.find_by_id_or_fingerprint(manifest.dataset_identity)
    assert cat_rec is not None
    assert cat_rec.file_sha256 == t_hash


def test_crash_recovery_publishing_conflict_preserved_byte_for_byte(
    recovery_env: tuple[ManifestManager, DuckDBCatalog, Path, RecoveryEngine],
) -> None:
    """Pre-existing conflicting target is preserved byte-for-byte; raises RecoveryConflictError."""
    mgr, _, base_dir, engine = recovery_env
    target_file = base_dir / "conflict_target.parquet"
    target_file.write_text("ORIGINAL CONFLICTING DATA BYTES", encoding="utf-8")
    orig_bytes = target_file.read_bytes()
    orig_sha = compute_file_sha256(target_file)

    manifest = IngestionManifest(
        ingestion_uuid="uuid_publishing_conflict",
        stage=ManifestStage.PUBLISHING.value,
        file_sha256="different_staging_sha256_abcdef",
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
    target_file = base_dir / "idempotent_target.parquet"
    candles = _make_test_candles(2)
    write_staging_parquet(target_file, candles, record_count=2)
    t_hash = compute_file_sha256(target_file)

    manifest = IngestionManifest(
        ingestion_uuid="uuid_committing_cat",
        stage=ManifestStage.COMMITTING_CATALOG.value,
        symbol="BTC/USDT",
        timeframe="1h",
        requested_since_ms=0,
        requested_until_ms=7_200_000,
        expected_records=2,
        record_count=2,
        file_sha256=t_hash,
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
    mgr, _, _, engine = recovery_env
    future_time = "2099-01-01T00:00:00Z"
    manifest = IngestionManifest(
        ingestion_uuid="uuid_active_lease",
        stage=ManifestStage.STAGING.value,
        lease_owner_pid=99999,
        lease_expires_at_utc=future_time,
    )
    mgr.write_manifest(manifest)

    with patch("open_quant_intelligence.storage.manifest.is_pid_alive", return_value=True):
        # By default, skip active leases
        actions = engine.scan_and_recover(allow_active_lease_skip=True)
        assert len(actions) == 0
        assert mgr.read_manifest("uuid_active_lease") is not None

        # When allow_active_lease_skip=False, raise LeaseActiveError
        with pytest.raises(LeaseActiveError, match="actively held by PID 99999"):
            engine.scan_and_recover(allow_active_lease_skip=False)
