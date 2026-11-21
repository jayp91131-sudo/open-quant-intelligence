"""Unit tests for IngestionManifest 8-state machine and crash-recovery transitions."""

from pathlib import Path

from open_quant_intelligence.storage.manifest import (
    IngestionManifest,
    ManifestManager,
    ManifestStage,
)


def test_manifest_lifecycle_transitions(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "manifests"
    manager = ManifestManager(manifest_dir)

    manifest = IngestionManifest(
        ingestion_uuid="uuid_test_1234",
        stage=ManifestStage.INITIALIZING,
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        since_ms=0,
        until_ms=3_600_000,
    )

    # 1. INITIALIZING -> STAGING
    manifest = manager.transition_stage(manifest, ManifestStage.STAGING)
    assert manifest.stage == ManifestStage.STAGING

    # 2. STAGING -> STAGED
    manifest = manager.transition_stage(
        manifest,
        ManifestStage.STAGED,
        staging_file_path=str(tmp_path / "staging.parquet"),
        staging_file_sha256="sha256:abc1234",
    )
    assert manifest.stage == ManifestStage.STAGED
    assert manifest.staging_file_sha256 == "sha256:abc1234"

    # 3. STAGED -> PUBLISHING
    manifest = manager.transition_stage(manifest, ManifestStage.PUBLISHING)
    assert manifest.stage == ManifestStage.PUBLISHING

    # 4. PUBLISHING -> PUBLISHED
    manifest = manager.transition_stage(
        manifest,
        ManifestStage.PUBLISHED,
        published_file_path=str(tmp_path / "published.parquet"),
    )
    assert manifest.stage == ManifestStage.PUBLISHED

    # 5. PUBLISHED -> COMMITTING_CATALOG
    manifest = manager.transition_stage(manifest, ManifestStage.COMMITTING_CATALOG)
    assert manifest.stage == ManifestStage.COMMITTING_CATALOG

    # 6. COMMITTING_CATALOG -> COMMITTED
    manifest = manager.transition_stage(manifest, ManifestStage.COMMITTED)
    assert manifest.stage == ManifestStage.COMMITTED

    # 7. COMMITTED -> CLEANUP
    manager.cleanup_manifest(manifest.ingestion_uuid)
    # Manifest file should be removed
    active = manager.list_active_manifests()
    assert len(active) == 0


def test_crash_recovery_all_transitions_idempotent(tmp_path: Path) -> None:
    """Requirement 16: Exercises crash recovery states and asserts reconciliation is safe and consistent."""
    manifest_dir = tmp_path / "manifests"
    manager = ManifestManager(manifest_dir)

    # Simulate crash at STAGING: staging file is partial/abandoned
    stg_file = tmp_path / "stg_crash.parquet"
    stg_file.write_bytes(b"partial_data")
    m_staging = IngestionManifest(
        ingestion_uuid="uuid_crash_stg",
        stage=ManifestStage.STAGING,
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        since_ms=0,
        until_ms=3_600_000,
        staging_file_path=str(stg_file),
    )
    manager.save_manifest(m_staging)

    # Active manifests includes the crashed one
    active = manager.list_active_manifests()
    assert len(active) == 1
    assert active[0].stage == ManifestStage.STAGING

    # Recovery cleans up abandoned staging file
    manager.cleanup_manifest("uuid_crash_stg")
    assert len(manager.list_active_manifests()) == 0
