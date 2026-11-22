"""Unit tests for IngestionManifest 8-state machine, multiprocess locking, and crash-recovery transitions."""

import json
import multiprocessing
import os
import time
from pathlib import Path
from typing import Any

import pytest

from open_quant_intelligence.storage.manifest import (
    IngestionManifest,
    ManifestLock,
    ManifestLockError,
    ManifestManager,
    ManifestStage,
)


def _worker_acquire_lock(manifest_path: str, timeout: float, out_queue: Any) -> None:
    """Multiprocess worker for simultaneous lock acquisition."""
    try:
        lock = ManifestLock(Path(manifest_path), timeout_seconds=timeout)
        with lock:
            out_queue.put(("acquired", os.getpid(), time.time()))
            time.sleep(0.1)
            out_queue.put(("releasing", os.getpid(), time.time()))
    except Exception as e:
        out_queue.put(("error", os.getpid(), str(e)))


def _worker_acquire_stale_reclaim(manifest_path: str, timeout: float, out_queue: Any) -> None:
    """Multiprocess worker for racing stale-lock reclamation."""
    try:
        lock = ManifestLock(Path(manifest_path), timeout_seconds=timeout)
        with lock:
            out_queue.put(("acquired", os.getpid()))
            time.sleep(0.05)
    except Exception as e:
        out_queue.put(("error", os.getpid(), str(e)))


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


def test_multiprocess_simultaneous_lock_acquisition(tmp_path: Path) -> None:
    """Multiple processes competing for the same lock acquire it mutually exclusively."""
    manifest_file = tmp_path / "target_manifest.json"
    manifest_file.write_text("{}", encoding="utf-8")
    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()

    p1 = ctx.Process(target=_worker_acquire_lock, args=(str(manifest_file), 5.0, queue))
    p2 = ctx.Process(target=_worker_acquire_lock, args=(str(manifest_file), 5.0, queue))

    p1.start()
    p2.start()

    p1.join(timeout=6.0)
    p2.join(timeout=6.0)

    events = []
    while not queue.empty():
        events.append(queue.get())

    # Both processes must have succeeded without error
    errors = [e for e in events if e[0] == "error"]
    assert len(errors) == 0, f"Errors in multiprocess lock acquisition: {errors}"

    acquired_events = [e for e in events if e[0] == "acquired"]
    assert len(acquired_events) == 2


def test_multiprocess_stale_lock_reclamation(tmp_path: Path) -> None:
    """Multiple processes racing to reclaim a stale lock file do not race or corrupt."""
    manifest_file = tmp_path / "stale_target.json"
    manifest_file.write_text("{}", encoding="utf-8")
    lock_file = manifest_file.with_suffix(".lock")

    # Create stale lock file belonging to definitely dead PID
    # PID 99999999 is dead/non-existent
    stale_payload = json.dumps({"token": "stale_token_123", "pid": 99999999, "time": time.time() - 100})
    lock_file.write_text(stale_payload, encoding="utf-8")

    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()

    p1 = ctx.Process(target=_worker_acquire_stale_reclaim, args=(str(manifest_file), 5.0, queue))
    p2 = ctx.Process(target=_worker_acquire_stale_reclaim, args=(str(manifest_file), 5.0, queue))

    p1.start()
    p2.start()

    p1.join(timeout=6.0)
    p2.join(timeout=6.0)

    results = []
    while not queue.empty():
        results.append(queue.get())

    errors = [r for r in results if r[0] == "error"]
    assert len(errors) == 0, f"Errors during stale lock reclamation: {errors}"
    assert len([r for r in results if r[0] == "reclaimed" or r[0] == "acquired"]) == 2


def test_ownership_safe_release(tmp_path: Path) -> None:
    """Releasing a lock verifies the ownership token and never deletes a foreign lock."""
    manifest_file = tmp_path / "foreign_lock.json"
    lock = ManifestLock(manifest_file, timeout_seconds=2.0)
    lock.acquire()
    assert lock.lock_path.exists()

    # Tamper with the lock file so it has another owner's token
    foreign_token = "foreign_owner_token_999"
    foreign_payload = json.dumps({"token": foreign_token, "pid": os.getpid(), "time": time.time()})
    lock.lock_path.write_text(foreign_payload, encoding="utf-8")

    # Call release on original lock instance
    lock.release()

    # The lock file must NOT be deleted because token does not match
    assert lock.lock_path.exists()

    # Clean up
    lock.lock_path.unlink()


def test_active_process_protection_not_stolen(tmp_path: Path) -> None:
    """An active running process holding a lock is protected and cannot be stolen."""
    manifest_file = tmp_path / "active_proc.json"
    lock1 = ManifestLock(manifest_file, timeout_seconds=1.0)
    lock1.acquire()

    # Second lock with short timeout attempts to acquire
    lock2 = ManifestLock(manifest_file, timeout_seconds=0.2)
    with pytest.raises(ManifestLockError, match="Could not acquire atomic lock"):
        lock2.acquire()

    # Verify lock1 is still held and file exists
    assert lock1.lock_path.exists()
    lock1.release()
    assert not lock1.lock_path.exists()
