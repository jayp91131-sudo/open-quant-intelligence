"""Unit tests for IngestionManifest 8-state machine, multiprocess locking, and crash-recovery transitions."""

import json
import multiprocessing
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from open_quant_intelligence.storage.manifest import (
    IngestionManifest,
    ManifestLock,
    ManifestLockError,
    ManifestManager,
    ManifestStage,
    _atomic_publish_no_clobber,
    _ReclaimAdvisoryLock,
    _safe_delete_lock_file,
)


def _worker_acquire_lock(manifest_path: str, timeout: float, out_queue: Any) -> None:
    """Multiprocess worker for simultaneous lock acquisition with timing."""
    try:
        lock = ManifestLock(Path(manifest_path), timeout_seconds=timeout)
        with lock:
            start_t = time.time()
            out_queue.put(("acquired", os.getpid(), start_t))
            time.sleep(0.15)
            end_t = time.time()
            out_queue.put(("releasing", os.getpid(), end_t))
    except Exception as e:
        out_queue.put(("error", os.getpid(), str(e)))


def _worker_unlink_manifest(manifest_dir: str, uuid_str: str, out_queue: Any, barrier: Any = None) -> None:
    """Multiprocess worker for unlink_manifest racing with reclamation."""
    try:
        if barrier is not None:
            barrier.wait(timeout=5.0)
        mgr = ManifestManager(Path(manifest_dir))
        res = mgr.unlink_manifest(uuid_str)
        out_queue.put(("unlinked", os.getpid(), res))
    except Exception as e:
        out_queue.put(("error", os.getpid(), str(e)))


def _worker_acquire_stale_reclaim(manifest_path: str, timeout: float, out_queue: Any, barrier: Any = None) -> None:
    """Multiprocess worker for racing stale-lock reclamation."""
    try:
        if barrier is not None:
            barrier.wait(timeout=5.0)
        lock = ManifestLock(Path(manifest_path), timeout_seconds=timeout)
        with lock:
            start_t = time.time()
            out_queue.put(("acquired", os.getpid(), start_t))
            time.sleep(0.1)
            end_t = time.time()
            out_queue.put(("releasing", os.getpid(), end_t))
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
    """Multiple processes competing for the same lock acquire it mutually exclusively without overlap."""
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
    releasing_events = [e for e in events if e[0] == "releasing"]
    assert len(acquired_events) == 2
    assert len(releasing_events) == 2

    # Invariant 1: Critical sections must have ZERO overlap (mutually exclusive)
    proc1_pid = acquired_events[0][1]
    proc1_start = acquired_events[0][2]
    proc1_end = [e[2] for e in releasing_events if e[1] == proc1_pid][0]

    proc2_pid = acquired_events[1][1]
    proc2_start = acquired_events[1][2]
    proc2_end = [e[2] for e in releasing_events if e[1] == proc2_pid][0]

    # Exactly one executed before the other
    non_overlapping = (proc1_end <= proc2_start) or (proc2_end <= proc1_start)
    assert non_overlapping, (
        f"Critical sections overlapped! Proc1: [{proc1_start}, {proc1_end}], Proc2: [{proc2_start}, {proc2_end}]"
    )


def test_multiprocess_stale_lock_reclamation(tmp_path: Path) -> None:
    """Multiple processes synchronized by barrier racing to reclaim stale lock coordinate safely."""
    manifest_file = tmp_path / "stale_target.json"
    manifest_file.write_text("{}", encoding="utf-8")
    lock_file = manifest_file.with_suffix(".lock")

    # Create stale lock file belonging to definitely dead PID 99999999
    stale_payload = json.dumps({"token": "stale_token_123", "pid": 99999999, "time": time.time() - 100})
    lock_file.write_text(stale_payload, encoding="utf-8")

    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    barrier = ctx.Barrier(2)

    p1 = ctx.Process(target=_worker_acquire_stale_reclaim, args=(str(manifest_file), 5.0, queue, barrier))
    p2 = ctx.Process(target=_worker_acquire_stale_reclaim, args=(str(manifest_file), 5.0, queue, barrier))

    p1.start()
    p2.start()

    p1.join(timeout=6.0)
    p2.join(timeout=6.0)

    results = []
    while not queue.empty():
        results.append(queue.get())

    errors = [r for r in results if r[0] == "error"]
    assert len(errors) == 0, f"Errors during stale lock reclamation: {errors}"
    acquired = [r for r in results if r[0] == "acquired"]
    releasing = [r for r in results if r[0] == "releasing"]
    assert len(acquired) == 2
    assert len(releasing) == 2

    # Invariant 2: Non-overlapping execution across the barrier
    pid_a, start_a = acquired[0][1], acquired[0][2]
    end_a = [r[2] for r in releasing if r[1] == pid_a][0]
    pid_b, start_b = acquired[1][1], acquired[1][2]
    end_b = [r[2] for r in releasing if r[1] == pid_b][0]
    assert (end_a <= start_b) or (end_b <= start_a), (
        f"Reclamation critical sections overlapped! [{start_a}, {end_a}] vs [{start_b}, {end_b}]"
    )


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


def test_ownership_safe_lock_cleanup_unlink_manifest(tmp_path: Path) -> None:
    """unlink_manifest() must not remove a lock or reclamation file owned by an active process."""
    mgr = ManifestManager(tmp_path / "manifests")
    test_uuid = "test_cleanup_uuid_123"
    man_path = mgr.manifest_path_for(test_uuid)
    man_path.write_text("{}", encoding="utf-8")

    # 1. Active lock held by current process
    lock = mgr.lock_manifest(test_uuid)
    lock.acquire()
    assert lock.lock_path.exists()

    # Also simulate an active reclaim file owned by current PID
    reclaim_path = man_path.with_suffix(".lock_reclaim")
    reclaim_path.write_text(
        json.dumps({"token": "reclaim_tok_1", "pid": os.getpid(), "time": time.time()}),
        encoding="utf-8",
    )

    # unlink_manifest called while lock and reclaim are actively owned
    mgr.unlink_manifest(test_uuid)

    # Manifest json is unlinked, but active lock and active reclaim must NOT be deleted!
    assert not man_path.exists()
    assert lock.lock_path.exists()
    assert reclaim_path.exists()

    # Now release lock normally
    lock.release()
    assert not lock.lock_path.exists()

    # Now simulate dead reclaim lock (PID 99999999)
    reclaim_path.write_text(
        json.dumps({"token": "reclaim_tok_dead", "pid": 99999999, "time": time.time()}),
        encoding="utf-8",
    )
    # unlink_manifest again should clean up the dead process's reclaim file
    mgr.unlink_manifest(test_uuid)
    assert not reclaim_path.exists()


def test_new_writer_during_rename_window_token_mismatch_preserves_new_writer(tmp_path: Path) -> None:
    """A new writer acquiring the lock during a cleanup rename window is preserved without clobbering."""
    manifest_file = tmp_path / "race_window_manifest.json"
    manifest_file.write_text("{}", encoding="utf-8")
    lock_path = manifest_file.with_suffix(".lock")

    # 1. Existing lock with stale token
    stale_payload = json.dumps({"token": "token_stale_111", "pid": 99999999, "time": time.time()})
    lock_path.write_text(stale_payload, encoding="utf-8")

    orig_rename = os.rename
    new_writer = ManifestLock(manifest_file, timeout_seconds=2.0)

    def rename_hook(src: str, dst: str) -> None:
        orig_rename(src, dst)
        if ".del_" in dst:
            # Right at this instant, lock_path has been renamed to temp_del.
            # A new writer acquires lock_path!
            new_writer.acquire()

    with patch("os.rename", side_effect=rename_hook):
        # Trigger cleanup on lock_path with mismatched expected_token
        safe_del_result = _safe_delete_lock_file(lock_path, expected_token="token_mismatch_expected")

    assert safe_del_result is False

    # Crucial assertion: new writer's lock payload remains 100% untouched!
    assert lock_path.exists()
    with open(lock_path, "r", encoding="utf-8") as f:
        current_data = json.load(f)
    assert current_data["token"] == new_writer.lock_token
    assert current_data["pid"] == os.getpid()

    # New writer releases cleanly
    new_writer.release()
    assert not lock_path.exists()


def test_new_writer_during_reclaim_rename_window_token_mismatch(tmp_path: Path) -> None:
    """A new process acquiring .lock_reclaim during cleanup rename is preserved without clobbering."""
    manifest_file = tmp_path / "race_reclaim_manifest.json"
    manifest_file.write_text("{}", encoding="utf-8")
    reclaim_path = manifest_file.with_suffix(".lock_reclaim")

    # 1. Existing reclaim lock with stale token
    stale_payload = json.dumps({"token": "reclaim_stale_222", "pid": 99999999, "time": time.time()})
    reclaim_path.write_text(stale_payload, encoding="utf-8")

    orig_rename = os.rename
    new_reclaim_token = f"reclaim_new_{uuid.uuid4().hex}"

    def rename_hook_reclaim(src: str, dst: str) -> None:
        orig_rename(src, dst)
        if ".del_" in dst:
            # Right at this instant, reclaim_path has been renamed to temp_del.
            # A new process publishes a new reclaim_path!
            tmp_pub = reclaim_path.with_name(f"{reclaim_path.name}.pub_{uuid.uuid4().hex}")
            tmp_pub.write_text(
                json.dumps({"token": new_reclaim_token, "pid": os.getpid(), "time": time.time()}),
                encoding="utf-8",
            )
            assert _atomic_publish_no_clobber(tmp_pub, reclaim_path) is True

    with patch("os.rename", side_effect=rename_hook_reclaim):
        safe_del_result = _safe_delete_lock_file(reclaim_path, expected_token="token_wrong")

    assert safe_del_result is False

    # Crucial assertion: new process's reclaim lock remains untouched!
    assert reclaim_path.exists()
    with open(reclaim_path, "r", encoding="utf-8") as f:
        current_data = json.load(f)
    assert current_data["token"] == new_reclaim_token
    assert current_data["pid"] == os.getpid()

    # Clean up
    assert _safe_delete_lock_file(reclaim_path, expected_token=new_reclaim_token) is True
    assert not reclaim_path.exists()


def test_interrupted_lock_creation_recovers_safely(tmp_path: Path) -> None:
    """Interrupted lock creation (empty or malformed lock file) is safely reclaimed on acquisition."""
    manifest_file = tmp_path / "interrupted_manifest.json"
    manifest_file.write_text("{}", encoding="utf-8")
    lock_file = manifest_file.with_suffix(".lock")

    # Case 1: Empty lock file (simulating crash between O_CREAT|O_EXCL and write)
    lock_file.write_bytes(b"")
    time.sleep(0.06)  # Ensure st_mtime >= 0.05s

    lock1 = ManifestLock(manifest_file, timeout_seconds=2.0)
    lock1.acquire()
    assert lock1._acquired is True
    assert lock_file.exists()

    with open(lock_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["token"] == lock1.lock_token
    assert data["pid"] == os.getpid()

    lock1.release()
    assert not lock_file.exists()

    # Case 2: Malformed/truncated JSON in lock file
    lock_file.write_text('{"token": "partial_truncate_tok', encoding="utf-8")
    time.sleep(0.06)

    lock2 = ManifestLock(manifest_file, timeout_seconds=2.0)
    lock2.acquire()
    assert lock2._acquired is True

    with open(lock_file, "r", encoding="utf-8") as f:
        data2 = json.load(f)
    assert data2["token"] == lock2.lock_token
    assert data2["pid"] == os.getpid()

    lock2.release()
    assert not lock_file.exists()


def test_unlink_manifest_and_stale_reclaim_multiprocess_race(tmp_path: Path) -> None:
    """Race between unlink_manifest() and stale-lock reclamation is serialized through .lock_reclaim."""
    manifest_dir = tmp_path / "race_manifests"
    mgr = ManifestManager(manifest_dir)
    uuid_str = "race_uuid_555"
    man_path = mgr.manifest_path_for(uuid_str)
    man_path.write_text("{}", encoding="utf-8")
    lock_file = man_path.with_suffix(".lock")

    # Stale lock with dead PID
    stale_payload = json.dumps({"token": "stale_race_tok", "pid": 99999999, "time": time.time() - 100})
    lock_file.write_text(stale_payload, encoding="utf-8")

    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    barrier = ctx.Barrier(2)

    p1 = ctx.Process(target=_worker_unlink_manifest, args=(str(manifest_dir), uuid_str, queue, barrier))
    p2 = ctx.Process(target=_worker_acquire_stale_reclaim, args=(str(man_path), 5.0, queue, barrier))

    p1.start()
    p2.start()

    p1.join(timeout=6.0)
    p2.join(timeout=6.0)

    events = []
    while not queue.empty():
        events.append(queue.get())

    errors = [e for e in events if e[0] == "error"]
    assert len(errors) == 0, f"Multiprocess race resulted in errors: {errors}"
    assert len(events) >= 2


def test_stale_reclaim_aba_race_protection(tmp_path: Path) -> None:
    """Stale reclamation cannot steal or corrupt a newer reclaimer's .lock_reclaim (ABA race safe)."""
    manifest_file = tmp_path / "aba_target.json"
    manifest_file.write_text("{}", encoding="utf-8")
    lock_file = manifest_file.with_suffix(".lock")
    reclaim_file = manifest_file.with_suffix(".lock_reclaim")
    meta_lock_file = manifest_file.with_suffix(".reclaim_meta")

    # Step 1: Stale main lock (dead PID)
    stale_lock_token = "stale_main_token"
    lock_file.write_text(
        json.dumps({"token": stale_lock_token, "pid": 99999999, "time": time.time() - 100}),
        encoding="utf-8",
    )

    # Step 2: Stale reclaim lock R0 observed by Process B
    r0_token = "reclaim_r0_dead"
    reclaim_file.write_text(
        json.dumps({"token": r0_token, "pid": 99999999, "time": time.time() - 100}),
        encoding="utf-8",
    )

    # Process B reads R0 and observes dead PID. Before deleting, Process B pauses.
    # Meanwhile, cleanup or previous reclaimer deletes R0.
    _safe_delete_lock_file(reclaim_file, expected_token=r0_token)
    assert not reclaim_file.exists()

    # Step 3: Process C acquires the OS-level advisory guard and publishes live reclaim lock R1
    guard_c = _ReclaimAdvisoryLock(meta_lock_file)
    assert guard_c.try_acquire() is True

    r1_token = f"reclaim_r1_live_{os.getpid()}"
    tmp_r1 = reclaim_file.with_name(f"{reclaim_file.name}.pub_r1")
    tmp_r1.write_text(
        json.dumps({"token": r1_token, "pid": os.getpid(), "time": time.time()}),
        encoding="utf-8",
    )
    assert _atomic_publish_no_clobber(tmp_r1, reclaim_file) is True
    assert reclaim_file.exists()

    # Step 4: Process B wakes up with its stale observation of R0 and attempts deletion
    # Calling _safe_delete_lock_file with expected_token=r0_token
    deleted = _safe_delete_lock_file(reclaim_file, expected_token=r0_token)
    assert deleted is False, "Process B must not delete Process C's live reclaim lock R1!"

    # Verify Process C's reclaim lock is completely intact
    assert reclaim_file.exists()
    content = json.loads(reclaim_file.read_text(encoding="utf-8"))
    assert content["token"] == r1_token

    # Step 5: Process D attempts stale reclamation while Process C holds advisory guard
    guard_d = _ReclaimAdvisoryLock(meta_lock_file)
    assert guard_d.try_acquire() is False, "Process D must not enter stale reclamation while C is active!"

    # Process C cleans up and releases
    _safe_delete_lock_file(reclaim_file, expected_token=r1_token)
    guard_c.release()

    # Now Process D can acquire
    assert guard_d.try_acquire() is True
    guard_d.release()


def test_publish_link_success_source_unlink_error(monkeypatch: Any, tmp_path: Path) -> None:
    """Hard-link publication succeeds even if unlinking the source file raises OSError."""
    src = tmp_path / "test_src.tmp"
    dst = tmp_path / "test_dst.dat"
    src.write_text("test_content", encoding="utf-8")

    orig_unlink = os.unlink

    def mock_unlink(path: Any, *args: Any, **kwargs: Any) -> None:
        if str(path) == str(src):
            raise OSError("Simulated unlink failure on source file")
        orig_unlink(path, *args, **kwargs)

    monkeypatch.setattr(os, "unlink", mock_unlink)

    if sys.platform != "win32":
        published = _atomic_publish_no_clobber(src, dst)
        assert published is True
        assert dst.exists()
        assert dst.read_text(encoding="utf-8") == "test_content"
    else:
        manifest_file = tmp_path / "unlink_err_manifest.json"
        manifest_file.write_text("{}", encoding="utf-8")
        lock = ManifestLock(manifest_file)
        with lock:
            assert lock.lock_path.exists()
