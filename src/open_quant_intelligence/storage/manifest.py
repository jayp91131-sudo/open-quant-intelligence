"""Durable ingestion manifest management and 8-state machine transitions."""

import json
import os
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class DurabilityMode(str, Enum):
    """Execution policy for filesystem directory synchronization failures."""

    STRICT = "STRICT"
    DEGRADED_ALLOWED = "DEGRADED_ALLOWED"


class DurabilityStatus(str, Enum):
    """Observable durability classification."""

    GUARANTEED = "GUARANTEED"
    DEGRADED = "DEGRADED"


class ManifestStage(str, Enum):
    """The 8 explicit lifecycle stages of an ingestion manifest."""

    INITIALIZING = "INITIALIZING"
    STAGING = "STAGING"
    STAGED = "STAGED"
    PUBLISHING = "PUBLISHING"
    PUBLISHED = "PUBLISHED"
    COMMITTING_CATALOG = "COMMITTING_CATALOG"
    COMMITTED = "COMMITTED"
    CLEANUP = "CLEANUP"


class InvalidStageTransitionError(ValueError):
    """Raised when an illegal or non-monotonic manifest stage transition is attempted."""


class DurabilityError(Exception):
    """Raised when critical filesystem durability synchronization fails."""


class ManifestLockError(Exception):
    """Raised when an atomic single-writer lock cannot be acquired."""


_ALLOWED_STAGE_TRANSITIONS: dict[str, set[str]] = {
    ManifestStage.INITIALIZING.value: {ManifestStage.STAGING.value, ManifestStage.CLEANUP.value},
    ManifestStage.STAGING.value: {ManifestStage.STAGED.value, ManifestStage.CLEANUP.value},
    ManifestStage.STAGED.value: {ManifestStage.PUBLISHING.value, ManifestStage.CLEANUP.value},
    ManifestStage.PUBLISHING.value: {ManifestStage.PUBLISHED.value, ManifestStage.CLEANUP.value},
    ManifestStage.PUBLISHED.value: {ManifestStage.COMMITTING_CATALOG.value, ManifestStage.CLEANUP.value},
    ManifestStage.COMMITTING_CATALOG.value: {ManifestStage.COMMITTED.value, ManifestStage.CLEANUP.value},
    ManifestStage.COMMITTED.value: {ManifestStage.CLEANUP.value},
    ManifestStage.CLEANUP.value: set(),
}


def is_pid_alive(pid: int) -> bool:
    """Check whether a process with the given PID is actively running.

    Conservative: Returns True if PID cannot be definitively determined to be dead.
    """
    if pid <= 0:
        return False
    if sys.platform == "win32":
        import ctypes

        kernel32 = ctypes.windll.kernel32
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        SYNCHRONIZE = 0x00100000
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE, False, pid)
        if not handle:
            err = kernel32.GetLastError()
            ERROR_ACCESS_DENIED = 5
            ERROR_INVALID_PARAMETER = 87
            if err == ERROR_ACCESS_DENIED:
                # Process exists, belongs to another user or elevated
                return True
            if err == ERROR_INVALID_PARAMETER:
                # Process definitely does not exist
                return False
            return True

        try:
            WAIT_OBJECT_0 = 0
            WAIT_TIMEOUT = 258
            res = kernel32.WaitForSingleObject(handle, 0)
            if res == WAIT_TIMEOUT:
                return True
            if res == WAIT_OBJECT_0:
                return False

            STILL_ACTIVE = 259
            exit_code = ctypes.c_ulong()
            if kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return exit_code.value == STILL_ACTIVE
            return True
        finally:
            kernel32.CloseHandle(handle)
    else:
        try:
            os.kill(pid, 0)
            return True
        except PermissionError:
            return True
        except ProcessLookupError:
            return False
        except OSError:
            return True


def is_lease_active(manifest: "IngestionManifest") -> bool:
    """Determine if a manifest's lease is actively held by a live process.

    A lease timestamp alone must not prove that a process is dead. If the process
    is actively alive, the lease remains active regardless of timestamp expiry.
    """
    if not manifest.lease_owner_pid:
        return False
    return is_pid_alive(manifest.lease_owner_pid)


def _atomic_publish_no_clobber(src: Path, dst: Path) -> bool:
    """Atomically move or link src to dst without clobbering dst if dst exists.

    Returns True if publication succeeded (dst was created/linked from src, dst did not exist).
    Returns False if dst already exists (no-clobber guarantee: dst remains untouched).
    """
    if sys.platform == "win32":
        # On Windows, os.rename(src, dst) raises FileExistsError if dst exists.
        # It never overwrites dst.
        try:
            os.rename(str(src), str(dst))
            return True
        except (FileExistsError, OSError):
            return False
    else:
        # On POSIX, os.rename(src, dst) silently overwrites dst.
        # Therefore, use os.link(src, dst) which atomically fails with FileExistsError
        # if dst exists, then unlink src.
        try:
            os.link(str(src), str(dst))
        except (FileExistsError, OSError):
            return False

        # Publication succeeded: dst is now created and points to the data!
        # Handle cleanup of temporary src independently so failure here does NOT
        # invalidate the successful publication.
        try:
            os.unlink(str(src))
        except OSError:
            pass
        return True


class _ReclaimAdvisoryLock:
    """OS-level advisory file lock for serializing stale lock reclamation.

    Guarantees kernel-level single-reclaimer mutual exclusion across processes.
    If a process terminates or crashes, the OS kernel automatically releases
    the advisory lock, eliminating unsynchronized observation races.
    """

    def __init__(self, lock_path: Path) -> None:
        self.path = lock_path
        self.fd: int | None = None

    def try_acquire(self) -> bool:
        """Attempt non-blocking acquisition of OS-level advisory lock."""
        try:
            if sys.platform == "win32":
                import msvcrt

                fd = os.open(str(self.path), os.O_RDWR | os.O_CREAT)
                try:
                    if os.fstat(fd).st_size == 0:
                        os.write(fd, b"0")
                    os.lseek(fd, 0, os.SEEK_SET)
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                    self.fd = fd
                    return True
                except (OSError, PermissionError):
                    os.close(fd)
                    return False
            else:
                import fcntl

                fd = os.open(str(self.path), os.O_RDWR | os.O_CREAT, 0o666)
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self.fd = fd
                    return True
                except (BlockingIOError, OSError):
                    os.close(fd)
                    return False
        except OSError:
            return False

    def release(self) -> None:
        """Release OS-level advisory lock."""
        if self.fd is not None:
            try:
                if sys.platform == "win32":
                    import msvcrt

                    try:
                        os.lseek(self.fd, 0, os.SEEK_SET)
                        msvcrt.locking(self.fd, msvcrt.LK_UNLCK, 1)
                    except OSError:
                        pass
                else:
                    import fcntl

                    try:
                        fcntl.flock(self.fd, fcntl.LOCK_UN)
                    except OSError:
                        pass
            finally:
                try:
                    os.close(self.fd)
                except OSError:
                    pass
                self.fd = None

    def __enter__(self) -> bool:
        return self.try_acquire()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


def _safe_delete_lock_file(path: Path, expected_token: str | None) -> bool:
    """Safely delete a lock file only if its token matches expected_token (or if None, abandoned/malformed).

    Atomically renames the file to a unique deletion temporary file, verifies
    the ownership token (or malformed state), and deletes it. If the verification fails,
    the file is restored via atomic no-clobber publication (never overwriting a newer lock).
    """
    if not path.is_file():
        return False
    temp_del = path.with_name(f"{path.name}.del_{uuid.uuid4().hex}")
    try:
        os.rename(str(path), str(temp_del))
    except (FileNotFoundError, PermissionError, OSError):
        return False

    should_delete = False
    try:
        with open(temp_del, "r", encoding="utf-8") as f:
            content = f.read().strip()

        if expected_token is not None:
            if content:
                data = json.loads(content)
                if isinstance(data, dict) and data.get("token") == expected_token:
                    should_delete = True
        else:
            # expected_token is None: removing abandoned empty or malformed lock file
            if not content:
                should_delete = True
            else:
                try:
                    data = json.loads(content)
                    if not isinstance(data, dict):
                        should_delete = True
                    else:
                        owner_pid = data.get("pid", 0)
                        owner_token = data.get("token", "")
                        if (
                            not isinstance(owner_pid, int)
                            or owner_pid <= 0
                            or not isinstance(owner_token, str)
                            or not owner_token
                        ):
                            should_delete = True
                        elif not is_pid_alive(owner_pid):
                            should_delete = True
                except Exception:
                    should_delete = True
    except Exception:
        # Could not read / invalid JSON
        if expected_token is None:
            should_delete = True
        else:
            should_delete = False

    if should_delete:
        temp_del.unlink(missing_ok=True)
        return True
    else:
        # Token mismatch or valid live owner: restore file using atomic no-clobber
        if not _atomic_publish_no_clobber(temp_del, path):
            # Destination path already exists (a new writer acquired it in the interim window).
            # Never overwrite destination! Clean up the temporary deletion artifact.
            temp_del.unlink(missing_ok=True)
        return False


class ManifestLock:
    """Atomic single-writer file lock for manifest mutation and recovery with race-safe reclamation."""

    def __init__(self, manifest_path: Path, timeout_seconds: float = 5.0) -> None:
        self.lock_path = manifest_path.with_suffix(".lock")
        self.reclaim_path = manifest_path.with_suffix(".lock_reclaim")
        self.meta_lock_path = manifest_path.with_suffix(".reclaim_meta")
        self.timeout_seconds = timeout_seconds
        self.lock_token = f"lock_{os.getpid()}_{time.time_ns()}_{uuid.uuid4().hex}"
        self._acquired = False

    def acquire(self) -> None:
        start_time = time.time()
        while True:
            # Atomic publication: write complete payload to temp file then publish with no-clobber
            tmp_pub = self.lock_path.with_name(f"{self.lock_path.name}.pub_{uuid.uuid4().hex}")
            published = False
            try:
                payload = json.dumps(
                    {
                        "token": self.lock_token,
                        "pid": os.getpid(),
                        "time": time.time(),
                    }
                ).encode("utf-8")
                with open(tmp_pub, "wb") as f:
                    f.write(payload)
                    f.flush()
                published = _atomic_publish_no_clobber(tmp_pub, self.lock_path)
                if published:
                    self._acquired = True
                    return
            finally:
                if not published:
                    tmp_pub.unlink(missing_ok=True)
                else:
                    try:
                        tmp_pub.unlink(missing_ok=True)
                    except OSError:
                        pass

            self._try_reclaim_stale_lock()

            if time.time() - start_time > self.timeout_seconds:
                raise ManifestLockError(
                    f"Could not acquire atomic lock on {self.lock_path} within {self.timeout_seconds}s."
                ) from None
            time.sleep(0.05)

    def _try_reclaim_stale_lock(self) -> None:
        guard = _ReclaimAdvisoryLock(self.meta_lock_path)
        if not guard.try_acquire():
            # Another process is actively performing reclamation; do not overlap
            return

        try:
            # 1. Under advisory lock guard, reconcile any existing .lock_reclaim
            if self.reclaim_path.is_file():
                try:
                    with open(self.reclaim_path, "r", encoding="utf-8") as rf:
                        rcontent = rf.read().strip()
                except Exception:
                    rcontent = ""

                if not rcontent:
                    if time.time() - self.reclaim_path.stat().st_mtime >= 0.05:
                        _safe_delete_lock_file(self.reclaim_path, expected_token=None)
                    else:
                        return
                else:
                    try:
                        rdata = json.loads(rcontent)
                    except Exception:
                        rdata = None

                    if not isinstance(rdata, dict):
                        if time.time() - self.reclaim_path.stat().st_mtime >= 0.05:
                            _safe_delete_lock_file(self.reclaim_path, expected_token=None)
                        else:
                            return
                    else:
                        rpid = rdata.get("pid", 0)
                        rtoken = rdata.get("token", "")
                        if not isinstance(rpid, int) or rpid <= 0 or not isinstance(rtoken, str) or not rtoken:
                            if time.time() - self.reclaim_path.stat().st_mtime >= 0.05:
                                _safe_delete_lock_file(self.reclaim_path, expected_token=None)
                            else:
                                return
                        else:
                            # Conservative PID liveness check
                            if not is_pid_alive(rpid):
                                _safe_delete_lock_file(self.reclaim_path, expected_token=rtoken)
                            else:
                                return

            # 2. Acquire .lock_reclaim under advisory lock guard
            reclaim_token = f"reclaim_{os.getpid()}_{time.time_ns()}_{uuid.uuid4().hex}"
            tmp_reclaim = self.reclaim_path.with_name(f"{self.reclaim_path.name}.pub_{uuid.uuid4().hex}")
            reclaim_published = False
            try:
                r_payload = json.dumps(
                    {
                        "token": reclaim_token,
                        "pid": os.getpid(),
                        "time": time.time(),
                    }
                ).encode("utf-8")
                with open(tmp_reclaim, "wb") as rf:
                    rf.write(r_payload)
                    rf.flush()
                reclaim_published = _atomic_publish_no_clobber(tmp_reclaim, self.reclaim_path)
            finally:
                if not reclaim_published:
                    tmp_reclaim.unlink(missing_ok=True)
                else:
                    try:
                        tmp_reclaim.unlink(missing_ok=True)
                    except OSError:
                        pass

            if not reclaim_published:
                return

            try:
                if not self.lock_path.is_file():
                    return

                try:
                    with open(self.lock_path, "r", encoding="utf-8") as f:
                        content = f.read().strip()
                except Exception:
                    content = ""

                if not content:
                    # Interrupted lock creation (empty file) or crash before writing
                    if time.time() - self.lock_path.stat().st_mtime >= 0.05:
                        _safe_delete_lock_file(self.lock_path, expected_token=None)
                    return

                try:
                    data = json.loads(content)
                except Exception:
                    # Malformed JSON in lock file
                    if time.time() - self.lock_path.stat().st_mtime >= 0.05:
                        _safe_delete_lock_file(self.lock_path, expected_token=None)
                    return

                if not isinstance(data, dict):
                    if time.time() - self.lock_path.stat().st_mtime >= 0.05:
                        _safe_delete_lock_file(self.lock_path, expected_token=None)
                    return

                owner_pid = data.get("pid", 0)
                owner_token = data.get("token", "")

                if (
                    not isinstance(owner_pid, int)
                    or owner_pid <= 0
                    or not isinstance(owner_token, str)
                    or not owner_token
                ):
                    if time.time() - self.lock_path.stat().st_mtime >= 0.05:
                        _safe_delete_lock_file(self.lock_path, expected_token=None)
                    return

                # Check if lock owner PID is dead (conservative: alive/access-denied treated as alive)
                if not is_pid_alive(owner_pid):
                    _safe_delete_lock_file(self.lock_path, expected_token=owner_token)
            finally:
                _safe_delete_lock_file(self.reclaim_path, expected_token=reclaim_token)
        finally:
            guard.release()

    @classmethod
    def reclaim_stale_locks(cls, manifest_path: Path) -> None:
        """Run the single, unified stale-lock reclamation protocol for a manifest."""
        lock = cls(manifest_path)
        lock._try_reclaim_stale_lock()

    def release(self) -> None:
        if not self._acquired:
            return
        try:
            _safe_delete_lock_file(self.lock_path, self.lock_token)
        finally:
            self._acquired = False

    def __enter__(self) -> "ManifestLock":
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


def sync_directory(
    dir_path: Path | str,
    raise_on_failure: bool | None = None,
    durability_mode: DurabilityMode | str = DurabilityMode.DEGRADED_ALLOWED,
) -> bool:
    """Synchronize directory metadata to persistent storage.

    Distinction between file content durability and directory entry durability:
    - File content durability: Handled via write-through flags and os.fsync() / _commit(),
      forcing physical file data bytes and file stream allocations to disk.
    - Directory entry durability: Synchronizes directory table entries (filenames, links).
      * POSIX: Supported via open(dir, O_RDONLY) + os.fsync(dir_fd).
      * Windows: The Win32 subsystem explicitly prohibits flushing directory handles
        (FlushFileBuffers on a directory handle fails with ERROR_ACCESS_DENIED).
        While NTFS journals metadata transactions via $LogFile, directory-level fsync
        cannot be verified or forced from user-mode. Therefore, full directory durability
        is unsupported on Windows and classified as DEGRADED.

    Args:
        dir_path: Path to directory to synchronize.
        raise_on_failure: Legacy flag; if True, raises DurabilityError on failure.
        durability_mode: DurabilityMode.STRICT (raises on failure or if unsupported) or
            DurabilityMode.DEGRADED_ALLOWED (returns False on failure or if unsupported).

    Returns:
        True if directory synchronization succeeded.
        False if directory synchronization is unsupported (Windows) or failed (POSIX).

    Raises:
        DurabilityError: In STRICT mode if directory synchronization fails or is unsupported.
    """
    strict = (
        raise_on_failure
        if raise_on_failure is not None
        else (durability_mode == DurabilityMode.STRICT or durability_mode == DurabilityMode.STRICT.value)
    )

    if sys.platform == "win32":
        if strict:
            raise DurabilityError(
                f"Directory synchronization for {dir_path} is unsupported on Windows: "
                "Win32 prohibits flushing directory handles. STRICT durability cannot be guaranteed."
            )
        return False

    path = Path(dir_path)
    if not path.is_dir():
        return True

    try:
        dir_fd = os.open(str(path), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
            return True
        finally:
            os.close(dir_fd)
    except OSError as e:
        if strict:
            raise DurabilityError(f"Directory fsync failed for {path}: {e}") from e
        return False


@dataclass
class IngestionManifest:
    """Durable state record for an in-flight or committed ingestion job."""

    ingestion_uuid: str
    stage: str = ManifestStage.INITIALIZING.value
    exchange_id: str = "binance"
    market_type: str = "spot"
    symbol: str = "BTC/USDT"
    timeframe: str = "1h"
    since_ms: int = 0
    until_ms: int = 0
    requested_since_ms: int = 0
    requested_until_ms: int = 0
    requested_since_utc: str = ""
    requested_until_utc: str = ""
    staging_file_path: str = ""
    target_file_path: str = ""
    expected_records: int = 0
    record_count: int = 0
    missing_intervals: int = 0
    gap_count: int = 0
    gaps: list[dict[str, Any]] = field(default_factory=list)
    content_fingerprint_sha256: str = ""
    file_sha256: str = ""
    lease_owner_pid: int = 0
    lease_expires_at_utc: str = ""
    schema_version: int = 1
    ccxt_version: str = "4.4.52"
    created_at_utc: str = ""
    updated_at_utc: str = ""
    durability_status: str = DurabilityStatus.GUARANTEED.value
    durability_warning: str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.stage, ManifestStage):
            self.stage = self.stage.value
        if self.requested_since_ms == 0 and self.since_ms != 0:
            self.requested_since_ms = self.since_ms
        if self.requested_until_ms == 0 and self.until_ms != 0:
            self.requested_until_ms = self.until_ms
        if self.gaps and not self.gap_count:
            self.gap_count = len(self.gaps)

    @property
    def staging_file_sha256(self) -> str:
        return self.file_sha256

    @staging_file_sha256.setter
    def staging_file_sha256(self, val: str) -> None:
        self.file_sha256 = val

    @property
    def published_file_path(self) -> str:
        return self.target_file_path

    @published_file_path.setter
    def published_file_path(self, val: str) -> None:
        self.target_file_path = val

    @property
    def dataset_identity(self) -> str:
        return (
            f"{self.exchange_id}:{self.market_type}:{self.symbol}:"
            f"{self.timeframe}:{self.requested_since_ms}:{self.requested_until_ms}"
        )


class ManifestManager:
    """Manages atomic creation, update, and durability of ingestion manifests."""

    def __init__(
        self,
        manifests_dir: Path | str,
        durability_mode: DurabilityMode | str = DurabilityMode.DEGRADED_ALLOWED,
    ) -> None:
        self.manifests_dir = Path(manifests_dir)
        self.manifests_dir.mkdir(parents=True, exist_ok=True)
        self.durability_mode = (
            durability_mode if isinstance(durability_mode, DurabilityMode) else DurabilityMode(str(durability_mode))
        )
        self.last_durability_status: DurabilityStatus = DurabilityStatus.GUARANTEED
        self.durability_warning: str | None = None

    def manifest_path_for(self, ingestion_uuid: str) -> Path:
        return self.manifests_dir / f".ingest_manifest_{ingestion_uuid}.json"

    def lock_manifest(self, ingestion_uuid: str, timeout_seconds: float = 5.0) -> ManifestLock:
        return ManifestLock(self.manifest_path_for(ingestion_uuid), timeout_seconds=timeout_seconds)

    def save_manifest(self, manifest: IngestionManifest) -> Path:
        return self.write_manifest(manifest)

    def transition_stage(
        self,
        manifest: IngestionManifest,
        new_stage: ManifestStage | str,
        force: bool = False,
        **kwargs: Any,
    ) -> IngestionManifest:
        stage_val = new_stage.value if isinstance(new_stage, ManifestStage) else str(new_stage)
        current_val = manifest.stage
        if not force and current_val != stage_val:
            allowed = _ALLOWED_STAGE_TRANSITIONS.get(current_val, set())
            if stage_val not in allowed:
                raise InvalidStageTransitionError(
                    f"Illegal stage transition from {current_val} to {stage_val} "
                    f"for ingestion {manifest.ingestion_uuid}."
                )
        manifest.stage = stage_val
        for k, v in kwargs.items():
            if hasattr(manifest, k):
                setattr(manifest, k, v)
        manifest.updated_at_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        self.write_manifest(manifest)
        return manifest

    def cleanup_manifest(self, ingestion_uuid: str) -> None:
        self.unlink_manifest(ingestion_uuid)

    def list_active_manifests(self) -> list[IngestionManifest]:
        res = []
        for p in self.manifests_dir.glob(".ingest_manifest_*.json"):
            if not p.name.endswith(".tmp") and not p.name.endswith(".lock"):
                uuid_str = p.stem.replace(".ingest_manifest_", "")
                m = self.read_manifest(uuid_str)
                if m:
                    res.append(m)
        return res

    def write_manifest(self, manifest: IngestionManifest) -> Path:
        final_path = self.manifest_path_for(manifest.ingestion_uuid)
        tmp_path = self.manifests_dir / f".ingest_manifest_{manifest.ingestion_uuid}.json.tmp"

        payload = json.dumps(asdict(manifest), indent=2)

        # Write to temporary manifest and flush
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())

        # Atomic replacement of main manifest
        if sys.platform == "win32":
            import ctypes

            kernel32 = ctypes.windll.kernel32
            MoveFileExW = kernel32.MoveFileExW
            MoveFileExW.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32]
            MoveFileExW.restype = ctypes.c_bool
            MOVEFILE_REPLACE_EXISTING = 0x1
            MOVEFILE_WRITE_THROUGH = 0x8
            success = MoveFileExW(str(tmp_path), str(final_path), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)
            if not success:
                os.replace(tmp_path, final_path)
        else:
            os.replace(tmp_path, final_path)

        # Directory sync
        synced = sync_directory(self.manifests_dir, durability_mode=self.durability_mode)
        if not synced:
            self.last_durability_status = DurabilityStatus.DEGRADED
            if sys.platform == "win32":
                self.durability_warning = (
                    f"Directory-entry durability is unsupported on Windows for {self.manifests_dir} "
                    "(file content flushed with write-through/commit, but directory fsync is unavailable)"
                )
            else:
                self.durability_warning = f"Directory sync failed for {self.manifests_dir}"
            manifest.durability_status = DurabilityStatus.DEGRADED.value
            manifest.durability_warning = self.durability_warning
        else:
            self.last_durability_status = DurabilityStatus.GUARANTEED
            self.durability_warning = None

        return final_path

    def update_stage(
        self,
        manifest: IngestionManifest,
        new_stage: ManifestStage | str,
        force: bool = False,
    ) -> None:
        self.transition_stage(manifest, new_stage, force=force)

    def read_manifest(self, ingestion_uuid: str) -> IngestionManifest | None:
        path = self.manifest_path_for(ingestion_uuid)
        if not path.is_file():
            return None
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return IngestionManifest(**data)

    def unlink_manifest(self, ingestion_uuid: str) -> bool:
        path = self.manifest_path_for(ingestion_uuid)
        synced = True
        if path.is_file():
            path.unlink(missing_ok=True)
            synced = sync_directory(self.manifests_dir, durability_mode=self.durability_mode)
        # Serialize cleanup and reclamation through the unified protocol:
        ManifestLock.reclaim_stale_locks(path)
        return synced
