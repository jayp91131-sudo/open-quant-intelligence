"""Durable ingestion manifest management and 8-state machine transitions."""

import json
import os
import sys
import time
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
    """Check whether a process with the given PID is actively running."""
    if pid <= 0:
        return False
    if sys.platform == "win32":
        import ctypes

        kernel32 = ctypes.windll.kernel32
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        SYNCHRONIZE = 0x00100000
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE, False, pid)
        if not handle:
            return False
        STILL_ACTIVE = 259
        exit_code = ctypes.c_ulong()
        try:
            if kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return exit_code.value == STILL_ACTIVE
            return False
        finally:
            kernel32.CloseHandle(handle)
    else:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False


def is_lease_active(manifest: "IngestionManifest") -> bool:
    """Determine if a manifest's lease is actively held by a live process."""
    if not manifest.lease_owner_pid:
        return False
    if manifest.lease_expires_at_utc:
        try:
            exp_time = time.strptime(manifest.lease_expires_at_utc, "%Y-%m-%dT%H:%M:%SZ")
            now_time = time.gmtime()
            if time.mktime(now_time) > time.mktime(exp_time):
                return False
        except (ValueError, TypeError):
            pass
    return is_pid_alive(manifest.lease_owner_pid)


class ManifestLock:
    """Atomic single-writer file lock for manifest mutation and recovery."""

    def __init__(self, manifest_path: Path, timeout_seconds: float = 5.0) -> None:
        self.lock_path = manifest_path.with_suffix(".lock")
        self.timeout_seconds = timeout_seconds

    def acquire(self) -> None:
        start_time = time.time()
        while True:
            try:
                flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
                fd = os.open(str(self.lock_path), flags)
                payload = json.dumps({"pid": os.getpid(), "time": time.time()}).encode("utf-8")
                os.write(fd, payload)
                os.close(fd)
                return
            except FileExistsError:
                try:
                    with open(self.lock_path, "r", encoding="utf-8") as f:
                        lock_data = json.load(f)
                    owner_pid = lock_data.get("pid", 0)
                    if not is_pid_alive(owner_pid):
                        self.lock_path.unlink(missing_ok=True)
                        continue
                except Exception:
                    pass

                if time.time() - start_time > self.timeout_seconds:
                    raise ManifestLockError(
                        f"Could not acquire atomic lock on {self.lock_path} within {self.timeout_seconds}s."
                    ) from None
                time.sleep(0.05)

    def release(self) -> None:
        self.lock_path.unlink(missing_ok=True)

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

    On POSIX, open directory with os.O_RDONLY and call os.fsync(dir_fd).
    On Windows, directory changes are journaled by NTFS/FAT transactions.

    Args:
        dir_path: Path to directory to synchronize.
        raise_on_failure: Legacy flag; if True, raises DurabilityError on failure.
        durability_mode: DurabilityMode.STRICT (raises on failure) or
            DurabilityMode.DEGRADED_ALLOWED (returns False on failure).

    Returns:
        True if synchronization succeeded or was a no-op on Windows.
        False if directory sync failed on POSIX.
    """
    strict = (
        raise_on_failure
        if raise_on_failure is not None
        else (durability_mode == DurabilityMode.STRICT or durability_mode == DurabilityMode.STRICT.value)
    )

    if sys.platform == "win32":
        return True

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
        os.replace(tmp_path, final_path)

        # Directory sync on POSIX
        synced = sync_directory(self.manifests_dir, durability_mode=self.durability_mode)
        if not synced:
            self.last_durability_status = DurabilityStatus.DEGRADED
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

    def unlink_manifest(self, ingestion_uuid: str) -> None:
        path = self.manifest_path_for(ingestion_uuid)
        if path.is_file():
            path.unlink(missing_ok=True)
            sync_directory(self.manifests_dir, durability_mode=self.durability_mode)
        lock_path = path.with_suffix(".lock")
        if lock_path.is_file():
            lock_path.unlink(missing_ok=True)
