"""Durable ingestion manifest management and 8-state machine transitions."""

import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path


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


@dataclass
class IngestionManifest:
    """Durable state record for an in-flight or committed ingestion job."""

    ingestion_uuid: str
    lease_owner_pid: int
    lease_expires_at_utc: str
    schema_version: int
    ccxt_version: str
    exchange_id: str
    market_type: str
    symbol: str
    timeframe: str
    requested_since_utc: str
    requested_until_utc: str
    requested_since_ms: int
    requested_until_ms: int
    staging_file_path: str
    target_file_path: str
    expected_records: int
    record_count: int
    missing_intervals: int
    content_fingerprint_sha256: str
    file_sha256: str
    stage: str
    created_at_utc: str
    updated_at_utc: str


class ManifestManager:
    """Manages atomic creation, update, and durability of ingestion manifests."""

    def __init__(self, manifests_dir: Path | str) -> None:
        self.manifests_dir = Path(manifests_dir)
        self.manifests_dir.mkdir(parents=True, exist_ok=True)

    def manifest_path_for(self, ingestion_uuid: str) -> Path:
        return self.manifests_dir / f".ingest_manifest_{ingestion_uuid}.json"

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
        if sys.platform != "win32":
            try:
                dir_fd = os.open(str(self.manifests_dir), os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:
                pass

        return final_path

    def update_stage(self, manifest: IngestionManifest, new_stage: ManifestStage) -> None:
        manifest.stage = new_stage.value
        manifest.updated_at_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        self.write_manifest(manifest)

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
            if sys.platform != "win32":
                try:
                    dir_fd = os.open(str(self.manifests_dir), os.O_RDONLY)
                    try:
                        os.fsync(dir_fd)
                    finally:
                        os.close(dir_fd)
                except OSError:
                    pass
