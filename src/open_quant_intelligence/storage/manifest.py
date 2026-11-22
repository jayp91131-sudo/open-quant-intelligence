"""Durable ingestion manifest management and 8-state machine transitions."""

import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any


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
    content_fingerprint_sha256: str = ""
    file_sha256: str = ""
    lease_owner_pid: int = 0
    lease_expires_at_utc: str = ""
    schema_version: int = 1
    ccxt_version: str = "4.4.52"
    created_at_utc: str = ""
    updated_at_utc: str = ""

    def __post_init__(self) -> None:
        if isinstance(self.stage, ManifestStage):
            self.stage = self.stage.value
        if self.requested_since_ms == 0 and self.since_ms != 0:
            self.requested_since_ms = self.since_ms
        if self.requested_until_ms == 0 and self.until_ms != 0:
            self.requested_until_ms = self.until_ms

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

    def __init__(self, manifests_dir: Path | str) -> None:
        self.manifests_dir = Path(manifests_dir)
        self.manifests_dir.mkdir(parents=True, exist_ok=True)

    def manifest_path_for(self, ingestion_uuid: str) -> Path:
        return self.manifests_dir / f".ingest_manifest_{ingestion_uuid}.json"

    def save_manifest(self, manifest: IngestionManifest) -> Path:
        return self.write_manifest(manifest)

    def transition_stage(
        self,
        manifest: IngestionManifest,
        new_stage: ManifestStage | str,
        **kwargs: Any,
    ) -> IngestionManifest:
        stage_val = new_stage.value if isinstance(new_stage, ManifestStage) else str(new_stage)
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
            if not p.name.endswith(".tmp"):
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
