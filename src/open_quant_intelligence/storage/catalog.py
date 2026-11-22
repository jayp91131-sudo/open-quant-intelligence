"""DuckDB immutable dataset catalog, edge gap recording, and verification history."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Sequence

import duckdb

from open_quant_intelligence.data.quality import QualityGapEvent


@dataclass(frozen=True)
class CatalogDatasetRecord:
    """Immutable catalog record representing a registered dataset snapshot."""

    snapshot_id: str
    dataset_identity: str
    exchange_id: str
    market_type: str
    symbol: str
    timeframe: str
    requested_since: datetime
    requested_until: datetime
    actual_first_open: datetime | None
    actual_last_close: datetime | None
    expected_records: int
    record_count: int
    gap_count: int
    missing_intervals: int
    content_fingerprint_sha256: str
    file_sha256: str | None
    file_path: str | None
    file_size_bytes: int | None
    ccxt_version: str
    ingestion_uuid: str
    ingested_at: datetime
    ingestion_status: str
    quality_status: str


class DuckDBCatalog:
    """Analytical SQL catalog managing dataset snapshots, gap statistics, and verification logs."""

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.db_path))

    def _init_schema(self) -> None:
        with self._get_connection() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS datasets (
                    snapshot_id VARCHAR PRIMARY KEY,
                    dataset_identity VARCHAR NOT NULL,
                    exchange_id VARCHAR NOT NULL,
                    market_type VARCHAR NOT NULL,
                    symbol VARCHAR NOT NULL,
                    timeframe VARCHAR NOT NULL,
                    requested_since TIMESTAMP NOT NULL,
                    requested_until TIMESTAMP NOT NULL,
                    actual_first_open TIMESTAMP,
                    actual_last_close TIMESTAMP,
                    expected_records BIGINT NOT NULL,
                    record_count BIGINT NOT NULL,
                    gap_count INTEGER NOT NULL,
                    missing_intervals BIGINT NOT NULL,
                    content_fingerprint_sha256 VARCHAR NOT NULL,
                    file_sha256 VARCHAR,
                    file_path VARCHAR,
                    file_size_bytes BIGINT,
                    ccxt_version VARCHAR NOT NULL,
                    ingestion_uuid VARCHAR NOT NULL,
                    ingested_at TIMESTAMP NOT NULL,
                    ingestion_status VARCHAR NOT NULL,
                    quality_status VARCHAR NOT NULL
                );

                CREATE TABLE IF NOT EXISTS dataset_gaps (
                    gap_id VARCHAR PRIMARY KEY,
                    snapshot_id VARCHAR REFERENCES datasets(snapshot_id),
                    gap_type VARCHAR NOT NULL,
                    expected_open TIMESTAMP NOT NULL,
                    subsequent_open TIMESTAMP,
                    missing_count BIGINT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS dataset_verification_history (
                    verification_id VARCHAR PRIMARY KEY,
                    snapshot_id VARCHAR REFERENCES datasets(snapshot_id),
                    verified_at TIMESTAMP NOT NULL,
                    verification_result VARCHAR NOT NULL,
                    computed_file_sha256 VARCHAR,
                    error_message VARCHAR
                );

                CREATE INDEX IF NOT EXISTS idx_datasets_identity
                ON datasets(dataset_identity, ingested_at DESC);
                """
            )

    def find_latest_committed(self, dataset_identity: str) -> CatalogDatasetRecord | None:
        with self._get_connection() as con:
            res = con.execute(
                """
                SELECT
                    snapshot_id, dataset_identity, exchange_id, market_type, symbol, timeframe,
                    requested_since, requested_until, actual_first_open, actual_last_close,
                    expected_records, record_count, gap_count, missing_intervals,
                    content_fingerprint_sha256, file_sha256, file_path, file_size_bytes,
                    ccxt_version, ingestion_uuid, ingested_at, ingestion_status, quality_status
                FROM datasets
                WHERE dataset_identity = ? AND ingestion_status = 'COMMITTED'
                ORDER BY ingested_at DESC
                LIMIT 1
                """,
                [dataset_identity],
            ).fetchone()

            if not res:
                return None

            return CatalogDatasetRecord(*res)

    def find_by_uuid(self, ingestion_uuid: str) -> CatalogDatasetRecord | None:
        with self._get_connection() as con:
            res = con.execute(
                """
                SELECT
                    snapshot_id, dataset_identity, exchange_id, market_type, symbol, timeframe,
                    requested_since, requested_until, actual_first_open, actual_last_close,
                    expected_records, record_count, gap_count, missing_intervals,
                    content_fingerprint_sha256, file_sha256, file_path, file_size_bytes,
                    ccxt_version, ingestion_uuid, ingested_at, ingestion_status, quality_status
                FROM datasets
                WHERE ingestion_uuid = ?
                LIMIT 1
                """,
                [ingestion_uuid],
            ).fetchone()

            if not res:
                return None

            return CatalogDatasetRecord(*res)

    def find_by_id_or_fingerprint(self, identifier: str) -> CatalogDatasetRecord | None:
        with self._get_connection() as con:
            res = con.execute(
                """
                SELECT
                    snapshot_id, dataset_identity, exchange_id, market_type, symbol, timeframe,
                    requested_since, requested_until, actual_first_open, actual_last_close,
                    expected_records, record_count, gap_count, missing_intervals,
                    content_fingerprint_sha256, file_sha256, file_path, file_size_bytes,
                    ccxt_version, ingestion_uuid, ingested_at, ingestion_status, quality_status
                FROM datasets
                WHERE snapshot_id = ?
                   OR content_fingerprint_sha256 = ?
                   OR content_fingerprint_sha256 = ('sha256:' || ?)
                   OR ingestion_uuid = ?
                   OR symbol = ?
                   OR dataset_identity = ?
                ORDER BY ingested_at DESC
                LIMIT 1
                """,
                [identifier, identifier, identifier, identifier, identifier, identifier],
            ).fetchone()

            if not res:
                return None

            return CatalogDatasetRecord(*res)

    def list_datasets(
        self,
        symbol: str | None = None,
        timeframe: str | None = None,
        quality_status: str | None = None,
    ) -> list[CatalogDatasetRecord]:
        query = """
            SELECT
                snapshot_id, dataset_identity, exchange_id, market_type, symbol, timeframe,
                requested_since, requested_until, actual_first_open, actual_last_close,
                expected_records, record_count, gap_count, missing_intervals,
                content_fingerprint_sha256, file_sha256, file_path, file_size_bytes,
                ccxt_version, ingestion_uuid, ingested_at, ingestion_status, quality_status
            FROM datasets
            WHERE 1=1
        """
        params: list[str] = []
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
        if timeframe:
            query += " AND timeframe = ?"
            params.append(timeframe)
        if quality_status:
            query += " AND quality_status = ?"
            params.append(quality_status)

        query += " ORDER BY ingested_at DESC"

        with self._get_connection() as con:
            rows = con.execute(query, params).fetchall()
            return [CatalogDatasetRecord(*row) for row in rows]

    def get_gaps(self, snapshot_id: str) -> list[dict]:
        with self._get_connection() as con:
            rows = con.execute(
                """
                SELECT gap_id, gap_type, expected_open, subsequent_open, missing_count
                FROM dataset_gaps
                WHERE snapshot_id = ?
                ORDER BY expected_open ASC
                """,
                [snapshot_id],
            ).fetchall()
            return [
                {
                    "gap_id": r[0],
                    "gap_type": r[1],
                    "expected_open": r[2],
                    "subsequent_open": r[3],
                    "missing_count": r[4],
                }
                for r in rows
            ]

    def commit_dataset_snapshot(
        self,
        record: CatalogDatasetRecord,
        gaps: Sequence[QualityGapEvent] = (),
    ) -> None:
        with self._get_connection() as con:
            con.begin()
            try:
                con.execute(
                    """
                    INSERT INTO datasets (
                        snapshot_id, dataset_identity, exchange_id, market_type, symbol, timeframe,
                        requested_since, requested_until, actual_first_open, actual_last_close,
                        expected_records, record_count, gap_count, missing_intervals,
                        content_fingerprint_sha256, file_sha256, file_path, file_size_bytes,
                        ccxt_version, ingestion_uuid, ingested_at, ingestion_status, quality_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        record.snapshot_id,
                        record.dataset_identity,
                        record.exchange_id,
                        record.market_type,
                        record.symbol,
                        record.timeframe,
                        record.requested_since,
                        record.requested_until,
                        record.actual_first_open,
                        record.actual_last_close,
                        record.expected_records,
                        record.record_count,
                        record.gap_count,
                        record.missing_intervals,
                        record.content_fingerprint_sha256,
                        record.file_sha256,
                        record.file_path,
                        record.file_size_bytes,
                        record.ccxt_version,
                        record.ingestion_uuid,
                        record.ingested_at,
                        record.ingestion_status,
                        record.quality_status,
                    ],
                )

                for idx, gap in enumerate(gaps):
                    gap_id = f"gap_{record.snapshot_id}_{idx}"
                    con.execute(
                        """
                        INSERT INTO dataset_gaps (
                            gap_id, snapshot_id, gap_type, expected_open, subsequent_open, missing_count
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        [
                            gap_id,
                            record.snapshot_id,
                            gap.gap_type.value,
                            gap.expected_open_utc,
                            gap.subsequent_open_utc,
                            gap.missing_count,
                        ],
                    )

                con.commit()
            except Exception:
                con.rollback()
                raise

    def record_verification(
        self,
        verification_id: str,
        snapshot_id: str,
        result: str,
        computed_file_sha256: str | None = None,
        error_message: str | None = None,
    ) -> None:
        with self._get_connection() as con:
            con.execute(
                """
                INSERT INTO dataset_verification_history (
                    verification_id, snapshot_id, verified_at, verification_result, computed_file_sha256, error_message
                ) VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?)
                """,
                [verification_id, snapshot_id, result, computed_file_sha256, error_message],
            )
