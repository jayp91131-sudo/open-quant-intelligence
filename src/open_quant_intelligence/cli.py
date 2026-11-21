"""Command-line interface for Open Quant Intelligence."""

import argparse
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from open_quant_intelligence.data.timeframe import SupportedTimeframe
from open_quant_intelligence.data.validation import validate_candle_csv
from open_quant_intelligence.ingestion.pipeline import IngestionPipeline, IngestionRequest
from open_quant_intelligence.ingestion.time_utils import (
    datetime_to_epoch_ms,
    epoch_ms_to_datetime,
)
from open_quant_intelligence.storage.catalog import DuckDBCatalog
from open_quant_intelligence.storage.fingerprint import compute_file_sha256


def _parse_timestamp(ts_val: str) -> int:
    """Parse ISO-8601 string or numeric millisecond string into epoch milliseconds."""
    val = ts_val.strip()
    if val.isdigit():
        return int(val)
    if val.endswith("Z"):
        val = val[:-1] + "+00:00"
    dt = datetime.fromisoformat(val)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return datetime_to_epoch_ms(dt)


def build_parser() -> argparse.ArgumentParser:
    """Construct argument parser for the Open Quant Intelligence CLI."""
    parser = argparse.ArgumentParser(
        prog="open-quant",
        description="Open Quant Intelligence (V2) — Local Quantitative Research Foundation",
    )
    subparsers = parser.add_subparsers(dest="subcommand", help="Available subcommands")

    # 1. Validate subcommand (M0 legacy compatibility)
    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate an OHLCV candle CSV file against canonical data contracts.",
    )
    validate_parser.add_argument(
        "path",
        type=str,
        help="Path to the OHLCV CSV file to validate.",
    )
    validate_parser.add_argument(
        "--fail-on-gaps",
        action="store_true",
        default=False,
        help="Fail validation with non-zero exit code if any missing-candle intervals are detected.",
    )
    validate_parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        default=False,
        help="Suppress detailed output; only print error status.",
    )

    # 2. Ingest subcommand (M1)
    ingest_parser = subparsers.add_parser(
        "ingest",
        help="Ingest historical public market data into local canonical storage.",
    )
    ingest_parser.add_argument(
        "--exchange",
        type=str,
        default="binance",
        help="Target exchange identifier (default: binance).",
    )
    ingest_parser.add_argument(
        "--symbol",
        type=str,
        required=True,
        help="Market symbol pair (e.g. BTC/USDT).",
    )
    ingest_parser.add_argument(
        "--timeframe",
        type=str,
        required=True,
        choices=[tf.value for tf in SupportedTimeframe],
        help="Timeframe interval (e.g. 1m, 5m, 15m, 30m, 1h, 4h, 1d).",
    )
    ingest_parser.add_argument(
        "--since",
        type=str,
        required=True,
        help="Start timestamp (ISO-8601 or epoch ms).",
    )
    ingest_parser.add_argument(
        "--until",
        type=str,
        required=True,
        help="Exclusive end timestamp (ISO-8601 or epoch ms).",
    )
    ingest_parser.add_argument(
        "--output-dir",
        type=str,
        default="data",
        help="Base root data directory (default: data).",
    )
    ingest_parser.add_argument(
        "--fail-on-gaps",
        action="store_true",
        default=False,
        help="Exit with error if dataset contains any missing intervals.",
    )
    ingest_parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Force re-query exchange even if committed snapshot already exists.",
    )

    # 3. Datasets subcommand (M1)
    datasets_parser = subparsers.add_parser(
        "datasets",
        help="Inspect and verify local dataset catalog snapshots.",
    )
    datasets_sub = datasets_parser.add_subparsers(dest="datasets_subcommand", help="Dataset operations")

    list_parser = datasets_sub.add_parser("list", help="List registered dataset snapshots.")
    list_parser.add_argument("--symbol", type=str, default=None, help="Filter by symbol.")
    list_parser.add_argument("--timeframe", type=str, default=None, help="Filter by timeframe.")
    list_parser.add_argument("--status", type=str, default=None, help="Filter by quality status.")
    list_parser.add_argument("--db-path", type=str, default="data/catalog.duckdb", help="Path to catalog.duckdb.")

    inspect_parser = datasets_sub.add_parser("inspect", help="Inspect dataset details and gap events.")
    inspect_parser.add_argument("identifier", type=str, help="Snapshot ID, content fingerprint, or ingestion UUID.")
    inspect_parser.add_argument("--db-path", type=str, default="data/catalog.duckdb", help="Path to catalog.duckdb.")

    verify_parser = datasets_sub.add_parser("verify", help="Verify physical Parquet integrity against catalog.")
    verify_parser.add_argument("identifier", type=str, help="Snapshot ID, content fingerprint, or ingestion UUID.")
    verify_parser.add_argument("--db-path", type=str, default="data/catalog.duckdb", help="Path to catalog.duckdb.")

    return parser


def handle_validate(args: argparse.Namespace) -> int:
    """Handle validate subcommand."""
    target_path = Path(args.path)
    result = validate_candle_csv(target_path, fail_on_gaps=args.fail_on_gaps)

    if not args.quiet:
        print(result.summary())

    if result.is_valid:
        return 0
    else:
        if args.quiet:
            print(f"FAILED: {len(result.errors)} errors, {len(result.gaps)} gaps", file=sys.stderr)
        return 1


def handle_ingest(args: argparse.Namespace) -> int:
    """Handle ingest subcommand."""
    try:
        since_ms = _parse_timestamp(args.since)
        until_ms = _parse_timestamp(args.until)
        timeframe = SupportedTimeframe(args.timeframe)
    except Exception as e:
        print(f"Error parsing ingestion arguments: {e}", file=sys.stderr)
        return 1

    request = IngestionRequest(
        exchange_id=args.exchange,
        symbol=args.symbol,
        timeframe=timeframe,
        since_ms=since_ms,
        until_ms=until_ms,
        fail_on_gaps=args.fail_on_gaps,
        force=args.force,
    )

    pipeline = IngestionPipeline(output_dir=args.output_dir)

    try:
        res = pipeline.ingest(request)
    except Exception as err:
        print(f"Ingestion failed: {err}", file=sys.stderr)
        return 1

    req_since_str = epoch_ms_to_datetime(since_ms).isoformat()
    req_until_str = epoch_ms_to_datetime(until_ms).isoformat()
    first_open_str = res.actual_first_open.isoformat() if res.actual_first_open else "None"
    last_close_str = res.actual_last_close.isoformat() if res.actual_last_close else "None"
    missing = res.quality_report.missing_intervals

    print("=" * 60)
    print("Open Quant Intelligence — Historical Ingestion Report")
    print("=" * 60)
    print(f"Snapshot ID:           {res.snapshot_id}")
    print(f"Ingestion UUID:        {res.ingestion_uuid}")
    print(f"Exchange:              {res.manifest.exchange_id} (public vision cluster)")
    market_tf = f"Market: {res.manifest.market_type} | Timeframe: {res.manifest.timeframe}"
    print(f"Symbol:                {res.manifest.symbol} | {market_tf}")
    print(f"Requested Range (UTC): {req_since_str} -> {req_until_str}")
    print(f"Coverage Range (UTC):  {first_open_str} -> {last_close_str}")
    print(f"Expected Intervals:    {res.quality_report.expected_records}")
    print(f"Recorded Intervals:    {res.quality_report.record_count} ({missing} missing)")
    print(f"Ingestion Status:      {res.ingestion_status}")
    print(f"Quality Status:        {res.quality_report.status.value}")
    print(f"Content Fingerprint:   {res.content_fingerprint}")
    print(f"Parquet Artifact:      {res.parquet_artifact_path or 'None (EMPTY dataset)'}")
    print(f"Artifact SHA-256:      {res.parquet_artifact_sha256 or 'None (EMPTY dataset)'}")
    print(f"Catalog Record:        REGISTERED ({pipeline.catalog.db_path})")
    print("=" * 60)

    if args.fail_on_gaps and res.quality_report.status.value == "GAP_CONTAINING":
        return 1

    return 0


def handle_datasets(args: argparse.Namespace) -> int:
    """Handle datasets inspect/list/verify subcommands."""
    sub = args.datasets_subcommand
    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"Catalog database not found at {db_path}", file=sys.stderr)
        return 1

    catalog = DuckDBCatalog(db_path)

    if sub == "list":
        records = catalog.list_datasets(
            symbol=args.symbol,
            timeframe=args.timeframe,
            quality_status=args.status,
        )
        if not records:
            print("No datasets found matching criteria.")
            return 0

        print(f"{'Snapshot ID':<20} {'Symbol':<12} {'TF':<6} {'Quality':<16} {'Records':<10} {'Ingested At'}")
        print("-" * 80)
        for r in records:
            ingested_str = r.ingested_at.strftime("%Y-%m-%d %H:%M:%S")
            line = (
                f"{r.snapshot_id:<20} {r.symbol:<12} {r.timeframe:<6} "
                f"{r.quality_status:<16} {r.record_count:<10} {ingested_str}"
            )
            print(line)
        return 0

    elif sub == "inspect":
        record = catalog.find_by_id_or_fingerprint(args.identifier)
        if not record:
            print(f"Dataset '{args.identifier}' not found in catalog.", file=sys.stderr)
            return 1

        gaps = catalog.get_gaps(record.snapshot_id)
        print("=" * 60)
        print(f"Dataset Snapshot: {record.snapshot_id}")
        print("=" * 60)
        print(f"Identity:            {record.dataset_identity}")
        print(f"Exchange / Market:   {record.exchange_id} / {record.market_type}")
        print(f"Symbol / Timeframe:  {record.symbol} / {record.timeframe}")
        print(f"Requested Window:    {record.requested_since.isoformat()} -> {record.requested_until.isoformat()}")
        print(f"Coverage Window:     {record.actual_first_open} -> {record.actual_last_close}")
        print(f"Expected Records:    {record.expected_records}")
        print(f"Actual Records:      {record.record_count}")
        print(f"Missing Intervals:   {record.missing_intervals}")
        print(f"Quality Status:      {record.quality_status}")
        print(f"Content SHA-256:     {record.content_fingerprint_sha256}")
        print(f"Parquet Path:        {record.file_path or 'None'}")
        print(f"Parquet SHA-256:     {record.file_sha256 or 'None'}")
        print(f"Parquet Size:        {record.file_size_bytes or 0} bytes")
        print(f"CCXT Version:        {record.ccxt_version}")
        print(f"Ingested At:         {record.ingested_at.isoformat()}")

        if gaps:
            print("-" * 60)
            print(f"Gap Events ({len(gaps)} total):")
            for g in gaps:
                exp_s = g["expected_open"]
                sub_s = g["subsequent_open"]
                cnt = g["missing_count"]
                print(f"  [{g['gap_type']}] expected={exp_s} subsequent={sub_s} missing={cnt}")
        print("=" * 60)
        return 0

    elif sub == "verify":
        record = catalog.find_by_id_or_fingerprint(args.identifier)
        if not record:
            print(f"Dataset '{args.identifier}' not found in catalog.", file=sys.stderr)
            return 1

        verification_id = f"ver_{uuid.uuid4().hex[:12]}"

        if record.quality_status == "EMPTY":
            catalog.record_verification(
                verification_id=verification_id,
                snapshot_id=record.snapshot_id,
                result="VERIFIED_EMPTY",
                computed_file_sha256=None,
            )
            print(f"VERIFIED: Dataset {record.snapshot_id} is an immutable EMPTY snapshot (no physical file expected).")
            return 0

        if not record.file_path:
            catalog.record_verification(
                verification_id=verification_id,
                snapshot_id=record.snapshot_id,
                result="FAILED_NO_PATH",
                error_message="Catalog record missing file_path",
            )
            print(f"FAILED: Snapshot {record.snapshot_id} missing file path in catalog.", file=sys.stderr)
            return 1

        file_path = Path(record.file_path)
        if not file_path.exists():
            catalog.record_verification(
                verification_id=verification_id,
                snapshot_id=record.snapshot_id,
                result="FAILED_FILE_MISSING",
                error_message=f"File not found on disk: {file_path}",
            )
            print(f"FAILED: Parquet file {file_path} does not exist.", file=sys.stderr)
            return 1

        computed_sha = compute_file_sha256(file_path)
        if computed_sha == record.file_sha256:
            catalog.record_verification(
                verification_id=verification_id,
                snapshot_id=record.snapshot_id,
                result="VERIFIED_MATCH",
                computed_file_sha256=computed_sha,
            )
            print(f"VERIFIED: Parquet artifact checksum matches catalog ({computed_sha}).")
            return 0
        else:
            catalog.record_verification(
                verification_id=verification_id,
                snapshot_id=record.snapshot_id,
                result="FAILED_HASH_MISMATCH",
                computed_file_sha256=computed_sha,
                error_message=f"Computed {computed_sha} != expected {record.file_sha256}",
            )
            print(f"FAILED: Checksum mismatch! Disk={computed_sha}, Catalog={record.file_sha256}", file=sys.stderr)
            return 1

    return 1


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint function."""
    if argv is None:
        argv = sys.argv[1:]

    parser = build_parser()
    if argv and argv[0] not in ("validate", "ingest", "datasets", "-h", "--help"):
        # Synthesize validate subcommand for backward compatibility
        argv = ["validate"] + argv

    args = parser.parse_args(argv)

    if args.subcommand == "validate":
        return handle_validate(args)
    elif args.subcommand == "ingest":
        return handle_ingest(args)
    elif args.subcommand == "datasets":
        return handle_datasets(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
