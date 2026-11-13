"""Command-line interface for Open Quant Intelligence."""

import argparse
import sys
from pathlib import Path

from open_quant_intelligence.data.validation import validate_candle_csv


def build_parser() -> argparse.ArgumentParser:
    """Construct argument parser for the Open Quant Intelligence CLI."""
    parser = argparse.ArgumentParser(
        prog="open-quant",
        description="Open Quant Intelligence (V2 M0) — Local Quantitative Research Foundation",
    )
    subparsers = parser.add_subparsers(dest="subcommand", help="Available subcommands")

    # Validate subcommand
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

    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint function.

    Returns:
        0 on success (valid data).
        1 on failure (validation error, malformed input, missing file).
    """
    if argv is None:
        argv = sys.argv[1:]

    # If the user directly calls `open-quant-validate file.csv` or `open-quant file.csv`
    # without explicitly typing `validate`, provide a convenient fallback
    parser = build_parser()
    if argv and argv[0] not in ("validate", "-h", "--help"):
        # Synthesize validate subcommand
        argv = ["validate"] + argv

    args = parser.parse_args(argv)

    if args.subcommand == "validate":
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

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
