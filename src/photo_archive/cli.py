from __future__ import annotations

import argparse
import logging
from pathlib import Path

from photo_archive.pipeline import run_pipeline
from photo_archive.reporting import format_run_summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="photo-archive",
        description="Phase 1 local-first photo metadata ingestion pipeline",
    )
    subparsers = parser.add_subparsers(dest="command")

    scan_parser = subparsers.add_parser("scan", help="Scan and index a root folder")
    scan_parser.add_argument("root_path", type=Path, help="Root folder to scan recursively")
    scan_parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/db/photo_archive.duckdb"),
        help="DuckDB file output path",
    )
    scan_parser.add_argument(
        "--export-path",
        type=Path,
        default=None,
        help="Optional export output (.csv or .parquet)",
    )
    scan_parser.add_argument(
        "--extension",
        action="append",
        default=None,
        help="Supported extension filter. Repeat for multiple values.",
    )
    scan_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan only, skip extraction and persistence",
    )
    scan_parser.add_argument(
        "--exif-batch-size",
        type=int,
        default=200,
        help="Number of files per ExifTool batch call",
    )
    scan_parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command != "scan":
        parser.print_help()
        return 1

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    result = run_pipeline(
        root_path=args.root_path,
        db_path=args.db_path,
        export_path=args.export_path,
        extensions=args.extension,
        dry_run=args.dry_run,
        exif_batch_size=args.exif_batch_size,
    )
    print(format_run_summary(result.summary))
    if args.dry_run:
        print("Dry run enabled: no DuckDB writes or exports were performed.")
    else:
        print(f"DuckDB path: {result.db_path}")
        if result.export_path:
            print(f"Export path: {result.export_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
