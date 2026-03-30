from __future__ import annotations

import argparse
import logging
from pathlib import Path

from photo_archive.database import DuckDBStore
from photo_archive.pipeline import run_pipeline
from photo_archive.progress import ProgressPrinter
from photo_archive.reporting import format_cli_report, format_run_summary


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
        "--full-rescan",
        action="store_true",
        help="Re-extract metadata for all supported files, including unchanged files",
    )
    scan_parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    scan_parser.add_argument(
        "--quiet-progress",
        action="store_true",
        help="Disable structured progress prints during pipeline execution",
    )

    report_parser = subparsers.add_parser("report", help="Show scan report from DuckDB")
    report_parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/db/photo_archive.duckdb"),
        help="DuckDB file path",
    )
    report_parser.add_argument(
        "--scan-id",
        type=str,
        default=None,
        help="Optional scan_id; defaults to latest scan",
    )
    report_parser.add_argument(
        "--failed-limit",
        type=int,
        default=50,
        help="Maximum number of failed files to show",
    )
    report_parser.add_argument(
        "--coverage-sort",
        choices=["asc", "desc"],
        default="asc",
        help="Sort non-null coverage by percentage (asc=least populated first)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 1

    if args.command == "report":
        store = DuckDBStore(db_path=args.db_path)
        scan = store.get_scan_history(scan_id=args.scan_id)
        if scan is None:
            print("No scan history found in the selected DuckDB file.")
            return 1

        unsupported_extensions = store.get_unsupported_extension_counts(scan.scan_id)
        failed_files = store.get_failed_files(scan.scan_id, limit=max(1, int(args.failed_limit)))
        coverage_total_rows, coverage_rows = store.get_column_non_null_coverage(
            scan.scan_id,
            sort_order=args.coverage_sort,
        )
        print(
            format_cli_report(
                scan=scan,
                unsupported_extensions=unsupported_extensions,
                failed_files=failed_files,
                coverage_rows=coverage_rows,
                coverage_total_rows=coverage_total_rows,
                failed_limit=max(1, int(args.failed_limit)),
            )
        )
        return 0

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
        full_rescan=args.full_rescan,
        progress=ProgressPrinter(enabled=not args.quiet_progress),
    )
    print(format_run_summary(result.summary))
    if args.dry_run:
        print("Dry run enabled: no DuckDB writes or exports were performed.")
    else:
        print(f"Scan ID: {result.scan_id}")
        print(f"DuckDB path: {result.db_path}")
        if result.export_path:
            print(f"Export path: {result.export_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
