from __future__ import annotations

import argparse
import logging
from pathlib import Path

from photo_archive.backup_audit import format_backup_audit_summary, run_backup_audit
from photo_archive.database import DuckDBStore
from photo_archive.pipeline import run_pipeline
from photo_archive.progress import ProgressPrinter
from photo_archive.reporting import format_cli_report, format_run_summary
from photo_archive.frame_pipeline import format_video_frame_summary, run_video_frame_pipeline
from photo_archive.thumbnail_pipeline import format_thumbnail_summary, run_thumbnail_pipeline


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

    thumbs_parser = subparsers.add_parser(
        "thumbs",
        help="Generate thumbnails from indexed files",
    )
    thumbs_parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/db/photo_archive.duckdb"),
        help="DuckDB file path",
    )
    thumbs_parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/thumbnails"),
        help="Thumbnail output directory",
    )
    thumbs_parser.add_argument(
        "--max-size",
        type=int,
        default=512,
        help="Maximum thumbnail width/height in pixels",
    )
    thumbs_parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    thumbs_parser.add_argument(
        "--quiet-progress",
        action="store_true",
        help="Disable structured progress prints during thumbnail generation",
    )

    frames_parser = subparsers.add_parser(
        "frames",
        help="Generate sampled video frames from indexed videos",
    )
    frames_parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/db/photo_archive.duckdb"),
        help="DuckDB file path",
    )
    frames_parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/frames"),
        help="Video frame output directory",
    )
    frames_parser.add_argument(
        "--interval-sec",
        type=float,
        default=10.0,
        help="Sampling interval in seconds between extracted frames",
    )
    frames_parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    frames_parser.add_argument(
        "--quiet-progress",
        action="store_true",
        help="Disable structured progress prints during frame generation",
    )

    backup_audit_parser = subparsers.add_parser(
        "backup-audit",
        help="Compare primary and backup scan roots and list primary files missing in backup",
    )
    backup_audit_parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/db/photo_archive.duckdb"),
        help="DuckDB file path",
    )
    backup_audit_parser.add_argument(
        "--primary-root",
        type=Path,
        required=True,
        help="Primary scan root to audit",
    )
    backup_audit_parser.add_argument(
        "--backup-root",
        type=Path,
        required=True,
        help="Backup scan root to compare against",
    )
    backup_audit_parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum missing files to show",
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
        thumbnail_statuses = store.get_thumbnail_status_counts()
        failed_thumbnails = store.get_failed_thumbnails(limit=max(1, int(args.failed_limit)))
        video_frame_statuses = store.get_video_frame_status_counts()
        failed_video_frames = store.get_failed_video_frames(limit=max(1, int(args.failed_limit)))
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
                thumbnail_statuses=thumbnail_statuses,
                failed_thumbnails=failed_thumbnails,
                video_frame_statuses=video_frame_statuses,
                failed_video_frames=failed_video_frames,
            )
        )
        return 0

    if args.command == "thumbs":
        logging.basicConfig(
            level=getattr(logging, args.log_level),
            format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        )
        result = run_thumbnail_pipeline(
            db_path=args.db_path,
            out_dir=args.out_dir,
            max_size=max(1, int(args.max_size)),
            progress=ProgressPrinter(enabled=not args.quiet_progress),
        )
        print(format_thumbnail_summary(result.summary))
        print(f"DuckDB path: {result.db_path}")
        print(f"Thumbnail output dir: {result.out_dir}")
        return 0

    if args.command == "frames":
        logging.basicConfig(
            level=getattr(logging, args.log_level),
            format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        )
        result = run_video_frame_pipeline(
            db_path=args.db_path,
            out_dir=args.out_dir,
            interval_sec=max(0.1, float(args.interval_sec)),
            progress=ProgressPrinter(enabled=not args.quiet_progress),
        )
        print(format_video_frame_summary(result.summary))
        print(f"DuckDB path: {result.db_path}")
        print(f"Frame output dir: {result.out_dir}")
        return 0

    if args.command == "backup-audit":
        store = DuckDBStore(db_path=args.db_path)
        store.initialize()
        primary_root = args.primary_root.expanduser().resolve()
        backup_root = args.backup_root.expanduser().resolve()
        primary_scan_id = store.get_latest_scan_id_for_root(str(primary_root))
        if primary_scan_id is None:
            print(f"No scan history found for primary root: {primary_root}")
            return 1
        backup_scan_id = store.get_latest_scan_id_for_root(str(backup_root))
        if backup_scan_id is None:
            print(f"No scan history found for backup root: {backup_root}")
            return 1

        primary_files = store.load_active_files_for_scan(
            scan_id=primary_scan_id,
            scan_root=str(primary_root),
        )
        backup_files = store.load_active_files_for_scan(
            scan_id=backup_scan_id,
            scan_root=str(backup_root),
        )
        result = run_backup_audit(
            primary_scan_id=primary_scan_id,
            backup_scan_id=backup_scan_id,
            primary_root=primary_root,
            backup_root=backup_root,
            primary_files=primary_files,
            backup_files=backup_files,
            limit=max(1, int(args.limit)),
        )
        print(
            format_backup_audit_summary(
                result,
                primary_root=primary_root,
                backup_root=backup_root,
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
