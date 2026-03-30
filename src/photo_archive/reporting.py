from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, Sequence

from photo_archive.models import (
    ColumnCoverageRecord,
    ExtensionCountRecord,
    FailedThumbnailRecord,
    FailedFileRecord,
    NormalizedRecord,
    ScanHistoryRecord,
    ThumbnailStatusCountRecord,
)


def build_run_summary(
    records: Sequence[NormalizedRecord],
    *,
    new_files: int = 0,
    changed_files: int = 0,
    unchanged_files: int = 0,
    missing_files: int = 0,
    extraction_attempted: int = 0,
    extraction_successful: int = 0,
    extraction_failed: int = 0,
    full_rescan: bool = False,
    scan_duration_seconds: float = 0.0,
    state_duration_seconds: float = 0.0,
    parse_duration_seconds: float = 0.0,
    extraction_duration_seconds: float = 0.0,
    normalize_duration_seconds: float = 0.0,
    persist_duration_seconds: float = 0.0,
    persist_upserted: int = 0,
    persist_touched_unchanged: int = 0,
    image_extraction_attempted: int = 0,
    image_extraction_successful: int = 0,
    image_extraction_failed: int = 0,
    video_extraction_attempted: int = 0,
    video_extraction_successful: int = 0,
    video_extraction_failed: int = 0,
) -> dict[str, Any]:
    files_discovered = len(records)
    supported_records = [record for record in records if record.is_supported]
    supported_files = len(supported_records)

    success_statuses = {"success", "success_cached"}
    failed_statuses = {"failed", "failed_cached"}

    successful_extractions = sum(
        1 for record in supported_records if record.extract_status in success_statuses
    )
    failed_extractions = sum(
        1 for record in supported_records if record.extract_status in failed_statuses
    )

    with_captured_at = sum(1 for record in supported_records if record.captured_at is not None)
    with_gps = sum(
        1
        for record in supported_records
        if record.gps_lat is not None and record.gps_lon is not None
    )
    with_camera_model = sum(
        1 for record in supported_records if record.camera_model not in (None, "")
    )

    extension_counter = Counter(record.extension or "" for record in records)
    error_counter = Counter(
        record.extract_error for record in supported_records if record.extract_error
    )

    return {
        "files_discovered": files_discovered,
        "supported_files": supported_files,
        "successful_extractions": successful_extractions,
        "failed_extractions": failed_extractions,
        "percent_with_captured_at": _percent(with_captured_at, supported_files),
        "percent_with_gps": _percent(with_gps, supported_files),
        "percent_with_camera_model": _percent(with_camera_model, supported_files),
        "top_extensions": extension_counter.most_common(10),
        "common_errors": error_counter.most_common(10),
        "new_files": new_files,
        "changed_files": changed_files,
        "unchanged_files": unchanged_files,
        "missing_files": missing_files,
        "extraction_attempted": extraction_attempted,
        "extraction_successful": extraction_successful,
        "extraction_failed": extraction_failed,
        "full_rescan": full_rescan,
        "scan_duration_seconds": scan_duration_seconds,
        "state_duration_seconds": state_duration_seconds,
        "parse_duration_seconds": parse_duration_seconds,
        "extraction_duration_seconds": extraction_duration_seconds,
        "normalize_duration_seconds": normalize_duration_seconds,
        "persist_duration_seconds": persist_duration_seconds,
        "persist_upserted": persist_upserted,
        "persist_touched_unchanged": persist_touched_unchanged,
        "image_extraction_attempted": image_extraction_attempted,
        "image_extraction_successful": image_extraction_successful,
        "image_extraction_failed": image_extraction_failed,
        "video_extraction_attempted": video_extraction_attempted,
        "video_extraction_successful": video_extraction_successful,
        "video_extraction_failed": video_extraction_failed,
    }


def format_run_summary(summary: dict[str, Any]) -> str:
    lines = [
        f"Files discovered: {summary['files_discovered']}",
        f"Supported files: {summary['supported_files']}",
        f"Successful extractions: {summary['successful_extractions']}",
        f"Failed extractions: {summary['failed_extractions']}",
        f"New files: {summary.get('new_files', 0)}",
        f"Changed files: {summary.get('changed_files', 0)}",
        f"Unchanged files: {summary.get('unchanged_files', 0)}",
        f"Missing files: {summary.get('missing_files', 0)}",
        f"Percent with captured_at: {summary['percent_with_captured_at']:.1f}%",
        f"Percent with GPS: {summary['percent_with_gps']:.1f}%",
        f"Percent with camera_model: {summary['percent_with_camera_model']:.1f}%",
    ]

    if "run_duration_seconds" in summary:
        lines.append(f"Run duration: {_format_duration(summary['run_duration_seconds'])}")
        lines.append(
            "Stage times:"
            f" scan={_format_duration(summary.get('scan_duration_seconds'))},"
            f" state={_format_duration(summary.get('state_duration_seconds'))},"
            f" parse={_format_duration(summary.get('parse_duration_seconds'))},"
            f" extract={_format_duration(summary.get('extraction_duration_seconds'))},"
            f" normalize={_format_duration(summary.get('normalize_duration_seconds'))},"
            f" persist={_format_duration(summary.get('persist_duration_seconds'))}"
        )
        lines.append(
            "Persist ops:"
            f" upserted={summary.get('persist_upserted', 0)},"
            f" touched_unchanged={summary.get('persist_touched_unchanged', 0)}"
        )

    if "extraction_attempted" in summary:
        lines.append(f"Extraction attempts this run: {summary['extraction_attempted']}")
        lines.append(f"Extraction successes this run: {summary.get('extraction_successful', 0)}")
        lines.append(f"Extraction failures this run: {summary.get('extraction_failed', 0)}")
        lines.append(
            "Image extraction this run: "
            f"attempted={summary.get('image_extraction_attempted', 0)}, "
            f"successful={summary.get('image_extraction_successful', 0)}, "
            f"failed={summary.get('image_extraction_failed', 0)}"
        )
        lines.append(
            "Video extraction this run: "
            f"attempted={summary.get('video_extraction_attempted', 0)}, "
            f"successful={summary.get('video_extraction_successful', 0)}, "
            f"failed={summary.get('video_extraction_failed', 0)}"
        )
        if summary.get("full_rescan"):
            lines.append("Rescan mode: full_rescan")
        else:
            lines.append("Rescan mode: incremental (new/changed only)")
        lines.append(
            "Comparison line: "
            + _comparison_line(summary)
        )
        lines.append(
            "Expected on repeat runs: incremental should attempt fewer extracts and run faster than full_rescan."
        )

    if summary["common_errors"]:
        lines.append("Common errors:")
        for error, count in summary["common_errors"]:
            lines.append(f"  - {error}: {count}")

    return "\n".join(lines)


def _percent(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return (numerator / denominator) * 100.0


def _format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "n/a"
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes = int(seconds // 60)
    remainder = seconds - (minutes * 60)
    return f"{minutes}m {remainder:.1f}s"


def _comparison_line(summary: dict[str, Any]) -> str:
    mode = "full_rescan" if summary.get("full_rescan") else "incremental"
    extraction_attempted = int(summary.get("extraction_attempted", 0))
    supported = int(summary.get("supported_files", 0))
    new_files = int(summary.get("new_files", 0))
    changed_files = int(summary.get("changed_files", 0))
    run_duration = _format_duration(summary.get("run_duration_seconds"))
    attempt_pct = _percent(extraction_attempted, supported)
    return (
        f"mode={mode} | duration={run_duration} | "
        f"extract_attempted={extraction_attempted}/{supported} ({attempt_pct:.1f}%) | "
        f"new+changed={new_files + changed_files}"
    )


def format_cli_report(
    scan: ScanHistoryRecord,
    unsupported_extensions: list[ExtensionCountRecord],
    failed_files: list[FailedFileRecord],
    coverage_rows: list[ColumnCoverageRecord],
    coverage_total_rows: int,
    *,
    failed_limit: int,
    thumbnail_statuses: list[ThumbnailStatusCountRecord] | None = None,
    failed_thumbnails: list[FailedThumbnailRecord] | None = None,
) -> str:
    thumbnail_statuses = thumbnail_statuses or []
    failed_thumbnails = failed_thumbnails or []
    lines: list[str] = []
    lines.append("Latest scan summary")
    lines.append(f"  scan_id: {scan.scan_id}")
    lines.append(f"  scan_root: {scan.scan_root}")
    lines.append(f"  started_at: {_format_timestamp(scan.started_at)}")
    lines.append(f"  finished_at: {_format_timestamp(scan.finished_at)}")
    lines.append(
        f"  duration: {_format_duration((scan.finished_at - scan.started_at).total_seconds())}"
    )
    lines.append(f"  files_discovered: {scan.files_discovered}")
    lines.append(f"  supported_files: {scan.supported_files}")
    if unsupported_extensions:
        unsupported_total = sum(item.count for item in unsupported_extensions)
        lines.append(f"  unsupported_files: {unsupported_total}")
        lines.append("  unsupported_extensions:")
        for item in unsupported_extensions:
            lines.append(f"    {item.extension}: {item.count}")
    else:
        lines.append("  unsupported_files: 0")
    lines.append("")

    lines.append("Change counts")
    lines.append(f"  new: {scan.new_files}")
    lines.append(f"  changed: {scan.changed_files}")
    lines.append(f"  missing: {scan.missing_files}")
    lines.append(f"  unchanged: {scan.unchanged_files}")
    lines.append("")

    lines.append("Extraction stats")
    lines.append(f"  attempted: {scan.extraction_attempted}")
    lines.append(f"  successful: {scan.extraction_successful}")
    lines.append(f"  failed: {scan.extraction_failed}")
    lines.append("")

    lines.append("Media extraction stats")
    lines.append(
        "  image:"
        f" attempted={scan.image_extraction_attempted},"
        f" successful={scan.image_extraction_successful},"
        f" failed={scan.image_extraction_failed}"
    )
    lines.append(
        "  video:"
        f" attempted={scan.video_extraction_attempted},"
        f" successful={scan.video_extraction_successful},"
        f" failed={scan.video_extraction_failed}"
    )
    lines.append("")

    lines.append(f"Failed files (up to {failed_limit})")
    if not failed_files:
        lines.append("  none")
    else:
        for item in failed_files:
            error_text = item.extract_error or "unknown_error"
            lines.append(f"  [{item.extract_status}] {item.path} | {error_text}")
    lines.append("")

    lines.append("Thumbnail stats")
    if not thumbnail_statuses:
        lines.append("  no thumbnail rows")
    else:
        thumbnail_total = sum(item.count for item in thumbnail_statuses)
        lines.append(f"  total_rows: {thumbnail_total}")
        for item in thumbnail_statuses:
            lines.append(f"  {item.status}: {item.count}")
    lines.append("")

    lines.append(f"Failed thumbnails (up to {failed_limit})")
    if not failed_thumbnails:
        lines.append("  none")
    else:
        for item in failed_thumbnails:
            error_text = item.error or "unknown_error"
            path_text = item.thumb_path or "[no_thumb_path]"
            lines.append(f"  [{item.status}] {item.file_id} | {path_text} | {error_text}")
    lines.append("")

    lines.append(f"Non-null coverage by column (rows={coverage_total_rows})")
    if not coverage_rows:
        lines.append("  no rows for selected scan")
    else:
        for row in coverage_rows:
            lines.append(
                "  "
                + f"{row.column_name}: {row.non_null_count}/{coverage_total_rows} "
                + f"({row.non_null_pct:.1f}%) type={row.column_type}"
            )
    return "\n".join(lines)


def _format_timestamp(value: datetime) -> str:
    return value.isoformat(sep=" ", timespec="seconds")
