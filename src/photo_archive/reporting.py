from __future__ import annotations

from collections import Counter
from typing import Any, Sequence

from photo_archive.models import NormalizedRecord


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

    if "extraction_attempted" in summary:
        lines.append(f"Extraction attempts this run: {summary['extraction_attempted']}")
        lines.append(f"Extraction successes this run: {summary.get('extraction_successful', 0)}")
        lines.append(f"Extraction failures this run: {summary.get('extraction_failed', 0)}")
        if summary.get("full_rescan"):
            lines.append("Rescan mode: full_rescan")
        else:
            lines.append("Rescan mode: incremental (new/changed only)")

    if summary["common_errors"]:
        lines.append("Common errors:")
        for error, count in summary["common_errors"]:
            lines.append(f"  - {error}: {count}")

    return "\n".join(lines)


def _percent(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return (numerator / denominator) * 100.0
