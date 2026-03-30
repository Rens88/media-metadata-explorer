from __future__ import annotations

from collections import Counter
from typing import Any, Sequence

from photo_archive.models import NormalizedRecord


def build_run_summary(records: Sequence[NormalizedRecord]) -> dict[str, Any]:
    files_discovered = len(records)
    supported_records = [record for record in records if record.is_supported]
    supported_files = len(supported_records)

    successful_extractions = sum(
        1 for record in supported_records if record.extract_status == "success"
    )
    failed_extractions = sum(
        1 for record in supported_records if record.extract_status == "failed"
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
    }


def format_run_summary(summary: dict[str, Any]) -> str:
    lines = [
        f"Files discovered: {summary['files_discovered']}",
        f"Supported files: {summary['supported_files']}",
        f"Successful extractions: {summary['successful_extractions']}",
        f"Failed extractions: {summary['failed_extractions']}",
        f"Percent with captured_at: {summary['percent_with_captured_at']:.1f}%",
        f"Percent with GPS: {summary['percent_with_gps']:.1f}%",
        f"Percent with camera_model: {summary['percent_with_camera_model']:.1f}%",
    ]

    if summary["common_errors"]:
        lines.append("Common errors:")
        for error, count in summary["common_errors"]:
            lines.append(f"  - {error}: {count}")

    return "\n".join(lines)


def _percent(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return (numerator / denominator) * 100.0
