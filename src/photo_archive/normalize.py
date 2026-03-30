from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from photo_archive.models import (
    ExtractionResult,
    FileScanRecord,
    FilenameParseRecord,
    NormalizedRecord,
)

ORIGINAL_CAPTURE_TAGS = [
    "DateTimeOriginal",
    "SubSecDateTimeOriginal",
]

SECONDARY_CAPTURE_TAGS = [
    "CreateDate",
    "MediaCreateDate",
    "CreationDate",
    "DateCreated",
    "TrackCreateDate",
    "ContentCreateDate",
]

GPS_LAT_TAGS = ["GPSLatitude"]
GPS_LON_TAGS = ["GPSLongitude"]
GPS_ALT_TAGS = ["GPSAltitude"]

MAKE_TAGS = ["Make"]
MODEL_TAGS = ["Model"]
LENS_TAGS = ["LensModel", "LensID"]
SOFTWARE_TAGS = ["Software"]
WIDTH_TAGS = ["ImageWidth", "ExifImageWidth"]
HEIGHT_TAGS = ["ImageHeight", "ExifImageHeight"]
ORIENTATION_TAGS = ["Orientation"]


def normalize_record(
    scan_record: FileScanRecord,
    extraction: ExtractionResult | None,
    filename_parse: FilenameParseRecord,
    scan_id: str,
    file_state: str,
    first_seen_at: datetime | None,
    last_seen_at: datetime | None,
) -> NormalizedRecord:
    raw_metadata = extraction.raw_metadata if extraction and extraction.raw_metadata else {}

    captured_at, captured_at_source = choose_best_timestamp(
        raw_metadata=raw_metadata,
        filename_parse=filename_parse,
        fs_created_at=scan_record.fs_created_at,
        fs_modified_at=scan_record.fs_modified_at,
    )

    extract_status, extract_error = derive_extract_status(scan_record, extraction)

    return NormalizedRecord(
        file_id=scan_record.file_id,
        scan_id=scan_id,
        path=scan_record.path,
        parent_folder=scan_record.parent_folder,
        filename=scan_record.filename,
        extension=scan_record.extension,
        media_type=scan_record.media_type,
        size_bytes=scan_record.size_bytes,
        fs_created_at=scan_record.fs_created_at,
        fs_modified_at=scan_record.fs_modified_at,
        scan_root=scan_record.scan_root,
        scan_time=scan_record.scan_time,
        is_supported=scan_record.is_supported,
        captured_at=captured_at,
        captured_at_source=captured_at_source,
        gps_lat=_extract_float(raw_metadata, GPS_LAT_TAGS),
        gps_lon=_extract_float(raw_metadata, GPS_LON_TAGS),
        gps_alt=_extract_float(raw_metadata, GPS_ALT_TAGS),
        camera_make=_extract_string(raw_metadata, MAKE_TAGS),
        camera_model=_extract_string(raw_metadata, MODEL_TAGS),
        lens_model=_extract_string(raw_metadata, LENS_TAGS),
        software=_extract_string(raw_metadata, SOFTWARE_TAGS),
        width=_extract_int(raw_metadata, WIDTH_TAGS),
        height=_extract_int(raw_metadata, HEIGHT_TAGS),
        orientation=_extract_string(raw_metadata, ORIENTATION_TAGS),
        raw_metadata_json=(
            json.dumps(raw_metadata, ensure_ascii=False, sort_keys=True)
            if raw_metadata
            else None
        ),
        extract_status=extract_status,
        extract_error=extract_error,
        file_state=file_state,
        first_seen_at=first_seen_at,
        last_seen_at=last_seen_at,
        parsed_datetime=filename_parse.parsed_datetime,
        parsed_pattern=filename_parse.parsed_pattern,
        parse_confidence=filename_parse.parse_confidence,
    )


def derive_extract_status(
    scan_record: FileScanRecord,
    extraction: ExtractionResult | None,
) -> tuple[str, str | None]:
    if not scan_record.is_supported:
        return "skipped_unsupported", scan_record.scan_error

    if extraction is None:
        if scan_record.scan_error:
            return "failed", scan_record.scan_error
        return "failed", "missing_extraction_result"

    if extraction.status == "success":
        return "success", scan_record.scan_error

    if extraction.status == "success_cached":
        return "success_cached", extraction.error or scan_record.scan_error

    if extraction.status == "skipped_dry_run":
        return "skipped_dry_run", scan_record.scan_error

    if extraction.status == "failed_cached":
        return "failed_cached", extraction.error or scan_record.scan_error

    error = extraction.error or scan_record.scan_error or "extraction_failed"
    return "failed", error


def choose_best_timestamp(
    raw_metadata: dict[str, Any],
    filename_parse: FilenameParseRecord,
    fs_created_at: datetime | None,
    fs_modified_at: datetime | None,
) -> tuple[datetime | None, str | None]:
    value, key = _extract_value_with_source(raw_metadata, ORIGINAL_CAPTURE_TAGS)
    parsed = _coerce_datetime(value)
    if parsed:
        return parsed, f"exif:{key}"

    value, key = _extract_value_with_source(raw_metadata, SECONDARY_CAPTURE_TAGS)
    parsed = _coerce_datetime(value)
    if parsed:
        return parsed, f"exif:{key}"

    if filename_parse.parsed_datetime:
        return filename_parse.parsed_datetime, f"filename:{filename_parse.parsed_pattern}"

    if fs_created_at:
        return fs_created_at, "filesystem:created"

    if fs_modified_at:
        return fs_modified_at, "filesystem:modified"

    return None, None


def _extract_value_with_source(
    raw_metadata: dict[str, Any],
    candidate_tags: list[str],
) -> tuple[Any | None, str | None]:
    for tag in candidate_tags:
        tag_lc = tag.lower()
        for key, value in raw_metadata.items():
            key_lc = str(key).lower()
            if key_lc == tag_lc or key_lc.rsplit(":", 1)[-1] == tag_lc:
                if value not in (None, ""):
                    return value, str(key)
    return None, None


def _extract_string(raw_metadata: dict[str, Any], candidate_tags: list[str]) -> str | None:
    value, _ = _extract_value_with_source(raw_metadata, candidate_tags)
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return str(value)


def _extract_float(raw_metadata: dict[str, Any], candidate_tags: list[str]) -> float | None:
    value, _ = _extract_value_with_source(raw_metadata, candidate_tags)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_int(raw_metadata: dict[str, Any], candidate_tags: list[str]) -> int | None:
    value, _ = _extract_value_with_source(raw_metadata, candidate_tags)
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if not isinstance(value, str):
        return None

    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    for fmt in (
        "%Y:%m:%d %H:%M:%S.%f%z",
        "%Y:%m:%d %H:%M:%S%z",
        "%Y:%m:%d %H:%M:%S.%f",
        "%Y:%m:%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None
