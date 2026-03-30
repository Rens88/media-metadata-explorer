from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class FileScanRecord:
    file_id: str
    path: str
    parent_folder: str
    filename: str
    extension: str
    media_type: str
    size_bytes: int | None
    fs_created_at: datetime | None
    fs_modified_at: datetime | None
    scan_root: str
    scan_time: datetime
    is_supported: bool
    scan_error: str | None = None


@dataclass(slots=True)
class ExtractionResult:
    path: str
    status: str
    raw_metadata: dict[str, Any] | None = None
    error: str | None = None
    stderr: str | None = None


@dataclass(slots=True)
class FilenameParseRecord:
    parsed_datetime: datetime | None = None
    parsed_pattern: str | None = None
    parse_confidence: float | None = None


@dataclass(slots=True)
class NormalizedRecord:
    file_id: str
    scan_id: str
    path: str
    parent_folder: str
    filename: str
    extension: str
    media_type: str
    size_bytes: int | None
    fs_created_at: datetime | None
    fs_modified_at: datetime | None
    scan_root: str
    scan_time: datetime
    is_supported: bool
    captured_at: datetime | None
    captured_at_source: str | None
    gps_lat: float | None
    gps_lon: float | None
    gps_alt: float | None
    camera_make: str | None
    camera_model: str | None
    lens_model: str | None
    software: str | None
    width: int | None
    height: int | None
    orientation: str | None
    raw_metadata_json: str | None
    extract_status: str
    extract_error: str | None
    file_state: str
    first_seen_at: datetime | None
    last_seen_at: datetime | None
    parsed_datetime: datetime | None
    parsed_pattern: str | None
    parse_confidence: float | None


@dataclass(slots=True)
class ExistingFileIndexRecord:
    file_id: str
    path: str
    scan_root: str
    size_bytes: int | None
    fs_modified_at: datetime | None
    raw_metadata_json: str | None
    extract_status: str | None
    extract_error: str | None
    is_supported: bool
    file_state: str | None
    first_seen_at: datetime | None


@dataclass(slots=True)
class ScanHistoryRecord:
    scan_id: str
    scan_root: str
    started_at: datetime
    finished_at: datetime
    files_discovered: int
    supported_files: int
    new_files: int
    changed_files: int
    unchanged_files: int
    missing_files: int
    extraction_attempted: int
    extraction_successful: int
    extraction_failed: int
    dry_run: bool


@dataclass(slots=True)
class FailedFileRecord:
    path: str
    extract_status: str
    extract_error: str | None


@dataclass(slots=True)
class ColumnCoverageRecord:
    column_name: str
    column_type: str
    non_null_count: int
    null_count: int
    non_null_pct: float


@dataclass(slots=True)
class ExtensionCountRecord:
    extension: str
    count: int
