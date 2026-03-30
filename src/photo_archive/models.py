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
    parsed_datetime: datetime | None
    parsed_pattern: str | None
    parse_confidence: float | None
