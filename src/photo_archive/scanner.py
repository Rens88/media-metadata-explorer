from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from photo_archive.config import media_type_for_extension
from photo_archive.models import FileScanRecord

LOGGER = logging.getLogger(__name__)


def build_file_id(path: Path) -> str:
    """Build a stable identifier from absolute path for Phase 1."""
    canonical = str(path.resolve(strict=False))
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()


def _safe_datetime_from_ts(timestamp: float | None) -> datetime | None:
    if timestamp is None:
        return None
    try:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def scan_directory(
    root_path: Path,
    supported_extensions: set[str],
    scan_time: datetime | None = None,
) -> list[FileScanRecord]:
    """Walk the directory recursively and emit one record per file."""
    root_abs = root_path.expanduser().resolve()
    if not root_abs.exists():
        raise FileNotFoundError(f"Scan root does not exist: {root_abs}")
    if not root_abs.is_dir():
        raise NotADirectoryError(f"Scan root is not a directory: {root_abs}")

    effective_scan_time = scan_time or datetime.now(timezone.utc)
    records: list[FileScanRecord] = []

    def on_walk_error(exc: OSError) -> None:
        LOGGER.warning("Scanner walk error at %s: %s", getattr(exc, "filename", "?"), exc)

    for dirpath, _, filenames in os.walk(root_abs, onerror=on_walk_error):
        parent = Path(dirpath)
        for filename in filenames:
            file_path = parent / filename
            extension = file_path.suffix.lower()
            is_supported = extension in supported_extensions
            media_type = media_type_for_extension(extension, supported_extensions)
            size_bytes: int | None = None
            fs_created_at: datetime | None = None
            fs_modified_at: datetime | None = None
            scan_error: str | None = None

            try:
                stat_result = file_path.stat()
                size_bytes = int(stat_result.st_size)
                fs_created_at = _safe_datetime_from_ts(getattr(stat_result, "st_ctime", None))
                fs_modified_at = _safe_datetime_from_ts(getattr(stat_result, "st_mtime", None))
            except OSError as exc:
                scan_error = f"stat_failed: {exc}"
                LOGGER.warning("Failed to stat %s: %s", file_path, exc)

            records.append(
                FileScanRecord(
                    file_id=build_file_id(file_path),
                    path=str(file_path.resolve(strict=False)),
                    parent_folder=str(parent.resolve(strict=False)),
                    filename=file_path.name,
                    extension=extension,
                    media_type=media_type,
                    size_bytes=size_bytes,
                    fs_created_at=fs_created_at,
                    fs_modified_at=fs_modified_at,
                    scan_root=str(root_abs),
                    scan_time=effective_scan_time,
                    is_supported=is_supported,
                    scan_error=scan_error,
                )
            )
    return records
