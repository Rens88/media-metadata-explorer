from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import time
from typing import Mapping, Sequence

from photo_archive.models import ExistingFileIndexRecord, FileScanRecord


@dataclass(slots=True)
class IncrementalClassification:
    state_by_path: dict[str, str]
    missing_paths: list[str]
    new_files: int
    changed_files: int
    unchanged_files: int


def classify_incremental_state(
    scan_records: Sequence[FileScanRecord],
    existing_by_path: Mapping[str, ExistingFileIndexRecord],
) -> IncrementalClassification:
    current_paths = {record.path for record in scan_records}
    previous_active_paths = {
        path
        for path, existing in existing_by_path.items()
        if (existing.file_state or "") != "missing"
    }
    missing_paths = sorted(previous_active_paths - current_paths)

    state_by_path: dict[str, str] = {}
    new_files = 0
    changed_files = 0
    unchanged_files = 0

    for record in scan_records:
        existing = existing_by_path.get(record.path)
        if existing is None or (existing.file_state or "") == "missing":
            state = "new"
            new_files += 1
        elif _same_file_version(
            current_size=record.size_bytes,
            current_modified=record.fs_modified_at,
            previous_size=existing.size_bytes,
            previous_modified=existing.fs_modified_at,
        ):
            state = "unchanged"
            unchanged_files += 1
        else:
            state = "changed"
            changed_files += 1
        state_by_path[record.path] = state

    return IncrementalClassification(
        state_by_path=state_by_path,
        missing_paths=missing_paths,
        new_files=new_files,
        changed_files=changed_files,
        unchanged_files=unchanged_files,
    )


def _same_file_version(
    current_size: int | None,
    current_modified: datetime | None,
    previous_size: int | None,
    previous_modified: datetime | None,
) -> bool:
    return (current_size == previous_size) and _timestamps_equivalent(
        current_modified, previous_modified
    )


def _timestamps_equivalent(left: datetime | None, right: datetime | None) -> bool:
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False

    left_candidates = _utc_candidates(left)
    right_candidates = _utc_candidates(right)
    for left_value in left_candidates:
        for right_value in right_candidates:
            if abs((left_value - right_value).total_seconds()) <= 0.001:
                return True
    return False


def _utc_candidates(value: datetime) -> list[datetime]:
    if value.tzinfo is not None:
        return [value.astimezone(timezone.utc)]

    # DuckDB TIMESTAMP can come back naive; depending on driver/platform,
    # that value may effectively represent UTC or local wall-clock time.
    # We include UTC plus local standard and DST offsets to avoid false
    # "changed" classifications across timezone/DST interpretation boundaries.
    offset_seconds = {0, -time.timezone}
    if time.daylight:
        offset_seconds.add(-time.altzone)

    candidates: list[datetime] = []
    seen: set[datetime] = set()
    for seconds in offset_seconds:
        tz = timezone(timedelta(seconds=seconds))
        normalized = value.replace(tzinfo=tz).astimezone(timezone.utc)
        if normalized not in seen:
            candidates.append(normalized)
            seen.add(normalized)
    return candidates
