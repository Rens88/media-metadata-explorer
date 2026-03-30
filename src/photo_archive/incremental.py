from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
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
    return (current_size == previous_size) and (current_modified == previous_modified)
