from datetime import datetime, timezone

from photo_archive.incremental import classify_incremental_state
from photo_archive.models import ExistingFileIndexRecord, FileScanRecord


def _scan_record(path: str, size_bytes: int, mtime: datetime) -> FileScanRecord:
    return FileScanRecord(
        file_id=f"id-{path}",
        path=path,
        parent_folder="/photos",
        filename=path.rsplit("/", 1)[-1],
        extension=".jpg",
        media_type="image",
        size_bytes=size_bytes,
        fs_created_at=mtime,
        fs_modified_at=mtime,
        scan_root="/photos",
        scan_time=datetime(2026, 3, 30, 8, 0, 0, tzinfo=timezone.utc),
        is_supported=True,
    )


def _existing_record(path: str, size_bytes: int, mtime: datetime) -> ExistingFileIndexRecord:
    return ExistingFileIndexRecord(
        file_id=f"id-{path}",
        path=path,
        scan_root="/photos",
        size_bytes=size_bytes,
        fs_modified_at=mtime,
        raw_metadata_json=None,
        extract_status="success",
        extract_error=None,
        is_supported=True,
        file_state="unchanged",
        first_seen_at=mtime,
    )


def test_classify_new_changed_unchanged_and_missing() -> None:
    t1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 2, tzinfo=timezone.utc)
    t3 = datetime(2026, 1, 3, tzinfo=timezone.utc)

    existing = {
        "/photos/a.jpg": _existing_record("/photos/a.jpg", 100, t1),
        "/photos/b.jpg": _existing_record("/photos/b.jpg", 200, t2),
        "/photos/c.jpg": _existing_record("/photos/c.jpg", 300, t2),
    }
    scan_records = [
        _scan_record("/photos/a.jpg", 100, t1),  # unchanged
        _scan_record("/photos/b.jpg", 999, t3),  # changed
        _scan_record("/photos/d.jpg", 50, t3),   # new
    ]

    result = classify_incremental_state(scan_records, existing)

    assert result.state_by_path["/photos/a.jpg"] == "unchanged"
    assert result.state_by_path["/photos/b.jpg"] == "changed"
    assert result.state_by_path["/photos/d.jpg"] == "new"
    assert result.missing_paths == ["/photos/c.jpg"]
    assert result.new_files == 1
    assert result.changed_files == 1
    assert result.unchanged_files == 1


def test_naive_and_utc_aware_timestamps_match_as_unchanged() -> None:
    aware_mtime = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    naive_mtime = datetime(2026, 1, 1, 12, 0, 0)

    existing = {
        "/photos/a.jpg": _existing_record("/photos/a.jpg", 100, naive_mtime),
    }
    scan_records = [
        _scan_record("/photos/a.jpg", 100, aware_mtime),
    ]

    result = classify_incremental_state(scan_records, existing)
    assert result.state_by_path["/photos/a.jpg"] == "unchanged"
    assert result.changed_files == 0
    assert result.unchanged_files == 1
