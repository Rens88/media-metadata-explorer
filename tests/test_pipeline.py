from datetime import datetime, timezone

from photo_archive.models import ExistingFileIndexRecord, FileScanRecord, NormalizedRecord
from photo_archive.pipeline import _select_persist_targets, _should_reuse_existing_record


def _scan_record(path: str) -> FileScanRecord:
    now = datetime(2026, 4, 1, tzinfo=timezone.utc)
    return FileScanRecord(
        file_id=f"id-{path}",
        path=path,
        parent_folder="/photos",
        filename=path.rsplit("/", 1)[-1],
        extension=".jpg",
        media_type="image",
        size_bytes=1,
        fs_created_at=now,
        fs_modified_at=now,
        scan_root="/photos",
        scan_time=now,
        is_supported=True,
    )


def _normalized(path: str, file_state: str) -> NormalizedRecord:
    now = datetime(2026, 4, 1, tzinfo=timezone.utc)
    return NormalizedRecord(
        file_id=f"id-{path}",
        scan_id="scan_x",
        path=path,
        parent_folder="/photos",
        filename=path.rsplit("/", 1)[-1],
        extension=".jpg",
        media_type="image",
        size_bytes=1,
        fs_created_at=now,
        fs_modified_at=now,
        scan_root="/photos",
        scan_time=now,
        is_supported=True,
        captured_at=now,
        captured_at_source="exif:DateTimeOriginal",
        gps_lat=None,
        gps_lon=None,
        gps_alt=None,
        camera_make=None,
        camera_model=None,
        lens_model=None,
        software=None,
        width=1,
        height=1,
        orientation=None,
        raw_metadata_json="{}",
        extract_status="success",
        extract_error=None,
        file_state=file_state,
        first_seen_at=now,
        last_seen_at=now,
        parsed_datetime=None,
        parsed_pattern=None,
        parse_confidence=None,
    )


def _existing(path: str) -> ExistingFileIndexRecord:
    now = datetime(2026, 4, 1, tzinfo=timezone.utc)
    return ExistingFileIndexRecord(
        file_id=f"id-{path}",
        path=path,
        scan_root="/photos",
        size_bytes=1,
        fs_modified_at=now,
        raw_metadata_json="{}",
        extract_status="success",
        extract_error=None,
        is_supported=True,
        file_state="unchanged",
        first_seen_at=now,
        captured_at=now,
        gps_lat=None,
        gps_lon=None,
        camera_model=None,
    )


def test_should_reuse_existing_record_respects_full_rescan() -> None:
    assert _should_reuse_existing_record(
        file_state="unchanged",
        existing_record=_existing("/photos/a.jpg"),
        full_rescan=False,
    )
    assert not _should_reuse_existing_record(
        file_state="unchanged",
        existing_record=_existing("/photos/a.jpg"),
        full_rescan=True,
    )
    assert not _should_reuse_existing_record(
        file_state="new",
        existing_record=_existing("/photos/a.jpg"),
        full_rescan=False,
    )


def test_select_persist_targets_full_rescan_upserts_unchanged() -> None:
    normalized = [
        _normalized("/photos/new.jpg", "new"),
        _normalized("/photos/changed.jpg", "changed"),
        _normalized("/photos/unchanged.jpg", "unchanged"),
        _normalized("/photos/missing.jpg", "missing"),
    ]
    scan_records = [
        _scan_record("/photos/new.jpg"),
        _scan_record("/photos/changed.jpg"),
        _scan_record("/photos/unchanged.jpg"),
        _scan_record("/photos/missing.jpg"),
    ]
    state_by_path = {
        "/photos/new.jpg": "new",
        "/photos/changed.jpg": "changed",
        "/photos/unchanged.jpg": "unchanged",
        "/photos/missing.jpg": "missing",
    }

    incremental_upserts, incremental_touch = _select_persist_targets(
        normalized_records=normalized,
        scan_records=scan_records,
        state_by_path=state_by_path,
        full_rescan=False,
    )
    assert [row.file_state for row in incremental_upserts] == ["new", "changed"]
    assert [row.path for row in incremental_touch] == ["/photos/unchanged.jpg"]

    full_upserts, full_touch = _select_persist_targets(
        normalized_records=normalized,
        scan_records=scan_records,
        state_by_path=state_by_path,
        full_rescan=True,
    )
    assert [row.file_state for row in full_upserts] == ["new", "changed", "unchanged"]
    assert full_touch == []
