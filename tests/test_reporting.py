from datetime import datetime, timezone

from photo_archive.models import (
    ColumnCoverageRecord,
    ExtensionCountRecord,
    FailedThumbnailRecord,
    FailedFileRecord,
    NormalizedRecord,
    ScanHistoryRecord,
    ThumbnailStatusCountRecord,
)
from photo_archive.reporting import build_run_summary, format_cli_report, format_run_summary


def _record(path: str, extract_status: str = "success") -> NormalizedRecord:
    return NormalizedRecord(
        file_id=f"id-{path}",
        scan_id="scan_1",
        path=path,
        parent_folder="/photos",
        filename=path.rsplit("/", 1)[-1],
        extension=".jpg",
        media_type="image",
        size_bytes=10,
        fs_created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        fs_modified_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        scan_root="/photos",
        scan_time=datetime(2026, 3, 30, tzinfo=timezone.utc),
        is_supported=True,
        captured_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        captured_at_source="exif:DateTimeOriginal",
        gps_lat=None,
        gps_lon=None,
        gps_alt=None,
        camera_make="Canon",
        camera_model="Canon 5D",
        lens_model=None,
        software=None,
        width=100,
        height=100,
        orientation=None,
        raw_metadata_json="{}",
        extract_status=extract_status,
        extract_error=None,
        file_state="unchanged",
        first_seen_at=datetime(2026, 3, 30, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 3, 30, tzinfo=timezone.utc),
        parsed_datetime=None,
        parsed_pattern=None,
        parse_confidence=None,
    )


def test_summary_contains_comparison_line() -> None:
    records = [_record("/photos/a.jpg"), _record("/photos/b.jpg")]
    summary = build_run_summary(
        records,
        new_files=0,
        changed_files=0,
        unchanged_files=2,
        missing_files=0,
        extraction_attempted=0,
        extraction_successful=0,
        extraction_failed=0,
        full_rescan=False,
        scan_duration_seconds=1.0,
        state_duration_seconds=0.2,
        parse_duration_seconds=0.1,
        extraction_duration_seconds=0.0,
        normalize_duration_seconds=0.4,
        persist_duration_seconds=0.3,
        image_extraction_attempted=0,
        image_extraction_successful=0,
        image_extraction_failed=0,
        video_extraction_attempted=0,
        video_extraction_successful=0,
        video_extraction_failed=0,
    )
    summary["run_duration_seconds"] = 2.0

    text = format_run_summary(summary)
    assert "Comparison line:" in text
    assert "mode=incremental" in text
    assert "extract_attempted=0/2" in text
    assert "Image extraction this run:" in text
    assert "Video extraction this run:" in text


def test_format_cli_report_sections() -> None:
    scan = ScanHistoryRecord(
        scan_id="scan_abc",
        scan_root="/photos",
        started_at=datetime(2026, 3, 30, 10, 0, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 3, 30, 10, 1, 0, tzinfo=timezone.utc),
        files_discovered=100,
        supported_files=90,
        new_files=2,
        changed_files=5,
        unchanged_files=83,
        missing_files=1,
        extraction_attempted=7,
        extraction_successful=7,
        extraction_failed=0,
        dry_run=False,
        image_extraction_attempted=5,
        image_extraction_successful=5,
        image_extraction_failed=0,
        video_extraction_attempted=2,
        video_extraction_successful=2,
        video_extraction_failed=0,
    )
    failed_files = [
        FailedFileRecord(
            path="/photos/bad.jpg",
            extract_status="failed",
            extract_error="exiftool_missing_output",
        )
    ]
    coverage_rows = [
        ColumnCoverageRecord(
            column_name="camera_model",
            column_type="VARCHAR",
            non_null_count=70,
            null_count=20,
            non_null_pct=77.78,
        )
    ]
    unsupported_extensions = [
        ExtensionCountRecord(extension=".mp4", count=8),
        ExtensionCountRecord(extension=".mov", count=2),
    ]
    thumbnail_statuses = [
        ThumbnailStatusCountRecord(status="success", count=120),
        ThumbnailStatusCountRecord(status="failed", count=4),
    ]
    failed_thumbnails = [
        FailedThumbnailRecord(
            file_id="file-1",
            thumb_path="/thumbs/file-1.jpg",
            status="failed",
            error="corrupt_image",
        )
    ]

    text = format_cli_report(
        scan=scan,
        unsupported_extensions=unsupported_extensions,
        failed_files=failed_files,
        coverage_rows=coverage_rows,
        coverage_total_rows=90,
        failed_limit=50,
        thumbnail_statuses=thumbnail_statuses,
        failed_thumbnails=failed_thumbnails,
    )
    assert "Latest scan summary" in text
    assert "unsupported_files: 10" in text
    assert ".mp4: 8" in text
    assert "Change counts" in text
    assert "Media extraction stats" in text
    assert "image: attempted=5, successful=5, failed=0" in text
    assert "video: attempted=2, successful=2, failed=0" in text
    assert "Failed files (up to 50)" in text
    assert "Thumbnail stats" in text
    assert "total_rows: 124" in text
    assert "Failed thumbnails (up to 50)" in text
    assert "corrupt_image" in text
    assert "camera_model: 70/90" in text
