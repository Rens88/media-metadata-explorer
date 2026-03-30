from datetime import datetime, timezone

from photo_archive.models import ExtractionResult, FileScanRecord, FilenameParseRecord
from photo_archive.normalize import normalize_record


def _build_scan_record(is_supported: bool = True) -> FileScanRecord:
    return FileScanRecord(
        file_id="file-1",
        path="/photos/IMG_20220304_153012.jpg",
        parent_folder="/photos",
        filename="IMG_20220304_153012.jpg",
        extension=".jpg",
        media_type="image" if is_supported else "unknown",
        size_bytes=1024,
        fs_created_at=datetime(2020, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        fs_modified_at=datetime(2020, 1, 2, 11, 0, 0, tzinfo=timezone.utc),
        scan_root="/photos",
        scan_time=datetime(2026, 3, 30, 8, 0, 0, tzinfo=timezone.utc),
        is_supported=is_supported,
    )


def test_timestamp_prefers_exif_original() -> None:
    scan_record = _build_scan_record()
    extraction = ExtractionResult(
        path=scan_record.path,
        status="success",
        raw_metadata={
            "EXIF:DateTimeOriginal": "2019:07:04 12:13:14",
            "XMP:CreateDate": "2018:01:01 00:00:00",
        },
    )
    filename_parse = FilenameParseRecord(parsed_datetime=datetime(2022, 3, 4, 15, 30, 12))

    normalized = normalize_record(scan_record, extraction, filename_parse)
    assert normalized.captured_at == datetime(2019, 7, 4, 12, 13, 14)
    assert normalized.captured_at_source == "exif:EXIF:DateTimeOriginal"


def test_timestamp_falls_back_to_filename_then_filesystem() -> None:
    scan_record = _build_scan_record()
    extraction = ExtractionResult(path=scan_record.path, status="success", raw_metadata={})
    filename_parse = FilenameParseRecord(
        parsed_datetime=datetime(2022, 3, 4, 15, 30, 12),
        parsed_pattern="img_yyyymmdd_hhmmss",
        parse_confidence=0.95,
    )

    normalized = normalize_record(scan_record, extraction, filename_parse)
    assert normalized.captured_at == datetime(2022, 3, 4, 15, 30, 12)
    assert normalized.captured_at_source == "filename:img_yyyymmdd_hhmmss"


def test_timestamp_falls_back_to_filesystem_when_filename_missing() -> None:
    scan_record = _build_scan_record()
    extraction = ExtractionResult(path=scan_record.path, status="success", raw_metadata={})
    filename_parse = FilenameParseRecord()

    normalized = normalize_record(scan_record, extraction, filename_parse)
    assert normalized.captured_at == scan_record.fs_created_at
    assert normalized.captured_at_source == "filesystem:created"


def test_unsupported_file_is_marked_skipped() -> None:
    scan_record = _build_scan_record(is_supported=False)
    normalized = normalize_record(scan_record, extraction=None, filename_parse=FilenameParseRecord())

    assert normalized.extract_status == "skipped_unsupported"
    assert normalized.captured_at == scan_record.fs_created_at
