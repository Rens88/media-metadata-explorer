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

    normalized = normalize_record(
        scan_record,
        extraction,
        filename_parse,
        scan_id="scan_1",
        file_state="new",
        first_seen_at=scan_record.scan_time,
        last_seen_at=scan_record.scan_time,
    )
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

    normalized = normalize_record(
        scan_record,
        extraction,
        filename_parse,
        scan_id="scan_1",
        file_state="new",
        first_seen_at=scan_record.scan_time,
        last_seen_at=scan_record.scan_time,
    )
    assert normalized.captured_at == datetime(2022, 3, 4, 15, 30, 12)
    assert normalized.captured_at_source == "filename:img_yyyymmdd_hhmmss"


def test_timestamp_falls_back_to_filesystem_when_filename_missing() -> None:
    scan_record = _build_scan_record()
    extraction = ExtractionResult(path=scan_record.path, status="success", raw_metadata={})
    filename_parse = FilenameParseRecord()

    normalized = normalize_record(
        scan_record,
        extraction,
        filename_parse,
        scan_id="scan_1",
        file_state="new",
        first_seen_at=scan_record.scan_time,
        last_seen_at=scan_record.scan_time,
    )
    assert normalized.captured_at == scan_record.fs_created_at
    assert normalized.captured_at_source == "filesystem:created"


def test_unsupported_file_is_marked_skipped() -> None:
    scan_record = _build_scan_record(is_supported=False)
    normalized = normalize_record(
        scan_record,
        extraction=None,
        filename_parse=FilenameParseRecord(),
        scan_id="scan_1",
        file_state="new",
        first_seen_at=scan_record.scan_time,
        last_seen_at=scan_record.scan_time,
    )

    assert normalized.extract_status == "skipped_unsupported"
    assert normalized.captured_at == scan_record.fs_created_at


def test_video_metadata_fields_from_ffprobe_payload() -> None:
    scan_record = FileScanRecord(
        file_id="file-video-1",
        path="/photos/video_1.mp4",
        parent_folder="/photos",
        filename="video_1.mp4",
        extension=".mp4",
        media_type="video",
        size_bytes=4096,
        fs_created_at=datetime(2020, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        fs_modified_at=datetime(2020, 1, 2, 11, 0, 0, tzinfo=timezone.utc),
        scan_root="/photos",
        scan_time=datetime(2026, 3, 30, 8, 0, 0, tzinfo=timezone.utc),
        is_supported=True,
    )
    extraction = ExtractionResult(
        path=scan_record.path,
        status="success",
        raw_metadata={
            "format": {
                "duration": "61.25",
                "bit_rate": "2500000",
                "tags": {"creation_time": "2024-05-10T08:30:00Z"},
            },
            "streams": [
                {
                    "codec_type": "video",
                    "codec_name": "h264",
                    "avg_frame_rate": "30000/1001",
                    "width": 1920,
                    "height": 1080,
                }
            ],
        },
    )
    normalized = normalize_record(
        scan_record,
        extraction,
        FilenameParseRecord(),
        scan_id="scan_1",
        file_state="new",
        first_seen_at=scan_record.scan_time,
        last_seen_at=scan_record.scan_time,
    )

    assert normalized.media_type == "video"
    assert normalized.width == 1920
    assert normalized.height == 1080
    assert normalized.video_duration_seconds == 61.25
    assert normalized.video_codec == "h264"
    assert normalized.video_bitrate == 2500000
    assert normalized.video_fps is not None
    assert round(normalized.video_fps, 3) == 29.97
    assert normalized.captured_at_source == "ffprobe:creation_time"


def test_gps_prefers_consistent_priority_pair() -> None:
    scan_record = _build_scan_record()
    extraction = ExtractionResult(
        path=scan_record.path,
        status="success",
        raw_metadata={
            "EXIF:GPSLatitude": 52.123,
            "EXIF:GPSLongitude": 5.456,
            "Composite:GPSLatitude": 52.000,
            "Composite:GPSLongitude": 5.000,
        },
    )
    normalized = normalize_record(
        scan_record,
        extraction,
        FilenameParseRecord(),
        scan_id="scan_1",
        file_state="new",
        first_seen_at=scan_record.scan_time,
        last_seen_at=scan_record.scan_time,
    )

    assert normalized.gps_lat == 52.0
    assert normalized.gps_lon == 5.0


def test_gps_discards_zero_zero_coordinates() -> None:
    scan_record = _build_scan_record()
    extraction = ExtractionResult(
        path=scan_record.path,
        status="success",
        raw_metadata={
            "EXIF:GPSLatitude": 0.0,
            "EXIF:GPSLongitude": 0.0,
        },
    )
    normalized = normalize_record(
        scan_record,
        extraction,
        FilenameParseRecord(),
        scan_id="scan_1",
        file_state="new",
        first_seen_at=scan_record.scan_time,
        last_seen_at=scan_record.scan_time,
    )

    assert normalized.gps_lat is None
    assert normalized.gps_lon is None


def test_gps_prefers_non_zero_pair_when_composite_contains_zero_component() -> None:
    scan_record = _build_scan_record()
    extraction = ExtractionResult(
        path=scan_record.path,
        status="success",
        raw_metadata={
            "Composite:GPSLatitude": 0.0,
            "Composite:GPSLongitude": 5.121,
            "EXIF:GPSLatitude": 52.0907,
            "EXIF:GPSLongitude": 5.1214,
        },
    )
    normalized = normalize_record(
        scan_record,
        extraction,
        FilenameParseRecord(),
        scan_id="scan_1",
        file_state="new",
        first_seen_at=scan_record.scan_time,
        last_seen_at=scan_record.scan_time,
    )

    assert normalized.gps_lat == 52.0907
    assert normalized.gps_lon == 5.1214
