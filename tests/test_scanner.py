from datetime import datetime, timezone
from pathlib import Path

from photo_archive.scanner import scan_directory


def test_scan_directory_assigns_video_media_type(tmp_path: Path) -> None:
    image = tmp_path / "photo.jpg"
    video = tmp_path / "clip.mp4"
    unsupported = tmp_path / "note.txt"
    image.write_bytes(b"jpg")
    video.write_bytes(b"mp4")
    unsupported.write_bytes(b"txt")

    records = scan_directory(
        root_path=tmp_path,
        supported_extensions={".jpg", ".mp4"},
        scan_time=datetime(2026, 3, 30, 10, 0, 0, tzinfo=timezone.utc),
    )
    by_name = {record.filename: record for record in records}

    assert by_name["photo.jpg"].is_supported is True
    assert by_name["photo.jpg"].media_type == "image"
    assert by_name["clip.mp4"].is_supported is True
    assert by_name["clip.mp4"].media_type == "video"
    assert by_name["note.txt"].is_supported is False
    assert by_name["note.txt"].media_type == "unknown"
