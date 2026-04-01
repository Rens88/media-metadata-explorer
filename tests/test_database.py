from datetime import datetime, timezone
from pathlib import Path

import duckdb

from photo_archive.database import DuckDBStore
from photo_archive.models import FileScanRecord


def test_touch_unchanged_records_refreshes_support_and_media_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "touch.duckdb"
    store = DuckDBStore(db_path=db_path)
    store.initialize()

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO file_metadata (
                file_id,
                scan_id,
                path,
                parent_folder,
                filename,
                extension,
                media_type,
                size_bytes,
                fs_created_at,
                fs_modified_at,
                scan_root,
                scan_time,
                is_supported,
                extract_status,
                file_state,
                first_seen_at,
                last_seen_at
            ) VALUES (
                'id-1',
                'scan_old',
                '/photos/clip.mp4',
                '/photos',
                'clip.mp4',
                '.mp4',
                'unknown',
                100,
                TIMESTAMP '2026-03-30 00:00:00',
                TIMESTAMP '2026-03-30 00:00:00',
                '/photos',
                TIMESTAMP '2026-03-30 00:00:00',
                FALSE,
                'skipped_unsupported',
                'unchanged',
                TIMESTAMP '2026-03-30 00:00:00',
                TIMESTAMP '2026-03-30 00:00:00'
            )
            """
        )

    scan_time = datetime(2026, 3, 31, tzinfo=timezone.utc)
    touched = store.touch_unchanged_records(
        scan_root="/photos",
        scan_id="scan_new",
        scan_time=scan_time,
        records=[
            FileScanRecord(
                file_id="id-1",
                path="/photos/clip.mp4",
                parent_folder="/photos",
                filename="clip.mp4",
                extension=".mp4",
                media_type="video",
                size_bytes=100,
                fs_created_at=scan_time,
                fs_modified_at=scan_time,
                scan_root="/photos",
                scan_time=scan_time,
                is_supported=True,
                scan_error=None,
            )
        ],
    )

    assert touched == 1

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT scan_id, media_type, is_supported, file_state
            FROM file_metadata
            WHERE path = '/photos/clip.mp4'
            """
        ).fetchone()

    assert row is not None
    assert row[0] == "scan_new"
    assert row[1] == "video"
    assert row[2] is True
    assert row[3] == "unchanged"


def test_get_latest_scan_id_for_root_and_load_active_files_for_scan(tmp_path: Path) -> None:
    db_path = tmp_path / "audit.duckdb"
    store = DuckDBStore(db_path=db_path)
    store.initialize()

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO scans (
                scan_id, scan_root, started_at, finished_at,
                files_discovered, supported_files,
                new_files, changed_files, unchanged_files, missing_files,
                extraction_attempted, extraction_successful, extraction_failed,
                dry_run
            )
            VALUES
                ('scan_old', '/primary', TIMESTAMP '2026-03-30 10:00:00', TIMESTAMP '2026-03-30 10:01:00', 1, 1, 1, 0, 0, 0, 1, 1, 0, FALSE),
                ('scan_new', '/primary', TIMESTAMP '2026-03-30 10:05:00', TIMESTAMP '2026-03-30 10:06:00', 2, 2, 1, 1, 0, 0, 2, 2, 0, FALSE),
                ('scan_backup', '/backup', TIMESTAMP '2026-03-30 11:00:00', TIMESTAMP '2026-03-30 11:01:00', 2, 2, 1, 1, 0, 0, 2, 2, 0, FALSE)
            """
        )
        conn.execute(
            """
            INSERT INTO file_metadata (
                file_id, scan_id, path, parent_folder, filename, extension,
                media_type, size_bytes, fs_created_at, fs_modified_at,
                scan_root, scan_time, is_supported, extract_status, file_state, content_sha256
            )
            VALUES
                ('p1', 'scan_new', '/primary/a.jpg', '/primary', 'a.jpg', '.jpg',
                 'image', 10, NOW(), NOW(), '/primary', NOW(), TRUE, 'success', 'unchanged', 'hash-1'),
                ('p2', 'scan_new', '/primary/b.jpg', '/primary', 'b.jpg', '.jpg',
                 'image', 11, NOW(), NOW(), '/primary', NOW(), TRUE, 'success', 'missing', 'hash-2'),
                ('b1', 'scan_backup', '/backup/x.jpg', '/backup', 'x.jpg', '.jpg',
                 'image', 12, NOW(), NOW(), '/backup', NOW(), TRUE, 'success', 'unchanged', 'hash-1')
            """
        )

    latest_primary = store.get_latest_scan_id_for_root("/primary")
    assert latest_primary == "scan_new"

    active_primary = store.load_active_files_for_scan(scan_id="scan_new", scan_root="/primary")
    assert len(active_primary) == 1
    assert active_primary[0].file_id == "p1"
    assert active_primary[0].content_sha256 == "hash-1"
