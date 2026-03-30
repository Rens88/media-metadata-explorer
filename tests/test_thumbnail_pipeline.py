from datetime import datetime, timezone
from pathlib import Path

import duckdb

from photo_archive.database import DuckDBStore
from photo_archive.models import ThumbnailRecord, ThumbnailSourceRecord
from photo_archive.thumbnail_pipeline import (
    cleanup_stale_thumbnails,
    select_thumbnail_jobs,
    thumbnail_path_for_file_id,
)


def test_select_thumbnail_jobs_incremental_rules(tmp_path: Path) -> None:
    out_dir = tmp_path / "thumbnails"
    now = datetime(2026, 3, 30, tzinfo=timezone.utc)

    existing_ok_path = thumbnail_path_for_file_id("id-c", out_dir)
    existing_ok_path.parent.mkdir(parents=True, exist_ok=True)
    existing_ok_path.write_bytes(b"ok")

    missing_file_path = thumbnail_path_for_file_id("id-g", out_dir)
    out_dir_changed_path = tmp_path / "legacy_thumbs" / "id-h.jpg"
    out_dir_changed_path.parent.mkdir(parents=True, exist_ok=True)
    out_dir_changed_path.write_bytes(b"legacy")

    source_records = [
        ThumbnailSourceRecord("id-a", "/photos/a.jpg", "new", True, "success"),
        ThumbnailSourceRecord("id-b", "/photos/b.jpg", "changed", True, "success"),
        ThumbnailSourceRecord("id-c", "/photos/c.jpg", "unchanged", True, "success"),
        ThumbnailSourceRecord("id-d", "/photos/d.jpg", "unchanged", True, "success"),
        ThumbnailSourceRecord("id-e", "/photos/e.jpg", "unchanged", True, "success"),
        ThumbnailSourceRecord("id-f", "/photos/f.jpg", "unchanged", False, "skipped_unsupported"),
        ThumbnailSourceRecord("id-g", "/photos/g.jpg", "unchanged", True, "success"),
        ThumbnailSourceRecord("id-h", "/photos/h.jpg", "unchanged", True, "success"),
        ThumbnailSourceRecord("id-i", "/photos/i.jpg", "unchanged", True, "success"),
    ]
    existing_by_file_id = {
        "id-c": ThumbnailRecord(
            file_id="id-c",
            thumb_path=str(existing_ok_path),
            width=320,
            height=200,
            status="success",
            error=None,
            generated_at=now,
        ),
        "id-e": ThumbnailRecord(
            file_id="id-e",
            thumb_path=str(thumbnail_path_for_file_id("id-e", out_dir)),
            width=None,
            height=None,
            status="failed",
            error="previous_failure",
            generated_at=now,
        ),
        "id-g": ThumbnailRecord(
            file_id="id-g",
            thumb_path=str(missing_file_path),
            width=250,
            height=180,
            status="success",
            error=None,
            generated_at=now,
        ),
        "id-h": ThumbnailRecord(
            file_id="id-h",
            thumb_path=str(out_dir_changed_path),
            width=250,
            height=180,
            status="success",
            error=None,
            generated_at=now,
        ),
        "id-i": ThumbnailRecord(
            file_id="id-i",
            thumb_path=str(thumbnail_path_for_file_id("id-i", out_dir)),
            width=None,
            height=180,
            status="success",
            error=None,
            generated_at=now,
        ),
    }

    jobs = select_thumbnail_jobs(
        source_records=source_records,
        existing_by_file_id=existing_by_file_id,
        out_dir=out_dir,
    )
    jobs_by_id = {item.file_id: item for item in jobs}

    assert set(jobs_by_id) == {"id-a", "id-b", "id-d", "id-e", "id-g", "id-h", "id-i"}
    assert jobs_by_id["id-a"].trigger == "file_state_new"
    assert jobs_by_id["id-b"].trigger == "file_state_changed"
    assert jobs_by_id["id-d"].trigger == "missing_thumbnail_row"
    assert jobs_by_id["id-e"].trigger == "thumbnail_status_failed"
    assert jobs_by_id["id-g"].trigger == "thumbnail_file_missing"
    assert jobs_by_id["id-h"].trigger == "out_dir_changed"
    assert jobs_by_id["id-i"].trigger == "missing_dimensions"


def test_thumbnail_db_upsert_and_source_loading(tmp_path: Path) -> None:
    db_path = tmp_path / "thumbs.duckdb"
    store = DuckDBStore(db_path=db_path)
    store.initialize()

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO file_metadata (file_id, path, is_supported, file_state, extract_status)
            VALUES
                ('file-1', '/photos/a.jpg', TRUE, 'new', 'success'),
                ('file-2', '/photos/b.jpg', TRUE, 'missing', 'missing')
            """
        )

    sources = store.load_thumbnail_sources()
    assert [item.file_id for item in sources] == ["file-1"]

    now = datetime(2026, 3, 30, tzinfo=timezone.utc)
    store.upsert_thumbnail_records(
        [
            ThumbnailRecord(
                file_id="file-1",
                thumb_path="/thumbs/file-1.jpg",
                width=256,
                height=128,
                status="success",
                error=None,
                generated_at=now,
            )
        ]
    )

    loaded = store.load_thumbnails_by_file_id()
    assert "file-1" in loaded
    assert loaded["file-1"].thumb_path == "/thumbs/file-1.jpg"
    assert loaded["file-1"].status == "success"

    store.upsert_thumbnail_records(
        [
            ThumbnailRecord(
                file_id="file-1",
                thumb_path="/thumbs/file-1.jpg",
                width=None,
                height=None,
                status="failed",
                error="corrupt_image",
                generated_at=now,
            )
        ]
    )

    loaded_after = store.load_thumbnails_by_file_id()
    assert loaded_after["file-1"].status == "failed"
    assert loaded_after["file-1"].error == "corrupt_image"


def test_cleanup_stale_thumbnails_removes_rows_and_files(tmp_path: Path) -> None:
    db_path = tmp_path / "thumbs_cleanup.duckdb"
    out_dir = tmp_path / "thumbnails"
    out_dir.mkdir(parents=True, exist_ok=True)

    active_thumb = out_dir / "aa" / "active.jpg"
    stale_thumb = out_dir / "bb" / "stale.jpg"
    outside_thumb = tmp_path / "outside.jpg"
    for item in (active_thumb, stale_thumb, outside_thumb):
        item.parent.mkdir(parents=True, exist_ok=True)
        item.write_bytes(b"x")

    store = DuckDBStore(db_path=db_path)
    store.initialize()
    now = datetime(2026, 3, 30, tzinfo=timezone.utc)

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO file_metadata (file_id, path, is_supported, file_state, extract_status)
            VALUES ('active-1', '/photos/active.jpg', TRUE, 'unchanged', 'success')
            """
        )

    store.upsert_thumbnail_records(
        [
            ThumbnailRecord(
                file_id="active-1",
                thumb_path=str(active_thumb),
                width=100,
                height=80,
                status="success",
                error=None,
                generated_at=now,
            ),
            ThumbnailRecord(
                file_id="stale-1",
                thumb_path=str(stale_thumb),
                width=100,
                height=80,
                status="success",
                error=None,
                generated_at=now,
            ),
            ThumbnailRecord(
                file_id="stale-2",
                thumb_path=str(outside_thumb),
                width=100,
                height=80,
                status="success",
                error=None,
                generated_at=now,
            ),
        ]
    )

    deleted_rows, files_removed, file_delete_errors = cleanup_stale_thumbnails(
        store=store,
        out_dir=out_dir,
    )

    assert deleted_rows == 2
    assert files_removed == 1
    assert file_delete_errors == 0
    assert active_thumb.exists()
    assert not stale_thumb.exists()
    assert outside_thumb.exists()

    remaining = store.load_thumbnails_by_file_id()
    assert set(remaining.keys()) == {"active-1"}
