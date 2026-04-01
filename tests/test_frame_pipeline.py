from datetime import datetime, timezone
from pathlib import Path

import duckdb
from PIL import Image

from photo_archive.database import DuckDBStore
from photo_archive.frame_pipeline import (
    cleanup_stale_video_frames,
    generate_video_frame,
    select_video_frame_jobs,
    video_frame_path_for,
)
from photo_archive.models import VideoFrameRecord, VideoFrameSourceRecord


def test_select_video_frame_jobs_incremental_rules(tmp_path: Path) -> None:
    out_dir = tmp_path / "frames"
    now = datetime(2026, 3, 31, tzinfo=timezone.utc)

    existing_ok_path = video_frame_path_for(file_id="vid-c", frame_index=0, out_dir=out_dir)
    existing_ok_path.parent.mkdir(parents=True, exist_ok=True)
    existing_ok_path.write_bytes(b"ok")

    missing_file_path = video_frame_path_for(file_id="vid-g", frame_index=0, out_dir=out_dir)
    out_dir_changed_path = tmp_path / "legacy_frames" / "vid-h_f00000.jpg"
    out_dir_changed_path.parent.mkdir(parents=True, exist_ok=True)
    out_dir_changed_path.write_bytes(b"legacy")

    source_records = [
        VideoFrameSourceRecord("vid-a", "/videos/a.mp4", "video", "new", True, "success", None),
        VideoFrameSourceRecord("vid-b", "/videos/b.mp4", "video", "changed", True, "success", None),
        VideoFrameSourceRecord("vid-c", "/videos/c.mp4", "video", "unchanged", True, "success", None),
        VideoFrameSourceRecord("vid-d", "/videos/d.mp4", "video", "unchanged", True, "success", None),
        VideoFrameSourceRecord("vid-e", "/videos/e.mp4", "video", "unchanged", True, "success", None),
        VideoFrameSourceRecord("vid-f", "/videos/f.mp4", "video", "unchanged", False, "failed", None),
        VideoFrameSourceRecord("vid-g", "/videos/g.mp4", "video", "unchanged", True, "success", None),
        VideoFrameSourceRecord("vid-h", "/videos/h.mp4", "video", "unchanged", True, "success", None),
        VideoFrameSourceRecord("img-1", "/photos/p.jpg", "image", "new", True, "success", None),
    ]
    existing_by_key = {
        ("vid-c", 0): VideoFrameRecord(
            file_id="vid-c",
            frame_index=0,
            frame_time_sec=0.0,
            frame_path=str(existing_ok_path),
            width=320,
            height=180,
            status="success",
            error=None,
            generated_at=now,
        ),
        ("vid-e", 0): VideoFrameRecord(
            file_id="vid-e",
            frame_index=0,
            frame_time_sec=0.0,
            frame_path=str(video_frame_path_for(file_id="vid-e", frame_index=0, out_dir=out_dir)),
            width=None,
            height=None,
            status="failed",
            error="previous_failure",
            generated_at=now,
        ),
        ("vid-g", 0): VideoFrameRecord(
            file_id="vid-g",
            frame_index=0,
            frame_time_sec=0.0,
            frame_path=str(missing_file_path),
            width=320,
            height=180,
            status="success",
            error=None,
            generated_at=now,
        ),
        ("vid-h", 0): VideoFrameRecord(
            file_id="vid-h",
            frame_index=0,
            frame_time_sec=0.0,
            frame_path=str(out_dir_changed_path),
            width=320,
            height=180,
            status="success",
            error=None,
            generated_at=now,
        ),
    }

    jobs = select_video_frame_jobs(
        source_records=source_records,
        existing_by_key=existing_by_key,
        out_dir=out_dir,
        interval_sec=10.0,
    )
    jobs_by_id = {item.file_id: item for item in jobs}

    assert set(jobs_by_id) == {"vid-a", "vid-b", "vid-d", "vid-e", "vid-g", "vid-h"}
    assert jobs_by_id["vid-a"].trigger == "file_state_new"
    assert jobs_by_id["vid-b"].trigger == "file_state_changed"
    assert jobs_by_id["vid-d"].trigger == "missing_frame_row"
    assert jobs_by_id["vid-e"].trigger == "frame_status_failed"
    assert jobs_by_id["vid-g"].trigger == "frame_file_missing"
    assert jobs_by_id["vid-h"].trigger == "out_dir_changed"


def test_video_frame_db_upsert_and_source_loading(tmp_path: Path) -> None:
    db_path = tmp_path / "frames.duckdb"
    store = DuckDBStore(db_path=db_path)
    store.initialize()

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO file_metadata (
                file_id, path, media_type, is_supported, file_state, extract_status, video_duration_seconds
            )
            VALUES
                ('vid-1', '/videos/a.mp4', 'video', TRUE, 'new', 'success', 42.0),
                ('img-1', '/photos/a.jpg', 'image', TRUE, 'new', 'success', NULL),
                ('vid-2', '/videos/b.mp4', 'video', TRUE, 'missing', 'missing', 10.0)
            """
        )

    sources = store.load_video_frame_sources()
    assert [item.file_id for item in sources] == ["vid-1"]
    assert sources[0].video_duration_seconds == 42.0

    now = datetime(2026, 3, 31, tzinfo=timezone.utc)
    store.upsert_video_frame_records(
        [
            VideoFrameRecord(
                file_id="vid-1",
                frame_index=0,
                frame_time_sec=0.0,
                frame_path="/frames/vid-1_f00000.jpg",
                width=256,
                height=144,
                status="success",
                error=None,
                generated_at=now,
            )
        ]
    )

    loaded = store.load_video_frames_by_key()
    assert ("vid-1", 0) in loaded
    assert loaded[("vid-1", 0)].status == "success"


def test_cleanup_stale_video_frames_removes_rows_and_files(tmp_path: Path) -> None:
    db_path = tmp_path / "frames_cleanup.duckdb"
    out_dir = tmp_path / "frames"
    out_dir.mkdir(parents=True, exist_ok=True)

    active_frame = out_dir / "aa" / "vid-active" / "vid-active_f00000.jpg"
    stale_frame = out_dir / "bb" / "vid-stale" / "vid-stale_f00000.jpg"
    outside_frame = tmp_path / "outside.jpg"
    for candidate in (active_frame, stale_frame, outside_frame):
        candidate.parent.mkdir(parents=True, exist_ok=True)
        candidate.write_bytes(b"x")

    store = DuckDBStore(db_path=db_path)
    store.initialize()
    now = datetime(2026, 3, 31, tzinfo=timezone.utc)

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO file_metadata (file_id, path, media_type, is_supported, file_state, extract_status)
            VALUES ('vid-active', '/videos/active.mp4', 'video', TRUE, 'unchanged', 'success')
            """
        )

    store.upsert_video_frame_records(
        [
            VideoFrameRecord(
                file_id="vid-active",
                frame_index=0,
                frame_time_sec=0.0,
                frame_path=str(active_frame),
                width=100,
                height=80,
                status="success",
                error=None,
                generated_at=now,
            ),
            VideoFrameRecord(
                file_id="vid-stale",
                frame_index=0,
                frame_time_sec=0.0,
                frame_path=str(stale_frame),
                width=100,
                height=80,
                status="success",
                error=None,
                generated_at=now,
            ),
            VideoFrameRecord(
                file_id="vid-stale-2",
                frame_index=0,
                frame_time_sec=0.0,
                frame_path=str(outside_frame),
                width=100,
                height=80,
                status="success",
                error=None,
                generated_at=now,
            ),
        ]
    )

    deleted_rows, files_removed, file_delete_errors = cleanup_stale_video_frames(
        store=store,
        out_dir=out_dir,
    )

    assert deleted_rows == 2
    assert files_removed == 1
    assert file_delete_errors == 0
    assert active_frame.exists()
    assert not stale_frame.exists()
    assert outside_frame.exists()


class _DummyCompletedProcess:
    def __init__(self, returncode: int, stderr: str = "") -> None:
        self.returncode = returncode
        self.stderr = stderr
        self.stdout = ""


def test_generate_video_frame_requires_ffmpeg(tmp_path: Path) -> None:
    source = tmp_path / "clip.mp4"
    source.write_bytes(b"video")
    target = tmp_path / "frame.jpg"

    try:
        generate_video_frame(
            source_path=source,
            frame_path=target,
            frame_time_sec=0.0,
            max_size=512,
            ffmpeg_available=False,
        )
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        assert "ffmpeg_not_found" in str(exc)


def test_generate_video_frame_success_path(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "clip.mp4"
    source.write_bytes(b"video")
    target = tmp_path / "frame.jpg"

    def _fake_run(command, capture_output, text, check):  # noqa: ANN001
        output_path = Path(command[-1])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with Image.new("RGB", (320, 180), color=(0, 255, 0)) as generated:
            generated.save(output_path, format="JPEG")
        return _DummyCompletedProcess(returncode=0)

    monkeypatch.setattr("photo_archive.frame_pipeline.subprocess.run", _fake_run)

    width, height = generate_video_frame(
        source_path=source,
        frame_path=target,
        frame_time_sec=1.5,
        max_size=512,
        ffmpeg_available=True,
    )

    assert (width, height) == (320, 180)
    assert target.exists()
