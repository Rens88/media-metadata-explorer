from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import shutil
import subprocess
from time import perf_counter

from photo_archive.database import DuckDBStore
from photo_archive.models import VideoFrameJob, VideoFrameRecord, VideoFrameSourceRecord
from photo_archive.progress import ProgressPrinter

try:
    from PIL import Image
except ImportError:  # pragma: no cover - exercised through runtime error path
    Image = None


@dataclass(slots=True)
class VideoFramePipelineResult:
    db_path: Path
    out_dir: Path
    interval_sec: float
    summary: dict[str, int | float]


def run_video_frame_pipeline(
    *,
    db_path: Path,
    out_dir: Path,
    interval_sec: float = 10.0,
    progress: ProgressPrinter | None = None,
) -> VideoFramePipelineResult:
    if interval_sec <= 0:
        raise ValueError("interval_sec must be a positive number")

    progress_printer = progress or ProgressPrinter(enabled=False)
    run_started_clock = perf_counter()
    resolved_out_dir = out_dir.expanduser().resolve()

    progress_printer.info(
        topic="RUN",
        purpose="start video frame pipeline",
        expectation="load videos -> select frames -> extract -> persist -> summary",
        details=f"out_dir={resolved_out_dir}, interval_sec={interval_sec}",
    )

    progress_printer.start(
        topic="STATE",
        purpose="load indexed videos and existing frame rows",
        expectation="identify frames that require generation",
    )
    store = DuckDBStore(db_path=db_path)
    store.initialize()
    sources = store.load_video_frame_sources()
    existing_by_key = store.load_video_frames_by_key()
    jobs = select_video_frame_jobs(
        source_records=sources,
        existing_by_key=existing_by_key,
        out_dir=resolved_out_dir,
        interval_sec=interval_sec,
    )
    supported_video_count = sum(1 for item in sources if item.is_supported)
    planned_frame_total = sum(
        len(_planned_frame_specs(item.video_duration_seconds, interval_sec))
        for item in sources
        if item.is_supported
    )
    skipped_count = max(0, planned_frame_total - len(jobs))
    state_duration_seconds = progress_printer.done(
        "STATE",
        details=(
            f"videos={len(sources)}, supported={supported_video_count}, "
            f"planned_frames={planned_frame_total}, to_generate={len(jobs)}, skipped={skipped_count}"
        ),
    )

    progress_printer.start(
        topic="CLEAN",
        purpose="remove stale frame rows/files for missing videos",
        expectation="cleanup should preserve active frame rows",
    )
    stale_rows_deleted, stale_files_removed, stale_file_delete_errors = cleanup_stale_video_frames(
        store=store,
        out_dir=resolved_out_dir,
    )
    cleanup_duration_seconds = progress_printer.done(
        "CLEAN",
        details=(
            f"stale_rows_deleted={stale_rows_deleted}, "
            f"files_removed={stale_files_removed}, "
            f"file_delete_errors={stale_file_delete_errors}"
        ),
    )

    progress_printer.start(
        topic="FRAMES",
        purpose="extract sampled video frames",
        expectation="per-frame success/failure with no run-wide crash",
        details=f"jobs={len(jobs)}",
    )
    ffmpeg_available = shutil.which("ffmpeg") is not None
    generated_count = 0
    failed_count = 0
    persisted_rows: list[VideoFrameRecord] = []
    generated_at = datetime.now(timezone.utc)

    for job in jobs:
        status = "success"
        error: str | None = None
        width: int | None = None
        height: int | None = None

        try:
            width, height = generate_video_frame(
                source_path=Path(job.source_path),
                frame_path=Path(job.frame_path),
                frame_time_sec=job.frame_time_sec,
                max_size=512,
                ffmpeg_available=ffmpeg_available,
            )
            generated_count += 1
        except Exception as exc:  # noqa: BLE001 - per-frame isolation
            status = "failed"
            error = f"{type(exc).__name__}: {exc}"
            failed_count += 1

        persisted_rows.append(
            VideoFrameRecord(
                file_id=job.file_id,
                frame_index=job.frame_index,
                frame_time_sec=job.frame_time_sec,
                frame_path=job.frame_path,
                width=width,
                height=height,
                status=status,
                error=error,
                generated_at=generated_at,
            )
        )

    generation_duration_seconds = progress_printer.done(
        "FRAMES",
        details=f"generated={generated_count}, failed={failed_count}",
    )

    progress_printer.start(
        topic="PERSIST",
        purpose="write frame statuses to duckdb",
        expectation="upsert rows by (file_id, frame_index)",
    )
    store.upsert_video_frame_records(persisted_rows)
    persist_duration_seconds = progress_printer.done(
        "PERSIST",
        details=f"upserted={len(persisted_rows)}",
    )

    run_duration_seconds = perf_counter() - run_started_clock
    progress_printer.info(
        topic="SUMMARY",
        purpose="finalize frame run metrics",
        expectation="print generated/skipped/failed frame counts",
        details=f"run_duration={run_duration_seconds:.2f}s",
    )

    summary = {
        "videos_indexed": len(sources),
        "videos_supported": supported_video_count,
        "frames_planned": planned_frame_total,
        "frames_selected": len(jobs),
        "frames_generated": generated_count,
        "frames_skipped": skipped_count,
        "frames_failed": failed_count,
        "state_duration_seconds": state_duration_seconds,
        "cleanup_duration_seconds": cleanup_duration_seconds,
        "generation_duration_seconds": generation_duration_seconds,
        "persist_duration_seconds": persist_duration_seconds,
        "run_duration_seconds": run_duration_seconds,
        "stale_rows_deleted": stale_rows_deleted,
        "stale_files_removed": stale_files_removed,
        "stale_file_delete_errors": stale_file_delete_errors,
    }

    return VideoFramePipelineResult(
        db_path=db_path.expanduser().resolve(),
        out_dir=resolved_out_dir,
        interval_sec=interval_sec,
        summary=summary,
    )


def select_video_frame_jobs(
    *,
    source_records: list[VideoFrameSourceRecord],
    existing_by_key: dict[tuple[str, int], VideoFrameRecord],
    out_dir: Path,
    interval_sec: float,
) -> list[VideoFrameJob]:
    jobs: list[VideoFrameJob] = []

    for source in source_records:
        if not source.is_supported:
            continue
        if (source.media_type or "").lower() != "video":
            continue

        for frame_index, frame_time_sec in _planned_frame_specs(source.video_duration_seconds, interval_sec):
            expected_path = video_frame_path_for(
                file_id=source.file_id,
                frame_index=frame_index,
                out_dir=out_dir,
            )
            existing = existing_by_key.get((source.file_id, frame_index))
            trigger = _video_frame_trigger(
                source=source,
                expected_path=expected_path,
                existing=existing,
            )
            if trigger is None:
                continue
            jobs.append(
                VideoFrameJob(
                    file_id=source.file_id,
                    source_path=source.path,
                    frame_index=frame_index,
                    frame_time_sec=frame_time_sec,
                    frame_path=str(expected_path),
                    trigger=trigger,
                )
            )
    return jobs


def video_frame_path_for(*, file_id: str, frame_index: int, out_dir: Path) -> Path:
    shard = file_id[:2] if len(file_id) >= 2 else "00"
    return out_dir / shard / file_id / f"{file_id}_f{frame_index:05d}.jpg"


def generate_video_frame(
    *,
    source_path: Path,
    frame_path: Path,
    frame_time_sec: float,
    max_size: int,
    ffmpeg_available: bool,
) -> tuple[int, int]:
    if not ffmpeg_available:
        raise RuntimeError("ffmpeg_not_found")
    if Image is None:
        raise RuntimeError("pillow_not_installed")
    if not source_path.exists():
        raise FileNotFoundError(f"source file not found: {source_path}")
    if max_size <= 0:
        raise ValueError("max_size must be positive")

    frame_path.parent.mkdir(parents=True, exist_ok=True)
    scale_filter = f"scale='if(gt(iw,ih),{max_size},-2)':'if(gt(iw,ih),-2,{max_size})'"
    command = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-ss",
        f"{max(0.0, float(frame_time_sec)):.3f}",
        "-i",
        str(source_path),
        "-frames:v",
        "1",
        "-vf",
        scale_filter,
        str(frame_path),
    ]
    process = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )
    if process.returncode != 0:
        message = (process.stderr or "").strip() or "no_stderr"
        raise RuntimeError(f"ffmpeg_failed_rc_{process.returncode}: {message}")
    if not frame_path.exists():
        raise RuntimeError("ffmpeg_missing_output_file")

    with Image.open(frame_path) as image:
        width, height = image.size
    return int(width), int(height)


def cleanup_stale_video_frames(
    *,
    store: DuckDBStore,
    out_dir: Path,
) -> tuple[int, int, int]:
    stale_records = store.load_stale_video_frames()
    if not stale_records:
        return 0, 0, 0

    files_removed = 0
    file_delete_errors = 0
    keys: list[tuple[str, int]] = []
    for record in stale_records:
        keys.append((record.file_id, int(record.frame_index)))
        removed, had_error = _remove_frame_file(record.frame_path, out_dir=out_dir)
        if removed:
            files_removed += 1
        if had_error:
            file_delete_errors += 1

    deleted_rows = store.delete_video_frames_by_keys(keys)
    return deleted_rows, files_removed, file_delete_errors


def format_video_frame_summary(summary: dict[str, int | float]) -> str:
    return "\n".join(
        [
            f"Indexed videos: {summary.get('videos_indexed', 0)}",
            f"Supported videos: {summary.get('videos_supported', 0)}",
            f"Planned frames: {summary.get('frames_planned', 0)}",
            f"Selected for frame extraction: {summary.get('frames_selected', 0)}",
            f"Frames generated: {summary.get('frames_generated', 0)}",
            f"Frames skipped: {summary.get('frames_skipped', 0)}",
            f"Frames failed: {summary.get('frames_failed', 0)}",
            (
                "Stale cleanup:"
                f" rows_deleted={summary.get('stale_rows_deleted', 0)},"
                f" files_removed={summary.get('stale_files_removed', 0)},"
                f" file_delete_errors={summary.get('stale_file_delete_errors', 0)}"
            ),
            f"Run duration: {_format_duration(float(summary.get('run_duration_seconds', 0.0)))}",
        ]
    )


def _planned_frame_specs(duration_seconds: float | None, interval_sec: float) -> list[tuple[int, float]]:
    if interval_sec <= 0:
        raise ValueError("interval_sec must be positive")
    if duration_seconds is None or duration_seconds <= 0:
        return [(0, 0.0)]

    specs: list[tuple[int, float]] = []
    frame_index = 0
    current_time = 0.0
    while current_time < duration_seconds:
        specs.append((frame_index, round(current_time, 3)))
        frame_index += 1
        current_time += interval_sec

    if not specs:
        return [(0, 0.0)]
    return specs


def _video_frame_trigger(
    *,
    source: VideoFrameSourceRecord,
    expected_path: Path,
    existing: VideoFrameRecord | None,
) -> str | None:
    file_state = (source.file_state or "").lower()
    if file_state in {"new", "changed"}:
        return f"file_state_{file_state}"

    if existing is None:
        return "missing_frame_row"

    if existing.status != "success":
        return f"frame_status_{existing.status}"

    if not existing.frame_path:
        return "missing_frame_path"

    existing_path = Path(existing.frame_path)
    if existing_path != expected_path:
        return "out_dir_changed"

    if existing.width is None or existing.height is None:
        return "missing_dimensions"

    if not existing_path.exists():
        return "frame_file_missing"

    return None


def _format_duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes = int(seconds // 60)
    rem = seconds - (minutes * 60)
    return f"{minutes}m {rem:.1f}s"


def _remove_frame_file(frame_path: str | None, *, out_dir: Path) -> tuple[bool, bool]:
    if not frame_path:
        return False, False

    out_dir_resolved = out_dir.expanduser().resolve(strict=False)
    candidate = Path(frame_path).expanduser().resolve(strict=False)
    try:
        candidate.relative_to(out_dir_resolved)
    except ValueError:
        return False, False

    if not candidate.exists():
        return False, False

    try:
        candidate.unlink()
        return True, False
    except OSError:
        return False, True
