from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import shutil
import subprocess
from time import perf_counter

from photo_archive.database import DuckDBStore
from photo_archive.models import ThumbnailJob, ThumbnailRecord, ThumbnailSourceRecord
from photo_archive.progress import ProgressPrinter

try:
    from PIL import Image, ImageOps, UnidentifiedImageError
except ImportError:  # pragma: no cover - exercised through runtime error path
    Image = None
    ImageOps = None
    UnidentifiedImageError = OSError


@dataclass(slots=True)
class ThumbnailPipelineResult:
    db_path: Path
    out_dir: Path
    max_size: int
    summary: dict[str, int | float]


def run_thumbnail_pipeline(
    *,
    db_path: Path,
    out_dir: Path,
    max_size: int = 512,
    progress: ProgressPrinter | None = None,
) -> ThumbnailPipelineResult:
    if max_size <= 0:
        raise ValueError("max_size must be a positive integer")

    progress_printer = progress or ProgressPrinter(enabled=False)
    run_started_clock = perf_counter()
    resolved_out_dir = out_dir.expanduser().resolve()

    progress_printer.info(
        topic="RUN",
        purpose="start thumbnail pipeline",
        expectation="read index -> select jobs -> generate -> persist -> summary",
        details=f"out_dir={resolved_out_dir}, max_size={max_size}",
    )

    progress_printer.start(
        topic="STATE",
        purpose="load indexed files and current thumbnail rows",
        expectation="identify supported files that need thumbnail generation",
    )
    store = DuckDBStore(db_path=db_path)
    store.initialize()
    source_records = store.load_thumbnail_sources()
    existing_by_file_id = store.load_thumbnails_by_file_id()
    jobs = select_thumbnail_jobs(
        source_records=source_records,
        existing_by_file_id=existing_by_file_id,
        out_dir=resolved_out_dir,
    )
    ffmpeg_available = shutil.which("ffmpeg") is not None
    eligible_media_types = {"image", "video"}
    supported_source_count = sum(
        1
        for item in source_records
        if item.is_supported and (item.media_type or "image").lower() in eligible_media_types
    )
    skipped_count = max(0, supported_source_count - len(jobs))
    state_duration_seconds = progress_printer.done(
        "STATE",
        details=(
            f"sources={len(source_records)}, supported={supported_source_count}, "
            f"to_generate={len(jobs)}, skipped={skipped_count}"
        ),
    )

    progress_printer.start(
        topic="CLEAN",
        purpose="remove stale thumbnail rows/files for missing indexed files",
        expectation="cleanup should be safe and not affect active file thumbnails",
    )
    stale_rows_deleted, stale_files_removed, stale_file_delete_errors = cleanup_stale_thumbnails(
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
        topic="THUMB",
        purpose="generate thumbnails",
        expectation="one output file per selected source with per-file failure isolation",
        details=f"jobs={len(jobs)}",
    )
    generated_count = 0
    failed_count = 0
    persisted_rows: list[ThumbnailRecord] = []
    generated_at = datetime.now(timezone.utc)

    for job in jobs:
        status = "success"
        error: str | None = None
        width: int | None = None
        height: int | None = None

        try:
            width, height = generate_thumbnail(
                source_path=Path(job.source_path),
                thumb_path=Path(job.thumb_path),
                max_size=max_size,
                media_type=job.media_type,
                ffmpeg_available=ffmpeg_available,
            )
            generated_count += 1
        except Exception as exc:  # noqa: BLE001 - per-file isolation
            status = "failed"
            error = f"{type(exc).__name__}: {exc}"
            failed_count += 1

        persisted_rows.append(
            ThumbnailRecord(
                file_id=job.file_id,
                thumb_path=job.thumb_path,
                width=width,
                height=height,
                status=status,
                error=error,
                generated_at=generated_at,
            )
        )

    generation_duration_seconds = progress_printer.done(
        "THUMB",
        details=f"generated={generated_count}, failed={failed_count}",
    )

    progress_printer.start(
        topic="PERSIST",
        purpose="write thumbnail statuses to duckdb",
        expectation="upsert rows by file_id",
    )
    store.upsert_thumbnail_records(persisted_rows)
    persist_duration_seconds = progress_printer.done(
        "PERSIST",
        details=f"upserted={len(persisted_rows)}",
    )

    run_duration_seconds = perf_counter() - run_started_clock
    progress_printer.info(
        topic="SUMMARY",
        purpose="finalize thumbnail run metrics",
        expectation="print generated/skipped/failed counts",
        details=f"run_duration={run_duration_seconds:.2f}s",
    )

    summary = {
        "files_indexed": len(source_records),
        "supported_files": supported_source_count,
        "thumbnails_selected": len(jobs),
        "thumbnails_generated": generated_count,
        "thumbnails_skipped": skipped_count,
        "thumbnails_failed": failed_count,
        "state_duration_seconds": state_duration_seconds,
        "cleanup_duration_seconds": cleanup_duration_seconds,
        "generation_duration_seconds": generation_duration_seconds,
        "persist_duration_seconds": persist_duration_seconds,
        "run_duration_seconds": run_duration_seconds,
        "stale_rows_deleted": stale_rows_deleted,
        "stale_files_removed": stale_files_removed,
        "stale_file_delete_errors": stale_file_delete_errors,
    }

    return ThumbnailPipelineResult(
        db_path=db_path.expanduser().resolve(),
        out_dir=resolved_out_dir,
        max_size=max_size,
        summary=summary,
    )


def select_thumbnail_jobs(
    *,
    source_records: list[ThumbnailSourceRecord],
    existing_by_file_id: dict[str, ThumbnailRecord],
    out_dir: Path,
) -> list[ThumbnailJob]:
    jobs: list[ThumbnailJob] = []
    eligible_media_types = {"image", "video"}
    for source in source_records:
        if not source.is_supported:
            continue
        source_media_type = (source.media_type or "image").lower()
        if source_media_type not in eligible_media_types:
            continue

        expected_thumb_path = thumbnail_path_for_file_id(source.file_id, out_dir)
        existing = existing_by_file_id.get(source.file_id)
        trigger = _thumbnail_trigger(
            source=source,
            expected_thumb_path=expected_thumb_path,
            existing=existing,
        )
        if trigger is None:
            continue

        jobs.append(
            ThumbnailJob(
                file_id=source.file_id,
                source_path=source.path,
                thumb_path=str(expected_thumb_path),
                media_type=source_media_type,
                trigger=trigger,
            )
        )
    return jobs


def thumbnail_path_for_file_id(file_id: str, out_dir: Path) -> Path:
    shard = file_id[:2] if len(file_id) >= 2 else "00"
    return out_dir / shard / f"{file_id}.jpg"


def generate_thumbnail(
    *,
    source_path: Path,
    thumb_path: Path,
    max_size: int,
    media_type: str,
    ffmpeg_available: bool,
) -> tuple[int, int]:
    media_type_value = media_type.lower().strip()
    if media_type_value == "video":
        return generate_video_thumbnail(
            source_path=source_path,
            thumb_path=thumb_path,
            max_size=max_size,
            ffmpeg_available=ffmpeg_available,
        )
    if media_type_value != "image":
        raise RuntimeError(f"unsupported_thumbnail_media_type: {media_type_value or 'unknown'}")
    try:
        return generate_image_thumbnail(
            source_path=source_path,
            thumb_path=thumb_path,
            max_size=max_size,
        )
    except Exception as exc:  # noqa: BLE001 - controlled fallback decision
        if _should_fallback_to_ffmpeg_image(
            source_path=source_path,
            error=exc,
            ffmpeg_available=ffmpeg_available,
        ):
            return generate_video_thumbnail(
                source_path=source_path,
                thumb_path=thumb_path,
                max_size=max_size,
                ffmpeg_available=ffmpeg_available,
            )
        raise


def generate_image_thumbnail(*, source_path: Path, thumb_path: Path, max_size: int) -> tuple[int, int]:
    if Image is None or ImageOps is None:
        raise RuntimeError("pillow_not_installed")
    if not source_path.exists():
        raise FileNotFoundError(f"source file not found: {source_path}")

    thumb_path.parent.mkdir(parents=True, exist_ok=True)
    with Image.open(source_path) as opened:
        image = ImageOps.exif_transpose(opened)
        if image.mode not in {"RGB", "L"}:
            image = image.convert("RGB")
        elif image.mode == "L":
            image = image.convert("RGB")
        image.thumbnail((max_size, max_size), _lanczos_resampling())
        width, height = image.size
        image.save(thumb_path, format="JPEG", quality=85, optimize=True)
        return width, height


def generate_video_thumbnail(
    *,
    source_path: Path,
    thumb_path: Path,
    max_size: int,
    ffmpeg_available: bool,
) -> tuple[int, int]:
    if not ffmpeg_available:
        raise RuntimeError("ffmpeg_not_found")
    if not source_path.exists():
        raise FileNotFoundError(f"source file not found: {source_path}")

    thumb_path.parent.mkdir(parents=True, exist_ok=True)
    scale_filter = (
        f"scale='if(gt(iw,ih),{max_size},-2)':'if(gt(iw,ih),-2,{max_size})'"
    )
    command = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(source_path),
        "-vf",
        scale_filter,
        "-frames:v",
        "1",
        str(thumb_path),
    ]
    process = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )
    if process.returncode != 0:
        stderr = (process.stderr or "").strip()
        message = stderr or "no_stderr"
        raise RuntimeError(f"ffmpeg_failed_rc_{process.returncode}: {message}")
    if not thumb_path.exists():
        raise RuntimeError("ffmpeg_missing_output_file")

    if Image is None:
        raise RuntimeError("pillow_not_installed")
    with Image.open(thumb_path) as image:
        width, height = image.size
    return int(width), int(height)


def format_thumbnail_summary(summary: dict[str, int | float]) -> str:
    return "\n".join(
        [
            f"Indexed files: {summary.get('files_indexed', 0)}",
            f"Supported files: {summary.get('supported_files', 0)}",
            f"Selected for thumbnail generation: {summary.get('thumbnails_selected', 0)}",
            f"Thumbnails generated: {summary.get('thumbnails_generated', 0)}",
            f"Thumbnails skipped: {summary.get('thumbnails_skipped', 0)}",
            f"Thumbnails failed: {summary.get('thumbnails_failed', 0)}",
            (
                "Stale cleanup:"
                f" rows_deleted={summary.get('stale_rows_deleted', 0)},"
                f" files_removed={summary.get('stale_files_removed', 0)},"
                f" file_delete_errors={summary.get('stale_file_delete_errors', 0)}"
            ),
            f"Run duration: {_format_duration(float(summary.get('run_duration_seconds', 0.0)))}",
        ]
    )


def cleanup_stale_thumbnails(
    *,
    store: DuckDBStore,
    out_dir: Path,
) -> tuple[int, int, int]:
    stale_records = store.load_stale_thumbnails()
    if not stale_records:
        return 0, 0, 0

    files_removed = 0
    file_delete_errors = 0
    for record in stale_records:
        removed, had_error = _remove_thumbnail_file(record.thumb_path, out_dir=out_dir)
        if removed:
            files_removed += 1
        if had_error:
            file_delete_errors += 1

    deleted_rows = store.delete_thumbnails_by_file_ids([item.file_id for item in stale_records])
    return deleted_rows, files_removed, file_delete_errors


def _thumbnail_trigger(
    *,
    source: ThumbnailSourceRecord,
    expected_thumb_path: Path,
    existing: ThumbnailRecord | None,
) -> str | None:
    file_state = (source.file_state or "").lower()
    if file_state in {"new", "changed"}:
        return f"file_state_{file_state}"

    if existing is None:
        return "missing_thumbnail_row"

    if existing.status != "success":
        return f"thumbnail_status_{existing.status}"

    if not existing.thumb_path:
        return "missing_thumbnail_path"

    existing_path = Path(existing.thumb_path)
    if existing_path != expected_thumb_path:
        return "out_dir_changed"

    if existing.width is None or existing.height is None:
        return "missing_dimensions"

    if not existing_path.exists():
        return "thumbnail_file_missing"

    return None


def _lanczos_resampling() -> int:
    if Image is None:
        return 1
    if hasattr(Image, "Resampling"):
        return int(Image.Resampling.LANCZOS)
    return int(Image.LANCZOS)


def _should_fallback_to_ffmpeg_image(
    *,
    source_path: Path,
    error: Exception,
    ffmpeg_available: bool,
) -> bool:
    if not ffmpeg_available:
        return False
    if isinstance(error, FileNotFoundError):
        return False
    if not source_path.exists():
        return False

    extension = source_path.suffix.lower()
    if extension in {".heic", ".heif", ".avif"}:
        return True
    if isinstance(error, UnidentifiedImageError):
        return True
    if isinstance(error, OSError):
        message = str(error).lower()
        return (
            "cannot identify image file" in message
            or "truncated file" in message
            or "cannot load this image" in message
        )
    return False


def _format_duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes = int(seconds // 60)
    rem = seconds - (minutes * 60)
    return f"{minutes}m {rem:.1f}s"


def _remove_thumbnail_file(thumb_path: str | None, *, out_dir: Path) -> tuple[bool, bool]:
    if not thumb_path:
        return False, False

    out_dir_resolved = out_dir.expanduser().resolve(strict=False)
    candidate = Path(thumb_path).expanduser().resolve(strict=False)
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
