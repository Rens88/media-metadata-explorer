from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from uuid import uuid4

from photo_archive.config import normalize_extensions
from photo_archive.database import DuckDBStore
from photo_archive.extractors.exiftool_extractor import ExifToolExtractor
from photo_archive.extractors.ffprobe_extractor import FFprobeExtractor
from photo_archive.extractors.filename_parser import parse_filename_datetime
from photo_archive.hash_utils import hash_file_sha256
from photo_archive.incremental import classify_incremental_state
from photo_archive.models import (
    ExistingFileIndexRecord,
    ExtractionResult,
    FileScanRecord,
    FilenameParseRecord,
    NormalizedRecord,
    ScanHistoryRecord,
)
from photo_archive.normalize import normalize_record
from photo_archive.progress import ProgressPrinter
from photo_archive.reporting import build_run_summary
from photo_archive.scanner import scan_directory


@dataclass(slots=True)
class PipelineResult:
    records: list[NormalizedRecord]
    summary: dict[str, object]
    db_path: Path
    export_path: Path | None
    scan_id: str
    dry_run: bool


def run_pipeline(
    root_path: Path,
    db_path: Path,
    export_path: Path | None = None,
    extensions: list[str] | None = None,
    dry_run: bool = False,
    exif_batch_size: int = 200,
    full_rescan: bool = False,
    progress: ProgressPrinter | None = None,
) -> PipelineResult:
    progress_printer = progress or ProgressPrinter(enabled=False)

    run_started_clock = perf_counter()
    started_at = datetime.now(timezone.utc)
    scan_id = _build_scan_id(started_at)
    progress_printer.info(
        topic="RUN",
        purpose="start pipeline",
        expectation="scan -> state -> parse -> extract -> normalize -> persist -> summary",
        details=f"scan_id={scan_id}",
    )

    progress_printer.start(
        topic="SCAN",
        purpose="walk root and stat files",
        expectation="1 scan record per file",
    )
    supported_extensions = normalize_extensions(extensions)
    scan_records = scan_directory(root_path=root_path, supported_extensions=supported_extensions)
    scan_root = str(root_path.expanduser().resolve())
    scan_time = scan_records[0].scan_time if scan_records else datetime.now(timezone.utc)
    scan_duration_seconds = progress_printer.done(
        "SCAN",
        details=f"files_discovered={len(scan_records)}",
    )

    progress_printer.start(
        topic="STATE",
        purpose="classify file states vs previous index",
        expectation="pick files that need extraction",
    )
    store = DuckDBStore(db_path=db_path)
    if not dry_run:
        store.initialize()
    existing_by_path = store.load_existing_records(scan_root=scan_root) if db_path.exists() else {}

    incremental = classify_incremental_state(scan_records, existing_by_path)
    state_duration_seconds = progress_printer.done(
        "STATE",
        details=(
            f"new={incremental.new_files}, changed={incremental.changed_files}, "
            f"unchanged={incremental.unchanged_files}, missing={len(incremental.missing_paths)}"
        ),
    )

    progress_printer.start(
        topic="PARSE",
        purpose="parse filename datetime patterns",
        expectation="optional timestamp hints for files needing refresh",
    )
    filename_parse_by_path: dict[str, FilenameParseRecord] = {}
    for record in scan_records:
        file_state = incremental.state_by_path.get(record.path, "new")
        should_parse = record.is_supported and (
            full_rescan or file_state in {"new", "changed"}
        )
        if should_parse:
            filename_parse_by_path[record.path] = parse_filename_datetime(record.filename)
    parse_duration_seconds = progress_printer.done(
        "PARSE",
        details=f"parsed_candidates={len(filename_parse_by_path)}",
    )

    extraction_by_path: dict[str, ExtractionResult] = {}
    candidate_supported_records = [
        record
        for record in scan_records
        if record.is_supported and _should_extract(record.path, incremental.state_by_path, full_rescan)
    ]
    candidate_image_paths = [
        Path(record.path) for record in candidate_supported_records if record.media_type == "image"
    ]
    candidate_video_paths = [
        Path(record.path) for record in candidate_supported_records if record.media_type == "video"
    ]
    extraction_attempted = len(candidate_supported_records)

    progress_printer.start(
        topic="HASH",
        purpose="compute content hashes for new/changed supported files",
        expectation="per-file hash failures should not fail the run",
        details=f"candidate_files={extraction_attempted}",
    )
    hash_by_path: dict[str, tuple[str | None, str, str | None, datetime]] = {}
    hash_attempted = 0
    hash_successful = 0
    hash_failed = 0
    hash_at = datetime.now(timezone.utc)
    if not dry_run:
        for candidate in candidate_supported_records:
            hash_attempted += 1
            source_path = Path(candidate.path)
            try:
                digest = hash_file_sha256(source_path)
                hash_by_path[candidate.path] = (digest, "success", None, hash_at)
                hash_successful += 1
            except Exception as exc:  # noqa: BLE001 - per-file isolation
                hash_by_path[candidate.path] = (
                    None,
                    "failed",
                    f"{type(exc).__name__}: {exc}",
                    hash_at,
                )
                hash_failed += 1
    hash_duration_seconds = progress_printer.done(
        "HASH",
        details=(
            "dry_run=yes"
            if dry_run
            else (
                f"attempted={hash_attempted}, "
                f"successful={hash_successful}, "
                f"failed={hash_failed}"
            )
        ),
    )

    progress_printer.start(
        topic="EXTRACT",
        purpose="run ExifTool/ffprobe on target files",
        expectation=(
            "incremental should target fewer files than full_rescan on repeat runs"
            if not full_rescan
            else "full_rescan refreshes all supported files"
        ),
        details=(
            f"candidate_files={extraction_attempted}, "
            f"image_candidates={len(candidate_image_paths)}, "
            f"video_candidates={len(candidate_video_paths)}"
        ),
    )
    extraction_duration_seconds = 0.0
    if not dry_run:
        image_extractor = ExifToolExtractor(batch_size=exif_batch_size)
        video_extractor = FFprobeExtractor()
        extraction_by_path.update(image_extractor.extract(candidate_image_paths))
        extraction_by_path.update(video_extractor.extract(candidate_video_paths))
    extraction_duration_seconds = progress_printer.done(
        "EXTRACT",
        details=(
            "dry_run=yes"
            if dry_run
            else f"extracted={len(extraction_by_path)}"
        ),
    )

    progress_printer.start(
        topic="NORMALIZE",
        purpose="map values to stable schema",
        expectation="keep raw metadata + choose best captured_at",
    )
    normalized_records: list[NormalizedRecord] = []
    for scan_record in scan_records:
        file_state = incremental.state_by_path.get(scan_record.path, "new")
        existing_record = existing_by_path.get(scan_record.path)

        if _should_reuse_existing_record(
            file_state=file_state,
            existing_record=existing_record,
            full_rescan=full_rescan,
        ):
            normalized_records.append(
                _normalized_from_existing(
                    scan_record=scan_record,
                    existing_record=existing_record,
                    scan_id=scan_id,
                    scan_time=scan_time,
                )
            )
            continue

        parse_record = filename_parse_by_path.get(scan_record.path, FilenameParseRecord())
        extraction = extraction_by_path.get(scan_record.path)
        should_extract = scan_record.is_supported and _should_extract(
            scan_record.path,
            incremental.state_by_path,
            full_rescan,
        )

        if dry_run and should_extract:
            extraction = ExtractionResult(path=scan_record.path, status="skipped_dry_run")

        content_sha256: str | None = None
        hash_status: str | None = None
        hash_error: str | None = None
        hash_recorded_at: datetime | None = None
        hash_result = hash_by_path.get(scan_record.path)
        if should_extract:
            if dry_run:
                hash_status = "skipped_dry_run"
            elif hash_result is not None:
                content_sha256 = hash_result[0]
                hash_status = hash_result[1]
                hash_error = hash_result[2]
                hash_recorded_at = hash_result[3]
            else:
                hash_status = "failed"
                hash_error = "missing_hash_result"
                hash_recorded_at = hash_at

        first_seen_at = (
            existing_record.first_seen_at if existing_record and existing_record.first_seen_at else scan_time
        )
        normalized_records.append(
            normalize_record(
                scan_record=scan_record,
                extraction=extraction,
                filename_parse=parse_record,
                scan_id=scan_id,
                file_state=file_state,
                first_seen_at=first_seen_at,
                last_seen_at=scan_time,
                content_sha256=content_sha256,
                hash_status=hash_status,
                hash_error=hash_error,
                hash_at=hash_recorded_at,
            )
        )
    normalize_duration_seconds = progress_printer.done(
        "NORMALIZE",
        details=f"normalized_records={len(normalized_records)}",
    )

    progress_printer.start(
        topic="PERSIST",
        purpose="write db state and optional export",
        expectation="persist latest rows + scan history",
    )
    upsert_records, unchanged_scan_records = _select_persist_targets(
        normalized_records=normalized_records,
        scan_records=scan_records,
        state_by_path=incremental.state_by_path,
        full_rescan=full_rescan,
    )
    persist_upserted = 0
    persist_touched = 0
    if not dry_run:
        store.upsert_records(upsert_records)
        persist_upserted = len(upsert_records)
        persist_touched = store.touch_unchanged_records(
            scan_root=scan_root,
            scan_id=scan_id,
            scan_time=scan_time,
            records=unchanged_scan_records,
        )
        store.mark_missing_files(
            scan_root=scan_root,
            missing_paths=incremental.missing_paths,
            scan_id=scan_id,
            scan_time=scan_time,
        )
        if export_path:
            store.export_table(export_path)
    persist_duration_seconds = progress_printer.done(
        "PERSIST",
        details=(
            "dry_run=yes"
            if dry_run
            else (
                f"upserted={persist_upserted}, "
                f"touched_unchanged={persist_touched}, "
                f"missing_marked={len(incremental.missing_paths)}"
            )
        ),
    )

    extraction_successful = sum(
        1 for item in extraction_by_path.values() if item.status == "success"
    )
    extraction_failed = sum(
        1 for item in extraction_by_path.values() if item.status != "success"
    )
    image_extraction_attempted = len(candidate_image_paths)
    image_extraction_successful = sum(
        1
        for path in candidate_image_paths
        if extraction_by_path.get(str(path.resolve(strict=False))) is not None
        and extraction_by_path[str(path.resolve(strict=False))].status == "success"
    )
    image_extraction_failed = sum(
        1
        for path in candidate_image_paths
        if extraction_by_path.get(str(path.resolve(strict=False))) is not None
        and extraction_by_path[str(path.resolve(strict=False))].status != "success"
    )

    video_extraction_attempted = len(candidate_video_paths)
    video_extraction_successful = sum(
        1
        for path in candidate_video_paths
        if extraction_by_path.get(str(path.resolve(strict=False))) is not None
        and extraction_by_path[str(path.resolve(strict=False))].status == "success"
    )
    video_extraction_failed = sum(
        1
        for path in candidate_video_paths
        if extraction_by_path.get(str(path.resolve(strict=False))) is not None
        and extraction_by_path[str(path.resolve(strict=False))].status != "success"
    )
    summary = build_run_summary(
        normalized_records,
        new_files=incremental.new_files,
        changed_files=incremental.changed_files,
        unchanged_files=incremental.unchanged_files,
        missing_files=len(incremental.missing_paths),
        extraction_attempted=extraction_attempted,
        extraction_successful=extraction_successful,
        extraction_failed=extraction_failed,
        full_rescan=full_rescan,
        scan_duration_seconds=scan_duration_seconds,
        state_duration_seconds=state_duration_seconds,
        parse_duration_seconds=parse_duration_seconds,
        extraction_duration_seconds=extraction_duration_seconds,
        normalize_duration_seconds=normalize_duration_seconds,
        persist_duration_seconds=persist_duration_seconds,
        persist_upserted=persist_upserted,
        persist_touched_unchanged=persist_touched,
        image_extraction_attempted=image_extraction_attempted,
        image_extraction_successful=image_extraction_successful,
        image_extraction_failed=image_extraction_failed,
        video_extraction_attempted=video_extraction_attempted,
        video_extraction_successful=video_extraction_successful,
        video_extraction_failed=video_extraction_failed,
        hash_attempted=hash_attempted,
        hash_successful=hash_successful,
        hash_failed=hash_failed,
    )
    finished_at = datetime.now(timezone.utc)
    run_duration_seconds = perf_counter() - run_started_clock
    summary["run_duration_seconds"] = run_duration_seconds
    summary["hash_duration_seconds"] = hash_duration_seconds

    if not dry_run:
        history = ScanHistoryRecord(
            scan_id=scan_id,
            scan_root=scan_root,
            started_at=started_at,
            finished_at=finished_at,
            files_discovered=summary["files_discovered"],
            supported_files=summary["supported_files"],
            new_files=summary.get("new_files", 0),
            changed_files=summary.get("changed_files", 0),
            unchanged_files=summary.get("unchanged_files", 0),
            missing_files=summary.get("missing_files", 0),
            extraction_attempted=summary.get("extraction_attempted", 0),
            extraction_successful=summary.get("extraction_successful", 0),
            extraction_failed=summary.get("extraction_failed", 0),
            dry_run=dry_run,
            image_extraction_attempted=summary.get("image_extraction_attempted", 0),
            image_extraction_successful=summary.get("image_extraction_successful", 0),
            image_extraction_failed=summary.get("image_extraction_failed", 0),
            video_extraction_attempted=summary.get("video_extraction_attempted", 0),
            video_extraction_successful=summary.get("video_extraction_successful", 0),
            video_extraction_failed=summary.get("video_extraction_failed", 0),
            hash_attempted=summary.get("hash_attempted", 0),
            hash_successful=summary.get("hash_successful", 0),
            hash_failed=summary.get("hash_failed", 0),
        )
        store.insert_scan_history(history)

    progress_printer.info(
        topic="SUMMARY",
        purpose="finalize run metrics",
        expectation="print comparison stats for incremental vs full_rescan",
        details=f"run_duration={run_duration_seconds:.2f}s",
    )

    return PipelineResult(
        records=normalized_records,
        summary=summary,
        db_path=db_path.expanduser().resolve(),
        export_path=export_path.expanduser().resolve() if export_path else None,
        scan_id=scan_id,
        dry_run=dry_run,
    )


def _build_scan_id(started_at: datetime) -> str:
    return f"scan_{started_at.strftime('%Y%m%dT%H%M%S')}_{uuid4().hex[:8]}"


def _should_extract(path: str, state_by_path: dict[str, str], full_rescan: bool) -> bool:
    if full_rescan:
        return True
    return state_by_path.get(path) in {"new", "changed"}


def _normalized_from_existing(
    *,
    scan_record: FileScanRecord,
    existing_record: ExistingFileIndexRecord,
    scan_id: str,
    scan_time: datetime,
) -> NormalizedRecord:
    return NormalizedRecord(
        file_id=scan_record.file_id,
        scan_id=scan_id,
        path=scan_record.path,
        parent_folder=scan_record.parent_folder,
        filename=scan_record.filename,
        extension=scan_record.extension,
        media_type=scan_record.media_type,
        size_bytes=scan_record.size_bytes,
        fs_created_at=scan_record.fs_created_at,
        fs_modified_at=scan_record.fs_modified_at,
        scan_root=scan_record.scan_root,
        scan_time=scan_record.scan_time,
        is_supported=scan_record.is_supported,
        captured_at=existing_record.captured_at,
        captured_at_source=None,
        gps_lat=existing_record.gps_lat,
        gps_lon=existing_record.gps_lon,
        gps_alt=None,
        camera_make=None,
        camera_model=existing_record.camera_model,
        lens_model=None,
        software=None,
        width=None,
        height=None,
        orientation=None,
        raw_metadata_json=None,
        extract_status=existing_record.extract_status or "success_cached",
        extract_error=existing_record.extract_error,
        file_state="unchanged",
        first_seen_at=existing_record.first_seen_at or scan_time,
        last_seen_at=scan_time,
        parsed_datetime=None,
        parsed_pattern=None,
        parse_confidence=None,
        content_sha256=existing_record.content_sha256,
        hash_status=existing_record.hash_status,
        hash_error=existing_record.hash_error,
        hash_at=existing_record.hash_at,
    )


def _should_reuse_existing_record(
    *,
    file_state: str,
    existing_record: ExistingFileIndexRecord | None,
    full_rescan: bool,
) -> bool:
    if full_rescan:
        return False
    return file_state == "unchanged" and existing_record is not None


def _select_persist_targets(
    *,
    normalized_records: list[NormalizedRecord],
    scan_records: list[FileScanRecord],
    state_by_path: dict[str, str],
    full_rescan: bool,
) -> tuple[list[NormalizedRecord], list[FileScanRecord]]:
    if full_rescan:
        upsert_records = [record for record in normalized_records if record.file_state != "missing"]
        return upsert_records, []

    upsert_records = [record for record in normalized_records if record.file_state in {"new", "changed"}]
    unchanged_scan_records = [
        scan_record
        for scan_record in scan_records
        if state_by_path.get(scan_record.path) == "unchanged"
    ]
    return upsert_records, unchanged_scan_records
