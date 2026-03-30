from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from uuid import uuid4

from photo_archive.config import normalize_extensions
from photo_archive.database import DuckDBStore
from photo_archive.extractors.exiftool_extractor import ExifToolExtractor
from photo_archive.extractors.filename_parser import parse_filename_datetime
from photo_archive.incremental import classify_incremental_state
from photo_archive.models import (
    ExistingFileIndexRecord,
    ExtractionResult,
    NormalizedRecord,
    ScanHistoryRecord,
)
from photo_archive.normalize import normalize_record
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
) -> PipelineResult:
    started_at = datetime.now(timezone.utc)
    scan_id = _build_scan_id(started_at)
    supported_extensions = normalize_extensions(extensions)
    scan_records = scan_directory(root_path=root_path, supported_extensions=supported_extensions)
    scan_root = str(root_path.expanduser().resolve())
    scan_time = scan_records[0].scan_time if scan_records else datetime.now(timezone.utc)

    store = DuckDBStore(db_path=db_path)
    if not dry_run:
        store.initialize()
    existing_by_path = store.load_existing_records(scan_root=scan_root) if db_path.exists() else {}

    incremental = classify_incremental_state(scan_records, existing_by_path)

    filename_parse_by_path = {
        record.path: parse_filename_datetime(record.filename) for record in scan_records
    }

    extraction_by_path: dict[str, ExtractionResult] = {}
    candidate_supported_paths = [
        Path(record.path)
        for record in scan_records
        if record.is_supported and _should_extract(record.path, incremental.state_by_path, full_rescan)
    ]
    extraction_attempted = len(candidate_supported_paths)
    if not dry_run:
        extractor = ExifToolExtractor(batch_size=exif_batch_size)
        extraction_by_path = extractor.extract(candidate_supported_paths)

    normalized_records: list[NormalizedRecord] = []
    for scan_record in scan_records:
        parse_record = filename_parse_by_path[scan_record.path]
        extraction = extraction_by_path.get(scan_record.path)
        file_state = incremental.state_by_path.get(scan_record.path, "new")
        existing_record = existing_by_path.get(scan_record.path)

        if dry_run and scan_record.is_supported and _should_extract(
            scan_record.path, incremental.state_by_path, full_rescan
        ):
            extraction = ExtractionResult(path=scan_record.path, status="skipped_dry_run")

        if extraction is None and scan_record.is_supported and file_state == "unchanged":
            extraction = _cached_extraction_result(existing_record)

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
            )
        )

    if not dry_run:
        store.upsert_records(normalized_records)
        store.mark_missing_files(
            scan_root=scan_root,
            missing_paths=incremental.missing_paths,
            scan_id=scan_id,
            scan_time=scan_time,
        )
        if export_path:
            store.export_table(export_path)

    extraction_successful = sum(
        1 for item in extraction_by_path.values() if item.status == "success"
    )
    extraction_failed = sum(
        1 for item in extraction_by_path.values() if item.status != "success"
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
    )
    finished_at = datetime.now(timezone.utc)

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
        )
        store.insert_scan_history(history)

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


def _cached_extraction_result(
    existing_record: ExistingFileIndexRecord | None,
) -> ExtractionResult | None:
    if existing_record is None:
        return None

    raw_metadata: dict[str, object] | None = None
    if existing_record.raw_metadata_json:
        try:
            parsed = json.loads(existing_record.raw_metadata_json)
            if isinstance(parsed, dict):
                raw_metadata = parsed
        except json.JSONDecodeError:
            raw_metadata = None

    if raw_metadata:
        return ExtractionResult(
            path=existing_record.path,
            status="success_cached",
            raw_metadata=raw_metadata,
        )

    return ExtractionResult(
        path=existing_record.path,
        status="failed_cached",
        error=existing_record.extract_error or "missing_cached_metadata",
    )
