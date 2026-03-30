from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from photo_archive.config import normalize_extensions
from photo_archive.database import DuckDBStore
from photo_archive.extractors.exiftool_extractor import ExifToolExtractor
from photo_archive.extractors.filename_parser import parse_filename_datetime
from photo_archive.models import ExtractionResult, NormalizedRecord
from photo_archive.normalize import normalize_record
from photo_archive.reporting import build_run_summary
from photo_archive.scanner import scan_directory


@dataclass(slots=True)
class PipelineResult:
    records: list[NormalizedRecord]
    summary: dict[str, object]
    db_path: Path
    export_path: Path | None
    dry_run: bool


def run_pipeline(
    root_path: Path,
    db_path: Path,
    export_path: Path | None = None,
    extensions: list[str] | None = None,
    dry_run: bool = False,
    exif_batch_size: int = 200,
) -> PipelineResult:
    supported_extensions = normalize_extensions(extensions)
    scan_records = scan_directory(root_path=root_path, supported_extensions=supported_extensions)

    filename_parse_by_path = {
        record.path: parse_filename_datetime(record.filename) for record in scan_records
    }

    extraction_by_path: dict[str, ExtractionResult] = {}
    if not dry_run:
        supported_paths = [Path(record.path) for record in scan_records if record.is_supported]
        extractor = ExifToolExtractor(batch_size=exif_batch_size)
        extraction_by_path = extractor.extract(supported_paths)

    normalized_records: list[NormalizedRecord] = []
    for scan_record in scan_records:
        parse_record = filename_parse_by_path[scan_record.path]
        extraction = extraction_by_path.get(scan_record.path)
        if dry_run and scan_record.is_supported:
            extraction = ExtractionResult(path=scan_record.path, status="skipped_dry_run")
        normalized_records.append(normalize_record(scan_record, extraction, parse_record))

    if not dry_run:
        store = DuckDBStore(db_path=db_path)
        store.initialize()
        store.upsert_records(normalized_records)
        if export_path:
            store.export_table(export_path)

    summary = build_run_summary(normalized_records)
    return PipelineResult(
        records=normalized_records,
        summary=summary,
        db_path=db_path.expanduser().resolve(),
        export_path=export_path.expanduser().resolve() if export_path else None,
        dry_run=dry_run,
    )
