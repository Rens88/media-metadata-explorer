from __future__ import annotations

from dataclasses import astuple
from datetime import datetime
from pathlib import Path

import duckdb

from photo_archive.models import ExistingFileIndexRecord, NormalizedRecord, ScanHistoryRecord

TABLE_NAME = "file_metadata"
SCANS_TABLE_NAME = "scans"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    file_id VARCHAR PRIMARY KEY,
    scan_id VARCHAR,
    path VARCHAR NOT NULL,
    parent_folder VARCHAR,
    filename VARCHAR,
    extension VARCHAR,
    media_type VARCHAR,
    size_bytes BIGINT,
    fs_created_at TIMESTAMP,
    fs_modified_at TIMESTAMP,
    scan_root VARCHAR,
    scan_time TIMESTAMP,
    is_supported BOOLEAN,
    captured_at TIMESTAMP,
    captured_at_source VARCHAR,
    gps_lat DOUBLE,
    gps_lon DOUBLE,
    gps_alt DOUBLE,
    camera_make VARCHAR,
    camera_model VARCHAR,
    lens_model VARCHAR,
    software VARCHAR,
    width INTEGER,
    height INTEGER,
    orientation VARCHAR,
    raw_metadata_json VARCHAR,
    extract_status VARCHAR,
    extract_error VARCHAR,
    file_state VARCHAR,
    first_seen_at TIMESTAMP,
    last_seen_at TIMESTAMP,
    parsed_datetime TIMESTAMP,
    parsed_pattern VARCHAR,
    parse_confidence DOUBLE
)
"""

INSERT_SQL = f"""
INSERT OR REPLACE INTO {TABLE_NAME} (
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
    captured_at,
    captured_at_source,
    gps_lat,
    gps_lon,
    gps_alt,
    camera_make,
    camera_model,
    lens_model,
    software,
    width,
    height,
    orientation,
    raw_metadata_json,
    extract_status,
    extract_error,
    file_state,
    first_seen_at,
    last_seen_at,
    parsed_datetime,
    parsed_pattern,
    parse_confidence
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
"""

CREATE_SCANS_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCANS_TABLE_NAME} (
    scan_id VARCHAR PRIMARY KEY,
    scan_root VARCHAR NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP NOT NULL,
    files_discovered BIGINT NOT NULL,
    supported_files BIGINT NOT NULL,
    new_files BIGINT NOT NULL,
    changed_files BIGINT NOT NULL,
    unchanged_files BIGINT NOT NULL,
    missing_files BIGINT NOT NULL,
    extraction_attempted BIGINT NOT NULL,
    extraction_successful BIGINT NOT NULL,
    extraction_failed BIGINT NOT NULL,
    dry_run BOOLEAN NOT NULL
)
"""

INSERT_SCAN_SQL = f"""
INSERT INTO {SCANS_TABLE_NAME} (
    scan_id,
    scan_root,
    started_at,
    finished_at,
    files_discovered,
    supported_files,
    new_files,
    changed_files,
    unchanged_files,
    missing_files,
    extraction_attempted,
    extraction_successful,
    extraction_failed,
    dry_run
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

FILE_METADATA_MIGRATIONS = [
    f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS scan_id VARCHAR",
    f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS file_state VARCHAR",
    f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMP",
    f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP",
]


class DuckDBStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path.expanduser().resolve()

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute(CREATE_TABLE_SQL)
            for migration_sql in FILE_METADATA_MIGRATIONS:
                conn.execute(migration_sql)
            conn.execute(
                f"""
                UPDATE {TABLE_NAME}
                SET
                    file_state = COALESCE(file_state, 'unchanged'),
                    first_seen_at = COALESCE(first_seen_at, scan_time),
                    last_seen_at = COALESCE(last_seen_at, scan_time)
                """
            )
            conn.execute(CREATE_SCANS_TABLE_SQL)

    def upsert_records(self, records: list[NormalizedRecord]) -> None:
        if not records:
            return
        with duckdb.connect(str(self.db_path)) as conn:
            conn.executemany(INSERT_SQL, [astuple(record) for record in records])

    def load_existing_records(self, scan_root: str) -> dict[str, ExistingFileIndexRecord]:
        if not self.db_path.exists():
            return {}
        try:
            with duckdb.connect(str(self.db_path)) as conn:
                rows = conn.execute(
                    f"""
                    SELECT
                        file_id,
                        path,
                        scan_root,
                        size_bytes,
                        fs_modified_at,
                        raw_metadata_json,
                        extract_status,
                        extract_error,
                        is_supported,
                        file_state,
                        first_seen_at
                    FROM {TABLE_NAME}
                    WHERE scan_root = ?
                    """,
                    [scan_root],
                ).fetchall()
        except duckdb.Error:
            return {}
        output: dict[str, ExistingFileIndexRecord] = {}
        for row in rows:
            output[row[1]] = ExistingFileIndexRecord(
                file_id=row[0],
                path=row[1],
                scan_root=row[2],
                size_bytes=row[3],
                fs_modified_at=_coerce_datetime(row[4]),
                raw_metadata_json=row[5],
                extract_status=row[6],
                extract_error=row[7],
                is_supported=bool(row[8]),
                file_state=row[9],
                first_seen_at=_coerce_datetime(row[10]),
            )
        return output

    def mark_missing_files(
        self,
        scan_root: str,
        missing_paths: list[str],
        scan_id: str,
        scan_time: datetime,
    ) -> None:
        if not missing_paths:
            return
        placeholders = ", ".join(["?"] * len(missing_paths))
        params = [scan_id, scan_time, scan_root, *missing_paths]
        query = f"""
            UPDATE {TABLE_NAME}
            SET
                scan_id = ?,
                scan_time = ?,
                file_state = 'missing',
                extract_status = 'missing',
                extract_error = 'file_missing_in_scan'
            WHERE scan_root = ?
              AND path IN ({placeholders})
              AND COALESCE(file_state, '') <> 'missing'
        """
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute(query, params)

    def insert_scan_history(self, history: ScanHistoryRecord) -> None:
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute(INSERT_SCAN_SQL, astuple(history))

    def export_table(self, export_path: Path) -> None:
        resolved = export_path.expanduser().resolve()
        resolved.parent.mkdir(parents=True, exist_ok=True)
        suffix = resolved.suffix.lower()
        escaped_path = str(resolved).replace("'", "''")

        if suffix == ".csv":
            copy_sql = (
                f"COPY (SELECT * FROM {TABLE_NAME}) "
                f"TO '{escaped_path}' (FORMAT CSV, HEADER TRUE)"
            )
        elif suffix == ".parquet":
            copy_sql = (
                f"COPY (SELECT * FROM {TABLE_NAME}) "
                f"TO '{escaped_path}' (FORMAT PARQUET)"
            )
        else:
            raise ValueError("Export path must end with .csv or .parquet")

        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute(copy_sql)


def _coerce_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return None
