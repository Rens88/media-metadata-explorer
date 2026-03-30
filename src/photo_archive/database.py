from __future__ import annotations

from dataclasses import astuple
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import duckdb

from photo_archive.models import (
    ColumnCoverageRecord,
    ExistingFileIndexRecord,
    ExtensionCountRecord,
    FailedFileRecord,
    NormalizedRecord,
    ScanHistoryRecord,
)

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

    def touch_unchanged_records(
        self,
        *,
        scan_root: str,
        scan_id: str,
        scan_time: datetime,
        paths: list[str],
    ) -> int:
        """Lightweight update for unchanged files to avoid full row upserts."""
        if not paths:
            return 0
        path_rows = [(path,) for path in paths]
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute("CREATE TEMP TABLE _touch_paths(path VARCHAR)")
            conn.executemany("INSERT INTO _touch_paths VALUES (?)", path_rows)
            conn.execute(
                f"""
                UPDATE {TABLE_NAME} AS f
                SET
                    scan_id = ?,
                    scan_time = ?,
                    file_state = 'unchanged',
                    last_seen_at = ?
                FROM _touch_paths AS t
                WHERE f.path = t.path
                  AND f.scan_root = ?
                """,
                [scan_id, scan_time, scan_time, scan_root],
            )
        return len(paths)

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

    def get_scan_history(self, scan_id: str | None = None) -> ScanHistoryRecord | None:
        if not self.db_path.exists():
            return None

        try:
            with duckdb.connect(str(self.db_path)) as conn:
                if scan_id:
                    row = conn.execute(
                        f"""
                        SELECT
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
                        FROM {SCANS_TABLE_NAME}
                        WHERE scan_id = ?
                        """,
                        [scan_id],
                    ).fetchone()
                else:
                    row = conn.execute(
                        f"""
                        SELECT
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
                        FROM {SCANS_TABLE_NAME}
                        ORDER BY finished_at DESC
                        LIMIT 1
                        """
                    ).fetchone()
        except duckdb.Error:
            return None

        if row is None:
            return None

        return ScanHistoryRecord(
            scan_id=row[0],
            scan_root=row[1],
            started_at=row[2],
            finished_at=row[3],
            files_discovered=int(row[4]),
            supported_files=int(row[5]),
            new_files=int(row[6]),
            changed_files=int(row[7]),
            unchanged_files=int(row[8]),
            missing_files=int(row[9]),
            extraction_attempted=int(row[10]),
            extraction_successful=int(row[11]),
            extraction_failed=int(row[12]),
            dry_run=bool(row[13]),
        )

    def get_failed_files(self, scan_id: str, limit: int = 50) -> list[FailedFileRecord]:
        if not self.db_path.exists():
            return []
        try:
            with duckdb.connect(str(self.db_path)) as conn:
                rows = conn.execute(
                    f"""
                    SELECT
                        path,
                        extract_status,
                        extract_error
                    FROM {TABLE_NAME}
                    WHERE scan_id = ?
                      AND extract_status IN ('failed', 'failed_cached')
                    ORDER BY path
                    LIMIT ?
                    """,
                    [scan_id, int(limit)],
                ).fetchall()
        except duckdb.Error:
            return []
        return [
            FailedFileRecord(
                path=row[0],
                extract_status=row[1],
                extract_error=row[2],
            )
            for row in rows
        ]

    def get_column_non_null_coverage(
        self,
        scan_id: str,
        *,
        treat_empty_strings_as_missing: bool = True,
        sort_order: Literal["asc", "desc"] = "asc",
    ) -> tuple[int, list[ColumnCoverageRecord]]:
        if not self.db_path.exists():
            return 0, []

        try:
            with duckdb.connect(str(self.db_path)) as conn:
                columns = conn.execute(
                    """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'main' AND table_name = ?
                    ORDER BY ordinal_position
                    """,
                    [TABLE_NAME],
                ).fetchall()

                total_rows = int(
                    conn.execute(
                        f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE scan_id = ?",
                        [scan_id],
                    ).fetchone()[0]
                )
                if total_rows == 0 or not columns:
                    return total_rows, []

                count_expressions: list[str] = []
                for index, (column_name, column_type) in enumerate(columns):
                    alias = f"c_{index}"
                    quoted = _quote_ident(column_name)
                    type_upper = str(column_type).upper()
                    if treat_empty_strings_as_missing and any(
                        token in type_upper for token in ("CHAR", "TEXT", "VARCHAR")
                    ):
                        expr = (
                            f"SUM(CASE WHEN {quoted} IS NOT NULL AND TRIM({quoted}) <> '' "
                            f"THEN 1 ELSE 0 END) AS {alias}"
                        )
                    else:
                        expr = f"COUNT({quoted}) AS {alias}"
                    count_expressions.append(expr)

                counts_query = (
                    f"SELECT {', '.join(count_expressions)} "
                    f"FROM {TABLE_NAME} WHERE scan_id = ?"
                )
                counts_row = conn.execute(counts_query, [scan_id]).fetchone()
        except duckdb.Error:
            return 0, []

        rows: list[ColumnCoverageRecord] = []
        for index, (column_name, column_type) in enumerate(columns):
            non_null_count = int(counts_row[index])
            null_count = int(total_rows - non_null_count)
            non_null_pct = (non_null_count / total_rows) * 100.0
            rows.append(
                ColumnCoverageRecord(
                    column_name=column_name,
                    column_type=column_type,
                    non_null_count=non_null_count,
                    null_count=null_count,
                    non_null_pct=round(non_null_pct, 2),
                )
            )

        reverse = sort_order == "desc"
        rows.sort(key=lambda item: (item.non_null_pct, item.column_name), reverse=reverse)
        return total_rows, rows

    def get_unsupported_extension_counts(self, scan_id: str) -> list[ExtensionCountRecord]:
        if not self.db_path.exists():
            return []
        try:
            with duckdb.connect(str(self.db_path)) as conn:
                rows = conn.execute(
                    f"""
                    SELECT
                        COALESCE(NULLIF(extension, ''), '[no_extension]') AS extension_label,
                        COUNT(*) AS file_count
                    FROM {TABLE_NAME}
                    WHERE scan_id = ?
                      AND is_supported = FALSE
                    GROUP BY extension_label
                    ORDER BY file_count DESC, extension_label ASC
                    """,
                    [scan_id],
                ).fetchall()
        except duckdb.Error:
            return []

        return [
            ExtensionCountRecord(extension=row[0], count=int(row[1]))
            for row in rows
        ]


def _coerce_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        # DuckDB TIMESTAMP is timezone-naive when read through Python.
        # We normalize to UTC-aware datetime so equality checks against
        # scanner timestamps (also UTC-aware) are stable across runs.
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return None


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'
