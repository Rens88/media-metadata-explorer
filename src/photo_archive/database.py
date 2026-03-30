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
    FailedThumbnailRecord,
    FailedFileRecord,
    NormalizedRecord,
    ScanHistoryRecord,
    ThumbnailStatusCountRecord,
    ThumbnailRecord,
    ThumbnailSourceRecord,
)

TABLE_NAME = "file_metadata"
SCANS_TABLE_NAME = "scans"
THUMBNAILS_TABLE_NAME = "thumbnails"

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

CREATE_THUMBNAILS_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {THUMBNAILS_TABLE_NAME} (
    file_id VARCHAR PRIMARY KEY,
    thumb_path VARCHAR,
    width INTEGER,
    height INTEGER,
    status VARCHAR NOT NULL,
    error VARCHAR,
    generated_at TIMESTAMP NOT NULL
)
"""

INSERT_THUMBNAIL_SQL = f"""
INSERT OR REPLACE INTO {THUMBNAILS_TABLE_NAME} (
    file_id,
    thumb_path,
    width,
    height,
    status,
    error,
    generated_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
"""

FILE_METADATA_MIGRATIONS = [
    f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS scan_id VARCHAR",
    f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS file_state VARCHAR",
    f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMP",
    f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP",
]

THUMBNAILS_TABLE_MIGRATIONS = [
    f"ALTER TABLE {THUMBNAILS_TABLE_NAME} ADD COLUMN IF NOT EXISTS thumb_path VARCHAR",
    f"ALTER TABLE {THUMBNAILS_TABLE_NAME} ADD COLUMN IF NOT EXISTS width INTEGER",
    f"ALTER TABLE {THUMBNAILS_TABLE_NAME} ADD COLUMN IF NOT EXISTS height INTEGER",
    f"ALTER TABLE {THUMBNAILS_TABLE_NAME} ADD COLUMN IF NOT EXISTS status VARCHAR",
    f"ALTER TABLE {THUMBNAILS_TABLE_NAME} ADD COLUMN IF NOT EXISTS error VARCHAR",
    f"ALTER TABLE {THUMBNAILS_TABLE_NAME} ADD COLUMN IF NOT EXISTS generated_at TIMESTAMP",
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
            conn.execute(CREATE_THUMBNAILS_TABLE_SQL)
            for migration_sql in THUMBNAILS_TABLE_MIGRATIONS:
                conn.execute(migration_sql)
            conn.execute(
                f"""
                UPDATE {THUMBNAILS_TABLE_NAME}
                SET
                    status = COALESCE(status, 'unknown'),
                    generated_at = COALESCE(generated_at, NOW())
                """
            )

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
                        first_seen_at,
                        captured_at,
                        gps_lat,
                        gps_lon,
                        camera_model
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
                captured_at=_coerce_datetime(row[11]),
                gps_lat=float(row[12]) if row[12] is not None else None,
                gps_lon=float(row[13]) if row[13] is not None else None,
                camera_model=row[14],
            )
        return output

    def load_thumbnail_sources(self) -> list[ThumbnailSourceRecord]:
        if not self.db_path.exists():
            return []
        try:
            with duckdb.connect(str(self.db_path)) as conn:
                rows = conn.execute(
                    f"""
                    SELECT
                        file_id,
                        path,
                        file_state,
                        is_supported,
                        extract_status
                    FROM {TABLE_NAME}
                    WHERE COALESCE(file_state, '') <> 'missing'
                    ORDER BY path
                    """
                ).fetchall()
        except duckdb.Error:
            return []

        return [
            ThumbnailSourceRecord(
                file_id=row[0],
                path=row[1],
                file_state=row[2],
                is_supported=bool(row[3]),
                extract_status=row[4],
            )
            for row in rows
        ]

    def load_thumbnails_by_file_id(self) -> dict[str, ThumbnailRecord]:
        if not self.db_path.exists():
            return {}
        try:
            with duckdb.connect(str(self.db_path)) as conn:
                rows = conn.execute(
                    f"""
                    SELECT
                        file_id,
                        thumb_path,
                        width,
                        height,
                        status,
                        error,
                        generated_at
                    FROM {THUMBNAILS_TABLE_NAME}
                    """
                ).fetchall()
        except duckdb.Error:
            return {}

        records: dict[str, ThumbnailRecord] = {}
        for row in rows:
            generated_at = _coerce_datetime(row[6]) or datetime.now(timezone.utc)
            records[row[0]] = ThumbnailRecord(
                file_id=row[0],
                thumb_path=row[1],
                width=int(row[2]) if row[2] is not None else None,
                height=int(row[3]) if row[3] is not None else None,
                status=row[4] or "unknown",
                error=row[5],
                generated_at=generated_at,
            )
        return records

    def load_stale_thumbnails(self) -> list[ThumbnailRecord]:
        if not self.db_path.exists():
            return []
        try:
            with duckdb.connect(str(self.db_path)) as conn:
                rows = conn.execute(
                    f"""
                    SELECT
                        t.file_id,
                        t.thumb_path,
                        t.width,
                        t.height,
                        t.status,
                        t.error,
                        t.generated_at
                    FROM {THUMBNAILS_TABLE_NAME} AS t
                    LEFT JOIN {TABLE_NAME} AS f
                      ON f.file_id = t.file_id
                     AND COALESCE(f.file_state, '') <> 'missing'
                    WHERE f.file_id IS NULL
                    """
                ).fetchall()
        except duckdb.Error:
            return []

        stale: list[ThumbnailRecord] = []
        for row in rows:
            stale.append(
                ThumbnailRecord(
                    file_id=row[0],
                    thumb_path=row[1],
                    width=int(row[2]) if row[2] is not None else None,
                    height=int(row[3]) if row[3] is not None else None,
                    status=row[4] or "unknown",
                    error=row[5],
                    generated_at=_coerce_datetime(row[6]) or datetime.now(timezone.utc),
                )
            )
        return stale

    def delete_thumbnails_by_file_ids(self, file_ids: list[str]) -> int:
        if not file_ids:
            return 0
        id_rows = [(file_id,) for file_id in file_ids]
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute("CREATE TEMP TABLE _thumb_delete_ids(file_id VARCHAR)")
            conn.executemany("INSERT INTO _thumb_delete_ids VALUES (?)", id_rows)
            delete_count = int(
                conn.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {THUMBNAILS_TABLE_NAME} AS t
                    INNER JOIN _thumb_delete_ids AS d
                      ON d.file_id = t.file_id
                    """
                ).fetchone()[0]
            )
            conn.execute(
                f"""
                DELETE FROM {THUMBNAILS_TABLE_NAME}
                WHERE file_id IN (SELECT file_id FROM _thumb_delete_ids)
                """
            )
        return delete_count

    def upsert_thumbnail_records(self, records: list[ThumbnailRecord]) -> None:
        if not records:
            return
        with duckdb.connect(str(self.db_path)) as conn:
            conn.executemany(INSERT_THUMBNAIL_SQL, [astuple(record) for record in records])

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

    def get_thumbnail_status_counts(self) -> list[ThumbnailStatusCountRecord]:
        if not self.db_path.exists():
            return []
        try:
            with duckdb.connect(str(self.db_path)) as conn:
                rows = conn.execute(
                    f"""
                    SELECT
                        COALESCE(NULLIF(status, ''), '[unknown]') AS status_label,
                        COUNT(*) AS row_count
                    FROM {THUMBNAILS_TABLE_NAME}
                    GROUP BY status_label
                    ORDER BY row_count DESC, status_label ASC
                    """
                ).fetchall()
        except duckdb.Error:
            return []

        return [
            ThumbnailStatusCountRecord(status=row[0], count=int(row[1]))
            for row in rows
        ]

    def get_failed_thumbnails(self, limit: int = 50) -> list[FailedThumbnailRecord]:
        if not self.db_path.exists():
            return []
        try:
            with duckdb.connect(str(self.db_path)) as conn:
                rows = conn.execute(
                    f"""
                    SELECT
                        file_id,
                        thumb_path,
                        status,
                        error
                    FROM {THUMBNAILS_TABLE_NAME}
                    WHERE status = 'failed'
                    ORDER BY generated_at DESC, file_id ASC
                    LIMIT ?
                    """,
                    [int(limit)],
                ).fetchall()
        except duckdb.Error:
            return []

        return [
            FailedThumbnailRecord(
                file_id=row[0],
                thumb_path=row[1],
                status=row[2] or "failed",
                error=row[3],
            )
            for row in rows
        ]


def _coerce_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        # Keep original timezone/naive shape. Incremental comparison logic
        # handles ambiguous naive timestamps explicitly.
        return value
    return None


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'
