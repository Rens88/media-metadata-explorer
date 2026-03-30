from __future__ import annotations

from dataclasses import astuple
from pathlib import Path

import duckdb

from photo_archive.models import NormalizedRecord

TABLE_NAME = "file_metadata"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    file_id VARCHAR PRIMARY KEY,
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
    parsed_datetime TIMESTAMP,
    parsed_pattern VARCHAR,
    parse_confidence DOUBLE
)
"""

INSERT_SQL = f"""
INSERT OR REPLACE INTO {TABLE_NAME} (
    file_id,
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
    parsed_datetime,
    parsed_pattern,
    parse_confidence
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
"""


class DuckDBStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path.expanduser().resolve()

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute(CREATE_TABLE_SQL)

    def upsert_records(self, records: list[NormalizedRecord]) -> None:
        if not records:
            return
        with duckdb.connect(str(self.db_path)) as conn:
            conn.executemany(INSERT_SQL, [astuple(record) for record in records])

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
