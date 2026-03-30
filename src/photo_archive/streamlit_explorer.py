from __future__ import annotations

from datetime import date, datetime, time, timedelta
from pathlib import Path
import re
from typing import Any

import duckdb
import pandas as pd
import streamlit as st
from photo_archive.query_filters import (
    SORT_LABEL_TO_SQL,
    TIMELINE_BUCKET_TO_SQL,
    build_media_filter_where_sql,
    resolve_media_sort_sql,
    resolve_timeline_bucket_sql,
)

try:
    import folium
    from folium.plugins import Draw
    from streamlit_folium import st_folium
except ImportError:  # pragma: no cover - dependency availability varies by env
    folium = None
    Draw = None
    st_folium = None

DEFAULT_DB_PATH = Path("data/db/photo_archive.duckdb")

FILTER_CONTROL_KEYS = [
    "flt_search_text",
    "flt_only_supported",
    "flt_file_states",
    "flt_extensions",
    "flt_extract_statuses",
    "flt_parent_folders",
    "flt_camera_models",
    "flt_gps_filter",
    "flt_apply_date_filter",
    "flt_captured_from_date",
    "flt_captured_to_date",
    "flt_thumb_statuses",
    "flt_sort_by",
    "flt_sort_direction",
    "flt_page_size",
    "flt_page_number",
    "flt_filtered_gallery_limit",
    "flt_timeline_bucket",
    "flt_timeline_chart_type",
    "flt_map_point_limit",
    "flt_map_summary_limit",
    "flt_export_format",
    "flt_export_name",
    "flt_export_dir",
    "flt_lat_min",
    "flt_lat_max",
    "flt_lon_min",
    "flt_lon_max",
]


def quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def reset_filter_controls() -> None:
    for key in FILTER_CONTROL_KEYS:
        st.session_state.pop(key, None)


def sanitize_filename_component(value: str) -> str:
    candidate = value.strip()
    if not candidate:
        return "filtered_media_export"
    sanitized = re.sub(r"[^\w\-. ]+", "_", candidate)
    sanitized = sanitized.strip().strip(".")
    return sanitized or "filtered_media_export"


def clear_bbox_filters() -> None:
    for key in ("flt_lat_min", "flt_lat_max", "flt_lon_min", "flt_lon_max"):
        st.session_state.pop(key, None)


def apply_bbox_filters(south: float, west: float, north: float, east: float) -> None:
    st.session_state["flt_lat_min"] = float(south)
    st.session_state["flt_lat_max"] = float(north)
    st.session_state["flt_lon_min"] = float(west)
    st.session_state["flt_lon_max"] = float(east)
    st.session_state["flt_page_number"] = 1


def get_bbox_filters() -> tuple[float | None, float | None, float | None, float | None]:
    values: list[float | None] = []
    for key in ("flt_lat_min", "flt_lat_max", "flt_lon_min", "flt_lon_max"):
        raw_value = st.session_state.get(key)
        if isinstance(raw_value, (int, float)):
            values.append(float(raw_value))
        else:
            values.append(None)
    return values[0], values[1], values[2], values[3]


def extract_drawn_bounds(draw_output: dict[str, Any] | None) -> tuple[float, float, float, float] | None:
    if not draw_output:
        return None

    candidate: dict[str, Any] | None = None
    last_active = draw_output.get("last_active_drawing")
    if isinstance(last_active, dict):
        candidate = last_active
    else:
        all_drawings = draw_output.get("all_drawings")
        if isinstance(all_drawings, list) and all_drawings:
            last = all_drawings[-1]
            if isinstance(last, dict):
                candidate = last

    if not candidate:
        return None

    bounds_payload = candidate.get("bounds")
    if isinstance(bounds_payload, dict):
        sw = bounds_payload.get("_southWest")
        ne = bounds_payload.get("_northEast")
        if isinstance(sw, dict) and isinstance(ne, dict):
            try:
                south = float(sw["lat"])
                west = float(sw["lng"])
                north = float(ne["lat"])
                east = float(ne["lng"])
                return south, west, north, east
            except (KeyError, TypeError, ValueError):
                pass

    geometry = candidate.get("geometry")
    if not isinstance(geometry, dict):
        return None
    if geometry.get("type") != "Polygon":
        return None

    coordinates = geometry.get("coordinates")
    if not isinstance(coordinates, list) or not coordinates:
        return None
    ring = coordinates[0]
    if not isinstance(ring, list) or not ring:
        return None

    lats: list[float] = []
    lons: list[float] = []
    for point in ring:
        if not isinstance(point, (list, tuple)) or len(point) < 2:
            continue
        try:
            lon = float(point[0])
            lat = float(point[1])
        except (TypeError, ValueError):
            continue
        lats.append(lat)
        lons.append(lon)

    if not lats or not lons:
        return None
    return min(lats), min(lons), max(lats), max(lons)


def render_interactive_bbox_map(
    *,
    map_points_df: pd.DataFrame,
) -> tuple[float, float, float, float] | None:
    if folium is None or Draw is None or st_folium is None:
        return None
    if map_points_df.empty:
        return None

    center_lat = float(map_points_df["latitude"].mean())
    center_lon = float(map_points_df["longitude"].mean())
    bbox_map = folium.Map(location=[center_lat, center_lon], zoom_start=6, tiles="OpenStreetMap")

    render_points = map_points_df.head(1000)
    for row in render_points.itertuples(index=False):
        lat_value = getattr(row, "latitude", None)
        lon_value = getattr(row, "longitude", None)
        if lat_value is None or lon_value is None:
            continue
        folium.CircleMarker(
            location=[float(lat_value), float(lon_value)],
            radius=2,
            color="#1f77b4",
            weight=1,
            fill=True,
            fill_opacity=0.6,
        ).add_to(bbox_map)

    Draw(
        export=False,
        draw_options={
            "polyline": False,
            "polygon": False,
            "circle": False,
            "marker": False,
            "circlemarker": False,
            "rectangle": True,
        },
        edit_options={"edit": False},
    ).add_to(bbox_map)

    draw_output = st_folium(
        bbox_map,
        height=460,
        key="flt_bbox_draw_map",
    )
    return extract_drawn_bounds(draw_output)


def list_tables(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
        """
    ).fetchall()
    return [row[0] for row in rows]


def get_table_columns(
    con: duckdb.DuckDBPyConnection, table_name: str
) -> list[dict[str, str]]:
    rows = con.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'main' AND table_name = ?
        ORDER BY ordinal_position
        """,
        [table_name],
    ).fetchall()
    return [{"name": row[0], "type": row[1]} for row in rows]


def build_coverage_dataframe(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    columns: list[dict[str, str]],
    treat_empty_strings_as_missing: bool,
) -> tuple[pd.DataFrame, int]:
    total_rows = con.execute(f"SELECT COUNT(*) FROM {quote_ident(table_name)}").fetchone()[0]
    if total_rows == 0:
        return (
            pd.DataFrame(
                columns=[
                    "column_name",
                    "column_type",
                    "non_null_count",
                    "null_count",
                    "non_null_pct",
                ]
            ),
            0,
        )

    count_expressions: list[str] = []
    for idx, column in enumerate(columns):
        column_name = column["name"]
        column_type = column["type"].upper()
        quoted = quote_ident(column_name)
        alias = f"c_{idx}"
        if treat_empty_strings_as_missing and any(
            token in column_type for token in ("CHAR", "TEXT", "VARCHAR")
        ):
            expr = (
                f"SUM(CASE WHEN {quoted} IS NOT NULL AND TRIM({quoted}) <> '' "
                f"THEN 1 ELSE 0 END) AS {alias}"
            )
        else:
            expr = f"COUNT({quoted}) AS {alias}"
        count_expressions.append(expr)

    counts_query = (
        f"SELECT {', '.join(count_expressions)} FROM {quote_ident(table_name)}"
    )
    counts_row = con.execute(counts_query).fetchone()

    rows: list[dict[str, Any]] = []
    for idx, column in enumerate(columns):
        non_null_count = int(counts_row[idx])
        null_count = int(total_rows - non_null_count)
        non_null_pct = (non_null_count / total_rows) * 100.0
        rows.append(
            {
                "column_name": column["name"],
                "column_type": column["type"],
                "non_null_count": non_null_count,
                "null_count": null_count,
                "non_null_pct": round(non_null_pct, 2),
            }
        )

    coverage_df = pd.DataFrame(rows).sort_values(
        by=["non_null_pct", "column_name"],
        ascending=[False, True],
    )
    return coverage_df, total_rows


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def build_thumbnail_status_dataframe(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(
        """
        SELECT
            status,
            COUNT(*) AS count
        FROM thumbnails
        GROUP BY status
        ORDER BY count DESC, status ASC
        """
    ).df()


def load_thumbnail_preview_dataframe(
    con: duckdb.DuckDBPyConnection,
    *,
    limit: int,
) -> pd.DataFrame:
    return con.execute(
        """
        SELECT
            t.file_id,
            t.status,
            t.thumb_path,
            t.width,
            t.height,
            t.generated_at,
            f.path AS source_path,
            f.filename
        FROM thumbnails AS t
        LEFT JOIN file_metadata AS f
          ON f.file_id = t.file_id
        ORDER BY t.generated_at DESC
        LIMIT ?
        """,
        [int(limit)],
    ).df()


def list_distinct_values(
    con: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    column_name: str,
    where_sql: str | None = None,
) -> list[str]:
    query = (
        f"SELECT DISTINCT {quote_ident(column_name)} AS value "
        f"FROM {quote_ident(table_name)} "
    )
    if where_sql:
        query += f"WHERE {where_sql} "
    query += "ORDER BY value"
    rows = con.execute(query).fetchall()
    return [str(row[0]) for row in rows if row[0] not in (None, "")]


def _filtered_from_and_select_sql(has_thumbnails: bool) -> tuple[str, str, str]:
    if has_thumbnails:
        from_sql = (
            "FROM file_metadata AS f "
            "LEFT JOIN thumbnails AS t ON t.file_id = f.file_id "
        )
        thumb_status_sql = "COALESCE(t.status, '[none]') AS thumbnail_status"
        thumb_select_sql = (
            "t.thumb_path, "
            "t.width AS thumb_width, "
            "t.height AS thumb_height "
        )
    else:
        from_sql = "FROM file_metadata AS f "
        thumb_status_sql = "'[none]' AS thumbnail_status"
        thumb_select_sql = (
            "NULL AS thumb_path, "
            "NULL AS thumb_width, "
            "NULL AS thumb_height "
        )
    return from_sql, thumb_status_sql, thumb_select_sql


def load_filtered_media_dataframe(
    con: duckdb.DuckDBPyConnection,
    *,
    search_text: str,
    only_supported: bool,
    file_states: list[str],
    extensions: list[str],
    extract_statuses: list[str],
    thumb_statuses: list[str],
    parent_folders: list[str],
    camera_models: list[str],
    gps_filter: str,
    captured_from: datetime | None,
    captured_to: datetime | None,
    lat_min: float | None,
    lat_max: float | None,
    lon_min: float | None,
    lon_max: float | None,
    has_thumbnails: bool,
    page_size: int,
    page_number: int,
    sort_by_label: str,
    sort_direction: str,
) -> tuple[pd.DataFrame, int, int, int]:
    where_sql, params = build_media_filter_where_sql(
        search_text=search_text,
        only_supported=only_supported,
        file_states=file_states,
        extensions=extensions,
        extract_statuses=extract_statuses,
        thumb_statuses=thumb_statuses,
        parent_folders=parent_folders,
        camera_models=camera_models,
        gps_filter=gps_filter,
        captured_from=captured_from,
        captured_to=captured_to,
        has_thumbnails=has_thumbnails,
        lat_min=lat_min,
        lat_max=lat_max,
        lon_min=lon_min,
        lon_max=lon_max,
    )

    from_sql, thumb_status_sql, thumb_select_sql = _filtered_from_and_select_sql(has_thumbnails)

    count_query = f"SELECT COUNT(*) {from_sql} {where_sql}"
    total_rows = int(con.execute(count_query, params).fetchone()[0])
    safe_page_size = max(1, int(page_size))
    total_pages = max(1, (total_rows + safe_page_size - 1) // safe_page_size)
    safe_page_number = min(max(1, int(page_number)), total_pages)
    offset_rows = (safe_page_number - 1) * safe_page_size
    order_sql = resolve_media_sort_sql(
        sort_by_label=sort_by_label,
        sort_direction=sort_direction,
        has_thumbnails=has_thumbnails,
    )

    data_query = (
        "SELECT "
        "f.file_id, "
        "f.path, "
        "f.filename, "
        "f.parent_folder, "
        "f.extension, "
        "f.file_state, "
        "f.extract_status, "
        "f.captured_at, "
        "f.camera_model, "
        "f.gps_lat, "
        "f.gps_lon, "
        f"{thumb_status_sql}, "
        f"{thumb_select_sql} "
        f"{from_sql} "
        f"{where_sql} "
        f"ORDER BY {order_sql} "
        "LIMIT ? OFFSET ?"
    )
    df = con.execute(data_query, [*params, safe_page_size, offset_rows]).df()
    return df, total_rows, total_pages, safe_page_number


def export_filtered_media(
    con: duckdb.DuckDBPyConnection,
    *,
    search_text: str,
    only_supported: bool,
    file_states: list[str],
    extensions: list[str],
    extract_statuses: list[str],
    thumb_statuses: list[str],
    parent_folders: list[str],
    camera_models: list[str],
    gps_filter: str,
    captured_from: datetime | None,
    captured_to: datetime | None,
    lat_min: float | None,
    lat_max: float | None,
    lon_min: float | None,
    lon_max: float | None,
    has_thumbnails: bool,
    sort_by_label: str,
    sort_direction: str,
    export_path: Path,
    export_format: str,
) -> tuple[Path, int]:
    where_sql, params = build_media_filter_where_sql(
        search_text=search_text,
        only_supported=only_supported,
        file_states=file_states,
        extensions=extensions,
        extract_statuses=extract_statuses,
        thumb_statuses=thumb_statuses,
        parent_folders=parent_folders,
        camera_models=camera_models,
        gps_filter=gps_filter,
        captured_from=captured_from,
        captured_to=captured_to,
        has_thumbnails=has_thumbnails,
        lat_min=lat_min,
        lat_max=lat_max,
        lon_min=lon_min,
        lon_max=lon_max,
    )

    from_sql, thumb_status_sql, thumb_select_sql = _filtered_from_and_select_sql(has_thumbnails)
    order_sql = resolve_media_sort_sql(
        sort_by_label=sort_by_label,
        sort_direction=sort_direction,
        has_thumbnails=has_thumbnails,
    )
    select_query = (
        "SELECT "
        "f.file_id, "
        "f.path, "
        "f.filename, "
        "f.parent_folder, "
        "f.extension, "
        "f.file_state, "
        "f.extract_status, "
        "f.captured_at, "
        "f.camera_model, "
        "f.gps_lat, "
        "f.gps_lon, "
        f"{thumb_status_sql}, "
        f"{thumb_select_sql} "
        f"{from_sql} "
        f"{where_sql} "
        f"ORDER BY {order_sql}"
    )

    resolved = export_path.expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    escaped_path = str(resolved).replace("'", "''")

    row_count = int(
        con.execute(
            f"SELECT COUNT(*) FROM ({select_query}) AS filtered_export",
            params,
        ).fetchone()[0]
    )

    export_format_normalized = export_format.lower().strip()
    if export_format_normalized == "parquet":
        copy_sql = f"COPY ({select_query}) TO '{escaped_path}' (FORMAT PARQUET)"
    elif export_format_normalized == "csv":
        copy_sql = f"COPY ({select_query}) TO '{escaped_path}' (FORMAT CSV, HEADER TRUE)"
    else:
        raise ValueError(f"unsupported_export_format: {export_format}")

    con.execute(copy_sql, params)
    return resolved, row_count


def load_timeline_dataframe(
    con: duckdb.DuckDBPyConnection,
    *,
    search_text: str,
    only_supported: bool,
    file_states: list[str],
    extensions: list[str],
    extract_statuses: list[str],
    thumb_statuses: list[str],
    parent_folders: list[str],
    camera_models: list[str],
    gps_filter: str,
    captured_from: datetime | None,
    captured_to: datetime | None,
    lat_min: float | None,
    lat_max: float | None,
    lon_min: float | None,
    lon_max: float | None,
    has_thumbnails: bool,
    bucket_label: str,
) -> pd.DataFrame:
    where_sql, params = build_media_filter_where_sql(
        search_text=search_text,
        only_supported=only_supported,
        file_states=file_states,
        extensions=extensions,
        extract_statuses=extract_statuses,
        thumb_statuses=thumb_statuses,
        parent_folders=parent_folders,
        camera_models=camera_models,
        gps_filter=gps_filter,
        captured_from=captured_from,
        captured_to=captured_to,
        has_thumbnails=has_thumbnails,
        lat_min=lat_min,
        lat_max=lat_max,
        lon_min=lon_min,
        lon_max=lon_max,
    )

    if has_thumbnails:
        from_sql = (
            "FROM file_metadata AS f "
            "LEFT JOIN thumbnails AS t ON t.file_id = f.file_id "
        )
    else:
        from_sql = "FROM file_metadata AS f "

    bucket_sql = resolve_timeline_bucket_sql(bucket_label)
    query = (
        "SELECT "
        f"{bucket_sql} AS bucket_start, "
        "COUNT(*) AS media_count "
        f"{from_sql} "
        f"{where_sql} "
        f"GROUP BY {bucket_sql} "
        f"HAVING {bucket_sql} IS NOT NULL "
        "ORDER BY bucket_start ASC"
    )
    return con.execute(query, params).df()


def load_map_dataframes(
    con: duckdb.DuckDBPyConnection,
    *,
    search_text: str,
    only_supported: bool,
    file_states: list[str],
    extensions: list[str],
    extract_statuses: list[str],
    thumb_statuses: list[str],
    parent_folders: list[str],
    camera_models: list[str],
    gps_filter: str,
    captured_from: datetime | None,
    captured_to: datetime | None,
    lat_min: float | None,
    lat_max: float | None,
    lon_min: float | None,
    lon_max: float | None,
    has_thumbnails: bool,
    map_point_limit: int,
    summary_limit: int,
) -> tuple[pd.DataFrame, int, pd.DataFrame]:
    where_sql, params = build_media_filter_where_sql(
        search_text=search_text,
        only_supported=only_supported,
        file_states=file_states,
        extensions=extensions,
        extract_statuses=extract_statuses,
        thumb_statuses=thumb_statuses,
        parent_folders=parent_folders,
        camera_models=camera_models,
        gps_filter=gps_filter,
        captured_from=captured_from,
        captured_to=captured_to,
        has_thumbnails=has_thumbnails,
        lat_min=lat_min,
        lat_max=lat_max,
        lon_min=lon_min,
        lon_max=lon_max,
    )

    if has_thumbnails:
        from_sql = (
            "FROM file_metadata AS f "
            "LEFT JOIN thumbnails AS t ON t.file_id = f.file_id "
        )
    else:
        from_sql = "FROM file_metadata AS f "

    gps_clause = "f.gps_lat IS NOT NULL AND f.gps_lon IS NOT NULL"
    if where_sql:
        where_map_sql = f"{where_sql} AND {gps_clause}"
    else:
        where_map_sql = f"WHERE {gps_clause}"

    total_points = int(
        con.execute(
            f"SELECT COUNT(*) {from_sql} {where_map_sql}",
            params,
        ).fetchone()[0]
    )

    points_df = con.execute(
        (
            "SELECT "
            "f.file_id, "
            "f.path, "
            "f.filename, "
            "COALESCE(f.captured_at, f.fs_modified_at) AS captured_or_modified_at, "
            "f.camera_model, "
            "f.gps_lat AS latitude, "
            "f.gps_lon AS longitude "
            f"{from_sql} "
            f"{where_map_sql} "
            "ORDER BY COALESCE(f.captured_at, f.fs_modified_at) DESC, f.path ASC "
            "LIMIT ?"
        ),
        [*params, max(1, int(map_point_limit))],
    ).df()

    lat_expr = "ROUND(f.gps_lat, 3)"
    lon_expr = "ROUND(f.gps_lon, 3)"
    summary_df = con.execute(
        (
            "SELECT "
            f"{lat_expr} AS lat_bucket, "
            f"{lon_expr} AS lon_bucket, "
            "COUNT(*) AS media_count, "
            "MIN(COALESCE(f.captured_at, f.fs_modified_at)) AS first_seen_at, "
            "MAX(COALESCE(f.captured_at, f.fs_modified_at)) AS last_seen_at "
            f"{from_sql} "
            f"{where_map_sql} "
            f"GROUP BY {lat_expr}, {lon_expr} "
            "ORDER BY media_count DESC, lat_bucket ASC, lon_bucket ASC "
            "LIMIT ?"
        ),
        [*params, max(1, int(summary_limit))],
    ).df()

    return points_df, total_points, summary_df


def main() -> None:
    st.set_page_config(page_title="Photo Metadata Explorer", layout="wide")
    st.title("Photo Metadata Explorer")
    st.caption("Local-first metadata and thumbnail explorer.")

    default_db = str(DEFAULT_DB_PATH)
    db_path_input = st.text_input("DuckDB file path", value=default_db)
    treat_empty_as_missing = st.checkbox(
        "Treat empty strings as missing",
        value=True,
        help="Useful for text fields that are present but blank.",
    )
    sample_rows = st.slider("Sample rows to preview", min_value=10, max_value=500, value=100)
    show_gallery = st.checkbox("Show thumbnail gallery (if available)", value=True)
    gallery_limit = st.slider("Gallery max items", min_value=8, max_value=120, value=24, step=4)
    gallery_columns = st.slider("Gallery columns", min_value=2, max_value=6, value=4)

    db_path = Path(db_path_input).expanduser()
    if not db_path.exists():
        st.error(f"DuckDB file not found: {db_path}")
        st.stop()

    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except duckdb.Error as exc:
        st.error(f"Failed to open DuckDB file: {exc}")
        st.stop()
    try:
        tables = list_tables(con)
        if not tables:
            st.warning("No tables found in this DuckDB database.")
            st.stop()

        default_table_index = tables.index("file_metadata") if "file_metadata" in tables else 0
        table_name = st.selectbox("Table", options=tables, index=default_table_index)
        has_thumbnails = table_exists(con, "thumbnails")

        columns = get_table_columns(con, table_name)
        if not columns:
            st.warning(f"Table '{table_name}' has no columns.")
            st.stop()

        coverage_df, total_rows = build_coverage_dataframe(
            con=con,
            table_name=table_name,
            columns=columns,
            treat_empty_strings_as_missing=treat_empty_as_missing,
        )

        col_a, col_b, col_c = st.columns(3)
        col_a.metric("Rows", f"{total_rows:,}")
        col_b.metric("Columns", f"{len(columns):,}")
        populated_half = int((coverage_df["non_null_pct"] >= 50.0).sum()) if not coverage_df.empty else 0
        col_c.metric("Fields >= 50% filled", f"{populated_half:,}")

        st.subheader("Field Coverage")
        st.dataframe(coverage_df, width="stretch")

        st.subheader("Least Populated Fields")
        st.dataframe(
            coverage_df.sort_values(by=["non_null_pct", "column_name"], ascending=[True, True]).head(15),
            width="stretch",
        )

        st.subheader("Data Preview")
        preview_df = con.execute(
            f"SELECT * FROM {quote_ident(table_name)} LIMIT {int(sample_rows)}"
        ).df()
        st.dataframe(preview_df, width="stretch")

        if has_thumbnails:
            st.divider()
            st.subheader("Thumbnails")

            status_df = build_thumbnail_status_dataframe(con)
            total_thumbs = int(status_df["count"].sum()) if not status_df.empty else 0
            success_count = (
                int(status_df.loc[status_df["status"] == "success", "count"].sum())
                if not status_df.empty
                else 0
            )
            failed_count = (
                int(status_df.loc[status_df["status"] == "failed", "count"].sum())
                if not status_df.empty
                else 0
            )
            col_t1, col_t2, col_t3 = st.columns(3)
            col_t1.metric("Thumbnail rows", f"{total_thumbs:,}")
            col_t2.metric("Successful", f"{success_count:,}")
            col_t3.metric("Failed", f"{failed_count:,}")

            st.caption("Status counts from the `thumbnails` table.")
            st.dataframe(status_df, width="stretch")

            thumb_preview_df = load_thumbnail_preview_dataframe(
                con,
                limit=max(100, gallery_limit * 4),
            )
            st.caption("Recent thumbnail rows")
            st.dataframe(thumb_preview_df.head(sample_rows), width="stretch")

            if show_gallery:
                st.subheader("Thumbnail Gallery Preview")
                cols = st.columns(gallery_columns)
                shown = 0
                for row in thumb_preview_df.itertuples(index=False):
                    if shown >= gallery_limit:
                        break
                    if str(row.status) != "success":
                        continue
                    thumb_path = Path(str(row.thumb_path)).expanduser()
                    if not thumb_path.exists():
                        continue
                    caption_parts = []
                    filename = str(getattr(row, "filename", "") or "")
                    if filename:
                        caption_parts.append(filename)
                    if row.width and row.height:
                        caption_parts.append(f"{int(row.width)}x{int(row.height)}")
                    caption = " | ".join(caption_parts) if caption_parts else thumb_path.name
                    cols[shown % gallery_columns].image(
                        str(thumb_path),
                        caption=caption,
                        width="stretch",
                    )
                    shown += 1

                if shown == 0:
                    st.info("No displayable successful thumbnails found yet.")
                else:
                    st.caption(f"Showing {shown} thumbnail(s).")
        else:
            st.info(
                "No `thumbnails` table found yet. Run `python -m photo_archive.cli thumbs` "
                "to generate thumbnails first."
            )

        st.divider()
        st.subheader("Filter + Gallery")
        st.caption("Filter indexed rows and preview matching thumbnails.")
        if st.button("Reset filters + paging", key="flt_reset_button"):
            reset_filter_controls()
            st.rerun()

        col_f1, col_f2 = st.columns(2)
        search_text = col_f1.text_input(
            "Search path / filename / camera model",
            value="",
            key="flt_search_text",
        )
        only_supported = col_f2.checkbox("Only supported files", value=True, key="flt_only_supported")

        file_state_options = list_distinct_values(
            con,
            table_name="file_metadata",
            column_name="file_state",
            where_sql="file_state IS NOT NULL",
        )
        parent_folder_options = list_distinct_values(
            con,
            table_name="file_metadata",
            column_name="parent_folder",
            where_sql="parent_folder IS NOT NULL",
        )
        camera_model_options = list_distinct_values(
            con,
            table_name="file_metadata",
            column_name="camera_model",
            where_sql="camera_model IS NOT NULL",
        )
        extension_options = list_distinct_values(
            con,
            table_name="file_metadata",
            column_name="extension",
            where_sql="extension IS NOT NULL",
        )
        extract_status_options = list_distinct_values(
            con,
            table_name="file_metadata",
            column_name="extract_status",
            where_sql="extract_status IS NOT NULL",
        )
        thumbnail_status_options = ["[none]"]
        if has_thumbnails:
            thumbnail_status_options.extend(
                list_distinct_values(
                    con,
                    table_name="thumbnails",
                    column_name="status",
                    where_sql="status IS NOT NULL",
                )
            )

        col_f3, col_f4, col_f5 = st.columns(3)
        selected_file_states = col_f3.multiselect(
            "File state",
            options=file_state_options,
            default=[item for item in file_state_options if item != "missing"] or file_state_options,
            key="flt_file_states",
        )
        selected_extensions = col_f4.multiselect(
            "Extension",
            options=extension_options,
            default=extension_options,
            key="flt_extensions",
        )
        selected_extract_statuses = col_f5.multiselect(
            "Extract status",
            options=extract_status_options,
            default=extract_status_options,
            key="flt_extract_statuses",
        )

        col_f6, col_f7, col_f8 = st.columns(3)
        selected_parent_folders = col_f6.multiselect(
            "Parent folder",
            options=parent_folder_options,
            default=[],
            help="Leave empty to include all folders.",
            key="flt_parent_folders",
        )
        selected_camera_models = col_f7.multiselect(
            "Camera model",
            options=camera_model_options,
            default=[],
            help="Leave empty to include all camera models.",
            key="flt_camera_models",
        )
        gps_filter = col_f8.selectbox(
            "GPS filter",
            options=["any", "has_gps", "no_gps"],
            index=0,
            key="flt_gps_filter",
        )

        col_f9, col_f10, col_f11 = st.columns(3)
        apply_date_filter = col_f9.checkbox(
            "Apply captured_at date filter",
            value=False,
            key="flt_apply_date_filter",
        )
        captured_from_date = col_f10.date_input(
            "Captured from",
            value=date(2000, 1, 1),
            disabled=not apply_date_filter,
            key="flt_captured_from_date",
        )
        captured_to_date = col_f11.date_input(
            "Captured to",
            value=date.today(),
            disabled=not apply_date_filter,
            key="flt_captured_to_date",
        )

        captured_from_dt: datetime | None = None
        captured_to_dt: datetime | None = None
        if apply_date_filter:
            if captured_from_date > captured_to_date:
                st.warning("Captured-from date is after captured-to date; swapping values.")
                captured_from_date, captured_to_date = captured_to_date, captured_from_date
            captured_from_dt = datetime.combine(captured_from_date, time.min)
            captured_to_dt = datetime.combine(captured_to_date + timedelta(days=1), time.min)

        lat_min, lat_max, lon_min, lon_max = get_bbox_filters()
        bbox_active = all(value is not None for value in (lat_min, lat_max, lon_min, lon_max))
        if bbox_active:
            st.caption(
                "Active map box filter: "
                f"lat [{lat_min:.5f}, {lat_max:.5f}] | "
                f"lon [{lon_min:.5f}, {lon_max:.5f}]"
            )
            if st.button("Clear map box filter", key="flt_clear_bbox_button"):
                clear_bbox_filters()
                st.rerun()

        col_f12, col_f13, col_f14 = st.columns(3)
        selected_thumb_statuses = col_f12.multiselect(
            "Thumbnail status",
            options=thumbnail_status_options,
            default=thumbnail_status_options,
            key="flt_thumb_statuses",
        )
        sort_by_label = col_f13.selectbox(
            "Sort by",
            options=list(SORT_LABEL_TO_SQL.keys()),
            index=0,
            key="flt_sort_by",
        )
        sort_direction = col_f14.selectbox(
            "Sort direction",
            options=["desc", "asc"],
            index=0,
            key="flt_sort_direction",
        )

        col_f15, col_f16, col_f17 = st.columns(3)
        page_size = col_f15.selectbox(
            "Page size",
            options=[25, 50, 100, 250, 500],
            index=2,
            key="flt_page_size",
        )
        requested_page = int(
            col_f16.number_input(
                "Page",
                min_value=1,
                value=1,
                step=1,
                key="flt_page_number",
            )
        )
        filtered_gallery_limit = col_f17.slider(
            "Filtered gallery max items",
            min_value=8,
            max_value=120,
            value=24,
            step=4,
            key="flt_filtered_gallery_limit",
        )

        filtered_df, filtered_total, total_pages, current_page = load_filtered_media_dataframe(
            con=con,
            search_text=search_text,
            only_supported=only_supported,
            file_states=selected_file_states,
            extensions=selected_extensions,
            extract_statuses=selected_extract_statuses,
            thumb_statuses=selected_thumb_statuses,
            parent_folders=selected_parent_folders,
            camera_models=selected_camera_models,
            gps_filter=gps_filter,
            captured_from=captured_from_dt,
            captured_to=captured_to_dt,
            lat_min=lat_min,
            lat_max=lat_max,
            lon_min=lon_min,
            lon_max=lon_max,
            has_thumbnails=has_thumbnails,
            page_size=page_size,
            page_number=requested_page,
            sort_by_label=sort_by_label,
            sort_direction=sort_direction,
        )

        if current_page != requested_page:
            st.caption(f"Requested page {requested_page} adjusted to {current_page} based on result size.")

        page_start = 0 if filtered_total == 0 else ((current_page - 1) * int(page_size)) + 1
        page_end = min(current_page * int(page_size), filtered_total)
        st.metric("Filtered matches", f"{filtered_total:,}")
        st.caption(
            f"Page {current_page}/{total_pages} | showing rows {page_start}-{page_end} | "
            f"sort={sort_by_label} ({sort_direction})"
        )
        st.dataframe(filtered_df, width="stretch")

        st.caption("Export the full filtered result set (not just this page).")
        col_ex1, col_ex2, col_ex3, col_ex4 = st.columns([1, 2, 2, 1])
        export_format = col_ex1.selectbox(
            "Export format",
            options=["csv", "parquet"],
            index=0,
            key="flt_export_format",
        )
        export_name = col_ex2.text_input(
            "Export filename",
            value="filtered_media_export",
            key="flt_export_name",
        )
        export_dir_text = col_ex3.text_input(
            "Export directory",
            value="data/exports",
            key="flt_export_dir",
        )
        export_clicked = col_ex4.button("Export", key="flt_export_button")
        if export_clicked:
            filename_base = sanitize_filename_component(export_name)
            suffix = ".parquet" if export_format == "parquet" else ".csv"
            final_name = (
                filename_base
                if filename_base.lower().endswith(suffix)
                else f"{filename_base}{suffix}"
            )
            export_dir = Path(export_dir_text).expanduser()
            if not export_dir.is_absolute():
                export_dir = (Path.cwd() / export_dir)
            export_path = export_dir / final_name

            try:
                written_path, exported_rows = export_filtered_media(
                    con=con,
                    search_text=search_text,
                    only_supported=only_supported,
                    file_states=selected_file_states,
                    extensions=selected_extensions,
                    extract_statuses=selected_extract_statuses,
                    thumb_statuses=selected_thumb_statuses,
                    parent_folders=selected_parent_folders,
                    camera_models=selected_camera_models,
                    gps_filter=gps_filter,
                    captured_from=captured_from_dt,
                    captured_to=captured_to_dt,
                    lat_min=lat_min,
                    lat_max=lat_max,
                    lon_min=lon_min,
                    lon_max=lon_max,
                    has_thumbnails=has_thumbnails,
                    sort_by_label=sort_by_label,
                    sort_direction=sort_direction,
                    export_path=export_path,
                    export_format=export_format,
                )
                st.success(f"Exported {exported_rows:,} rows to {written_path}")
            except Exception as exc:  # noqa: BLE001
                st.error(f"Export failed: {type(exc).__name__}: {exc}")

        st.subheader("Timeline")
        col_tl1, col_tl2 = st.columns(2)
        timeline_bucket = col_tl1.selectbox(
            "Timeline bucket",
            options=list(TIMELINE_BUCKET_TO_SQL.keys()),
            index=0,
            key="flt_timeline_bucket",
        )
        timeline_chart_type = col_tl2.selectbox(
            "Timeline chart",
            options=["line", "bar"],
            index=0,
            key="flt_timeline_chart_type",
        )

        timeline_df = load_timeline_dataframe(
            con=con,
            search_text=search_text,
            only_supported=only_supported,
            file_states=selected_file_states,
            extensions=selected_extensions,
            extract_statuses=selected_extract_statuses,
            thumb_statuses=selected_thumb_statuses,
            parent_folders=selected_parent_folders,
            camera_models=selected_camera_models,
            gps_filter=gps_filter,
            captured_from=captured_from_dt,
            captured_to=captured_to_dt,
            lat_min=lat_min,
            lat_max=lat_max,
            lon_min=lon_min,
            lon_max=lon_max,
            has_thumbnails=has_thumbnails,
            bucket_label=timeline_bucket,
        )

        if timeline_df.empty:
            st.info("No timeline data for current filters.")
        else:
            timeline_df["bucket_start"] = pd.to_datetime(timeline_df["bucket_start"])
            timeline_series = timeline_df.set_index("bucket_start")["media_count"]
            if timeline_chart_type == "line":
                st.line_chart(timeline_series, width="stretch")
            else:
                st.bar_chart(timeline_series, width="stretch")

            col_tm1, col_tm2, col_tm3 = st.columns(3)
            col_tm1.metric("Timeline buckets", f"{len(timeline_df):,}")
            col_tm2.metric("Timeline media total", f"{int(timeline_df['media_count'].sum()):,}")
            col_tm3.metric("Peak bucket count", f"{int(timeline_df['media_count'].max()):,}")

            st.dataframe(timeline_df, width="stretch")

        st.subheader("Map")
        col_map1, col_map2 = st.columns(2)
        map_point_limit = col_map1.slider(
            "Map point cap",
            min_value=100,
            max_value=10000,
            value=2000,
            step=100,
            help="Limit rendered map points for responsive local browsing.",
            key="flt_map_point_limit",
        )
        map_summary_limit = col_map2.slider(
            "Map location summary rows",
            min_value=10,
            max_value=500,
            value=100,
            step=10,
            key="flt_map_summary_limit",
        )

        map_points_df, map_total_points, map_summary_df = load_map_dataframes(
            con=con,
            search_text=search_text,
            only_supported=only_supported,
            file_states=selected_file_states,
            extensions=selected_extensions,
            extract_statuses=selected_extract_statuses,
            thumb_statuses=selected_thumb_statuses,
            parent_folders=selected_parent_folders,
            camera_models=selected_camera_models,
            gps_filter=gps_filter,
            captured_from=captured_from_dt,
            captured_to=captured_to_dt,
            lat_min=lat_min,
            lat_max=lat_max,
            lon_min=lon_min,
            lon_max=lon_max,
            has_thumbnails=has_thumbnails,
            map_point_limit=map_point_limit,
            summary_limit=map_summary_limit,
        )

        if map_total_points == 0:
            st.info("No mappable points for current filters.")
        else:
            displayed_points = len(map_points_df)
            col_mp1, col_mp2, col_mp3 = st.columns(3)
            col_mp1.metric("Points with GPS", f"{map_total_points:,}")
            col_mp2.metric("Map points shown", f"{displayed_points:,}")
            col_mp3.metric("Unique map buckets", f"{len(map_summary_df):,}")

            if map_total_points > displayed_points:
                st.caption(
                    f"Showing first {displayed_points:,} of {map_total_points:,} points. "
                    "Increase `Map point cap` to render more."
                )

            st.caption("Draw a rectangle to set lon/lat filters for the full page.")
            if folium is None or Draw is None or st_folium is None:
                st.map(
                    map_points_df[["latitude", "longitude"]],
                )
                st.info(
                    "Install optional UI dependencies to enable box selection: "
                    "`pip install -e .[ui]`"
                )
            else:
                drawn_bounds = render_interactive_bbox_map(map_points_df=map_points_df)
                if drawn_bounds is None:
                    st.caption("No rectangle selected yet.")
                else:
                    south, west, north, east = drawn_bounds
                    st.caption(
                        "Drawn box: "
                        f"lat [{south:.5f}, {north:.5f}] | "
                        f"lon [{west:.5f}, {east:.5f}]"
                    )
                    st.button(
                        "Apply drawn box to filters",
                        key="flt_apply_bbox_button",
                        on_click=apply_bbox_filters,
                        args=(south, west, north, east),
                    )

            st.caption("Top rounded GPS buckets (3-decimal precision) for current filters.")
            st.dataframe(map_summary_df, width="stretch")

        if show_gallery:
            st.subheader("Filtered Gallery")
            gallery_cols = st.columns(gallery_columns)
            shown_filtered = 0
            for row in filtered_df.itertuples(index=False):
                if shown_filtered >= filtered_gallery_limit:
                    break
                if str(getattr(row, "thumbnail_status", "")) != "success":
                    continue
                thumb_path_value = getattr(row, "thumb_path", None)
                if thumb_path_value in (None, ""):
                    continue
                thumb_path = Path(str(thumb_path_value)).expanduser()
                if not thumb_path.exists():
                    continue
                file_label = str(getattr(row, "filename", "") or "")
                dim_parts = []
                thumb_width = getattr(row, "thumb_width", None)
                thumb_height = getattr(row, "thumb_height", None)
                if thumb_width and thumb_height:
                    dim_parts.append(f"{int(thumb_width)}x{int(thumb_height)}")
                caption = " | ".join(
                    [part for part in [file_label, *dim_parts] if part]
                ) or thumb_path.name
                gallery_cols[shown_filtered % gallery_columns].image(
                    str(thumb_path),
                    caption=caption,
                    width="stretch",
                )
                shown_filtered += 1

            if shown_filtered == 0:
                st.info("No successful thumbnails in current filtered selection.")
            else:
                st.caption(f"Showing {shown_filtered} filtered thumbnail(s).")
    finally:
        con.close()


if __name__ == "__main__":
    main()
