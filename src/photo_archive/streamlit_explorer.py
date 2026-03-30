from __future__ import annotations

from datetime import date, datetime, time, timedelta
from pathlib import Path
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

DEFAULT_DB_PATH = Path("data/db/photo_archive.duckdb")


def quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


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
    )

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

        col_f1, col_f2 = st.columns(2)
        search_text = col_f1.text_input(
            "Search path / filename / camera model",
            value="",
        )
        only_supported = col_f2.checkbox("Only supported files", value=True)

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
        )
        selected_extensions = col_f4.multiselect(
            "Extension",
            options=extension_options,
            default=extension_options,
        )
        selected_extract_statuses = col_f5.multiselect(
            "Extract status",
            options=extract_status_options,
            default=extract_status_options,
        )

        col_f6, col_f7, col_f8 = st.columns(3)
        selected_parent_folders = col_f6.multiselect(
            "Parent folder",
            options=parent_folder_options,
            default=[],
            help="Leave empty to include all folders.",
        )
        selected_camera_models = col_f7.multiselect(
            "Camera model",
            options=camera_model_options,
            default=[],
            help="Leave empty to include all camera models.",
        )
        gps_filter = col_f8.selectbox(
            "GPS filter",
            options=["any", "has_gps", "no_gps"],
            index=0,
        )

        col_f9, col_f10, col_f11 = st.columns(3)
        apply_date_filter = col_f9.checkbox("Apply captured_at date filter", value=False)
        captured_from_date = col_f10.date_input(
            "Captured from",
            value=date(2000, 1, 1),
            disabled=not apply_date_filter,
        )
        captured_to_date = col_f11.date_input(
            "Captured to",
            value=date.today(),
            disabled=not apply_date_filter,
        )

        captured_from_dt: datetime | None = None
        captured_to_dt: datetime | None = None
        if apply_date_filter:
            if captured_from_date > captured_to_date:
                st.warning("Captured-from date is after captured-to date; swapping values.")
                captured_from_date, captured_to_date = captured_to_date, captured_from_date
            captured_from_dt = datetime.combine(captured_from_date, time.min)
            captured_to_dt = datetime.combine(captured_to_date + timedelta(days=1), time.min)

        col_f12, col_f13, col_f14 = st.columns(3)
        selected_thumb_statuses = col_f12.multiselect(
            "Thumbnail status",
            options=thumbnail_status_options,
            default=thumbnail_status_options,
        )
        sort_by_label = col_f13.selectbox(
            "Sort by",
            options=list(SORT_LABEL_TO_SQL.keys()),
            index=0,
        )
        sort_direction = col_f14.selectbox(
            "Sort direction",
            options=["desc", "asc"],
            index=0,
        )

        col_f15, col_f16, col_f17 = st.columns(3)
        page_size = col_f15.selectbox(
            "Page size",
            options=[25, 50, 100, 250, 500],
            index=2,
        )
        requested_page = int(
            col_f16.number_input(
                "Page",
                min_value=1,
                value=1,
                step=1,
            )
        )
        filtered_gallery_limit = col_f17.slider(
            "Filtered gallery max items",
            min_value=8,
            max_value=120,
            value=24,
            step=4,
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

        st.subheader("Timeline")
        col_tl1, col_tl2 = st.columns(2)
        timeline_bucket = col_tl1.selectbox(
            "Timeline bucket",
            options=list(TIMELINE_BUCKET_TO_SQL.keys()),
            index=0,
        )
        timeline_chart_type = col_tl2.selectbox(
            "Timeline chart",
            options=["line", "bar"],
            index=0,
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
