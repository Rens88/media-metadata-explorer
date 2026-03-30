from __future__ import annotations

from datetime import datetime
from typing import Any

SORT_LABEL_TO_SQL = {
    "Captured time": "COALESCE(f.captured_at, f.fs_modified_at)",
    "Path": "f.path",
    "Filename": "f.filename",
    "File size": "f.size_bytes",
    "Modified time": "f.fs_modified_at",
    "Camera model": "f.camera_model",
    "Thumbnail generated": "t.generated_at",
}

TIMELINE_BUCKET_TO_SQL = {
    "Day": "DATE_TRUNC('day', COALESCE(f.captured_at, f.fs_modified_at))",
    "Month": "DATE_TRUNC('month', COALESCE(f.captured_at, f.fs_modified_at))",
    "Year": "DATE_TRUNC('year', COALESCE(f.captured_at, f.fs_modified_at))",
}


def _in_clause(column_sql: str, values: list[str], params: list[Any]) -> str:
    placeholders = ", ".join(["?"] * len(values))
    params.extend(values)
    return f"{column_sql} IN ({placeholders})"


def build_media_filter_where_sql(
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
) -> tuple[str, list[Any]]:
    filters: list[str] = []
    params: list[Any] = []

    if only_supported:
        filters.append("f.is_supported = TRUE")

    if file_states:
        filters.append(_in_clause("f.file_state", file_states, params))
    if extensions:
        filters.append(_in_clause("f.extension", extensions, params))
    if extract_statuses:
        filters.append(_in_clause("f.extract_status", extract_statuses, params))
    if parent_folders:
        filters.append(_in_clause("f.parent_folder", parent_folders, params))
    if camera_models:
        filters.append(_in_clause("f.camera_model", camera_models, params))

    if gps_filter == "has_gps":
        filters.append("f.gps_lat IS NOT NULL AND f.gps_lon IS NOT NULL")
    elif gps_filter == "no_gps":
        filters.append("(f.gps_lat IS NULL OR f.gps_lon IS NULL)")

    if captured_from is not None:
        filters.append("f.captured_at >= ?")
        params.append(captured_from)
    if captured_to is not None:
        filters.append("f.captured_at < ?")
        params.append(captured_to)

    normalized_search = search_text.strip().lower()
    if normalized_search:
        like_value = f"%{normalized_search}%"
        filters.append(
            "("
            "LOWER(f.path) LIKE ? "
            "OR LOWER(COALESCE(f.filename, '')) LIKE ? "
            "OR LOWER(COALESCE(f.camera_model, '')) LIKE ?"
            ")"
        )
        params.extend([like_value, like_value, like_value])

    if thumb_statuses:
        if not has_thumbnails:
            if "[none]" not in thumb_statuses:
                filters.append("1 = 0")
        else:
            selected_statuses = [item for item in thumb_statuses if item != "[none]"]
            thumb_filters: list[str] = []
            if selected_statuses:
                thumb_filters.append(_in_clause("t.status", selected_statuses, params))
            if "[none]" in thumb_statuses:
                thumb_filters.append("t.file_id IS NULL")
            if thumb_filters:
                filters.append("(" + " OR ".join(thumb_filters) + ")")

    if not filters:
        return "", params
    return "WHERE " + " AND ".join(filters), params


def resolve_media_sort_sql(
    *,
    sort_by_label: str,
    sort_direction: str,
    has_thumbnails: bool,
) -> str:
    default_expr = SORT_LABEL_TO_SQL["Captured time"]
    sort_expr = SORT_LABEL_TO_SQL.get(sort_by_label, default_expr)
    if sort_expr == "t.generated_at" and not has_thumbnails:
        sort_expr = default_expr

    direction = "ASC" if str(sort_direction).lower() == "asc" else "DESC"
    return f"{sort_expr} {direction} NULLS LAST, f.path ASC"


def resolve_timeline_bucket_sql(bucket_label: str) -> str:
    return TIMELINE_BUCKET_TO_SQL.get(bucket_label, TIMELINE_BUCKET_TO_SQL["Day"])
