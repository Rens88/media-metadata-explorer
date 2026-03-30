from datetime import datetime

from photo_archive.query_filters import (
    build_media_filter_where_sql,
    resolve_media_sort_sql,
    resolve_timeline_bucket_sql,
)


def test_build_media_filter_where_sql_with_extended_filters() -> None:
    captured_from = datetime(2024, 1, 1, 0, 0, 0)
    captured_to = datetime(2024, 2, 1, 0, 0, 0)

    where_sql, params = build_media_filter_where_sql(
        search_text="holiday",
        only_supported=True,
        file_states=["new", "changed"],
        extensions=[".jpg", ".heic"],
        extract_statuses=["success", "failed"],
        thumb_statuses=["success", "[none]"],
        parent_folders=["/photos/Trips", "/photos/Family"],
        camera_models=["iPhone 15", "Canon R6"],
        gps_filter="has_gps",
        captured_from=captured_from,
        captured_to=captured_to,
        has_thumbnails=True,
    )

    assert where_sql.startswith("WHERE ")
    assert "f.is_supported = TRUE" in where_sql
    assert "f.file_state IN (?, ?)" in where_sql
    assert "f.extension IN (?, ?)" in where_sql
    assert "f.extract_status IN (?, ?)" in where_sql
    assert "f.parent_folder IN (?, ?)" in where_sql
    assert "f.camera_model IN (?, ?)" in where_sql
    assert "f.gps_lat IS NOT NULL AND f.gps_lon IS NOT NULL" in where_sql
    assert "f.captured_at >= ?" in where_sql
    assert "f.captured_at < ?" in where_sql
    assert "LOWER(f.path) LIKE ?" in where_sql
    assert "(t.status IN (?) OR t.file_id IS NULL)" in where_sql

    assert params == [
        "new",
        "changed",
        ".jpg",
        ".heic",
        "success",
        "failed",
        "/photos/Trips",
        "/photos/Family",
        "iPhone 15",
        "Canon R6",
        captured_from,
        captured_to,
        "%holiday%",
        "%holiday%",
        "%holiday%",
        "success",
    ]


def test_build_media_filter_where_sql_no_thumbnail_table_blocks_specific_status_filter() -> None:
    where_sql, params = build_media_filter_where_sql(
        search_text="",
        only_supported=False,
        file_states=[],
        extensions=[],
        extract_statuses=[],
        thumb_statuses=["failed"],
        parent_folders=[],
        camera_models=[],
        gps_filter="any",
        captured_from=None,
        captured_to=None,
        has_thumbnails=False,
    )

    assert "1 = 0" in where_sql
    assert params == []


def test_build_media_filter_where_sql_no_filters_returns_empty_where() -> None:
    where_sql, params = build_media_filter_where_sql(
        search_text="",
        only_supported=False,
        file_states=[],
        extensions=[],
        extract_statuses=[],
        thumb_statuses=[],
        parent_folders=[],
        camera_models=[],
        gps_filter="any",
        captured_from=None,
        captured_to=None,
        has_thumbnails=True,
    )

    assert where_sql == ""
    assert params == []


def test_resolve_media_sort_sql_uses_whitelist_and_direction() -> None:
    order_sql = resolve_media_sort_sql(
        sort_by_label="Path",
        sort_direction="asc",
        has_thumbnails=True,
    )
    assert order_sql == "f.path ASC NULLS LAST, f.path ASC"


def test_resolve_media_sort_sql_falls_back_when_thumbnail_sort_without_table() -> None:
    order_sql = resolve_media_sort_sql(
        sort_by_label="Thumbnail generated",
        sort_direction="desc",
        has_thumbnails=False,
    )
    assert order_sql == "COALESCE(f.captured_at, f.fs_modified_at) DESC NULLS LAST, f.path ASC"


def test_resolve_timeline_bucket_sql_defaults_to_day() -> None:
    expr = resolve_timeline_bucket_sql("not-a-bucket")
    assert expr == "DATE_TRUNC('day', COALESCE(f.captured_at, f.fs_modified_at))"


def test_resolve_timeline_bucket_sql_month() -> None:
    expr = resolve_timeline_bucket_sql("Month")
    assert expr == "DATE_TRUNC('month', COALESCE(f.captured_at, f.fs_modified_at))"
