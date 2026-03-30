from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import streamlit as st

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


def main() -> None:
    st.set_page_config(page_title="Photo Metadata Explorer", layout="wide")
    st.title("Photo Metadata Explorer")
    st.caption("Phase 1 quick view: field availability and null coverage.")

    default_db = str(DEFAULT_DB_PATH)
    db_path_input = st.text_input("DuckDB file path", value=default_db)
    treat_empty_as_missing = st.checkbox(
        "Treat empty strings as missing",
        value=True,
        help="Useful for text fields that are present but blank.",
    )
    sample_rows = st.slider("Sample rows to preview", min_value=10, max_value=500, value=100)

    db_path = Path(db_path_input).expanduser()
    if not db_path.exists():
        st.error(f"DuckDB file not found: {db_path}")
        st.stop()

    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except duckdb.Error as exc:
        st.error(f"Failed to open DuckDB file: {exc}")
        st.stop()

    tables = list_tables(con)
    if not tables:
        st.warning("No tables found in this DuckDB database.")
        st.stop()

    default_table_index = tables.index("file_metadata") if "file_metadata" in tables else 0
    table_name = st.selectbox("Table", options=tables, index=default_table_index)

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
    st.dataframe(coverage_df, use_container_width=True)

    st.subheader("Least Populated Fields")
    st.dataframe(
        coverage_df.sort_values(by=["non_null_pct", "column_name"], ascending=[True, True]).head(15),
        use_container_width=True,
    )

    st.subheader("Data Preview")
    preview_df = con.execute(
        f"SELECT * FROM {quote_ident(table_name)} LIMIT {int(sample_rows)}"
    ).df()
    st.dataframe(preview_df, use_container_width=True)


if __name__ == "__main__":
    main()
