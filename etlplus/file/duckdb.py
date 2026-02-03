"""
:mod:`etlplus.file.duckdb` module.

Helpers for reading/writing DuckDB database (DUCKDB) files.

Notes
-----
- A DUCKDB file is a self-contained, serverless database file format used by
    DuckDB.
- Common cases:
    - Analytical data storage and processing.
    - Embedded database applications.
    - Fast querying of large datasets.
- Rule of thumb:
    - If the file follows the DUCKDB specification, use this module for reading
        and writing.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..types import JSONData
from ..types import JSONList
from ._imports import get_optional_module
from ._io import normalize_records

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL CONSTANTS ============================================== #


DEFAULT_TABLE = 'data'


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _coerce_sql_value(
    value: Any,
) -> Any:
    """
    Normalize values into DuckDB-compatible types.

    Parameters
    ----------
    value : Any
        The value to normalize.

    Returns
    -------
    Any
        The normalized value.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, ensure_ascii=True)


def _get_duckdb() -> Any:
    """
    Return the duckdb module, importing it on first use.

    Returns
    -------
    Any
        The duckdb module.
    """
    return get_optional_module(
        'duckdb',
        error_message=(
            'DUCKDB support requires optional dependency "duckdb".\n'
            'Install with: pip install duckdb'
        ),
    )


def _infer_column_type(values: list[Any]) -> str:
    """Infer a basic DuckDB column type from sample values."""
    seen_bool = False
    seen_int = False
    seen_float = False
    seen_other = False
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            seen_bool = True
        elif isinstance(value, int):
            seen_int = True
        elif isinstance(value, float):
            seen_float = True
        else:
            seen_other = True
            break
    if seen_other:
        return 'VARCHAR'
    if seen_float:
        return 'DOUBLE'
    if seen_int:
        return 'BIGINT'
    if seen_bool:
        return 'BOOLEAN'
    return 'VARCHAR'


def _quote_identifier(value: str) -> str:
    """Return a safely quoted SQL identifier."""
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def _resolve_table(tables: list[str]) -> str | None:
    """Pick a table name for read operations."""
    if not tables:
        return None
    if DEFAULT_TABLE in tables:
        return DEFAULT_TABLE
    if len(tables) == 1:
        return tables[0]
    raise ValueError(
        'Multiple tables found in DuckDB file; expected "data" or a '
        'single table',
    )


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONList:
    """
    Read DUCKDB content from *path*.

    Parameters
    ----------
    path : Path
        Path to the DUCKDB file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the DUCKDB file.
    """
    duckdb = _get_duckdb()
    conn = duckdb.connect(str(path))
    try:
        tables = [row[0] for row in conn.execute('SHOW TABLES').fetchall()]
        table = _resolve_table(tables)
        if table is None:
            return []
        query = f'SELECT * FROM {_quote_identifier(table)}'
        cursor = conn.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description or []]
        if not columns:
            info = conn.execute(
                f'PRAGMA table_info({_quote_identifier(table)})',
            ).fetchall()
            columns = [row[1] for row in info]
        return [dict(zip(columns, row, strict=True)) for row in rows]
    finally:
        conn.close()


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to DUCKDB at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the DUCKDB file on disk.
    data : JSONData
        Data to write as DUCKDB. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the DUCKDB file.
    """
    records = normalize_records(data, 'DUCKDB')
    if not records:
        return 0

    columns = sorted({key for row in records for key in row})
    if not columns:
        return 0

    column_values: dict[str, list[Any]] = {col: [] for col in columns}
    for row in records:
        for column in columns:
            column_values[column].append(row.get(column))

    column_defs = ', '.join(
        f'{_quote_identifier(column)} {_infer_column_type(values)}'
        for column, values in column_values.items()
    )
    table_ident = _quote_identifier(DEFAULT_TABLE)
    insert_columns = ', '.join(_quote_identifier(column) for column in columns)
    placeholders = ', '.join('?' for _ in columns)
    insert_sql = (
        f'INSERT INTO {table_ident} ({insert_columns}) VALUES ({placeholders})'
    )

    duckdb = _get_duckdb()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    try:
        conn.execute(f'DROP TABLE IF EXISTS {table_ident}')
        conn.execute(f'CREATE TABLE {table_ident} ({column_defs})')
        rows = [
            tuple(_coerce_sql_value(row.get(column)) for column in columns)
            for row in records
        ]
        conn.executemany(insert_sql, rows)
    finally:
        conn.close()
    return len(records)
