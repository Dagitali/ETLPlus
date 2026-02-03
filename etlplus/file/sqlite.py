"""
:mod:`etlplus.file.sqlite` module.

Helpers for reading/writing SQLite database (SQLITE) files.

Notes
-----
- A SQLITE file is a self-contained, serverless database file format used by
    SQLite.
- Common cases:
    - Lightweight database applications.
    - Embedded database solutions.
    - Mobile and desktop applications requiring local data storage.
- Rule of thumb:
    - If the file follows the SQLITE specification, use this module for reading
        and writing.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from ..types import JSONData
from ..types import JSONList
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


def _quote_identifier(value: str) -> str:
    """Return a safely quoted SQL identifier."""
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def _coerce_sql_value(value: Any) -> Any:
    """Normalize values into SQLite-compatible types."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, ensure_ascii=True)


def _infer_column_type(values: list[Any]) -> str:
    """Infer a basic SQLite column type from sample values."""
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
        return 'TEXT'
    if seen_float:
        return 'REAL'
    if seen_int or seen_bool:
        return 'INTEGER'
    return 'TEXT'


def _resolve_table(tables: list[str]) -> str | None:
    """Pick a table name for read operations."""
    if not tables:
        return None
    if DEFAULT_TABLE in tables:
        return DEFAULT_TABLE
    if len(tables) == 1:
        return tables[0]
    raise ValueError(
        'Multiple tables found in SQLite file; expected "data" or a '
        'single table',
    )


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONList:
    """
    Read SQLITE content from *path*.

    Parameters
    ----------
    path : Path
        Path to the SQLITE file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the SQLITE file.
    """
    conn = sqlite3.connect(str(path))
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            'SELECT name FROM sqlite_master '
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            'ORDER BY name',
        )
        tables = [row[0] for row in cursor.fetchall()]
        table = _resolve_table(tables)
        if table is None:
            return []
        query = f'SELECT * FROM {_quote_identifier(table)}'
        rows = conn.execute(query).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to SQLITE at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the SQLITE file on disk.
    data : JSONData
        Data to write as SQLITE. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the SQLITE file.
    """
    records = normalize_records(data, 'SQLITE')
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

    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(f'DROP TABLE IF EXISTS {table_ident}')
        conn.execute(f'CREATE TABLE {table_ident} ({column_defs})')
        rows = [
            tuple(_coerce_sql_value(row.get(column)) for column in columns)
            for row in records
        ]
        conn.executemany(insert_sql, rows)
        conn.commit()
    finally:
        conn.close()
    return len(records)
