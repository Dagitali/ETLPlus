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

import sqlite3
from pathlib import Path

from ..types import JSONData
from ..types import JSONList
from ._io import ensure_parent_dir
from ._io import normalize_records
from ._sql import DEFAULT_TABLE
from ._sql import SQLITE_DIALECT
from ._sql import coerce_sql_value
from ._sql import collect_column_values
from ._sql import infer_column_type
from ._sql import quote_identifier
from ._sql import resolve_table

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


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
        table = resolve_table(tables, engine_name='SQLite')
        if table is None:
            return []
        query = f'SELECT * FROM {quote_identifier(table)}'
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

    columns, column_values = collect_column_values(records)
    if not columns:
        return 0

    column_defs = ', '.join(
        f'{quote_identifier(column)} '
        f'{infer_column_type(values, SQLITE_DIALECT)}'
        for column, values in column_values.items()
    )
    table_ident = quote_identifier(DEFAULT_TABLE)
    insert_columns = ', '.join(quote_identifier(column) for column in columns)
    placeholders = ', '.join('?' for _ in columns)
    insert_sql = (
        f'INSERT INTO {table_ident} ({insert_columns}) VALUES ({placeholders})'
    )

    ensure_parent_dir(path)
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(f'DROP TABLE IF EXISTS {table_ident}')
        conn.execute(f'CREATE TABLE {table_ident} ({column_defs})')
        rows = [
            tuple(coerce_sql_value(row.get(column)) for column in columns)
            for row in records
        ]
        conn.executemany(insert_sql, rows)
        conn.commit()
    finally:
        conn.close()
    return len(records)
