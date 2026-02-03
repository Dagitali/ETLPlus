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

from pathlib import Path

from ..types import JSONData
from ..types import JSONList
from ._imports import get_dependency
from ._io import ensure_parent_dir
from ._io import normalize_records
from ._sql import DEFAULT_TABLE
from ._sql import DUCKDB_DIALECT
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
    duckdb = get_dependency('duckdb', format_name='DUCKDB')
    conn = duckdb.connect(str(path))
    try:
        tables = [row[0] for row in conn.execute('SHOW TABLES').fetchall()]
        table = resolve_table(tables, engine_name='DuckDB')
        if table is None:
            return []
        query = f'SELECT * FROM {quote_identifier(table)}'
        cursor = conn.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description or []]
        if not columns:
            info = conn.execute(
                f'PRAGMA table_info({quote_identifier(table)})',
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

    columns, column_values = collect_column_values(records)
    if not columns:
        return 0

    column_defs = ', '.join(
        f'{quote_identifier(column)} '
        f'{infer_column_type(values, DUCKDB_DIALECT)}'
        for column, values in column_values.items()
    )
    table_ident = quote_identifier(DEFAULT_TABLE)
    insert_columns = ', '.join(quote_identifier(column) for column in columns)
    placeholders = ', '.join('?' for _ in columns)
    insert_sql = (
        f'INSERT INTO {table_ident} ({insert_columns}) VALUES ({placeholders})'
    )

    duckdb = get_dependency('duckdb', format_name='DUCKDB')
    ensure_parent_dir(path)
    conn = duckdb.connect(str(path))
    try:
        conn.execute(f'DROP TABLE IF EXISTS {table_ident}')
        conn.execute(f'CREATE TABLE {table_ident} ({column_defs})')
        rows = [
            tuple(coerce_sql_value(row.get(column)) for column in columns)
            for row in records
        ]
        conn.executemany(insert_sql, rows)
    finally:
        conn.close()
    return len(records)
