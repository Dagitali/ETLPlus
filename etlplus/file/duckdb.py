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
from typing import TYPE_CHECKING

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._imports import get_dependency
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._sql import DEFAULT_TABLE
from ._sql import DUCKDB_DIALECT
from ._sql import coerce_sql_value
from ._sql import collect_column_values
from ._sql import infer_column_type
from ._sql import quote_identifier
from .base import EmbeddedDatabaseFileHandlerABC
from .enums import FileFormat

if TYPE_CHECKING:
    import duckdb

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'DuckdbFile',
    # Functions
    'read',
    'write',
]

# SECTION: CLASSES ========================================================== #


class DuckdbFile(EmbeddedDatabaseFileHandlerABC):
    """
    Handler implementation for DuckDB files.
    """

    # -- Class Attributes -- #

    format = FileFormat.DUCKDB
    engine_name = 'DuckDB'
    default_table = DEFAULT_TABLE

    # -- Instance Methods -- #

    def connect(
        self,
        path: Path,
    ) -> duckdb.DuckDBPyConnection:
        """
        Open and return a DuckDB connection for *path*.

        Parameters
        ----------
        path : Path
            Path to the DuckDB file on disk.

        Returns
        -------
        duckdb.DuckDBPyConnection
            DuckDB connection object.
        """
        duckdb_mod = get_dependency('duckdb', format_name='DUCKDB')
        return duckdb_mod.connect(str(path))

    def list_tables(
        self,
        connection: duckdb.DuckDBPyConnection,
    ) -> list[str]:
        """
        Return table names from a DuckDB connection.

        Parameters
        ----------
        connection : duckdb.DuckDBPyConnection
            Open DuckDB connection.

        Returns
        -------
        list[str]
            List of table names.
        """
        return [row[0] for row in connection.execute('SHOW TABLES').fetchall()]

    def read_table(
        self,
        connection: duckdb.DuckDBPyConnection,
        table: str,
    ) -> JSONList:
        """
        Read rows from *table* in DuckDB connection.

        Parameters
        ----------
        connection : duckdb.DuckDBPyConnection
            Open DuckDB connection.
        table : str
            Table name.

        Returns
        -------
        JSONList
            Table rows as records.
        """
        query = f'SELECT * FROM {quote_identifier(table)}'
        cursor = connection.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description or []]
        if not columns:
            info = connection.execute(
                f'PRAGMA table_info({quote_identifier(table)})',
            ).fetchall()
            columns = [row[1] for row in info]
        return [dict(zip(columns, row, strict=True)) for row in rows]

    def write_table(
        self,
        connection: duckdb.DuckDBPyConnection,
        table: str,
        rows: JSONList,
    ) -> int:
        """
        Write *rows* to *table* in DuckDB connection.

        Parameters
        ----------
        connection : duckdb.DuckDBPyConnection
            Open DuckDB connection.
        table : str
            Table name.
        rows : JSONList
            Rows to write.

        Returns
        -------
        int
            Number of rows written.
        """
        if not rows:
            return 0

        columns, column_values = collect_column_values(rows)
        if not columns:
            return 0

        column_defs = ', '.join(
            f'{quote_identifier(column)} '
            f'{infer_column_type(values, DUCKDB_DIALECT)}'
            for column, values in column_values.items()
        )
        table_ident = quote_identifier(table)
        insert_columns = ', '.join(
            quote_identifier(column) for column in columns
        )
        placeholders = ', '.join('?' for _ in columns)
        insert_sql = (
            f'INSERT INTO {table_ident} ({insert_columns}) '
            f'VALUES ({placeholders})'
        )

        connection.execute(f'DROP TABLE IF EXISTS {table_ident}')
        connection.execute(f'CREATE TABLE {table_ident} ({column_defs})')
        values = [
            tuple(coerce_sql_value(row.get(column)) for column in columns)
            for row in rows
        ]
        connection.executemany(insert_sql, values)
        return len(rows)


# SECTION: INTERNAL CONSTANTS =============================================== #

_DUCKDB_HANDLER = DuckdbFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``DuckdbFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the DUCKDB file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the DUCKDB file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _DUCKDB_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``DuckdbFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the DUCKDB file on disk.
    data : JSONData
        Data to write as DUCKDB. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the DUCKDB file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _DUCKDB_HANDLER.write,
    )
