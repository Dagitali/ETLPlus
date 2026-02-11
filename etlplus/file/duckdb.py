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
from ._io import coerce_path
from ._io import ensure_parent_dir
from ._io import normalize_records
from ._io import warn_deprecated_module_io
from ._sql import DEFAULT_TABLE
from ._sql import DUCKDB_DIALECT
from ._sql import coerce_sql_value
from ._sql import collect_column_values
from ._sql import infer_column_type
from ._sql import quote_identifier
from ._sql import resolve_table
from .base import EmbeddedDatabaseFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
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

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return DUCKDB content from *path*.

        Parameters
        ----------
        path : Path
            Path to the DUCKDB file on disk.
        options : ReadOptions | None, optional
            Optional read parameters. ``table`` can override auto-detection.

        Returns
        -------
        JSONList
            The list of dictionaries read from the DUCKDB file.
        """
        conn = self.connect(path)
        try:
            tables = self.list_tables(conn)
            table = self.table_from_read_options(options)
            if table is None:
                table = resolve_table(tables, engine_name='DuckDB')
            if table is None:
                return []
            return self.read_table(conn, table)
        finally:
            conn.close()

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

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to DUCKDB at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the DUCKDB file on disk.
        data : JSONData
            Data to write as DUCKDB.
        options : WriteOptions | None, optional
            Optional write parameters. ``table`` can override the table name.

        Returns
        -------
        int
            The number of rows written to the DUCKDB file.
        """
        records = normalize_records(data, 'DUCKDB')
        if not records:
            return 0

        table = self.table_from_write_options(
            options,
            default=self.default_table,
        )
        if table is None:  # pragma: no cover - guarded by default
            raise ValueError('DUCKDB write requires a table name')
        ensure_parent_dir(path)
        conn = self.connect(path)
        try:
            return self.write_table(conn, table, records)
        finally:
            conn.close()

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
    Read and return DUCKDB content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the DUCKDB file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the DUCKDB file.
    """
    warn_deprecated_module_io(__name__, 'read')
    return _DUCKDB_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to DUCKDB at *path* and return record count.

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
    warn_deprecated_module_io(__name__, 'write')
    return _DUCKDB_HANDLER.write(coerce_path(path), data)
