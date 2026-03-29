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
from typing import Any

from ..utils._types import JSONList
from ._enums import FileFormat
from ._imports import get_dependency
from ._sql import DEFAULT_TABLE
from ._sql import DUCKDB_DIALECT
from ._sql import quote_identifier
from ._sql import write_table_rows
from .base import EmbeddedDatabaseFileHandlerABC

if TYPE_CHECKING:
    import duckdb

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'DuckdbFile',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _duckdb() -> Any:
    """Return the required duckdb module."""
    return get_dependency(
        'duckdb',
        format_name='DUCKDB',
        required=True,
    )


# SECTION: CLASSES ========================================================== #


class DuckdbFile(EmbeddedDatabaseFileHandlerABC):
    """Handler implementation for DuckDB files."""

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
        duckdb_mod = _duckdb()
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
        return write_table_rows(
            connection,
            table,
            rows,
            dialect=DUCKDB_DIALECT,
        )
