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

from ..utils._types import JSONList
from ._enums import FileFormat
from ._sql import DEFAULT_TABLE
from ._sql import SQLITE_DIALECT
from ._sql import quote_identifier
from ._sql import write_table_rows
from .base import EmbeddedDatabaseFileHandlerABC

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'SqliteFile',
]

# SECTION: CLASSES ========================================================== #


class SqliteFile(EmbeddedDatabaseFileHandlerABC):
    """Handler implementation for SQLite files."""

    # -- Class Attributes -- #

    format = FileFormat.SQLITE
    engine_name = 'SQLite'
    default_table = DEFAULT_TABLE

    # -- Instance Methods -- #

    def connect(
        self,
        path: Path,
    ) -> sqlite3.Connection:
        """
        Open and return a SQLite connection for *path*.

        Parameters
        ----------
        path : Path
            Path to the SQLite file on disk.

        Returns
        -------
        sqlite3.Connection
            SQLite connection.
        """
        conn = sqlite3.connect(str(path))
        conn.row_factory = sqlite3.Row
        return conn

    def list_tables(
        self,
        connection: sqlite3.Connection,
    ) -> list[str]:
        """
        Return non-system SQLite tables.

        Parameters
        ----------
        connection : sqlite3.Connection
            Open SQLite connection.

        Returns
        -------
        list[str]
            List of table names.
        """
        cursor = connection.execute(
            'SELECT name FROM sqlite_master '
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            'ORDER BY name',
        )
        return [row[0] for row in cursor.fetchall()]

    def read_table(
        self,
        connection: sqlite3.Connection,
        table: str,
    ) -> JSONList:
        """
        Read rows from *table* in SQLite connection.

        Parameters
        ----------
        connection : sqlite3.Connection
            Open SQLite connection.
        table : str
            Table name.

        Returns
        -------
        JSONList
            Table rows as records.
        """
        query = f'SELECT * FROM {quote_identifier(table)}'
        rows = connection.execute(query).fetchall()
        return [dict(row) for row in rows]

    def write_table(
        self,
        connection: sqlite3.Connection,
        table: str,
        rows: JSONList,
    ) -> int:
        """
        Write *rows* to *table* in SQLite connection.

        Parameters
        ----------
        connection : sqlite3.Connection
            Open SQLite connection.
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
            dialect=SQLITE_DIALECT,
            commit=True,
        )
