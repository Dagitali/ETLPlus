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
from ..types import StrPath
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._sql import DEFAULT_TABLE
from ._sql import SQLITE_DIALECT
from ._sql import coerce_sql_value
from ._sql import collect_column_values
from ._sql import infer_column_type
from ._sql import quote_identifier
from .base import EmbeddedDatabaseFileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'SqliteFile',
    # Functions
    'read',
    'write',
]

# SECTION: CLASSES ========================================================== #


class SqliteFile(EmbeddedDatabaseFileHandlerABC):
    """
    Handler implementation for SQLite files.
    """

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
        if not rows:
            return 0

        columns, column_values = collect_column_values(rows)
        if not columns:
            return 0

        column_defs = ', '.join(
            f'{quote_identifier(column)} '
            f'{infer_column_type(values, SQLITE_DIALECT)}'
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
        connection.commit()
        return len(rows)


# SECTION: INTERNAL CONSTANTS =============================================== #

_SQLITE_HANDLER = SqliteFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``SqliteFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the SQLITE file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the SQLITE file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _SQLITE_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``SqliteFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the SQLITE file on disk.
    data : JSONData
        Data to write as SQLITE. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the SQLITE file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _SQLITE_HANDLER.write,
    )
