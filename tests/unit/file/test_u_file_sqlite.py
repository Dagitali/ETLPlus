"""
:mod:`tests.unit.file.test_u_file_sqlite` module.

Unit tests for :mod:`etlplus.file.sqlite`.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path

from etlplus.file import sqlite as mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions
from tests.unit.file.conftest import EmbeddedDatabaseModuleContract

# SECTION: TESTS ============================================================ #


class TestSqlite(EmbeddedDatabaseModuleContract):
    """Unit tests for :mod:`etlplus.file.sqlite`."""

    # pylint: disable=unused-variable

    module = mod
    format_name = 'sqlite'
    multi_table_error_pattern = 'Multiple tables found in SQLite'

    def build_empty_database_path(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> Path:
        """Build an empty SQLite database file."""
        path = tmp_path / 'empty.sqlite'
        sqlite3.connect(path).close()
        return path

    def build_multi_table_database_path(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> Path:
        """Build a SQLite database with more than one table."""
        path = tmp_path / 'multi.sqlite'
        conn = sqlite3.connect(path)
        try:
            conn.execute('CREATE TABLE alpha (id INTEGER)')
            conn.execute('CREATE TABLE beta (id INTEGER)')
            conn.commit()
        finally:
            conn.close()
        return path

    def test_read_uses_explicit_table_option_with_multiple_tables(
        self,
        tmp_path: Path,
    ) -> None:
        """Test explicit table selection bypassing multi-table ambiguity."""
        path = tmp_path / 'multi.sqlite'
        conn = sqlite3.connect(path)
        try:
            conn.execute('CREATE TABLE alpha (id INTEGER)')
            conn.execute('CREATE TABLE beta (id INTEGER)')
            conn.execute('INSERT INTO alpha (id) VALUES (1)')
            conn.execute('INSERT INTO beta (id) VALUES (2)')
            conn.commit()
        finally:
            conn.close()

        result = mod.SqliteFile().read(
            path,
            options=ReadOptions(table='beta'),
        )

        assert result == [{'id': 2}]

    def test_write_round_trip(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that writing and then reading returns the original data."""
        path = tmp_path / 'data.sqlite'
        payload = [{'id': 1, 'name': 'Ada'}, {'id': 2, 'name': 'Bob'}]

        written = mod.write(path, payload)

        assert written == 2
        assert mod.read(path) == payload

    def test_write_table_returns_zero_for_rows_without_columns(self) -> None:
        """Test write_table short-circuiting records with no columns."""
        conn = sqlite3.connect(':memory:')
        handler = mod.SqliteFile()
        try:
            written = handler.write_table(conn, 'data', [{}])

            assert written == 0
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'",
            ).fetchall()
            assert tables == []
        finally:
            conn.close()

    def test_write_uses_explicit_table_option(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writes honoring explicit table names via options."""
        path = tmp_path / 'data.sqlite'
        payload = [{'id': 1}]

        written = mod.SqliteFile().write(
            path,
            payload,
            options=WriteOptions(table='events'),
        )

        assert written == 1
        conn = sqlite3.connect(path)
        try:
            tables = [
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'",
                ).fetchall()
            ]
            assert tables == ['events']
            rows = conn.execute('SELECT id FROM events').fetchall()
            assert rows == [(1,)]
        finally:
            conn.close()
