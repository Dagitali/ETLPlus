"""
:mod:`tests.unit.file.test_u_file_sqlite` module.

Unit tests for :mod:`etlplus.file.sqlite`.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path

from etlplus.file import sqlite as mod
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
