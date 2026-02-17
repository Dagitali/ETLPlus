"""
:mod:`tests.unit.file.test_u_file_sqlite` module.

Unit tests for :mod:`etlplus.file.sqlite`.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from etlplus.file import sqlite as mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions

from .pytest_file_contract_mixins import RoundtripUnitModuleContract
from .pytest_file_contracts import EmbeddedDatabaseModuleContract
from .pytest_file_roundtrip_cases import build_roundtrip_spec
from .pytest_file_types import OptionalModuleInstaller

# SECTION: TESTS ============================================================ #


class TestSqlite(
    EmbeddedDatabaseModuleContract,
    RoundtripUnitModuleContract,
):
    """Unit tests for :mod:`etlplus.file.sqlite`."""

    # pylint: disable=unused-variable

    module = mod
    format_name = 'sqlite'
    multi_table_error_pattern = 'Multiple tables found in SQLite'
    roundtrip_spec = build_roundtrip_spec(
        [{'id': 1, 'name': 'Ada'}, {'id': 2, 'name': 'Bob'}],
        [{'id': 1, 'name': 'Ada'}, {'id': 2, 'name': 'Bob'}],
    )

    def build_empty_database_path(
        self,
        tmp_path: Path,
        _optional_module_stub: OptionalModuleInstaller,
    ) -> Path:
        """Build an empty SQLite database file."""
        path = self.format_path(tmp_path)
        with sqlite3.connect(path):
            pass
        return path

    def build_multi_table_database_path(
        self,
        tmp_path: Path,
        _optional_module_stub: OptionalModuleInstaller,
    ) -> Path:
        """Build a SQLite database with more than one table."""
        path = self.format_path(tmp_path)
        self._create_multi_table_db(path)
        return path

    def test_read_uses_explicit_table_option_with_multiple_tables(
        self,
        tmp_path: Path,
    ) -> None:
        """Test explicit table selection bypassing multi-table ambiguity."""
        path = self.format_path(tmp_path)
        self._create_multi_table_db(
            path,
            rows={
                'alpha': [(1,)],
                'beta': [(2,)],
            },
        )

        result = mod.SqliteFile().read(
            path,
            options=ReadOptions(table='beta'),
        )

        assert result == [{'id': 2}]

    def test_write_table_returns_zero_for_rows_without_columns(self) -> None:
        """Test write_table short-circuiting records with no columns."""
        handler = mod.SqliteFile()
        with sqlite3.connect(':memory:') as conn:
            written = handler.write_table(conn, 'data', [{}])

            assert written == 0
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'",
            ).fetchall()
            assert tables == []

    def test_write_uses_explicit_table_option(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writes honoring explicit table names via options."""
        path = self.format_path(tmp_path)
        payload = [{'id': 1}]

        written = mod.SqliteFile().write(
            path,
            payload,
            options=WriteOptions(table='events'),
        )

        assert written == 1
        with sqlite3.connect(path) as conn:
            tables = [
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'",
                ).fetchall()
            ]
            assert tables == ['events']
            rows = conn.execute('SELECT id FROM events').fetchall()
            assert rows == [(1,)]

    @staticmethod
    def _create_multi_table_db(
        path: Path,
        *,
        rows: dict[str, list[tuple[object, ...]]] | None = None,
    ) -> None:
        """Create a deterministic two-table SQLite fixture database."""
        with sqlite3.connect(path) as conn:
            conn.execute('CREATE TABLE alpha (id INTEGER)')
            conn.execute('CREATE TABLE beta (id INTEGER)')
            for table, values in (rows or {}).items():
                conn.executemany(
                    f'INSERT INTO {table} (id) VALUES (?)',
                    values,
                )
            conn.commit()
