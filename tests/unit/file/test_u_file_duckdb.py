"""
:mod:`tests.unit.file.test_u_file_duckdb` module.

Unit tests for :mod:`etlplus.file.duckdb`.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from typing import cast

import pytest

from etlplus.file import duckdb as mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions
from tests.unit.file.pytest_file_contract_contracts import (
    EmbeddedDatabaseModuleContract,
)
from tests.unit.file.pytest_file_contract_mixins import OptionalModuleInstaller

if TYPE_CHECKING:
    import duckdb

# SECTION: HELPERS ========================================================== #


class _Connection:
    """Stub DuckDB connection."""

    def __init__(
        self,
        *,
        tables: list[str] | None = None,
        rows: list[tuple[object, ...]] | None = None,
        description: list[tuple[str, ...]] | None = None,
        pragma_info: list[tuple[object, ...]] | None = None,
    ) -> None:
        self.tables = tables or []
        self.rows = rows or []
        self.description = description
        self.pragma_info = pragma_info or []
        self.closed = False
        self.executed: list[str] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def close(self) -> None:  # noqa: D401 - simple stub
        """Record close calls for lifecycle assertions."""
        self.closed = True

    def execute(
        self,
        sql: str,
    ) -> _Cursor:
        """Simulate executing a SQL statement."""
        self.executed.append(sql)
        if sql == 'SHOW TABLES':
            return _Cursor([(name,) for name in self.tables])
        if sql.startswith('PRAGMA table_info'):
            return _Cursor(self.pragma_info)
        return _Cursor(self.rows, description=self.description)

    def executemany(
        self,
        sql: str,
        rows: list[tuple[object, ...]],
    ) -> None:
        """Simulate executing a SQL statement with multiple rows."""
        self.executemany_calls.append((sql, rows))


class _Cursor:
    """Stub cursor for DuckDB queries."""

    def __init__(
        self,
        rows: list[tuple[object, ...]],
        *,
        description: list[tuple[str, ...]] | None = None,
    ) -> None:
        self._rows = rows
        self.description = description

    def fetchall(self) -> list[tuple[object, ...]]:
        """Simulate fetching all rows from a query."""
        return list(self._rows)


class _DuckdbStub:
    """Stub module exposing ``connect``."""

    # pylint: disable=unused-argument

    def __init__(self, connection: _Connection) -> None:
        self._connection = connection

    def connect(self, path: str) -> _Connection:
        """Simulate connecting to a DuckDB database."""
        return self._connection


# SECTION: TESTS ============================================================ #


class TestDuckdb(EmbeddedDatabaseModuleContract):
    """Unit tests for :mod:`etlplus.file.duckdb`."""

    module = mod
    format_name = 'duckdb'
    multi_table_error_pattern = 'Multiple tables found in DuckDB'

    def _prepare_connection(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
        connection: _Connection | None = None,
    ) -> tuple[_Connection, Path]:
        """Install a DuckDB connection stub and return connection/path."""
        conn = _Connection() if connection is None else connection
        self._install_connection(optional_module_stub, conn)
        return conn, self.format_path(tmp_path)

    def build_empty_database_path(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> Path:
        """Build a DuckDB fixture with no tables."""
        _conn, path = self._prepare_connection(
            tmp_path,
            optional_module_stub,
        )
        return path

    def build_multi_table_database_path(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> Path:
        """Build a DuckDB fixture with multiple tables."""
        _conn, path = self._prepare_connection(
            tmp_path,
            optional_module_stub,
            _Connection(tables=['a', 'b']),
        )
        return path

    def test_read_closes_connection_after_query(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test reads always closing the DuckDB connection."""
        conn, path = self._prepare_connection(
            tmp_path,
            optional_module_stub,
            _Connection(
                tables=['data'],
                rows=[(1,)],
                description=[('id',)],
            ),
        )

        _ = mod.DuckdbFile().read(path)

        assert conn.closed is True

    @pytest.mark.parametrize(
        ('description', 'pragma_info'),
        [
            (None, [(0, 'id'), (1, 'name')]),
            ([('id',), ('name',)], []),
        ],
        ids=['pragma_columns_fallback', 'description_columns'],
    )
    def test_read_maps_columns_from_description_or_pragma(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
        description: list[tuple[str, ...]] | None,
        pragma_info: list[tuple[object, ...]],
    ) -> None:
        """Test read column mapping via description or pragma fallback."""
        _conn, path = self._prepare_connection(
            tmp_path,
            optional_module_stub,
            _Connection(
                tables=['data'],
                rows=[(1, 'Ada')],
                description=description,
                pragma_info=pragma_info,
            ),
        )

        result = mod.DuckdbFile().read(path)

        assert result == [{'id': 1, 'name': 'Ada'}]

    @pytest.mark.parametrize(
        ('tables', 'table_option'),
        [
            (['a', 'b'], 'b'),
            (['my table'], 'my table'),
        ],
        ids=['multiple_tables_explicit_selection', 'quoted_table_name'],
    )
    def test_read_uses_explicit_table_option(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
        tables: list[str],
        table_option: str,
    ) -> None:
        """
        Test :meth:`read` honoring explicit table options, including quoting.
        """
        conn, path = self._prepare_connection(
            tmp_path,
            optional_module_stub,
            _Connection(
                tables=tables,
                rows=[(1,)],
                description=[('id',)],
            ),
        )
        handler = mod.DuckdbFile()

        result = handler.read(
            path,
            options=ReadOptions(table=table_option),
        )

        assert result == [{'id': 1}]
        assert f'SELECT * FROM "{table_option}"' in conn.executed

    @pytest.mark.parametrize(
        ('payload', 'options', 'expected_table'),
        [
            ([{'id': 1}, {'id': 2}], None, 'data'),
            ([{'id': 1}], WriteOptions(table='events'), 'events'),
        ],
        ids=['default_table', 'explicit_table_option'],
    )
    def test_write_creates_table_inserts_records_and_closes_connection(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
        payload: list[dict[str, object]],
        options: WriteOptions | None,
        expected_table: str,
    ) -> None:
        """
        Test that :meth:`write` creates a table, inserts rows, and closes.
        """
        conn, path = self._prepare_connection(tmp_path, optional_module_stub)
        handler = mod.DuckdbFile()

        written = handler.write(path, payload, options=options)

        assert written == len(payload)
        assert any(
            f'CREATE TABLE "{expected_table}"' in stmt
            for stmt in conn.executed
        )
        assert conn.executemany_calls
        assert conn.closed is True

    def test_write_table_returns_zero_for_rows_with_no_columns(self) -> None:
        """
        Test :meth:`write_table` short-circuiting rows that provide no columns.
        """
        conn = _Connection()
        handler = mod.DuckdbFile()

        written = handler.write_table(
            cast('duckdb.DuckDBPyConnection', conn),
            'data',
            [{}],
        )

        assert written == 0
        assert not conn.executemany_calls

    @staticmethod
    def _install_connection(
        optional_module_stub: OptionalModuleInstaller,
        connection: _Connection,
    ) -> None:
        """Install one DuckDB connection stub for a test case."""
        optional_module_stub({'duckdb': _DuckdbStub(connection)})
