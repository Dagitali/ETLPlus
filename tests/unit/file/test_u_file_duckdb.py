"""
:mod:`tests.unit.file.test_u_file_duckdb` module.

Unit tests for :mod:`etlplus.file.duckdb`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from etlplus.file import duckdb as mod
from tests.unit.file.conftest import EmbeddedDatabaseModuleContract

# SECTION: HELPERS ========================================================== #


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
        self.executed: list[str] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []

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

    def close(self) -> None:  # noqa: D401 - simple stub
        """No-op close."""


class _DuckdbStub:
    """Stub module exposing ``connect``."""

    # pylint: disable=unused-argument

    def __init__(self, connection: _Connection) -> None:
        self._connection = connection

    def connect(self, path: str) -> _Connection:  # noqa: ARG002
        return self._connection


# SECTION: TESTS ============================================================ #


class TestDuckdb(EmbeddedDatabaseModuleContract):
    """Unit tests for :mod:`etlplus.file.duckdb`."""

    module = mod
    format_name = 'duckdb'
    multi_table_error_pattern = 'Multiple tables found in DuckDB'

    def build_empty_database_path(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> Path:
        """Build a DuckDB fixture with no tables."""
        conn = _Connection()
        optional_module_stub({'duckdb': _DuckdbStub(conn)})
        return tmp_path / 'data.duckdb'

    def build_multi_table_database_path(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> Path:
        """Build a DuckDB fixture with multiple tables."""
        conn = _Connection(tables=['a', 'b'])
        optional_module_stub({'duckdb': _DuckdbStub(conn)})
        return tmp_path / 'data.duckdb'

    def test_read_uses_description_columns(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that :func:`read` uses description columns when available."""
        conn = _Connection(
            tables=['data'],
            rows=[(1, 'Ada')],
            description=[('id',), ('name',)],
        )
        optional_module_stub({'duckdb': _DuckdbStub(conn)})

        result = mod.read(tmp_path / 'data.duckdb')

        assert result == [{'id': 1, 'name': 'Ada'}]

    def test_read_falls_back_to_pragma_columns(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that :func:`read` falls back to pragma columns."""
        conn = _Connection(
            tables=['data'],
            rows=[(1, 'Ada')],
            description=None,
            pragma_info=[(0, 'id'), (1, 'name')],
        )
        optional_module_stub({'duckdb': _DuckdbStub(conn)})

        result = mod.read(tmp_path / 'data.duckdb')

        assert result == [{'id': 1, 'name': 'Ada'}]

    def test_write_inserts_records(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test that :func:`write` creates a table and inserts records."""
        conn = _Connection()
        optional_module_stub({'duckdb': _DuckdbStub(conn)})
        path = tmp_path / 'data.duckdb'

        written = mod.write(path, [{'id': 1}, {'id': 2}])

        assert written == 2
        assert any(stmt.startswith('CREATE TABLE') for stmt in conn.executed)
        assert conn.executemany_calls
