"""
:mod:`tests.unit.file.test_u_file_arrow` module.

Unit tests for :mod:`etlplus.file.arrow`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import Literal

import pytest

from etlplus.file import arrow as mod

from .pytest_file_contracts import PyarrowMissingDependencyMixin

# SECTION: HELPERS ========================================================== #


class _ContextStub:
    """Simple context manager stub returning one wrapped value."""

    def __init__(self, value: object) -> None:
        self._value = value

    def __enter__(self) -> object:
        return self._value

    def __exit__(
        self,
        exc_type: object,
        exc: object,
        tb: object,
    ) -> Literal[False]:
        _ = exc_type
        _ = exc
        _ = tb
        return False


class _PyarrowStub:
    """Minimal pyarrow-like module for Arrow handler tests."""

    # pylint: disable=invalid-name

    def __init__(self) -> None:
        self.from_pylist_calls: list[list[dict[str, object]]] = []
        self.memory_map_calls: list[tuple[str, str]] = []
        self.osfile_calls: list[tuple[str, str]] = []
        self.new_file_calls: list[tuple[object, object]] = []
        self.last_writer: _WriterStub | None = None
        self.read_table: object = _TableStub([{'id': 1}])
        self.Table = type(
            'Table',
            (),
            {'from_pylist': staticmethod(self._from_pylist)},
        )
        self.ipc = type(
            'Ipc',
            (),
            {
                'open_file': staticmethod(self._open_file),
                'new_file': staticmethod(self._new_file),
            },
        )

    def _from_pylist(
        self,
        rows: list[dict[str, object]],
    ) -> _TableStub:
        self.from_pylist_calls.append(rows)
        return _TableStub(rows)

    def _new_file(
        self,
        sink: object,
        schema: object,
    ) -> _WriterStub:
        self.new_file_calls.append((sink, schema))
        writer = _WriterStub()
        self.last_writer = writer
        return writer

    def _open_file(
        self,
        source: object,
    ) -> _ReaderStub:
        _ = source
        return _ReaderStub(self.read_table)

    def memory_map(
        self,
        path: str,
        mode: str,
    ) -> _ContextStub:
        """Return context for mapped source."""
        self.memory_map_calls.append((path, mode))
        return _ContextStub(object())

    def OSFile(
        self,
        path: str,
        mode: str,
    ) -> _ContextStub:
        """Return context for output sink."""
        self.osfile_calls.append((path, mode))
        return _ContextStub(object())


class _ReaderStub:
    """IPC reader stub returning one prepared table."""

    def __init__(self, table: object) -> None:
        self._table = table

    def read_all(self) -> object:
        """Return prepared table."""
        return self._table


class _TableStub:
    """Arrow-like table stub with schema and pylist conversion."""

    def __init__(
        self,
        rows: list[dict[str, object]],
        *,
        schema: object | None = None,
    ) -> None:
        self._rows = rows
        self.schema = schema if schema is not None else object()

    def to_pylist(self) -> list[dict[str, object]]:
        """Return row payload for ``table_to_records``."""
        return list(self._rows)


class _WriterStub:
    """IPC writer stub tracking written tables."""

    def __init__(self) -> None:
        self.tables: list[object] = []

    def write_table(self, table: object) -> None:
        """Record a table write call."""
        self.tables.append(table)

    def __enter__(self) -> _WriterStub:
        return self

    def __exit__(
        self,
        exc_type: object,
        exc: object,
        tb: object,
    ) -> Literal[False]:
        _ = exc_type
        _ = exc
        _ = tb
        return False


# SECTION: TESTS ============================================================ #


class TestArrow(PyarrowMissingDependencyMixin):
    """Unit tests for :mod:`etlplus.file.arrow`."""

    module = mod
    format_name = 'arrow'

    def test_read_table_uses_memory_map_and_ipc_reader(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test ``read_table`` routing through pyarrow map/IPC APIs."""
        pyarrow_stub = _PyarrowStub()
        expected_table: Any = object()
        pyarrow_stub.read_table = expected_table

        def _dependency(*_a: object, **_k: object) -> _PyarrowStub:
            return pyarrow_stub

        monkeypatch.setattr(mod, 'get_dependency', _dependency)
        path = self.format_path(tmp_path)

        result = mod.ArrowFile().read_table(path)

        assert result is expected_table
        assert pyarrow_stub.memory_map_calls == [(str(path), 'r')]

    def test_records_to_table_uses_pyarrow_from_pylist(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test ``records_to_table`` converting records via pyarrow."""
        pyarrow_stub = _PyarrowStub()

        def _dependency(*_a: object, **_k: object) -> _PyarrowStub:
            return pyarrow_stub

        monkeypatch.setattr(mod, 'get_dependency', _dependency)

        table = mod.ArrowFile().records_to_table({'id': 1})

        assert isinstance(table, _TableStub)
        assert pyarrow_stub.from_pylist_calls == [[{'id': 1}]]

    def test_table_to_records_returns_pylist_rows(self) -> None:
        """Test ``table_to_records`` delegating to ``table.to_pylist()``."""
        table = _TableStub([{'id': 1}, {'id': 2}])
        assert mod.ArrowFile().table_to_records(table) == [
            {'id': 1},
            {'id': 2},
        ]

    def test_write_returns_zero_for_empty_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """Test empty writes short-circuiting without file creation."""
        path = self.format_path(tmp_path)
        assert self.module_handler.write(path, []) == 0
        assert not path.exists()

    def test_write_table_uses_osfile_and_ipc_writer(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test ``write_table`` routing through pyarrow sink/writer APIs."""
        pyarrow_stub = _PyarrowStub()

        def _dependency(*_a: object, **_k: object) -> _PyarrowStub:
            return pyarrow_stub

        monkeypatch.setattr(mod, 'get_dependency', _dependency)
        path = self.format_path(tmp_path)
        table = _TableStub([{'id': 1}], schema='schema')

        mod.ArrowFile().write_table(path, table)

        assert pyarrow_stub.osfile_calls == [(str(path), 'wb')]
        assert pyarrow_stub.new_file_calls
        assert pyarrow_stub.last_writer is not None
        assert pyarrow_stub.last_writer.tables == [table]
