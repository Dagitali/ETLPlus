"""
:mod:`tests.unit.file.test_u_file_base` module.

Unit tests for :mod:`etlplus.file.base`.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from etlplus.file import FileFormat
from etlplus.file.base import DelimitedTextFileHandlerABC
from etlplus.file.base import FileHandlerABC
from etlplus.file.base import ReadOnlyFileHandlerABC
from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions
from etlplus.types import JSONData
from etlplus.types import JSONList

# SECTION: HELPERS ========================================================== #


class _DelimitedStub(DelimitedTextFileHandlerABC):
    """Concrete delimited handler used for abstract contract tests."""

    format = FileFormat.CSV
    delimiter = ','

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        return self.read_rows(path, options=options)

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        rows: JSONList = data if isinstance(data, list) else [data]
        return self.write_rows(path, rows, options=options)

    def read_rows(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        _ = path
        _ = options
        return [{'id': 1}]

    def write_rows(
        self,
        path: Path,
        rows: JSONList,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        _ = path
        _ = options
        return len(rows)


class _ReadOnlyStub(ReadOnlyFileHandlerABC):
    """Minimal concrete read-only handler for contract checks."""

    format = FileFormat.XLS

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        _ = path
        _ = options
        return []


# SECTION: TESTS ============================================================ #


class TestOptionsContracts:
    """Unit tests for base option data classes."""

    def test_read_options_use_independent_extras_dicts(self) -> None:
        """Test each ReadOptions instance getting its own extras dict."""
        first = ReadOptions()
        second = ReadOptions()

        assert not first.extras
        assert not second.extras
        assert first.extras is not second.extras

    def test_write_options_are_frozen(self) -> None:
        """Test WriteOptions immutability contract."""
        options = WriteOptions()

        with pytest.raises(FrozenInstanceError):
            options.encoding = 'latin-1'  # type: ignore[misc]


class TestBaseAbcContracts:
    """Unit tests for abstract base class contracts."""

    def test_file_handler_abc_cannot_be_instantiated(self) -> None:
        """Test FileHandlerABC remaining abstract."""
        with pytest.raises(TypeError):
            FileHandlerABC()  # type: ignore[abstract]

    def test_delimited_abc_requires_row_methods(self) -> None:
        """Test DelimitedTextFileHandlerABC requiring row-level methods."""

        class _IncompleteDelimited(DelimitedTextFileHandlerABC):
            format = FileFormat.CSV
            delimiter = ','

            def read(
                self,
                path: Path,
                *,
                options: ReadOptions | None = None,
            ) -> JSONData:
                _ = path
                _ = options
                return []

            def write(
                self,
                path: Path,
                data: JSONData,
                *,
                options: WriteOptions | None = None,
            ) -> int:
                _ = path
                _ = data
                _ = options
                return 0

        with pytest.raises(TypeError):
            _IncompleteDelimited()  # type: ignore[abstract]

    def test_read_only_handler_rejects_write(self) -> None:
        """Test read-only handlers raising on write."""
        handler = _ReadOnlyStub()

        with pytest.raises(RuntimeError, match='read-only'):
            handler.write(Path('ignored.xls'), [{'a': 1}])

    def test_delimited_concrete_subclass_satisfies_contract(self) -> None:
        """Test a concrete delimited subclass being fully instantiable."""
        handler = _DelimitedStub()

        assert handler.category == 'tabular_delimited_text'
        assert handler.read(Path('ignored.csv')) == [{'id': 1}]
        assert handler.write(Path('ignored.csv'), [{'id': 1}, {'id': 2}]) == 2
