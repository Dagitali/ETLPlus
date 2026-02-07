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
from etlplus.file.base import ScientificDatasetFileHandlerABC
from etlplus.file.base import WriteOptions
from etlplus.file.dta import DtaFile
from etlplus.file.nc import NcFile
from etlplus.file.rda import RdaFile
from etlplus.file.rds import RdsFile
from etlplus.file.sav import SavFile
from etlplus.file.xpt import XptFile
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


class TestScientificDatasetContracts:
    """Unit tests for scientific dataset handler contracts."""

    @pytest.mark.parametrize(
        'handler_cls',
        [
            DtaFile,
            NcFile,
            RdaFile,
            RdsFile,
            SavFile,
            XptFile,
        ],
    )
    def test_handlers_use_scientific_dataset_abc(
        self,
        handler_cls: type[FileHandlerABC],
    ) -> None:
        """Test key scientific handlers inheriting ScientificDataset ABC."""
        assert issubclass(handler_cls, ScientificDatasetFileHandlerABC)
        assert handler_cls.dataset_key == 'data'

    @pytest.mark.parametrize(
        ('handler', 'path'),
        [
            (DtaFile(), Path('ignored.dta')),
            (NcFile(), Path('ignored.nc')),
            (SavFile(), Path('ignored.sav')),
            (XptFile(), Path('ignored.xpt')),
        ],
    )
    def test_single_dataset_handlers_reject_unknown_dataset_key(
        self,
        handler: ScientificDatasetFileHandlerABC,
        path: Path,
    ) -> None:
        """Test single-dataset scientific handlers rejecting unknown keys."""
        with pytest.raises(ValueError, match='supports only dataset key'):
            handler.read_dataset(path, dataset='unknown')

        with pytest.raises(ValueError, match='supports only dataset key'):
            handler.write_dataset(path, [], dataset='unknown')
