"""
:mod:`tests.unit.file.test_u_file_base` module.

Unit tests for :mod:`etlplus.file.base`.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import cast

import pytest

from etlplus.file import FileFormat
from etlplus.file.base import DelimitedTextFileHandlerABC
from etlplus.file.base import FileHandlerABC
from etlplus.file.base import ReadOptions
from etlplus.file.base import ScientificDatasetFileHandlerABC
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from etlplus.file.base import TextFixedWidthFileHandlerABC
from etlplus.file.base import WriteOptions
from etlplus.file.csv import CsvFile
from etlplus.file.dat import DatFile
from etlplus.file.dta import DtaFile
from etlplus.file.fwf import FwfFile
from etlplus.file.nc import NcFile
from etlplus.file.ods import OdsFile
from etlplus.file.psv import PsvFile
from etlplus.file.rda import RdaFile
from etlplus.file.rds import RdsFile
from etlplus.file.sav import SavFile
from etlplus.file.sqlite import SqliteFile
from etlplus.file.tab import TabFile
from etlplus.file.tsv import TsvFile
from etlplus.file.txt import TxtFile
from etlplus.file.xls import XlsFile
from etlplus.file.xlsm import XlsmFile
from etlplus.file.xlsx import XlsxFile
from etlplus.file.xpt import XptFile
from etlplus.file.zip import ZipFile
from etlplus.types import JSONData
from etlplus.types import JSONList
from tests.unit.file.conftest import (
    assert_single_dataset_rejects_non_default_key,
)

# SECTION: HELPERS ========================================================== #


_NO_DEFAULT: object = object()

_DELIMITED_HANDLER_CLASSES: tuple[type[FileHandlerABC], ...] = (
    CsvFile,
    DatFile,
    PsvFile,
    TabFile,
    TsvFile,
    TxtFile,
    FwfFile,
)
_EMBEDDED_DB_HANDLER_CLASSES: tuple[type[FileHandlerABC], ...] = (SqliteFile,)
_SCIENTIFIC_HANDLER_CLASSES: tuple[
    type[ScientificDatasetFileHandlerABC],
    ...,
] = (
    DtaFile,
    NcFile,
    RdaFile,
    RdsFile,
    SavFile,
    XptFile,
)
_SINGLE_DATASET_HANDLER_CLASSES: tuple[
    type[SingleDatasetScientificFileHandlerABC],
    ...,
] = (
    DtaFile,
    NcFile,
    SavFile,
    XptFile,
)
_SPREADSHEET_HANDLER_CLASSES: tuple[type[FileHandlerABC], ...] = (
    XlsFile,
    XlsxFile,
    XlsmFile,
    OdsFile,
)
_NAMING_METHOD_CASES: list[
    tuple[tuple[type[FileHandlerABC], ...], str, str]
] = [
    (_DELIMITED_HANDLER_CLASSES, 'read_rows', 'write_rows'),
    (_EMBEDDED_DB_HANDLER_CLASSES, 'read_table', 'write_table'),
    (_SCIENTIFIC_HANDLER_CLASSES, 'read_dataset', 'write_dataset'),
    (_SPREADSHEET_HANDLER_CLASSES, 'read_sheet', 'write_sheet'),
]


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


class _TextFixedWidthStub(TextFixedWidthFileHandlerABC):
    """Concrete text/fixed-width handler used for abstract contract tests."""

    format = FileFormat.TXT

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        return self.read_rows(path, options=options)

    def read_rows(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        _ = path
        _ = options
        return [{'text': 'ok'}]

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        rows: JSONList = data if isinstance(data, list) else [data]
        return self.write_rows(path, rows, options=options)

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


# SECTION: TESTS ============================================================ #


class TestBaseAbcContracts:
    """Unit tests for abstract base class contracts."""

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

        assert inspect.isabstract(_IncompleteDelimited)
        assert {'read_rows', 'write_rows'} <= (
            _IncompleteDelimited.__abstractmethods__
        )

    def test_delimited_concrete_subclass_satisfies_contract(self) -> None:
        """Test a concrete delimited subclass being fully instantiable."""
        handler = _DelimitedStub()

        assert handler.category == 'tabular_delimited_text'
        assert handler.read(Path('ignored.csv')) == [{'id': 1}]
        assert handler.write(Path('ignored.csv'), [{'id': 1}, {'id': 2}]) == 2

    def test_file_handler_abc_declares_read_write_as_abstract(self) -> None:
        """Test FileHandlerABC preserving read/write abstract methods."""
        assert inspect.isabstract(FileHandlerABC)
        assert {'read', 'write'} <= FileHandlerABC.__abstractmethods__

    def test_read_only_handler_rejects_write(self) -> None:
        """Test read-only handlers raising on write."""
        handler = XlsFile()

        with pytest.raises(RuntimeError, match='read-only'):
            handler.write(Path('ignored.xls'), [{'a': 1}])

    def test_read_only_spreadsheet_handler_rejects_sheet_write(self) -> None:
        """Test read-only spreadsheet handlers rejecting write_sheet calls."""
        handler = XlsFile()

        with pytest.raises(RuntimeError, match='read-only'):
            handler.write_sheet(
                Path('ignored.xls'),
                [{'a': 1}],
                sheet=0,
            )

    def test_text_fixed_width_abc_requires_row_methods(self) -> None:
        """Test TextFixedWidthFileHandlerABC requiring row-level methods."""

        class _IncompleteText(TextFixedWidthFileHandlerABC):
            format = FileFormat.TXT

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

        assert inspect.isabstract(_IncompleteText)
        assert {'read_rows', 'write_rows'} <= (
            _IncompleteText.__abstractmethods__
        )

    def test_text_fixed_width_concrete_subclass_satisfies_contract(
        self,
    ) -> None:
        """Test a concrete text/fixed-width subclass being instantiable."""
        handler = _TextFixedWidthStub()

        assert handler.category == 'text_fixed_width'
        assert handler.read(Path('ignored.txt')) == [{'text': 'ok'}]
        assert handler.write(Path('ignored.txt'), [{'text': 'ok'}]) == 1


class TestNamingConventions:
    """Unit tests for category-level internal naming conventions."""

    @pytest.mark.parametrize(
        ('handlers', 'read_method', 'write_method'),
        _NAMING_METHOD_CASES,
        ids=['delimited', 'embedded_db', 'scientific', 'spreadsheet'],
    )
    def test_handlers_expose_category_methods(
        self,
        handlers: tuple[type[FileHandlerABC], ...],
        read_method: str,
        write_method: str,
    ) -> None:
        """Test handlers exposing category-level read/write methods."""
        for handler_cls in handlers:
            assert callable(getattr(handler_cls, 'read', None))
            assert callable(getattr(handler_cls, 'write', None))
            assert callable(getattr(handler_cls, read_method, None))
            assert callable(getattr(handler_cls, write_method, None))


class TestOptionsContracts:
    """Unit tests for base option data classes."""

    def test_dataset_option_helpers_use_override_then_default(self) -> None:
        """Test scientific dataset helpers using explicit then default data."""
        handler = DtaFile()
        cases = (
            (
                handler.dataset_from_read_options,
                handler.resolve_read_dataset,
                ReadOptions(dataset='features'),
            ),
            (
                handler.dataset_from_write_options,
                handler.resolve_write_dataset,
                WriteOptions(dataset='labels'),
            ),
        )
        for from_options, resolve, options in cases:
            expected = cast(str, options.dataset)
            from_options_method = cast(Callable[..., object], from_options)
            resolve_method = cast(Callable[..., object], resolve)
            assert from_options_method(None) is None
            assert from_options_method(options) == expected
            assert resolve_method(None, options=options) == expected
            assert resolve_method('explicit', options=options) == 'explicit'
            assert resolve_method(None, default='fallback') == 'fallback'

    @pytest.mark.parametrize(
        (
            'handler_cls',
            'read_method_name',
            'write_method_name',
            'read_options',
            'write_options',
            'baseline',
            'read_expected',
            'write_expected',
            'read_default',
            'write_default',
        ),
        [
            (
                _DelimitedStub,
                'delimiter_from_read_options',
                'delimiter_from_write_options',
                ReadOptions(extras={'delimiter': '|'}),
                WriteOptions(extras={'delimiter': '\t'}),
                ',',
                '|',
                '\t',
                ';',
                ':',
            ),
            (
                XlsFile,
                'encoding_from_read_options',
                'encoding_from_write_options',
                ReadOptions(encoding='latin-1'),
                WriteOptions(encoding='utf-16'),
                'utf-8',
                'latin-1',
                'utf-16',
                'utf-16',
                'ascii',
            ),
            (
                ZipFile,
                'inner_name_from_read_options',
                'inner_name_from_write_options',
                ReadOptions(inner_name='data.json'),
                WriteOptions(inner_name='payload.csv'),
                None,
                'data.json',
                'payload.csv',
                _NO_DEFAULT,
                _NO_DEFAULT,
            ),
            (
                XlsxFile,
                'sheet_from_read_options',
                'sheet_from_write_options',
                ReadOptions(sheet='Sheet2'),
                WriteOptions(sheet=3),
                0,
                'Sheet2',
                3,
                'fallback_sheet',
                99,
            ),
            (
                SqliteFile,
                'table_from_read_options',
                'table_from_write_options',
                ReadOptions(table='events'),
                WriteOptions(table='staging'),
                None,
                'events',
                'staging',
                'fallback_table',
                'fallback_table',
            ),
        ],
        ids=['delimiter', 'encoding', 'inner_name', 'sheet', 'table'],
    )
    def test_option_helper_pairs_use_override_then_default(
        self,
        handler_cls: type[object],
        read_method_name: str,
        write_method_name: str,
        read_options: ReadOptions,
        write_options: WriteOptions,
        baseline: object,
        read_expected: object,
        write_expected: object,
        read_default: object,
        write_default: object,
    ) -> None:
        """Test paired read/write option helpers with override/default."""
        handler = handler_cls()
        read_call = cast(
            Callable[..., object],
            getattr(handler, read_method_name),
        )
        write_call = cast(
            Callable[..., object],
            getattr(handler, write_method_name),
        )

        assert read_call(None) == baseline
        assert write_call(None) == baseline
        assert read_call(read_options) == read_expected
        assert write_call(write_options) == write_expected
        if read_default is not _NO_DEFAULT:
            assert read_call(None, default=read_default) == read_default
        if write_default is not _NO_DEFAULT:
            assert write_call(None, default=write_default) == write_default

    def test_read_options_use_independent_extras_dicts(self) -> None:
        """Test each ReadOptions instance getting its own extras dict."""
        first = ReadOptions()
        second = ReadOptions()

        assert not first.extras
        assert not second.extras
        assert first.extras is not second.extras

    def test_root_tag_option_helper_use_override_then_default(self) -> None:
        """Test root-tag helper using explicit values then defaults."""
        handler = XlsFile()
        assert handler.root_tag_from_write_options(None) == 'root'
        assert (
            handler.root_tag_from_write_options(WriteOptions(root_tag='items'))
            == 'items'
        )
        assert (
            handler.root_tag_from_write_options(None, default='dataset')
            == 'dataset'
        )

    def test_write_options_are_frozen(self) -> None:
        """Test WriteOptions immutability contract."""
        options = WriteOptions()

        with pytest.raises(FrozenInstanceError):
            options.encoding = 'latin-1'  # type: ignore[misc]


class TestScientificDatasetContracts:
    """Unit tests for scientific dataset handler contracts."""

    def test_handlers_use_scientific_dataset_abc(self) -> None:
        """Test scientific handlers inheriting ScientificDataset ABC."""
        for handler_cls in _SCIENTIFIC_HANDLER_CLASSES:
            assert issubclass(handler_cls, ScientificDatasetFileHandlerABC)
            assert handler_cls.dataset_key == 'data'

    def test_single_dataset_handlers_use_single_dataset_scientific_abc(
        self,
    ) -> None:
        """Test single-dataset handlers inheriting the subtype ABC."""
        for handler_cls in _SINGLE_DATASET_HANDLER_CLASSES:
            assert issubclass(
                handler_cls,
                SingleDatasetScientificFileHandlerABC,
            )

    def test_single_dataset_handlers_reject_unknown_dataset_key(self) -> None:
        """Test single-dataset scientific handlers rejecting unknown keys."""
        for handler_cls in _SINGLE_DATASET_HANDLER_CLASSES:
            assert_single_dataset_rejects_non_default_key(
                handler_cls(),
                suffix=handler_cls.format.value,
            )
