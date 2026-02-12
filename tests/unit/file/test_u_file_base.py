"""
:mod:`tests.unit.file.test_u_file_base` module.

Unit tests for :mod:`etlplus.file.base`.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import NoReturn
from typing import cast

import pytest

from etlplus.file import FileFormat
from etlplus.file._stub_categories import (
    StubSingleDatasetScientificFileHandlerABC,
)
from etlplus.file.base import DelimitedTextFileHandlerABC
from etlplus.file.base import FileHandlerABC
from etlplus.file.base import ReadOnlyFileHandlerABC
from etlplus.file.base import ReadOptions
from etlplus.file.base import ScientificDatasetFileHandlerABC
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from etlplus.file.base import TextFixedWidthFileHandlerABC
from etlplus.file.base import WriteOptions
from etlplus.file.csv import CsvFile
from etlplus.file.dat import DatFile
from etlplus.file.dta import DtaFile
from etlplus.file.fwf import FwfFile
from etlplus.file.mat import MatFile
from etlplus.file.nc import NcFile
from etlplus.file.ods import OdsFile
from etlplus.file.psv import PsvFile
from etlplus.file.rda import RdaFile
from etlplus.file.rds import RdsFile
from etlplus.file.sav import SavFile
from etlplus.file.sqlite import SqliteFile
from etlplus.file.sylk import SylkFile
from etlplus.file.tab import TabFile
from etlplus.file.tsv import TsvFile
from etlplus.file.txt import TxtFile
from etlplus.file.xls import XlsFile
from etlplus.file.xlsm import XlsmFile
from etlplus.file.xlsx import XlsxFile
from etlplus.file.xpt import XptFile
from etlplus.file.zip import ZipFile
from etlplus.file.zsav import ZsavFile
from etlplus.types import JSONData
from etlplus.types import JSONList
from tests.unit.file.conftest import (
    assert_single_dataset_rejects_non_default_key,
)

# SECTION: HELPERS ========================================================== #


_NO_DEFAULT: object = object()


def _raise_read_only_write(format_name: FileFormat) -> NoReturn:
    """Raise the canonical read-only write error for one format."""
    raise RuntimeError(
        f'{format_name.value.upper()} is read-only and does not support '
        'write operations',
    )


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
_SCIENTIFIC_STUB_HANDLER_CLASSES: tuple[
    type[StubSingleDatasetScientificFileHandlerABC],
    ...,
] = (
    MatFile,
    SylkFile,
    ZsavFile,
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
_NAMING_METHOD_CASES: tuple[
    tuple[tuple[type[FileHandlerABC], ...], str, str],
    ...,
] = (
    (_DELIMITED_HANDLER_CLASSES, 'read_rows', 'write_rows'),
    (_EMBEDDED_DB_HANDLER_CLASSES, 'read_table', 'write_table'),
    (_SCIENTIFIC_HANDLER_CLASSES, 'read_dataset', 'write_dataset'),
    (_SPREADSHEET_HANDLER_CLASSES, 'read_sheet', 'write_sheet'),
)


class _RowReadWriteMixin:
    """Provide shared row-oriented read/write glue for contract stubs."""

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """Provide a read implementation delegating to row-level methods."""
        handler = cast(
            DelimitedTextFileHandlerABC | TextFixedWidthFileHandlerABC,
            self,
        )
        return handler.read_rows(path, options=options)

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """Provide a write implementation delegating to row-level methods."""
        handler = cast(
            DelimitedTextFileHandlerABC | TextFixedWidthFileHandlerABC,
            self,
        )
        rows: JSONList = data if isinstance(data, list) else [data]
        return handler.write_rows(path, rows, options=options)


class _DelimitedStub(_RowReadWriteMixin, DelimitedTextFileHandlerABC):
    """Concrete delimited handler used for abstract contract tests."""

    # pylint: disable=unused-argument

    format = FileFormat.CSV
    delimiter = ','

    def read_rows(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:  # noqa: ARG002
        return [{'id': 1}]

    def write_rows(
        self,
        path: Path,
        rows: JSONList,
        *,
        options: WriteOptions | None = None,
    ) -> int:  # noqa: ARG002
        return len(rows)


class _IncompleteDelimited(_RowReadWriteMixin, DelimitedTextFileHandlerABC):
    """Incomplete delimited handler used for abstract-method checks."""

    format = FileFormat.CSV
    delimiter = ','


class _IncompleteTextFixedWidth(
    _RowReadWriteMixin,
    TextFixedWidthFileHandlerABC,
):
    """Incomplete text/fixed-width handler used for abstract checks."""

    format = FileFormat.TXT


class _TextFixedWidthStub(_RowReadWriteMixin, TextFixedWidthFileHandlerABC):
    """Concrete text/fixed-width handler used for abstract contract tests."""

    # pylint: disable=unused-argument

    format = FileFormat.TXT

    def read_rows(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:  # noqa: ARG002
        return [{'text': 'ok'}]

    def write_rows(
        self,
        path: Path,
        rows: JSONList,
        *,
        options: WriteOptions | None = None,
    ) -> int:  # noqa: ARG002
        return len(rows)


class _ReadOnlyScientificStub(
    ReadOnlyFileHandlerABC,
    ScientificDatasetFileHandlerABC,
):
    """Concrete read-only scientific handler for base-contract tests."""

    # pylint: disable=unused-argument

    format = FileFormat.HDF5
    dataset_key = 'data'

    def list_datasets(
        self,
        path: Path,
    ) -> list[str]:  # noqa: ARG002
        return [self.dataset_key]

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONData:  # noqa: ARG002
        return []

    def write_dataset(
        self,
        path: Path,
        data: JSONData,
        *,
        dataset: str | None = None,
        options: WriteOptions | None = None,
    ) -> int:  # noqa: ARG002
        _raise_read_only_write(self.format)


class _ReadOnlySingleScientificStub(
    ReadOnlyFileHandlerABC,
    SingleDatasetScientificFileHandlerABC,
):
    """Concrete read-only single-dataset handler for base-contract tests."""

    # pylint: disable=unused-argument

    format = FileFormat.SAS7BDAT
    dataset_key = 'data'

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONData:  # noqa: ARG002
        self.resolve_single_read_dataset(dataset, options=options)
        return []

    def write_dataset(
        self,
        path: Path,
        data: JSONData,
        *,
        dataset: str | None = None,
        options: WriteOptions | None = None,
    ) -> int:  # noqa: ARG002
        self.resolve_single_write_dataset(dataset, options=options)
        _raise_read_only_write(self.format)


# SECTION: TESTS ============================================================ #


class TestBaseAbcContracts:
    """Unit tests for abstract base class contracts."""

    @pytest.mark.parametrize(
        (
            'handler_cls',
            'path_name',
            'expected_category',
            'expected_read',
            'write_payload',
            'expected_written',
        ),
        [
            (
                _DelimitedStub,
                'ignored.csv',
                'tabular_delimited_text',
                [{'id': 1}],
                [{'id': 1}, {'id': 2}],
                2,
            ),
            (
                _TextFixedWidthStub,
                'ignored.txt',
                'text_fixed_width',
                [{'text': 'ok'}],
                [{'text': 'ok'}],
                1,
            ),
        ],
        ids=['delimited', 'text_fixed_width'],
    )
    def test_concrete_row_subclass_satisfies_contract(
        self,
        handler_cls: type[FileHandlerABC],
        path_name: str,
        expected_category: str,
        expected_read: JSONData,
        write_payload: JSONData,
        expected_written: int,
    ) -> None:
        """Test concrete row-oriented subclasses being fully instantiable."""
        handler = handler_cls()
        path = Path(path_name)

        assert handler.category == expected_category
        assert handler.read(path) == expected_read
        assert handler.write(path, write_payload) == expected_written

    def test_file_handler_abc_declares_read_write_as_abstract(self) -> None:
        """Test FileHandlerABC preserving read/write abstract methods."""
        assert inspect.isabstract(FileHandlerABC)
        assert {'read', 'write'} <= FileHandlerABC.__abstractmethods__

    def test_read_only_scientific_write_dataset_rejects_writes(
        self,
    ) -> None:
        """Test read-only scientific handlers rejecting dataset writes."""
        handler = _ReadOnlyScientificStub()

        with pytest.raises(RuntimeError, match='read-only'):
            handler.write_dataset(
                Path('ignored.hdf5'),
                [{'id': 1}],
                dataset='other',
            )

    @pytest.mark.parametrize(
        ('dataset', 'expected_error', 'error_pattern'),
        [
            ('data', RuntimeError, 'read-only'),
            ('other', ValueError, 'supports only dataset key'),
        ],
        ids=['default_dataset_write', 'invalid_dataset_key'],
    )
    def test_read_only_single_scientific_write_dataset_contract(
        self,
        dataset: str,
        expected_error: type[Exception],
        error_pattern: str,
    ) -> None:
        """Test single-dataset read-only write validation and guardrails."""
        handler = _ReadOnlySingleScientificStub()

        with pytest.raises(expected_error, match=error_pattern):
            handler.write_dataset(
                Path('ignored.sas7bdat'),
                [{'id': 1}],
                dataset=dataset,
            )

    @pytest.mark.parametrize(
        'operation',
        ['write', 'write_sheet'],
        ids=['module_write', 'sheet_write'],
    )
    def test_read_only_spreadsheet_handler_rejects_writes(
        self,
        operation: str,
    ) -> None:
        """Test read-only spreadsheet handlers rejecting write operations."""
        handler = XlsFile()
        path = Path('ignored.xls')

        with pytest.raises(RuntimeError, match='read-only'):
            if operation == 'write':
                handler.write(path, [{'a': 1}])
            else:
                handler.write_sheet(
                    path,
                    [{'a': 1}],
                    sheet=0,
                )

    @pytest.mark.parametrize(
        'incomplete_handler_cls',
        (_IncompleteDelimited, _IncompleteTextFixedWidth),
        ids=['delimited', 'text_fixed_width'],
    )
    def test_row_abcs_require_row_methods(
        self,
        incomplete_handler_cls: type[FileHandlerABC],
    ) -> None:
        """Test row-oriented ABCs requiring row-level methods."""
        assert inspect.isabstract(incomplete_handler_cls)
        assert {'read_rows', 'write_rows'} <= (
            incomplete_handler_cls.__abstractmethods__
        )


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

    @pytest.mark.parametrize(
        'handler_cls',
        _SCIENTIFIC_HANDLER_CLASSES,
    )
    def test_handlers_use_scientific_dataset_abc(
        self,
        handler_cls: type[ScientificDatasetFileHandlerABC],
    ) -> None:
        """Test scientific handlers inheriting ScientificDataset ABC."""
        assert issubclass(handler_cls, ScientificDatasetFileHandlerABC)
        assert handler_cls.dataset_key == 'data'

    @pytest.mark.parametrize(
        'handler_cls',
        _SINGLE_DATASET_HANDLER_CLASSES,
    )
    def test_single_dataset_handlers_reject_unknown_dataset_key(
        self,
        handler_cls: type[SingleDatasetScientificFileHandlerABC],
    ) -> None:
        """Test single-dataset scientific handlers rejecting unknown keys."""
        assert issubclass(
            handler_cls,
            SingleDatasetScientificFileHandlerABC,
        )
        assert_single_dataset_rejects_non_default_key(
            handler_cls(),
            suffix=handler_cls.format.value,
        )


class TestScientificStubConventions:
    """Unit tests for scientific placeholder handler conventions."""

    @pytest.mark.parametrize(
        'handler_cls',
        _SCIENTIFIC_STUB_HANDLER_CLASSES,
        ids=['mat', 'sylk', 'zsav'],
    )
    def test_scientific_stubs_inherit_stub_single_dataset_abc(
        self,
        handler_cls: type[StubSingleDatasetScientificFileHandlerABC],
    ) -> None:
        """Test scientific stubs inheriting the unified stub base."""
        assert issubclass(
            handler_cls,
            StubSingleDatasetScientificFileHandlerABC,
        )
