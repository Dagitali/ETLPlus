"""
:mod:`tests.unit.file.test_u_file_base` module.

Unit tests for :mod:`etlplus.file.base`.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any

import pytest

from etlplus.file import FileFormat
from etlplus.file.base import ArchiveWrapperFileHandlerABC
from etlplus.file.base import DelimitedTextFileHandlerABC
from etlplus.file.base import EmbeddedDatabaseFileHandlerABC
from etlplus.file.base import FileHandlerABC
from etlplus.file.base import ReadOnlyFileHandlerABC
from etlplus.file.base import ReadOnlySpreadsheetFileHandlerABC
from etlplus.file.base import ReadOptions
from etlplus.file.base import ScientificDatasetFileHandlerABC
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from etlplus.file.base import SpreadsheetFileHandlerABC
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
from etlplus.types import JSONData
from etlplus.types import JSONList

# SECTION: HELPERS ========================================================== #


class _ArchiveStub(ArchiveWrapperFileHandlerABC):
    """Concrete archive handler used for option helper tests."""

    format = FileFormat.ZIP

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        _ = path
        return {'inner_name': self.inner_name_from_read_options(options)}

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        _ = path
        _ = data
        _ = self.inner_name_from_write_options(options)
        return 1

    def read_inner_bytes(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> bytes:
        _ = path
        _ = options
        return b''

    def write_inner_bytes(
        self,
        path: Path,
        payload: bytes,
        *,
        options: WriteOptions | None = None,
    ) -> None:
        _ = path
        _ = payload
        _ = options


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


class _EmbeddedDbStub(EmbeddedDatabaseFileHandlerABC):
    """Concrete embedded-db handler used for option helper tests."""

    format = FileFormat.SQLITE

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        _ = path
        return [{'table': self.table_from_read_options(options)}]

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        _ = path
        _ = data
        _ = self.table_from_write_options(options)
        return 1

    def connect(
        self,
        path: Path,
    ) -> Any:
        _ = path
        return object()

    def list_tables(
        self,
        connection: Any,
    ) -> list[str]:
        _ = connection
        return ['data']

    def read_table(
        self,
        connection: Any,
        table: str,
    ) -> JSONList:
        _ = connection
        _ = table
        return []

    def write_table(
        self,
        connection: Any,
        table: str,
        rows: JSONList,
    ) -> int:
        _ = connection
        _ = table
        return len(rows)


class _ReadOnlySpreadsheetStub(ReadOnlySpreadsheetFileHandlerABC):
    """Minimal concrete read-only spreadsheet handler for contract checks."""

    format = FileFormat.XLS
    engine_name = 'xlrd'

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        sheet = self.sheet_from_read_options(options)
        return self.read_sheet(path, sheet=sheet, options=options)

    def read_sheet(
        self,
        path: Path,
        *,
        sheet: str | int,
        options: ReadOptions | None = None,
    ) -> JSONList:
        _ = path
        _ = options
        return [{'sheet': sheet}]


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


class _SpreadsheetStub(SpreadsheetFileHandlerABC):
    """Concrete spreadsheet handler used for option helper tests."""

    format = FileFormat.XLSX
    engine_name = 'stub'

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        _ = path
        return [{'sheet': self.sheet_from_read_options(options)}]

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        _ = path
        _ = data
        _ = self.sheet_from_write_options(options)
        return 1

    def read_sheet(
        self,
        path: Path,
        *,
        sheet: str | int,
        options: ReadOptions | None = None,
    ) -> JSONList:
        _ = path
        _ = options
        return [{'sheet': sheet}]

    def write_sheet(
        self,
        path: Path,
        rows: JSONList,
        *,
        sheet: str | int,
        options: WriteOptions | None = None,
    ) -> int:
        _ = path
        _ = sheet
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
        return [{'text': 'ok'}]

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

    def test_delimited_concrete_subclass_satisfies_contract(self) -> None:
        """Test a concrete delimited subclass being fully instantiable."""
        handler = _DelimitedStub()

        assert handler.category == 'tabular_delimited_text'
        assert handler.read(Path('ignored.csv')) == [{'id': 1}]
        assert handler.write(Path('ignored.csv'), [{'id': 1}, {'id': 2}]) == 2

    def test_read_only_handler_rejects_write(self) -> None:
        """Test read-only handlers raising on write."""
        handler = _ReadOnlyStub()

        with pytest.raises(RuntimeError, match='read-only'):
            handler.write(Path('ignored.xls'), [{'a': 1}])

    def test_read_only_spreadsheet_handler_rejects_sheet_write(self) -> None:
        """Test read-only spreadsheet handlers rejecting write_sheet calls."""
        handler = _ReadOnlySpreadsheetStub()

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

        with pytest.raises(TypeError):
            _IncompleteText()  # type: ignore[abstract]

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
        'handler_cls',
        [
            CsvFile,
            DatFile,
            PsvFile,
            TabFile,
            TsvFile,
            TxtFile,
            FwfFile,
        ],
    )
    def test_delimited_text_handlers_expose_row_methods(
        self,
        handler_cls: type[FileHandlerABC],
    ) -> None:
        """Test delimited/text handlers exposing read_rows/write_rows."""
        assert callable(getattr(handler_cls, 'read', None))
        assert callable(getattr(handler_cls, 'write', None))
        assert callable(getattr(handler_cls, 'read_rows', None))
        assert callable(getattr(handler_cls, 'write_rows', None))

    @pytest.mark.parametrize(
        'handler_cls',
        [
            XlsFile,
            XlsxFile,
            XlsmFile,
            OdsFile,
        ],
    )
    def test_spreadsheet_handlers_expose_sheet_methods(
        self,
        handler_cls: type[FileHandlerABC],
    ) -> None:
        """Test spreadsheet handlers exposing read_sheet/write_sheet."""
        assert callable(getattr(handler_cls, 'read', None))
        assert callable(getattr(handler_cls, 'write', None))
        assert callable(getattr(handler_cls, 'read_sheet', None))
        assert callable(getattr(handler_cls, 'write_sheet', None))

    @pytest.mark.parametrize(
        'handler_cls',
        [
            SqliteFile,
        ],
    )
    def test_embedded_db_handlers_expose_table_methods(
        self,
        handler_cls: type[FileHandlerABC],
    ) -> None:
        """Test embedded database handlers exposing read_table/write_table."""
        assert callable(getattr(handler_cls, 'read', None))
        assert callable(getattr(handler_cls, 'write', None))
        assert callable(getattr(handler_cls, 'read_table', None))
        assert callable(getattr(handler_cls, 'write_table', None))

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
    def test_scientific_handlers_expose_dataset_methods(
        self,
        handler_cls: type[FileHandlerABC],
    ) -> None:
        """Test scientific handlers exposing read_dataset/write_dataset."""
        assert callable(getattr(handler_cls, 'read', None))
        assert callable(getattr(handler_cls, 'write', None))
        assert callable(getattr(handler_cls, 'read_dataset', None))
        assert callable(getattr(handler_cls, 'write_dataset', None))


class TestOptionsContracts:
    """Unit tests for base option data classes."""

    def test_inner_name_option_helpers_use_override_then_default(self) -> None:
        """
        Test archive option helpers using explicit then default inner name.
        """
        handler = _ArchiveStub()

        assert handler.inner_name_from_read_options(None) is None
        assert handler.inner_name_from_write_options(None) is None
        assert handler.inner_name_from_read_options(
            ReadOptions(inner_name='data.json'),
        ) == 'data.json'
        assert handler.inner_name_from_write_options(
            WriteOptions(inner_name='payload.csv'),
        ) == 'payload.csv'

    def test_read_options_use_independent_extras_dicts(self) -> None:
        """Test each ReadOptions instance getting its own extras dict."""
        first = ReadOptions()
        second = ReadOptions()

        assert not first.extras
        assert not second.extras
        assert first.extras is not second.extras

    def test_sheet_option_helpers_use_override_then_default(self) -> None:
        """
        Test spreadsheet option helpers using explicit then default sheet.
        """
        handler = _SpreadsheetStub()

        assert handler.sheet_from_read_options(None) == 0
        assert handler.sheet_from_write_options(None) == 0
        assert handler.sheet_from_read_options(
            ReadOptions(sheet='Sheet2'),
        ) == 'Sheet2'
        assert handler.sheet_from_write_options(
            WriteOptions(sheet=3),
        ) == 3

    def test_table_option_helpers_use_override_then_default(self) -> None:
        """
        Test embedded-db option helpers using explicit then default table.
        """
        handler = _EmbeddedDbStub()

        assert handler.table_from_read_options(None) is None
        assert handler.table_from_write_options(None) is None
        assert handler.table_from_read_options(
            ReadOptions(table='events'),
        ) == 'events'
        assert handler.table_from_write_options(
            WriteOptions(table='staging'),
        ) == 'staging'

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

    @pytest.mark.parametrize(
        'handler_cls',
        [
            DtaFile,
            NcFile,
            SavFile,
            XptFile,
        ],
    )
    def test_single_dataset_handlers_use_single_dataset_scientific_abc(
        self,
        handler_cls: type[FileHandlerABC],
    ) -> None:
        """Test single-dataset scientific handlers using subtype contract."""
        assert issubclass(handler_cls, SingleDatasetScientificFileHandlerABC)
