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
from tests.unit.file.conftest import BaseOptionResolutionContract
from tests.unit.file.conftest import HandlerMethodNamingContract
from tests.unit.file.conftest import ScientificDatasetInheritanceContract

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


class TestNamingConventions(HandlerMethodNamingContract):
    """Unit tests for category-level internal naming conventions."""

    delimited_handlers = [
        CsvFile,
        DatFile,
        PsvFile,
        TabFile,
        TsvFile,
        TxtFile,
        FwfFile,
    ]
    spreadsheet_handlers = [
        XlsFile,
        XlsxFile,
        XlsmFile,
        OdsFile,
    ]
    embedded_db_handlers = [SqliteFile]
    scientific_handlers = [
        DtaFile,
        NcFile,
        RdaFile,
        RdsFile,
        SavFile,
        XptFile,
    ]


class TestOptionsContracts(BaseOptionResolutionContract):
    """Unit tests for base option data classes."""

    def make_scientific_handler(self) -> DtaFile:
        """Build a scientific handler for option contract checks."""
        return DtaFile()

    def make_delimited_handler(self) -> _DelimitedStub:
        """Build a delimited handler for option contract checks."""
        return _DelimitedStub()

    def make_read_only_handler(self) -> _ReadOnlyStub:
        """Build a read-only handler for option contract checks."""
        return _ReadOnlyStub()

    def make_archive_handler(self) -> _ArchiveStub:
        """Build an archive handler for option contract checks."""
        return _ArchiveStub()

    def make_spreadsheet_handler(self) -> _SpreadsheetStub:
        """Build a spreadsheet handler for option contract checks."""
        return _SpreadsheetStub()

    def make_embedded_handler(self) -> _EmbeddedDbStub:
        """Build an embedded-db handler for option contract checks."""
        return _EmbeddedDbStub()

    def test_write_options_are_frozen(self) -> None:
        """Test WriteOptions immutability contract."""
        options = WriteOptions()

        with pytest.raises(FrozenInstanceError):
            options.encoding = 'latin-1'  # type: ignore[misc]


class TestScientificDatasetContracts(ScientificDatasetInheritanceContract):
    """Unit tests for scientific dataset handler contracts."""

    scientific_handlers = [
        DtaFile,
        NcFile,
        RdaFile,
        RdsFile,
        SavFile,
        XptFile,
    ]
    single_dataset_handlers = [
        DtaFile,
        NcFile,
        SavFile,
        XptFile,
    ]
