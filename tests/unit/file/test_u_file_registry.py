"""
:mod:`tests.unit.file.test_u_file_registry` module.

Unit tests for :mod:`etlplus.file.registry`.
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from types import SimpleNamespace
from typing import Any

import pytest

import etlplus.file as file_package
from etlplus.file import FileFormat
from etlplus.file import registry as mod
from etlplus.file.arrow import ArrowFile
from etlplus.file.avro import AvroFile
from etlplus.file.base import ArchiveWrapperFileHandlerABC
from etlplus.file.base import BinarySerializationFileHandlerABC
from etlplus.file.base import ColumnarFileHandlerABC
from etlplus.file.base import DelimitedTextFileHandlerABC
from etlplus.file.base import EmbeddedDatabaseFileHandlerABC
from etlplus.file.base import FileHandlerABC
from etlplus.file.base import ReadOnlyFileHandlerABC
from etlplus.file.base import ReadOnlySpreadsheetFileHandlerABC
from etlplus.file.base import ScientificDatasetFileHandlerABC
from etlplus.file.base import SemiStructuredTextFileHandlerABC
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from etlplus.file.base import SpreadsheetFileHandlerABC
from etlplus.file.base import TextFixedWidthFileHandlerABC
from etlplus.file.base import WriteOptions
from etlplus.file.dta import DtaFile
from etlplus.file.duckdb import DuckdbFile
from etlplus.file.feather import FeatherFile
from etlplus.file.fwf import FwfFile
from etlplus.file.json import JsonFile
from etlplus.file.mat import MatFile
from etlplus.file.nc import NcFile
from etlplus.file.ods import OdsFile
from etlplus.file.orc import OrcFile
from etlplus.file.parquet import ParquetFile
from etlplus.file.rda import RdaFile
from etlplus.file.rds import RdsFile
from etlplus.file.sas7bdat import Sas7bdatFile
from etlplus.file.sav import SavFile
from etlplus.file.sqlite import SqliteFile
from etlplus.file.stub import StubFileHandlerABC
from etlplus.file.sylk import SylkFile
from etlplus.file.txt import TxtFile
from etlplus.file.xlsm import XlsmFile
from etlplus.file.xlsx import XlsxFile
from etlplus.file.xpt import XptFile
from etlplus.file.zsav import ZsavFile

# SECTION: CONTRACTS ======================================================== #


class RegistryAbcConformanceContract:
    """Reusable contract suite for registry handler-ABC conformance checks."""

    registry_module: Any
    abc_cases: list[tuple[FileFormat, type[Any]]]

    def test_mapped_handler_class_inherits_expected_abc(self) -> None:
        """Test mapped handlers inheriting each expected category ABC."""
        for file_format, expected_abc in self.abc_cases:
            handler_class = self.registry_module.get_handler_class(file_format)
            assert issubclass(handler_class, expected_abc)


class RegistryFallbackPolicyContract:
    """Reusable contract suite for registry fallback/deprecation policies."""

    registry_module: Any
    fallback_format: FileFormat = FileFormat.GZ

    # pylint: disable=protected-access

    def test_deprecated_fallback_builds_module_adapter_and_delegates_calls(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test deprecated fallback building an adapter and delegating I/O."""
        registry = self.registry_module
        calls: dict[str, object] = {}

        def _read(path: Path) -> dict[str, object]:
            calls['read_path'] = path
            return {'ok': True}

        def _write(
            path: Path,
            data: object,
            *,
            root_tag: str = 'root',
        ) -> int:
            calls['write_path'] = path
            calls['write_data'] = data
            calls['root_tag'] = root_tag
            return 7

        monkeypatch.delitem(
            registry._HANDLER_CLASS_SPECS,
            self.fallback_format,
            raising=False,
        )
        fake_module = SimpleNamespace(read=_read, write=_write)
        monkeypatch.setattr(
            registry,
            '_module_for_format',
            lambda _fmt: fake_module,
        )

        with pytest.warns(DeprecationWarning, match='deprecated'):
            handler_class = registry.get_handler_class(
                self.fallback_format,
                allow_module_adapter_fallback=True,
            )

        assert issubclass(handler_class, FileHandlerABC)
        assert handler_class.category == 'module_adapter'

        handler = handler_class()
        path = Path('payload.gz')
        assert handler.read(path) == {'ok': True}
        written = handler.write(
            path,
            {'row': 1},
            options=WriteOptions(root_tag='records'),
        )

        assert written == 7
        assert calls['read_path'] == path
        assert calls['write_path'] == path
        assert calls['write_data'] == {'row': 1}
        assert calls['root_tag'] == 'records'

    def test_get_handler_class_raises_without_explicit_mapping(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test strict mode rejecting unmapped formats by default."""
        registry = self.registry_module
        monkeypatch.delitem(
            registry._HANDLER_CLASS_SPECS,
            self.fallback_format,
            raising=False,
        )

        with pytest.raises(ValueError, match='Unsupported format'):
            registry.get_handler_class(self.fallback_format)

    def test_module_adapter_builder_raises_for_missing_module(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test module-adapter builder raising when module import fails."""
        registry = self.registry_module

        def _raise_module_not_found(_file_format: FileFormat) -> object:
            raise ModuleNotFoundError('missing test module')

        monkeypatch.delitem(
            registry._HANDLER_CLASS_SPECS,
            self.fallback_format,
            raising=False,
        )
        monkeypatch.setattr(
            registry,
            '_module_for_format',
            _raise_module_not_found,
        )

        with pytest.raises(ModuleNotFoundError, match='missing test module'):
            registry._module_adapter_class_for_format(self.fallback_format)


class RegistryMappedResolutionContract:
    """Reusable contract suite for explicit registry mapping resolution."""

    # pylint: disable=protected-access

    registry_module: Any
    file_package: ModuleType
    mapped_class_cases: list[tuple[FileFormat, type[Any]]]
    placeholder_spec_cases: list[tuple[FileFormat, str]]
    singleton_format: FileFormat = FileFormat.JSON
    singleton_class: type[Any]

    def test_explicit_for_implemented_formats(self) -> None:
        """Test implemented handler class formats being explicitly mapped."""
        registry = self.registry_module
        implemented_formats: set[FileFormat] = set()
        for module_info in pkgutil.iter_modules(self.file_package.__path__):
            if module_info.ispkg or module_info.name.startswith('_'):
                continue
            module_name = f'{self.file_package.__name__}.{module_info.name}'
            module = importlib.import_module(module_name)
            for _, symbol in inspect.getmembers(module, inspect.isclass):
                if symbol.__module__ != module_name:
                    continue
                if not issubclass(symbol, FileHandlerABC):
                    continue
                if not symbol.__name__.endswith('File'):
                    continue
                format_value = getattr(symbol, 'format', None)
                if isinstance(format_value, FileFormat):
                    implemented_formats.add(format_value)

        mapped_formats = set(registry._HANDLER_CLASS_SPECS.keys())
        missing = implemented_formats - mapped_formats
        assert not missing

        for file_format, spec in registry._HANDLER_CLASS_SPECS.items():
            mapped_class = registry._coerce_handler_class(
                registry._import_symbol(spec),
                file_format=file_format,
            )
            assert mapped_class.format == file_format

    def test_get_handler_class_uses_mapped_class(self) -> None:
        """Test mapped formats resolving to concrete handler classes."""
        registry = self.registry_module
        for file_format, expected_class in self.mapped_class_cases:
            handler_class = registry.get_handler_class(file_format)
            assert handler_class is expected_class

    def test_get_handler_returns_singleton_instance(self) -> None:
        """Test get_handler returning a cached singleton for mapped formats."""
        registry = self.registry_module
        first = registry.get_handler(self.singleton_format)
        second = registry.get_handler(self.singleton_format)

        assert first is second
        assert isinstance(first, self.singleton_class)

    def test_unstubbed_placeholder_modules_use_module_owned_classes(
        self,
    ) -> None:
        """Test placeholder modules mapping to their own class symbols."""
        registry = self.registry_module
        for file_format, expected_spec in self.placeholder_spec_cases:
            assert registry._HANDLER_CLASS_SPECS[file_format] == expected_spec

# SECTION: FIXTURES ========================================================= #


@pytest.fixture(autouse=True)
def clear_registry_caches() -> Iterator[None]:
    """
    Clear registry caches before and after each test.
    """
    # pylint: disable=protected-access

    mod.get_handler.cache_clear()
    mod.get_handler_class.cache_clear()
    mod._module_adapter_class_for_format.cache_clear()
    mod._module_for_format.cache_clear()
    yield
    mod.get_handler.cache_clear()
    mod.get_handler_class.cache_clear()
    mod._module_adapter_class_for_format.cache_clear()
    mod._module_for_format.cache_clear()


# SECTION: TESTS ============================================================ #


class TestRegistryAbcConformance(RegistryAbcConformanceContract):
    """Unit tests for category-to-ABC inheritance conformance."""

    registry_module = mod
    abc_cases = [
        # Delimited text
        (FileFormat.CSV, DelimitedTextFileHandlerABC),
        (FileFormat.DAT, DelimitedTextFileHandlerABC),
        (FileFormat.PSV, DelimitedTextFileHandlerABC),
        (FileFormat.TAB, DelimitedTextFileHandlerABC),
        (FileFormat.TSV, DelimitedTextFileHandlerABC),
        # Semi-structured
        (FileFormat.INI, SemiStructuredTextFileHandlerABC),
        (FileFormat.JSON, SemiStructuredTextFileHandlerABC),
        (FileFormat.NDJSON, SemiStructuredTextFileHandlerABC),
        (FileFormat.PROPERTIES, SemiStructuredTextFileHandlerABC),
        (FileFormat.TOML, SemiStructuredTextFileHandlerABC),
        (FileFormat.XML, SemiStructuredTextFileHandlerABC),
        (FileFormat.YAML, SemiStructuredTextFileHandlerABC),
        # Columnar
        (FileFormat.ARROW, ColumnarFileHandlerABC),
        (FileFormat.FEATHER, ColumnarFileHandlerABC),
        (FileFormat.ORC, ColumnarFileHandlerABC),
        (FileFormat.PARQUET, ColumnarFileHandlerABC),
        # Binary serialization/interchange
        (FileFormat.AVRO, BinarySerializationFileHandlerABC),
        (FileFormat.BSON, BinarySerializationFileHandlerABC),
        (FileFormat.CBOR, BinarySerializationFileHandlerABC),
        (FileFormat.MSGPACK, BinarySerializationFileHandlerABC),
        (FileFormat.PB, BinarySerializationFileHandlerABC),
        (FileFormat.PROTO, BinarySerializationFileHandlerABC),
        # Embedded databases
        (FileFormat.DUCKDB, EmbeddedDatabaseFileHandlerABC),
        (FileFormat.SQLITE, EmbeddedDatabaseFileHandlerABC),
        # Spreadsheets
        (FileFormat.ODS, SpreadsheetFileHandlerABC),
        (FileFormat.XLS, SpreadsheetFileHandlerABC),
        (FileFormat.XLSM, SpreadsheetFileHandlerABC),
        (FileFormat.XLSX, SpreadsheetFileHandlerABC),
        # Plain text/fixed-width text
        (FileFormat.FWF, TextFixedWidthFileHandlerABC),
        (FileFormat.TXT, TextFixedWidthFileHandlerABC),
        # Archives
        (FileFormat.GZ, ArchiveWrapperFileHandlerABC),
        (FileFormat.ZIP, ArchiveWrapperFileHandlerABC),
        # Scientific/statistical
        (FileFormat.DTA, ScientificDatasetFileHandlerABC),
        (FileFormat.HDF5, ScientificDatasetFileHandlerABC),
        (FileFormat.MAT, ScientificDatasetFileHandlerABC),
        (FileFormat.NC, ScientificDatasetFileHandlerABC),
        (FileFormat.RDA, ScientificDatasetFileHandlerABC),
        (FileFormat.RDS, ScientificDatasetFileHandlerABC),
        (FileFormat.SAS7BDAT, ScientificDatasetFileHandlerABC),
        (FileFormat.SAV, ScientificDatasetFileHandlerABC),
        (FileFormat.SYLK, ScientificDatasetFileHandlerABC),
        (FileFormat.XPT, ScientificDatasetFileHandlerABC),
        (FileFormat.ZSAV, ScientificDatasetFileHandlerABC),
        # Single-dataset scientific subtype
        (FileFormat.DTA, SingleDatasetScientificFileHandlerABC),
        (FileFormat.MAT, SingleDatasetScientificFileHandlerABC),
        (FileFormat.NC, SingleDatasetScientificFileHandlerABC),
        (FileFormat.SAS7BDAT, SingleDatasetScientificFileHandlerABC),
        (FileFormat.SAV, SingleDatasetScientificFileHandlerABC),
        (FileFormat.SYLK, SingleDatasetScientificFileHandlerABC),
        (FileFormat.XPT, SingleDatasetScientificFileHandlerABC),
        (FileFormat.ZSAV, SingleDatasetScientificFileHandlerABC),
        # Placeholder/stubbed module-owned formats
        (FileFormat.STUB, StubFileHandlerABC),
        (FileFormat.ACCDB, StubFileHandlerABC),
        (FileFormat.CFG, StubFileHandlerABC),
        (FileFormat.CONF, StubFileHandlerABC),
        (FileFormat.HBS, StubFileHandlerABC),
        (FileFormat.ION, StubFileHandlerABC),
        (FileFormat.JINJA2, StubFileHandlerABC),
        (FileFormat.LOG, StubFileHandlerABC),
        (FileFormat.MDB, StubFileHandlerABC),
        (FileFormat.MUSTACHE, StubFileHandlerABC),
        (FileFormat.NUMBERS, StubFileHandlerABC),
        (FileFormat.PBF, StubFileHandlerABC),
        (FileFormat.VM, StubFileHandlerABC),
        (FileFormat.WKS, StubFileHandlerABC),
        # Read-only
        (FileFormat.HDF5, ReadOnlyFileHandlerABC),
        (FileFormat.SAS7BDAT, ReadOnlyFileHandlerABC),
        (FileFormat.XLS, ReadOnlyFileHandlerABC),
        # Read-only spreadsheets
        (FileFormat.XLS, ReadOnlySpreadsheetFileHandlerABC),
    ]


class TestRegistryMappedResolution(RegistryMappedResolutionContract):
    """Unit tests for explicitly mapped handler class resolution."""

    registry_module = mod
    file_package = file_package
    singleton_class = JsonFile
    mapped_class_cases = [
        (FileFormat.AVRO, AvroFile),
        (FileFormat.ARROW, ArrowFile),
        (FileFormat.DTA, DtaFile),
        (FileFormat.DUCKDB, DuckdbFile),
        (FileFormat.FEATHER, FeatherFile),
        (FileFormat.FWF, FwfFile),
        (FileFormat.JSON, JsonFile),
        (FileFormat.MAT, MatFile),
        (FileFormat.NC, NcFile),
        (FileFormat.ODS, OdsFile),
        (FileFormat.ORC, OrcFile),
        (FileFormat.PARQUET, ParquetFile),
        (FileFormat.RDA, RdaFile),
        (FileFormat.RDS, RdsFile),
        (FileFormat.SAS7BDAT, Sas7bdatFile),
        (FileFormat.SAV, SavFile),
        (FileFormat.SQLITE, SqliteFile),
        (FileFormat.SYLK, SylkFile),
        (FileFormat.TXT, TxtFile),
        (FileFormat.XLSM, XlsmFile),
        (FileFormat.XLSX, XlsxFile),
        (FileFormat.XPT, XptFile),
        (FileFormat.ZSAV, ZsavFile),
    ]
    placeholder_spec_cases = [
        (FileFormat.ACCDB, 'etlplus.file.accdb:AccdbFile'),
        (FileFormat.CFG, 'etlplus.file.cfg:CfgFile'),
        (FileFormat.CONF, 'etlplus.file.conf:ConfFile'),
        (FileFormat.HBS, 'etlplus.file.hbs:HbsFile'),
        (FileFormat.ION, 'etlplus.file.ion:IonFile'),
        (FileFormat.JINJA2, 'etlplus.file.jinja2:Jinja2File'),
        (FileFormat.LOG, 'etlplus.file.log:LogFile'),
        (FileFormat.MDB, 'etlplus.file.mdb:MdbFile'),
        (FileFormat.MUSTACHE, 'etlplus.file.mustache:MustacheFile'),
        (FileFormat.NUMBERS, 'etlplus.file.numbers:NumbersFile'),
        (FileFormat.PBF, 'etlplus.file.pbf:PbfFile'),
        (FileFormat.VM, 'etlplus.file.vm:VmFile'),
        (FileFormat.WKS, 'etlplus.file.wks:WksFile'),
    ]


class TestRegistryFallbackPolicy(RegistryFallbackPolicyContract):
    """Unit tests for registry fallback/deprecation and strict policies."""

    registry_module = mod
