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
from types import SimpleNamespace

import pytest

import etlplus.file as file_pkg
from etlplus.file import FileFormat
from etlplus.file import registry as mod
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
from etlplus.file.stub import StubFileHandlerABC

# SECTION: INTERNAL CONSTANTS =============================================== #


_ABC_CASES: list[tuple[FileFormat, type[object]]] = [
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

_MAPPED_CLASS_FORMATS: tuple[FileFormat, ...] = (
    FileFormat.AVRO,
    FileFormat.ARROW,
    FileFormat.DTA,
    FileFormat.DUCKDB,
    FileFormat.FEATHER,
    FileFormat.FWF,
    FileFormat.JSON,
    FileFormat.MAT,
    FileFormat.NC,
    FileFormat.ODS,
    FileFormat.ORC,
    FileFormat.PARQUET,
    FileFormat.RDA,
    FileFormat.RDS,
    FileFormat.SAS7BDAT,
    FileFormat.SAV,
    FileFormat.SQLITE,
    FileFormat.SYLK,
    FileFormat.TXT,
    FileFormat.XLSM,
    FileFormat.XLSX,
    FileFormat.XPT,
    FileFormat.ZSAV,
)

_PLACEHOLDER_SPEC_CASES: list[tuple[FileFormat, str]] = [
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


def _clear_registry_caches() -> None:
    """Clear memoized registry lookups used by tests."""
    # pylint: disable=protected-access
    for cacheable in (
        mod.get_handler,
        mod.get_handler_class,
        mod._module_adapter_class_for_format,
        mod._module_for_format,
    ):
        cacheable.cache_clear()


def _expected_handler_class(file_format: FileFormat) -> type[object]:
    """Return the expected class for one mapped format from registry specs."""
    spec = mod._HANDLER_CLASS_SPECS[file_format]
    return _import_handler_class_from_spec(spec)


def _import_handler_class_from_spec(spec: str) -> type[object]:
    """Import one handler class from a ``module:symbol`` spec string."""
    module_name, _, symbol_name = spec.partition(':')
    module = importlib.import_module(module_name)
    symbol = getattr(module, symbol_name)
    if not isinstance(symbol, type):
        raise TypeError(f'Expected class for spec {spec!r}')
    return symbol


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(autouse=True)
def clear_registry_caches() -> Iterator[None]:
    """Clear registry caches before and after each test."""
    _clear_registry_caches()
    yield
    _clear_registry_caches()


# SECTION: TESTS ============================================================ #


class TestRegistryAbcConformance:
    """Unit tests for category-to-ABC inheritance conformance."""

    @pytest.mark.parametrize(
        ('file_format', 'expected_abc'),
        _ABC_CASES,
    )
    def test_mapped_handler_class_inherits_expected_abc(
        self,
        file_format: FileFormat,
        expected_abc: type[object],
    ) -> None:
        """Test mapped handlers inheriting each expected category ABC."""
        handler_class = mod.get_handler_class(file_format)
        assert issubclass(handler_class, expected_abc)


class TestRegistryMappedResolution:
    """Unit tests for explicitly mapped handler class resolution."""

    # pylint: disable=protected-access

    singleton_format = FileFormat.JSON

    def test_explicit_for_implemented_formats(self) -> None:
        """Test implemented handler class formats being explicitly mapped."""
        implemented_formats: set[FileFormat] = set()
        for module_info in pkgutil.iter_modules(file_pkg.__path__):
            if module_info.ispkg or module_info.name.startswith('_'):
                continue
            module_name = f'{file_pkg.__name__}.{module_info.name}'
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

        mapped_formats = set(mod._HANDLER_CLASS_SPECS.keys())
        missing = implemented_formats - mapped_formats
        assert not missing

        for file_format, spec in mod._HANDLER_CLASS_SPECS.items():
            mapped_class = mod._coerce_handler_class(
                mod._import_symbol(spec),
                file_format=file_format,
            )
            assert mapped_class.format == file_format

    @pytest.mark.parametrize('file_format', _MAPPED_CLASS_FORMATS)
    def test_get_handler_class_uses_mapped_class(
        self,
        file_format: FileFormat,
    ) -> None:
        """Test mapped formats resolving to concrete handler classes."""
        expected_class = _expected_handler_class(file_format)
        handler_class = mod.get_handler_class(file_format)
        assert handler_class is expected_class

    def test_get_handler_returns_singleton_instance(self) -> None:
        """Test get_handler returning a cached singleton for mapped formats."""
        expected_class = _expected_handler_class(self.singleton_format)
        first = mod.get_handler(self.singleton_format)
        second = mod.get_handler(self.singleton_format)

        assert first is second
        assert first.__class__ is expected_class

    @pytest.mark.parametrize(
        ('file_format', 'expected_spec'),
        _PLACEHOLDER_SPEC_CASES,
    )
    def test_unstubbed_placeholder_modules_use_module_owned_classes(
        self,
        file_format: FileFormat,
        expected_spec: str,
    ) -> None:
        """Test placeholder modules mapping to their own class symbols."""
        assert mod._HANDLER_CLASS_SPECS[file_format] == expected_spec


class TestRegistryFallbackPolicy:
    """Unit tests for registry fallback/deprecation and strict policies."""

    fallback_format = FileFormat.GZ

    # pylint: disable=protected-access

    def _remove_fallback_mapping(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Remove the explicit class mapping for fallback tests."""
        monkeypatch.delitem(
            mod._HANDLER_CLASS_SPECS,
            self.fallback_format,
            raising=False,
        )

    def test_deprecated_fallback_builds_module_adapter_and_delegates_calls(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test deprecated fallback building an adapter and delegating I/O."""
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

        self._remove_fallback_mapping(monkeypatch)
        fake_module = SimpleNamespace(read=_read, write=_write)
        self._patch_module_loader(monkeypatch, lambda _fmt: fake_module)

        with pytest.warns(DeprecationWarning, match='deprecated'):
            handler_class = mod.get_handler_class(
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
        self._remove_fallback_mapping(monkeypatch)

        with pytest.raises(ValueError, match='Unsupported format'):
            mod.get_handler_class(self.fallback_format)

    def test_module_adapter_builder_raises_for_missing_module(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test module-adapter builder raising when module import fails."""

        def _raise_module_not_found(_file_format: FileFormat) -> object:
            raise ModuleNotFoundError('missing test module')

        self._remove_fallback_mapping(monkeypatch)
        self._patch_module_loader(monkeypatch, _raise_module_not_found)

        with pytest.raises(ModuleNotFoundError, match='missing test module'):
            mod._module_adapter_class_for_format(self.fallback_format)

    @staticmethod
    def _patch_module_loader(
        monkeypatch: pytest.MonkeyPatch,
        loader: object,
    ) -> None:
        """Patch ``_module_for_format`` with a deterministic loader."""
        monkeypatch.setattr(mod, '_module_for_format', loader)
