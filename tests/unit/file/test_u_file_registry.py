"""
:mod:`tests.unit.file.test_u_file_registry` module.

Unit tests for :mod:`etlplus.file.registry`.
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil
from collections.abc import Iterator

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
from etlplus.file.stub import StubFileHandlerABC

# SECTION: INTERNAL CONSTANTS =============================================== #


_ABC_GROUPS: tuple[tuple[type[object], tuple[FileFormat, ...]], ...] = (
    (
        DelimitedTextFileHandlerABC,
        (
            FileFormat.CSV, FileFormat.DAT, FileFormat.PSV,
            FileFormat.TAB, FileFormat.TSV,
        ),
    ),
    (
        SemiStructuredTextFileHandlerABC,
        (
            FileFormat.INI, FileFormat.JSON, FileFormat.NDJSON,
            FileFormat.PROPERTIES, FileFormat.TOML, FileFormat.XML,
            FileFormat.YAML,
        ),
    ),
    (
        ColumnarFileHandlerABC,
        (
            FileFormat.ARROW, FileFormat.FEATHER, FileFormat.ORC,
            FileFormat.PARQUET,
        ),
    ),
    (
        BinarySerializationFileHandlerABC,
        (
            FileFormat.AVRO, FileFormat.BSON, FileFormat.CBOR,
            FileFormat.MSGPACK, FileFormat.PB, FileFormat.PROTO,
        ),
    ),
    (EmbeddedDatabaseFileHandlerABC, (FileFormat.DUCKDB, FileFormat.SQLITE)),
    (
        SpreadsheetFileHandlerABC,
        (FileFormat.ODS, FileFormat.XLS, FileFormat.XLSM, FileFormat.XLSX),
    ),
    (TextFixedWidthFileHandlerABC, (FileFormat.FWF, FileFormat.TXT)),
    (ArchiveWrapperFileHandlerABC, (FileFormat.GZ, FileFormat.ZIP)),
    (
        ScientificDatasetFileHandlerABC,
        (
            FileFormat.DTA, FileFormat.HDF5, FileFormat.MAT, FileFormat.NC,
            FileFormat.RDA, FileFormat.RDS, FileFormat.SAS7BDAT,
            FileFormat.SAV, FileFormat.SYLK, FileFormat.XPT, FileFormat.ZSAV,
        ),
    ),
    (
        SingleDatasetScientificFileHandlerABC,
        (
            FileFormat.DTA, FileFormat.MAT, FileFormat.NC,
            FileFormat.SAS7BDAT, FileFormat.SAV, FileFormat.SYLK,
            FileFormat.XPT, FileFormat.ZSAV,
        ),
    ),
    (
        StubFileHandlerABC,
        (
            FileFormat.STUB, FileFormat.ACCDB, FileFormat.CFG,
            FileFormat.CONF, FileFormat.HBS, FileFormat.ION,
            FileFormat.JINJA2, FileFormat.LOG, FileFormat.MDB,
            FileFormat.MUSTACHE, FileFormat.NUMBERS, FileFormat.PBF,
            FileFormat.VM, FileFormat.WKS,
        ),
    ),
    (
        ReadOnlyFileHandlerABC,
        (FileFormat.HDF5, FileFormat.SAS7BDAT, FileFormat.XLS),
    ),
    (ReadOnlySpreadsheetFileHandlerABC, (FileFormat.XLS,)),
)

_ABC_CASES: tuple[tuple[FileFormat, type[object]], ...] = tuple(
    (file_format, expected_abc)
    for expected_abc, file_formats in _ABC_GROUPS
    for file_format in file_formats
)

_CACHEABLES = (
    mod.get_handler,
    mod.get_handler_class,
)

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

_PLACEHOLDER_SPEC_CASES: tuple[tuple[FileFormat, str], ...] = (
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
)


# SECTION: FIXTURES ========================================================= #


def _clear_registry_caches() -> None:
    """Clear all cached registry resolution helpers."""
    for cacheable in _CACHEABLES:
        cacheable.cache_clear()


@pytest.fixture(name='clear_registry_caches', autouse=True)
def clear_registry_caches_fixture() -> Iterator[None]:
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
        expected_class = mod._coerce_handler_class(
            mod._import_symbol(mod._HANDLER_CLASS_SPECS[file_format]),
            file_format=file_format,
        )
        handler_class = mod.get_handler_class(file_format)
        assert handler_class is expected_class

    def test_get_handler_returns_singleton_instance(self) -> None:
        """Test get_handler returning a cached singleton for mapped formats."""
        expected_class = mod._coerce_handler_class(
            mod._import_symbol(
                mod._HANDLER_CLASS_SPECS[self.singleton_format],
            ),
            file_format=self.singleton_format,
        )
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


class TestRegistryStrictPolicy:
    """Unit tests for strict explicit-map registry behavior."""

    fallback_format = FileFormat.GZ

    # pylint: disable=protected-access

    def _remove_fallback_mapping(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Remove one explicit class mapping for strict-policy tests."""
        monkeypatch.delitem(
            mod._HANDLER_CLASS_SPECS,
            self.fallback_format,
            raising=False,
        )

    def test_get_handler_class_raises_without_explicit_mapping(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test strict mode rejecting unmapped formats for class lookup."""
        self._remove_fallback_mapping(monkeypatch)

        with pytest.raises(ValueError, match='Unsupported format'):
            mod.get_handler_class(self.fallback_format)

    def test_get_handler_raises_without_explicit_mapping(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test strict mode rejecting unmapped formats for instance lookup."""
        self._remove_fallback_mapping(monkeypatch)
        with pytest.raises(ValueError, match='Unsupported format'):
            mod.get_handler(self.fallback_format)
