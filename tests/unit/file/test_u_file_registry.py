"""
:mod:`tests.unit.file.test_u_file_registry` module.

Unit tests for :mod:`etlplus.file.registry`.
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil
from collections.abc import Callable
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
            FileFormat.CSV,
            FileFormat.DAT,
            FileFormat.PSV,
            FileFormat.TAB,
            FileFormat.TSV,
        ),
    ),
    (
        SemiStructuredTextFileHandlerABC,
        (
            FileFormat.INI,
            FileFormat.JSON,
            FileFormat.NDJSON,
            FileFormat.PROPERTIES,
            FileFormat.TOML,
            FileFormat.XML,
            FileFormat.YAML,
        ),
    ),
    (
        ColumnarFileHandlerABC,
        (
            FileFormat.ARROW,
            FileFormat.FEATHER,
            FileFormat.ORC,
            FileFormat.PARQUET,
        ),
    ),
    (
        BinarySerializationFileHandlerABC,
        (
            FileFormat.AVRO,
            FileFormat.BSON,
            FileFormat.CBOR,
            FileFormat.MSGPACK,
            FileFormat.PB,
            FileFormat.PROTO,
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
            FileFormat.DTA,
            FileFormat.HDF5,
            FileFormat.MAT,
            FileFormat.NC,
            FileFormat.RDA,
            FileFormat.RDS,
            FileFormat.SAS7BDAT,
            FileFormat.SAV,
            FileFormat.SYLK,
            FileFormat.XPT,
            FileFormat.ZSAV,
        ),
    ),
    (
        SingleDatasetScientificFileHandlerABC,
        (
            FileFormat.DTA,
            FileFormat.MAT,
            FileFormat.NC,
            FileFormat.SAS7BDAT,
            FileFormat.SAV,
            FileFormat.SYLK,
            FileFormat.XPT,
            FileFormat.ZSAV,
        ),
    ),
    (
        StubFileHandlerABC,
        (
            FileFormat.STUB,
            FileFormat.ACCDB,
            FileFormat.CFG,
            FileFormat.CONF,
            FileFormat.HBS,
            FileFormat.ION,
            FileFormat.JINJA2,
            FileFormat.LOG,
            FileFormat.MDB,
            FileFormat.MUSTACHE,
            FileFormat.NUMBERS,
            FileFormat.PBF,
            FileFormat.VM,
            FileFormat.WKS,
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


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _mapped_handler_class(file_format: FileFormat) -> type[FileHandlerABC]:
    """Resolve one explicitly mapped handler class."""
    # pylint: disable=protected-access

    return mod._coerce_handler_class(
        mod._import_symbol(mod._HANDLER_CLASS_SPECS[file_format]),
        file_format=file_format,
    )


def _implemented_handler_formats() -> set[FileFormat]:
    """Collect all implemented handler formats from non-private modules."""
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
    return implemented_formats


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='clear_registry_caches', autouse=True)
def clear_registry_caches_fixture() -> Iterator[None]:
    """Clear registry caches before and after each test."""
    mod.get_handler.cache_clear()
    mod.get_handler_class.cache_clear()
    yield
    mod.get_handler.cache_clear()
    mod.get_handler_class.cache_clear()


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


class TestRegistryInternalHelpers:
    """Unit tests for internal symbol import and coercion helpers."""

    # pylint: disable=protected-access

    @pytest.mark.parametrize(
        ('symbol', 'error_pattern'),
        (
            (object(), 'must be a class'),
            (type('_NotAHandler', (), {}), 'must inherit FileHandlerABC'),
        ),
        ids=('non_class', 'wrong_base_class'),
    )
    def test_coerce_handler_class_rejects_invalid_symbols(
        self,
        symbol: object,
        error_pattern: str,
    ) -> None:
        """Test class coercion rejecting invalid symbols."""
        with pytest.raises(ValueError, match=error_pattern):
            mod._coerce_handler_class(
                symbol,
                file_format=FileFormat.JSON,
            )

    def test_get_handler_class_wraps_malformed_mapping_specs(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test malformed map entries surfacing as unsupported format errors.
        """
        monkeypatch.setitem(
            mod._HANDLER_CLASS_SPECS,
            FileFormat.JSON,
            'bad-spec',
        )
        with pytest.raises(ValueError, match='Unsupported format'):
            mod.get_handler_class(FileFormat.JSON)

    def test_import_symbol_raises_when_attribute_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test missing module attributes raising ValueError."""
        monkeypatch.setattr(
            mod.importlib,
            'import_module',
            lambda _name: object(),
        )
        with pytest.raises(ValueError, match='Handler symbol'):
            mod._import_symbol('etlplus.file.csv:MissingHandler')

    def test_import_symbol_rejects_invalid_spec(self) -> None:
        """Test malformed import specs raising ValueError."""
        with pytest.raises(ValueError, match='Invalid handler spec'):
            mod._import_symbol('invalid-spec')


class TestRegistryMappedResolution:
    """Unit tests for explicitly mapped handler class resolution."""

    # pylint: disable=protected-access

    singleton_format = FileFormat.JSON

    def test_explicit_for_implemented_formats(self) -> None:
        """Test implemented handler class formats being explicitly mapped."""
        mapped_formats = set(mod._HANDLER_CLASS_SPECS)
        missing = _implemented_handler_formats() - mapped_formats
        assert not missing

        for file_format in mod._HANDLER_CLASS_SPECS:
            mapped_class = _mapped_handler_class(file_format)
            assert mapped_class.format == file_format

    @pytest.mark.parametrize(
        'file_format',
        tuple(mod._HANDLER_CLASS_SPECS),
    )
    def test_get_handler_class_uses_mapped_class(
        self,
        file_format: FileFormat,
    ) -> None:
        """Test mapped formats resolving to concrete handler classes."""
        expected_class = _mapped_handler_class(file_format)
        handler_class = mod.get_handler_class(file_format)
        assert handler_class is expected_class

    def test_get_handler_returns_singleton_instance(self) -> None:
        """Test get_handler returning a cached singleton for mapped formats."""
        expected_class = _mapped_handler_class(self.singleton_format)
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

    @pytest.mark.parametrize(
        'resolver',
        (
            mod.get_handler_class,
            mod.get_handler,
        ),
        ids=('class_lookup', 'instance_lookup'),
    )
    def test_lookups_raise_without_explicit_mapping(
        self,
        monkeypatch: pytest.MonkeyPatch,
        resolver: Callable[[FileFormat], object],
    ) -> None:
        """Test strict mode rejecting unmapped formats across lookups."""
        self._remove_fallback_mapping(monkeypatch)
        with pytest.raises(ValueError, match='Unsupported format'):
            resolver(self.fallback_format)
