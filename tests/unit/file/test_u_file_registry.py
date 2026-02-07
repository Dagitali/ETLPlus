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

import etlplus.file as file_package
from etlplus.file import FileFormat
from etlplus.file import registry as mod
from etlplus.file.arrow import ArrowFile
from etlplus.file.base import FileHandlerABC
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
from etlplus.file.sylk import SylkFile
from etlplus.file.txt import TxtFile
from etlplus.file.xlsm import XlsmFile
from etlplus.file.xlsx import XlsxFile
from etlplus.file.xpt import XptFile
from etlplus.file.zsav import ZsavFile

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


class TestRegistryMappedResolution:
    """Unit tests for explicitly mapped handler class resolution."""

    # pylint: disable=protected-access

    def test_explicit_for_implemented_formats(self) -> None:
        """
        Test every implemented handler class format being explicitly mapped,
        and every mapped class matching its registry format key.
        """
        implemented_formats: set[FileFormat] = set()
        for module_info in pkgutil.iter_modules(file_package.__path__):
            if module_info.ispkg or module_info.name.startswith('_'):
                continue
            module_name = f'{file_package.__name__}.{module_info.name}'
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

    @pytest.mark.parametrize(
        ('file_format', 'expected_class'),
        [
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
        ],
    )
    def test_get_handler_class_uses_mapped_class(
        self,
        file_format: FileFormat,
        expected_class: type[FileHandlerABC],
    ) -> None:
        """Test mapped formats resolving to their concrete handler classes."""
        handler_class = mod.get_handler_class(file_format)

        assert handler_class is expected_class

    def test_get_handler_returns_singleton_instance(self) -> None:
        """Test get_handler returning a cached singleton for mapped formats."""
        first = mod.get_handler(FileFormat.JSON)
        second = mod.get_handler(FileFormat.JSON)

        assert first is second
        assert isinstance(first, JsonFile)


class TestRegistryModuleAdapterFallback:
    """Unit tests for module-adapter fallback resolution."""

    # pylint: disable=protected-access

    def test_fallback_builds_module_adapter_and_delegates_calls(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test fallback building a module-adapter class and delegating
        read/write.
        """
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
            mod._HANDLER_CLASS_SPECS,
            FileFormat.GZ,
            raising=False,
        )
        fake_module = SimpleNamespace(read=_read, write=_write)
        monkeypatch.setattr(
            mod,
            '_module_for_format',
            lambda _fmt: fake_module,
        )

        handler_class = mod.get_handler_class(FileFormat.GZ)

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


class TestRegistryUnsupportedFormat:
    """Unit tests for unsupported-format errors."""

    # pylint: disable=protected-access

    def test_get_handler_class_raises_for_missing_module(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test unsupported-format error wrapping when module import fails."""

        def _raise_module_not_found(_file_format: FileFormat) -> object:
            raise ModuleNotFoundError('missing test module')

        monkeypatch.delitem(
            mod._HANDLER_CLASS_SPECS,
            FileFormat.GZ,
            raising=False,
        )
        monkeypatch.setattr(mod, '_module_for_format', _raise_module_not_found)

        with pytest.raises(ValueError, match='Unsupported format'):
            mod.get_handler_class(FileFormat.GZ)
