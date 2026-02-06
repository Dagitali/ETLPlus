"""
:mod:`tests.unit.file.test_u_file_registry` module.

Unit tests for :mod:`etlplus.file.registry`.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest

from etlplus.file import FileFormat
from etlplus.file import registry as mod
from etlplus.file.base import FileHandlerABC
from etlplus.file.base import WriteOptions
from etlplus.file.json import JsonFile

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

    def test_get_handler_class_uses_mapped_class(self) -> None:
        """Test mapped formats resolving to their concrete handler classes."""
        handler_class = mod.get_handler_class(FileFormat.JSON)

        assert handler_class is JsonFile

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
