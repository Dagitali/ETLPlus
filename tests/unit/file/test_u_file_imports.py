"""
:mod:`tests.unit.file.test_u_file_imports` module.

Unit tests for :mod:`etlplus.file._imports`.
"""

from __future__ import annotations

from typing import Any

import pytest

from etlplus.file import _imports as mod

# SECTION: TESTS ============================================================ #


class TestImportsHelpers:
    """Unit tests for optional import helpers."""

    # pylint: disable=protected-access, unused-argument

    def test_get_dependency_routes_through_standard_message(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test dependency resolution with normalized format messages."""
        calls: list[tuple[str, str]] = []
        sentinel = object()

        def _optional(module_name: str, *, error_message: str) -> object:
            calls.append((module_name, error_message))
            return sentinel

        monkeypatch.setattr(mod, 'get_optional_module', _optional)
        result = mod.get_dependency('duckdb', format_name='DUCKDB')
        assert result is sentinel
        assert calls == [
            (
                'duckdb',
                'DUCKDB support requires optional dependency "duckdb".\n'
                'Install with: pip install duckdb',
            ),
        ]

    def test_error_message_defaults_to_module_name(self) -> None:
        """Test default pip package suggestion in import errors."""
        message = mod._error_message('pyarrow', format_name='PARQUET')
        assert (
            'PARQUET support requires optional dependency "pyarrow"' in message
        )
        assert 'pip install pyarrow' in message

    def test_error_message_uses_explicit_pip_name(self) -> None:
        """Test explicit pip package suggestion in import errors."""
        message = mod._error_message(
            'yaml',
            format_name='YAML',
            pip_name='PyYAML',
        )
        assert '"PyYAML"' in message
        assert 'pip install PyYAML' in message

    def test_get_optional_module_imports_and_caches(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test first import path storing module in cache."""
        loaded: dict[str, Any] = {}

        def _import(name: str) -> object:
            result = object()
            loaded[name] = result
            return result

        monkeypatch.setattr(mod, 'import_module', _import)
        result = mod.get_optional_module(
            'example_dep',
            error_message='unused',
        )
        assert result is loaded['example_dep']
        assert mod._MODULE_CACHE['example_dep'] is result
        mod._MODULE_CACHE.pop('example_dep', None)

    def test_get_optional_module_raises_custom_error_message(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test missing dependency error propagation."""
        monkeypatch.setattr(
            mod,
            'import_module',
            lambda _name: (_ for _ in ()).throw(ImportError('missing')),
        )
        with pytest.raises(ImportError, match='custom import failure'):
            mod.get_optional_module(
                'missing_dep',
                error_message='custom import failure',
            )

    def test_get_optional_module_uses_cache_when_available(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test cache-first behavior avoiding import lookups."""
        sentinel = object()
        monkeypatch.setitem(mod._MODULE_CACHE, 'cached_mod', sentinel)
        monkeypatch.setattr(
            mod,
            'import_module',
            lambda _name: (_ for _ in ()).throw(AssertionError('unexpected')),
        )
        assert (
            mod.get_optional_module(
                'cached_mod',
                error_message='ignored',
            )
            is sentinel
        )
        mod._MODULE_CACHE.pop('cached_mod', None)

    def test_get_pandas_uses_dependency_helper(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test pandas helper delegation."""
        calls: list[tuple[str, str, str | None]] = []
        sentinel = object()

        def _dependency(
            module_name: str,
            *,
            format_name: str,
            pip_name: str | None = None,
        ) -> object:
            calls.append((module_name, format_name, pip_name))
            return sentinel

        monkeypatch.setattr(mod, 'get_dependency', _dependency)
        assert mod.get_pandas('CSV') is sentinel
        assert calls == [('pandas', 'CSV', None)]

    def test_get_yaml_uses_dependency_helper(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test PyYAML helper delegation."""
        calls: list[tuple[str, str, str | None]] = []
        sentinel = object()

        def _dependency(
            module_name: str,
            *,
            format_name: str,
            pip_name: str | None = None,
        ) -> object:
            calls.append((module_name, format_name, pip_name))
            return sentinel

        monkeypatch.setattr(mod, 'get_dependency', _dependency)
        assert mod.get_yaml() is sentinel
        assert calls == [('yaml', 'YAML', 'PyYAML')]
