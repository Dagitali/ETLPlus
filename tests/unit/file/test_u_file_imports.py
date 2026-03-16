"""
:mod:`tests.unit.file.test_u_file_imports` module.

Unit tests for :mod:`etlplus.file._imports`.
"""

from __future__ import annotations

import re

import pytest

from etlplus.file import _imports as mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestImportsHelpers:
    """Unit tests for dependency import helpers."""

    @pytest.mark.parametrize(
        ('method_name', 'method_args', 'expected_call'),
        [
            ('get_pandas', ('CSV',), ('pandas', 'CSV', None, True)),
            ('get_pyarrow', ('PARQUET',), ('pyarrow', 'PARQUET', None, True)),
            ('get_yaml', (), ('yaml', 'YAML', 'PyYAML', True)),
        ],
        ids=['pandas', 'pyarrow', 'yaml'],
    )
    def test_dependency_helpers_delegate_to_get_dependency(
        self,
        monkeypatch: pytest.MonkeyPatch,
        method_name: str,
        method_args: tuple[object, ...],
        expected_call: tuple[str, str, str | None, bool],
    ) -> None:
        """
        Test that dependency helper wrappers forwarding expected arguments.
        """
        calls: list[tuple[str, str, str | None, bool]] = []
        sentinel = object()

        def _dependency(
            module_name: str,
            *,
            format_name: str,
            pip_name: str | None = None,
            required: bool = False,
        ) -> object:
            calls.append((module_name, format_name, pip_name, required))
            return sentinel

        monkeypatch.setattr(mod, 'get_dependency', _dependency)
        method = getattr(mod, method_name)
        assert method(*method_args) is sentinel
        assert calls == [expected_call]

    def test_dependency_label_formats_three_or_more_dependencies(self) -> None:
        """
        Test that dependency-label helper formatting 3+ dependency
        alternatives.
        """
        label = mod._dependency_label(('netCDF4', 'h5netcdf', 'xarray'))
        assert label == '"netCDF4", "h5netcdf", or "xarray"'

    def test_dependency_label_raises_for_empty_names(self) -> None:
        """
        Test that dependency-label helper rejecting empty dependency sets.
        """
        with pytest.raises(ValueError, match='must not be empty'):
            mod._dependency_label(())

    @pytest.mark.parametrize(
        ('module_name', 'format_name', 'pip_name', 'dependency_name'),
        [
            ('pyarrow', 'PARQUET', None, 'pyarrow'),
            ('yaml', 'YAML', 'PyYAML', 'PyYAML'),
        ],
        ids=['default_pip_name', 'explicit_pip_name'],
    )
    def test_error_message_uses_expected_dependency_name(
        self,
        module_name: str,
        format_name: str,
        pip_name: str | None,
        dependency_name: str,
    ) -> None:
        """Test import-error messages with dependency and ``pip`` hints."""
        message = mod._error_message(
            module_name,
            format_name=format_name,
            pip_name=pip_name,
        )
        assert (
            f'{format_name} support requires optional dependency '
            f'"{dependency_name}"' in message
        )
        assert f'pip install {dependency_name}' in message

    def test_get_dependency_raises_optional_standard_message(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that optional dependency failures using normalized format
        messages.
        """
        monkeypatch.setattr(mod, '_MODULE_CACHE', {})
        monkeypatch.setattr(
            mod,
            'import_module',
            lambda _name: (_ for _ in ()).throw(ImportError('missing')),
        )
        expected = (
            'NC support requires optional dependency "xarray".\n'
            'Install with: pip install xarray'
        )
        with pytest.raises(ImportError, match=re.escape(expected)):
            mod.get_dependency(
                'xarray',
                format_name='NC',
            )

    def test_get_dependency_raises_required_standard_message(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that required dependency failures using normalized format
        messages.
        """
        monkeypatch.setattr(mod, '_MODULE_CACHE', {})
        monkeypatch.setattr(
            mod,
            'import_module',
            lambda _name: (_ for _ in ()).throw(ImportError('missing')),
        )
        expected = (
            'BSON support requires dependency "pymongo".\n'
            'Install with: pip install pymongo'
        )
        with pytest.raises(ImportError, match=re.escape(expected)):
            mod.get_dependency(
                'bson',
                format_name='BSON',
                pip_name='pymongo',
                required=True,
            )

    def test_get_dependency_imports_and_caches(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that first import path store module in cache."""
        cache: dict[str, object] = {}
        sentinel = object()

        monkeypatch.setattr(mod, '_MODULE_CACHE', cache)
        monkeypatch.setattr(mod, 'import_module', lambda _name: sentinel)
        result = mod.get_dependency(
            'example_dep',
            format_name='EXAMPLE',
        )
        assert result is sentinel
        assert cache['example_dep'] is sentinel

    def test_get_dependency_raises_formatted_error_message(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that missing dependency errors use formatted messages."""
        monkeypatch.setattr(
            mod,
            'import_module',
            lambda _name: (_ for _ in ()).throw(ImportError('missing')),
        )
        expected = (
            'CUSTOM support requires optional dependency "missing_dep".\n'
            'Install with: pip install missing_dep'
        )
        with pytest.raises(ImportError, match=re.escape(expected)):
            mod.get_dependency(
                'missing_dep',
                format_name='CUSTOM',
            )

    def test_get_dependency_uses_cache_when_available(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that cache-first behavior avoids import lookups."""
        sentinel = object()
        monkeypatch.setitem(mod._MODULE_CACHE, 'cached_mod', sentinel)
        monkeypatch.setattr(
            mod,
            'import_module',
            lambda _name: (_ for _ in ()).throw(AssertionError('unexpected')),
        )
        assert (
            mod.get_dependency(
                'cached_mod',
                format_name='IGNORED',
            )
            is sentinel
        )

    def test_normalize_dependency_names_rejects_empty_tuple(self) -> None:
        """Test that dependency-name normalization rejects empty tuples."""
        with pytest.raises(ValueError, match='must not be an empty tuple'):
            mod._normalize_dependency_names((), None)

    @pytest.mark.parametrize(
        ('module_name', 'format_name', 'pip_name', 'dependency_name'),
        [
            ('duckdb', 'DUCKDB', None, 'duckdb'),
            ('bson', 'BSON', 'pymongo', 'pymongo'),
        ],
        ids=['default_pip_name', 'explicit_pip_name'],
    )
    def test_required_error_message_uses_expected_dependency_name(
        self,
        module_name: str,
        format_name: str,
        pip_name: str | None,
        dependency_name: str,
    ) -> None:
        """
        Test that required import error messages renders dependency hints.
        """
        message = mod._error_message(
            module_name,
            format_name=format_name,
            pip_name=pip_name,
            required=True,
        )
        assert (
            f'{format_name} support requires dependency "{dependency_name}"' in message
        )
        assert f'pip install {dependency_name}' in message

    def test_resolve_module_callable_returns_none_when_module_is_missing(
        self,
    ) -> None:
        """
        Test that callable resolution returns ``None`` for missing modules.
        """
        handler_type = type(
            '_DetachedHandler',
            (),
            {'__module__': 'not.loaded'},
        )
        handler = handler_type()
        assert mod.resolve_module_callable(handler, 'anything') is None

    def test_resolve_with_module_override_falls_back(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that module override helper uses fallback when override is
        missing.
        """
        monkeypatch.setattr(mod, 'resolve_module_callable', lambda *_: None)
        calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

        def _fallback(*args: object, **kwargs: object) -> str:
            calls.append((args, kwargs))
            return 'fallback'

        result = mod._resolve_with_module_override(
            object(),
            'missing_override',
            _fallback,
            1,
            token='x',
        )

        assert result == 'fallback'
        assert calls == [((1,), {'token': 'x'})]

    def test_raise_engine_import_error_uses_shared_message(self) -> None:
        """
        Test that shared engine error helper raises standardized NC message.
        """
        expected = (
            'NC support requires optional dependency '
            '"netCDF4" or "h5netcdf".\n'
            'Install with: pip install netCDF4'
        )
        with pytest.raises(ImportError, match=re.escape(expected)):
            mod.raise_engine_import_error(
                ImportError('engine missing'),
                format_name='NC',
                dependency_names=('netCDF4', 'h5netcdf'),
                pip_name='netCDF4',
            )

    def test_raise_engine_import_error_reraises_without_metadata(self) -> None:
        """
        Test that engine helper re-raises original error when metadata is
        missing.
        """
        error = ImportError('engine missing')
        with pytest.raises(ImportError, match='engine missing'):
            mod.raise_engine_import_error(
                error,
                format_name='NC',
            )
