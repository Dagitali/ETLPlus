"""
:mod:`tests.unit.utils.test_u_utils_imports` module.

Unit tests for :mod:`etlplus.utils._imports`.
"""

from __future__ import annotations

import re
from typing import Any

import pytest

from etlplus.utils._imports import DependencyImporter
from etlplus.utils._imports import build_dependency_error_message
from etlplus.utils._imports import dependency_label
from etlplus.utils._imports import import_package
from etlplus.utils._imports import normalize_dependency_names

# SECTION: TESTS ============================================================ #


class TestDependencyMessages:
    """Unit tests for dependency message formatting helpers."""

    @pytest.mark.parametrize(
        ('required', 'label'),
        [
            pytest.param(False, 'optional dependency', id='optional'),
            pytest.param(True, 'dependency', id='required'),
        ],
    )
    def test_build_dependency_error_message(
        self,
        required: bool,
        label: str,
    ) -> None:
        """Test dependency error wording and install hint."""
        message = build_dependency_error_message(
            ('yaml', 'toml'),
            format_name='CONFIG',
            pip_name='PyYAML',
            required=required,
        )

        assert f'CONFIG support requires {label} "yaml" or "toml"' in message
        assert 'Install with: pip install PyYAML' in message

    @pytest.mark.parametrize(
        ('dependency_names', 'expected'),
        [
            pytest.param(('jsonschema',), '"jsonschema"', id='one'),
            pytest.param(('yaml', 'toml'), '"yaml" or "toml"', id='two'),
            pytest.param(
                ('netCDF4', 'h5netcdf', 'xarray'),
                '"netCDF4", "h5netcdf", or "xarray"',
                id='three',
            ),
        ],
    )
    def test_dependency_label_formats_names(
        self,
        dependency_names: tuple[str, ...],
        expected: str,
    ) -> None:
        """Test readable dependency-list formatting."""
        assert dependency_label(dependency_names) == expected

    def test_dependency_label_rejects_empty_names(self) -> None:
        """Test that dependency labels require at least one name."""
        with pytest.raises(ValueError, match='must not be empty'):
            dependency_label(())

    @pytest.mark.parametrize(
        ('module_name', 'pip_name', 'expected_names', 'expected_target'),
        [
            pytest.param('yaml', None, ('yaml',), 'yaml', id='string-default'),
            pytest.param('yaml', 'PyYAML', ('PyYAML',), 'PyYAML', id='string-pip'),
            pytest.param(
                ('netCDF4', 'h5netcdf'),
                None,
                ('netCDF4', 'h5netcdf'),
                'netCDF4',
                id='tuple-default',
            ),
            pytest.param(
                ('netCDF4', 'h5netcdf'),
                'xarray',
                ('netCDF4', 'h5netcdf'),
                'xarray',
                id='tuple-pip',
            ),
        ],
    )
    def test_normalize_dependency_names(
        self,
        module_name: str | tuple[str, ...],
        pip_name: str | None,
        expected_names: tuple[str, ...],
        expected_target: str,
    ) -> None:
        """Test dependency name and install-target normalization."""
        assert normalize_dependency_names(module_name, pip_name) == (
            expected_names,
            expected_target,
        )

    def test_normalize_dependency_names_rejects_empty_tuple(self) -> None:
        """Test that tuple dependency names cannot be empty."""
        with pytest.raises(ValueError, match='must not be an empty tuple'):
            normalize_dependency_names((), None)


class TestImportPackage:
    """Unit tests for lazy dependency importing."""

    def test_import_package_can_reraise_nested_import_errors(self) -> None:
        """Test strict missing-name mode preserves nested import errors."""

        def _missing_nested(_name: str) -> object:
            raise ImportError('nested missing', name='nested')

        with pytest.raises(ImportError, match='nested missing'):
            import_package(
                'outer',
                error_message='wrapped',
                importer=_missing_nested,
                strict_missing_name=True,
            )

    def test_import_package_imports_and_stores_cache(self) -> None:
        """Test successful imports populate the optional cache."""
        cache: dict[str, Any] = {}
        sentinel = object()

        result = import_package(
            'new_module',
            error_message='unused',
            cache=cache,
            importer=lambda _name: sentinel,
        )

        assert result is sentinel
        assert cache == {'new_module': sentinel}

    def test_import_package_supports_uncached_imports(self) -> None:
        """Test successful imports do not require a cache object."""
        sentinel = object()

        assert (
            import_package(
                'uncached_module',
                error_message='unused',
                cache=None,
                importer=lambda _name: sentinel,
            )
            is sentinel
        )

    def test_import_package_uses_cache_before_importer(self) -> None:
        """Test cache hits bypass importer calls."""
        sentinel = object()

        result = import_package(
            'cached',
            error_message='unused',
            cache={'cached': sentinel},
            importer=lambda _name: (_ for _ in ()).throw(AssertionError),
        )

        assert result is sentinel

    def test_import_package_wraps_missing_dependency(self) -> None:
        """Test import failures are translated to the configured error type."""

        def _missing(_name: str) -> object:
            raise ImportError('missing')

        with pytest.raises(RuntimeError, match='install it'):
            import_package(
                'missing',
                error_message='install it',
                importer=_missing,
                error_type=RuntimeError,
            )

    def test_import_package_wraps_matching_strict_import_error(self) -> None:
        """Test strict missing-name mode still wraps the requested module."""

        def _missing_requested(_name: str) -> object:
            raise ImportError('requested missing', name='outer')

        with pytest.raises(RuntimeError, match='wrapped'):
            import_package(
                'outer',
                error_message='wrapped',
                importer=_missing_requested,
                error_type=RuntimeError,
                strict_missing_name=True,
            )


class TestDependencyImporter:
    """Unit tests for the importer dataclass facade."""

    def test_get_builds_standard_message_and_caches_import(self) -> None:
        """Test :class:`DependencyImporter` delegates through shared helpers."""
        cache: dict[str, Any] = {}
        sentinel = object()
        importer = DependencyImporter(
            importer=lambda _name: sentinel,
            cache=cache,
        )

        assert (
            importer.get(
                'yaml',
                format_name='YAML',
                pip_name='PyYAML',
                required=True,
            )
            is sentinel
        )
        assert cache == {'yaml': sentinel}

    def test_get_uses_standard_dependency_error(self) -> None:
        """Test missing imports receive the standard formatted error."""

        def _missing(_name: str) -> object:
            raise ImportError('missing')

        importer = DependencyImporter(importer=_missing, cache={})
        expected = (
            'YAML support requires optional dependency "PyYAML".\n'
            'Install with: pip install PyYAML'
        )

        with pytest.raises(ImportError, match=re.escape(expected)):
            importer.get('yaml', format_name='YAML', pip_name='PyYAML')
