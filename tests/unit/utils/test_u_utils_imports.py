"""
:mod:`tests.unit.utils.test_u_utils_imports` module.

Unit tests for :mod:`etlplus.utils._imports`.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

import pytest

from etlplus.utils._imports import DependencyImporter
from etlplus.utils._imports import ImportRequirement
from etlplus.utils._imports import build_dependency_error_message
from etlplus.utils._imports import dependency_label
from etlplus.utils._imports import import_package
from etlplus.utils._imports import module_available
from etlplus.utils._imports import normalize_dependency_names
from etlplus.utils._imports import safe_module_available

# SECTION: HELPERS ========================================================== #


@pytest.fixture(name='sentinel')
def sentinel_fixture() -> object:
    """Return one stable sentinel object for importer/cache tests."""
    return object()


@pytest.fixture(name='missing_importer')
def missing_importer_fixture() -> Callable[[str], object]:
    """Return one importer callable that always raises ``ImportError``."""

    def _missing(_name: str) -> object:
        raise ImportError('missing')

    return _missing


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
        'dependency_names',
        [
            pytest.param(('',), id='empty-name'),
            pytest.param(('yaml', '   '), id='blank-name'),
        ],
    )
    def test_dependency_label_rejects_blank_names(
        self,
        dependency_names: tuple[str, ...],
    ) -> None:
        """Test that dependency labels reject blank dependency names."""
        with pytest.raises(ValueError, match='dependency name must not be empty'):
            dependency_label(dependency_names)

    @pytest.mark.parametrize(
        ('available_modules', 'expected'),
        [
            pytest.param({'netCDF4'}, True, id='one-module-available'),
            pytest.param(set(), False, id='missing'),
        ],
    )
    def test_import_requirement_availability(
        self,
        available_modules: set[str],
        expected: bool,
    ) -> None:
        """Test import requirements accept alternate available modules."""
        requirement = ImportRequirement(
            modules=('netCDF4', 'h5netcdf'),
            package='netCDF4',
        )

        assert (
            requirement.is_available(
                availability_checker=available_modules.__contains__,
            )
            is expected
        )

    def test_import_requirement_stores_optional_dependency_metadata(self) -> None:
        """Test import requirement metadata is immutable and explicit."""
        requirement = ImportRequirement(
            modules=('boto3',),
            package='boto3',
            extra='queue-aws',
        )

        assert requirement.modules == ('boto3',)
        assert requirement.package == 'boto3'
        assert requirement.extra == 'queue-aws'

    @pytest.mark.parametrize(
        ('module_name', 'pip_name', 'expected_names', 'expected_target'),
        [
            pytest.param('yaml', None, ('yaml',), 'yaml', id='string-default'),
            pytest.param('yaml', 'PyYAML', ('PyYAML',), 'PyYAML', id='string-pip'),
            pytest.param(
                ' yaml ',
                ' PyYAML ',
                ('PyYAML',),
                'PyYAML',
                id='strip-string-and-pip',
            ),
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

    @pytest.mark.parametrize(
        ('module_name', 'pip_name', 'match'),
        [
            pytest.param('', None, 'module_name must not be empty', id='empty-string'),
            pytest.param(
                '   ',
                None,
                'module_name must not be empty',
                id='blank-string',
            ),
            pytest.param(
                ('yaml', ''),
                None,
                'module_name must not be empty',
                id='blank-tuple-member',
            ),
            pytest.param(
                'yaml',
                ' ',
                'pip_name must not be empty',
                id='blank-pip-name',
            ),
        ],
    )
    def test_normalize_dependency_names_rejects_blank_names(
        self,
        module_name: str | tuple[str, ...],
        pip_name: str | None,
        match: str,
    ) -> None:
        """Test that dependency-name normalization rejects blank names."""
        with pytest.raises(ValueError, match=match):
            normalize_dependency_names(module_name, pip_name)

    def test_normalize_dependency_names_rejects_empty_tuple(self) -> None:
        """Test that tuple dependency names cannot be empty."""
        with pytest.raises(ValueError, match='must not be an empty tuple'):
            normalize_dependency_names((), None)


class TestImportPackage:
    """Unit tests for the module-level import compatibility wrapper."""

    @pytest.mark.parametrize(
        ('cache', 'module_name'),
        [
            pytest.param({}, 'cached_module', id='caller-cache'),
            pytest.param(None, 'uncached_module', id='uncached'),
        ],
    )
    def test_import_package_cache_policy(
        self,
        cache: dict[str, Any] | None,
        module_name: str,
        sentinel: object,
    ) -> None:
        """Test wrapper imports support cached and uncached calls."""
        result = import_package(
            module_name,
            error_message='unused',
            cache=cache,
            importer=lambda _name: sentinel,
        )

        assert result is sentinel
        if cache is not None:
            assert cache == {module_name: sentinel}


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

    def test_get_uses_standard_dependency_error(
        self,
        missing_importer: Callable[[str], object],
    ) -> None:
        """Test missing imports receive the standard formatted error."""
        importer = DependencyImporter(importer=missing_importer, cache={})
        expected = (
            'YAML support requires optional dependency "PyYAML".\n'
            'Install with: pip install PyYAML'
        )

        with pytest.raises(ImportError, match=re.escape(expected)):
            importer.get('yaml', format_name='YAML', pip_name='PyYAML')

    def test_import_package_can_reraise_nested_import_errors(self) -> None:
        """Test strict missing-name mode preserves nested import errors."""

        def _missing_nested(_name: str) -> object:
            raise ImportError('nested missing', name='nested')

        importer = DependencyImporter(
            importer=_missing_nested,
            strict_missing_name=True,
        )

        with pytest.raises(ImportError, match='nested missing'):
            importer.import_package('outer', error_message='wrapped')

    def test_import_package_imports_and_stores_cache(
        self,
        sentinel: object,
    ) -> None:
        """Test successful imports populate the importer cache."""
        cache: dict[str, Any] = {}
        importer = DependencyImporter(importer=lambda _name: sentinel, cache=cache)

        result = importer.import_package('new_module', error_message='unused')

        assert result is sentinel
        assert cache == {'new_module': sentinel}

    def test_import_package_rejects_blank_module_name(self) -> None:
        """Test direct imports reject blank module names before import."""
        calls: list[str] = []

        def _record_call(name: str) -> object:
            calls.append(name)
            return object()

        importer = DependencyImporter(importer=_record_call)

        with pytest.raises(ValueError, match='module_name must not be empty'):
            importer.import_package(' ', error_message='unused')
        assert not calls

    def test_import_package_strips_module_name_for_import_and_cache(self) -> None:
        """Test direct imports normalize module names before cache/import use."""
        calls: list[str] = []
        sentinel = object()

        def _record_and_return(name: str) -> object:
            calls.append(name)
            return sentinel

        importer = DependencyImporter(
            cache={},
            importer=_record_and_return,
        )

        result = importer.import_package(' new_module ', error_message='unused')

        assert result is sentinel
        assert calls == ['new_module']

    def test_import_package_uses_cache_before_importer(self) -> None:
        """Test cache hits bypass importer calls."""
        sentinel = object()
        importer = DependencyImporter(
            cache={'cached': sentinel},
            importer=lambda _name: (_ for _ in ()).throw(AssertionError),
        )

        result = importer.import_package('cached', error_message='unused')

        assert result is sentinel

    def test_import_package_wraps_missing_dependency(
        self,
        missing_importer: Callable[[str], object],
    ) -> None:
        """Test import failures are translated to the configured error type."""
        importer = DependencyImporter(
            error_type=RuntimeError,
            importer=missing_importer,
        )

        with pytest.raises(RuntimeError, match='install it'):
            importer.import_package('missing', error_message='install it')

    def test_import_package_wraps_matching_strict_import_error(self) -> None:
        """Test strict missing-name mode still wraps the requested module."""

        def _missing_requested(_name: str) -> object:
            raise ImportError('requested missing', name='outer')

        importer = DependencyImporter(
            error_type=RuntimeError,
            importer=_missing_requested,
            strict_missing_name=True,
        )

        with pytest.raises(RuntimeError, match='wrapped'):
            importer.import_package('outer', error_message='wrapped')


class TestModuleAvailable:
    """Unit tests for non-importing module availability checks."""

    @pytest.mark.parametrize(
        ('module_name', 'finder_result', 'expected'),
        [
            pytest.param(' json ', object(), True, id='available-stripped'),
            pytest.param('missing', None, False, id='missing'),
        ],
    )
    def test_module_available_uses_injected_spec_finder(
        self,
        module_name: str,
        finder_result: object | None,
        expected: bool,
    ) -> None:
        """Test availability checks normalize names before metadata lookup."""
        calls: list[str] = []

        def _find_spec(name: str) -> object | None:
            calls.append(name)
            return finder_result

        assert (
            module_available(
                module_name,
                spec_finder=_find_spec,
            )
            is expected
        )
        assert calls == [module_name.strip()]

    @pytest.mark.parametrize(
        'module_name',
        [
            pytest.param(' ', id='blank'),
        ],
    )
    def test_module_available_returns_false_for_invalid_module_names(
        self,
        module_name: str,
    ) -> None:
        """Test invalid module names return false without spec lookup."""
        calls: list[str] = []

        assert module_available(module_name, spec_finder=calls.append) is False
        assert not calls

    @pytest.mark.parametrize(
        'error',
        [
            pytest.param(ImportError('missing'), id='import-error'),
            pytest.param(ModuleNotFoundError('missing'), id='module-not-found'),
            pytest.param(ValueError('bad name'), id='value-error'),
        ],
    )
    def test_module_available_returns_false_for_finder_errors(
        self,
        error: Exception,
    ) -> None:
        """Test finder failures are treated as unavailable modules."""

        def _raise(_module_name: str) -> object:
            raise error

        assert module_available('broken', spec_finder=_raise) is False

    @pytest.mark.parametrize(
        ('available', 'expected'),
        [
            pytest.param(True, True, id='available'),
            pytest.param(False, False, id='unavailable'),
        ],
    )
    def test_safe_module_available_delegates_to_checker(
        self,
        available: bool,
        expected: bool,
    ) -> None:
        """Test safe availability checks preserve successful checker results."""
        assert (
            safe_module_available(
                'json',
                availability_checker=lambda _module_name: available,
            )
            is expected
        )

    @pytest.mark.parametrize(
        'error',
        [
            pytest.param(ImportError('missing'), id='import-error'),
            pytest.param(ModuleNotFoundError('missing'), id='module-not-found'),
            pytest.param(ValueError('bad name'), id='value-error'),
        ],
    )
    def test_safe_module_available_returns_false_for_checker_errors(
        self,
        error: Exception,
    ) -> None:
        """Test safe availability checks treat checker errors as unavailable."""

        def _raise(_module_name: str) -> bool:
            raise error

        assert safe_module_available('broken', availability_checker=_raise) is False
