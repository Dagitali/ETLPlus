"""
:mod:`tests.unit.file.test_u_file_pandas_handlers` module.

Unit tests for :mod:`etlplus.file._pandas_handlers`.
"""

from __future__ import annotations

import sys

import pytest

from etlplus.file import _pandas_handlers as mod

# SECTION: HELPERS ========================================================== #


class _Handler:
    """Simple handler stub for dependency resolver tests."""


# SECTION: TESTS ============================================================ #


class TestResolvePyarrowDependency:
    """Unit tests for pyarrow dependency resolution helper."""

    # pylint: disable=protected-access

    def test_resolve_pyarrow_dependency_falls_back_to_resolve_dependency(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test fallback path when no module-level override exists."""
        monkeypatch.delattr(
            sys.modules[__name__],
            'get_pyarrow',
            raising=False,
        )
        calls: list[tuple[str, str, bool]] = []

        def _resolve(
            _handler: object,
            dependency_name: str,
            *,
            format_name: str,
            pip_name: str | None = None,
            required: bool = False,
        ) -> str:
            assert pip_name is None
            calls.append((dependency_name, format_name, required))
            return 'fallback'

        monkeypatch.setattr(mod, 'resolve_dependency', _resolve)

        result = mod._resolve_pyarrow_dependency(
            _Handler(),
            format_name='PARQUET',
        )

        assert result == 'fallback'
        assert calls == [('pyarrow', 'PARQUET', True)]

    def test_resolve_pyarrow_dependency_prefers_module_override(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test concrete-module ``get_pyarrow`` override path."""
        sentinel = object()
        calls: list[str] = []

        def _get_pyarrow(format_name: str) -> object:
            calls.append(format_name)
            return sentinel

        monkeypatch.setattr(
            sys.modules[__name__],
            'get_pyarrow',
            _get_pyarrow,
            raising=False,
        )
        monkeypatch.setattr(
            mod,
            'resolve_dependency',
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError('fallback should not run'),
            ),
        )

        result = mod._resolve_pyarrow_dependency(
            _Handler(),
            format_name='PARQUET',
        )

        assert result is sentinel
        assert calls == ['PARQUET']


class TestResolveSpreadsheetEngineDependency:
    """Unit tests for spreadsheet engine dependency resolution helper."""

    # pylint: disable=protected-access

    def test_resolve_spreadsheet_engine_dependency_noops_for_unknown_engine(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test unknown engine names bypassing dependency resolution."""
        monkeypatch.setattr(
            mod,
            'resolve_dependency',
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError('resolver should not run'),
            ),
        )
        mod._resolve_spreadsheet_engine_dependency(
            _Handler(),
            engine='unknown',
            format_name='XLSX',
        )

    def test_resolve_spreadsheet_engine_dependency_uses_required_dependency(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test known engine resolution delegating with required semantics."""
        calls: list[tuple[str, str, str | None, bool]] = []

        def _resolve(
            _handler: object,
            dependency_name: str,
            *,
            format_name: str,
            pip_name: str | None = None,
            required: bool = False,
        ) -> object:
            calls.append((dependency_name, format_name, pip_name, required))
            return object()

        monkeypatch.setattr(mod, 'resolve_dependency', _resolve)

        mod._resolve_spreadsheet_engine_dependency(
            _Handler(),
            engine='odf',
            format_name='ODS',
        )

        assert calls == [('odf', 'ODS', 'odfpy', True)]


class TestSpreadsheetDependencySpec:
    """Unit tests for spreadsheet dependency metadata helper."""

    # pylint: disable=protected-access

    @pytest.mark.parametrize(
        ('engine', 'expected'),
        [
            (None, None),
            ('openpyxl', ('openpyxl', None)),
            ('xlrd', ('xlrd', None)),
            ('odf', ('odf', 'odfpy')),
            ('unknown', None),
        ],
        ids=['none', 'openpyxl', 'xlrd', 'odf', 'unknown'],
    )
    def test_spreadsheet_dependency_spec(
        self,
        engine: str | None,
        expected: tuple[str, str | None] | None,
    ) -> None:
        """Test spreadsheet engine metadata lookup behavior."""
        assert mod._spreadsheet_dependency_spec(engine) == expected
