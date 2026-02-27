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

    def test_resolve_pyarrow_dependency_falls_back_to_get_dependency(
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

        def _dependency(
            dependency_name: str,
            *,
            format_name: str,
            pip_name: str | None = None,
            required: bool = False,
        ) -> str:
            assert pip_name is None
            calls.append((dependency_name, format_name, required))
            return 'fallback'

        monkeypatch.setattr(mod, 'get_dependency', _dependency)

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
            'get_dependency',
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
